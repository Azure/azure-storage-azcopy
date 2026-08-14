// Copyright © Microsoft <wastore@microsoft.com>
//
// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files (the "Software"), to deal
// in the Software without restriction, including without limitation the rights
// to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
// copies of the Software, and to permit persons to whom the Software is
// furnished to do so, subject to the following conditions:
//
// The above copyright notice and this permission notice shall be included in
// all copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
// THE SOFTWARE.

package common

import (
	"time"
)

// throttleResource identifies which dimension of the dual bucket a throttle
// signal applies to. The two resources are metered independently.
type throttleResource int

const (
	resourceIops throttleResource = iota
	resourceBandwidth
	resourceCount
)

// respEvent is a single timestamped throttle/success observation.
type respEvent struct {
	at        time.Time
	throttled bool
}

// sustainedDetector tracks throttle vs. success responses in a sliding window
// and decides when throttling is "sustained" (proactive -> reactive switch) or
// "sustained quiet" (reactive -> proactive restore). It exists so momentary
// 429s do not flip modes.
type sustainedDetector struct {
	window    time.Duration
	minEvents int
	minRatio  float64
	quiet     time.Duration

	events         []respEvent
	lastThrottleAt time.Time
}

func newSustainedDetector(window time.Duration, minEvents int, minRatio float64, quiet time.Duration) *sustainedDetector {
	return &sustainedDetector{window: window, minEvents: minEvents, minRatio: minRatio, quiet: quiet}
}

func (d *sustainedDetector) prune(now time.Time) {
	cutoff := now.Add(-d.window)
	i := 0
	for i < len(d.events) && !d.events[i].at.After(cutoff) {
		i++
	}
	if i > 0 {
		d.events = append(d.events[:0], d.events[i:]...)
	}
}

func (d *sustainedDetector) addThrottle(now time.Time) {
	d.lastThrottleAt = now
	d.events = append(d.events, respEvent{at: now, throttled: true})
	d.prune(now)
}

func (d *sustainedDetector) addSuccess(now time.Time) {
	d.events = append(d.events, respEvent{at: now, throttled: false})
	d.prune(now)
}

// isSustainedThrottling reports whether both the count and ratio thresholds are
// met inside the current window.
func (d *sustainedDetector) isSustainedThrottling(now time.Time) bool {
	d.prune(now)
	total := len(d.events)
	if total == 0 {
		return false
	}
	throttles := 0
	for _, e := range d.events {
		if e.throttled {
			throttles++
		}
	}
	if throttles < d.minEvents {
		return false
	}
	return float64(throttles)/float64(total) >= d.minRatio
}

// isSustainedQuiet reports whether no throttle has occurred for the quiet window.
func (d *sustainedDetector) isSustainedQuiet(now time.Time) bool {
	if d.lastThrottleAt.IsZero() {
		return true
	}
	return now.Sub(d.lastThrottleAt) >= d.quiet
}

// responseThrottleDetector is the GENERIC, service-neutral real-time signal:
// HTTP 429/503 responses gated through a per-resource sliding-window
// sustainedDetector so momentary bursts do not flip modes. Both Azure Files and
// Blob feed it classified responses; it has no knowledge of any poll API.
type responseThrottleDetector struct {
	resp [resourceCount]*sustainedDetector
}

func newResponseThrottleDetector(window time.Duration, minEvents int, minRatio float64, quiet time.Duration) *responseThrottleDetector {
	d := &responseThrottleDetector{}
	for i := range d.resp {
		d.resp[i] = newSustainedDetector(window, minEvents, minRatio, quiet)
	}
	return d
}

// observe feeds a single real-time HTTP outcome into a resource's window.
func (d *responseThrottleDetector) observe(r throttleResource, throttled bool, now time.Time) {
	if throttled {
		d.resp[r].addThrottle(now)
	} else {
		d.resp[r].addSuccess(now)
	}
}

func (d *responseThrottleDetector) sustainedThrottling(r throttleResource, now time.Time) bool {
	return d.resp[r].isSustainedThrottling(now)
}

func (d *responseThrottleDetector) quiet(r throttleResource, now time.Time) bool {
	return d.resp[r].isSustainedQuiet(now)
}

// statsThrottleSignal is the SERVICE-SPECIFIC, OPTIONAL poll-based signal: the
// wall-clock time each resource last saw a non-zero throttle-counter delta from
// an authoritative poll API (e.g. Azure Files GetShareStats). Services that have
// no such API (e.g. Blob) simply omit it, so only the generic response signal
// governs mode there.
type statsThrottleSignal struct {
	at    [resourceCount]time.Time // last poll-delta time per resource
	quiet time.Duration            // how long without a delta before "quiet"
}

func newStatsThrottleSignal(quiet time.Duration) *statsThrottleSignal {
	return &statsThrottleSignal{quiet: quiet}
}

func (s *statsThrottleSignal) observe(r throttleResource, now time.Time) {
	s.at[r] = now
}

func (s *statsThrottleSignal) isQuiet(r throttleResource, now time.Time) bool {
	if s.at[r].IsZero() {
		return true
	}
	return now.Sub(s.at[r]) >= s.quiet
}

// dualThrottleDetector fuses the generic real-time response signal with the
// OPTIONAL poll-based stats signal, per resource:
//
//   - Fast, real-time signal (always present): responseThrottleDetector.
//   - Slow, authoritative signal (optional): statsThrottleSignal. When absent
//     (stats == nil, e.g. Blob), only the response signal governs.
//
// A resource is "throttled now" when the response signal fires; it is "quiet"
// only when the response signal is quiet AND (the stats signal is absent OR also
// quiet).
type dualThrottleDetector struct {
	resp  *responseThrottleDetector
	stats *statsThrottleSignal // nil => no poll-based signal (e.g. Blob)
}

// newDualThrottleDetector builds the detector. When withStatsSignal is false the
// poll-based stats signal is omitted entirely, so GetShareStats-style deltas play
// no part (the Blob case).
func newDualThrottleDetector(window time.Duration, minEvents int, minRatio float64, quiet time.Duration, withStatsSignal bool) *dualThrottleDetector {
	d := &dualThrottleDetector{resp: newResponseThrottleDetector(window, minEvents, minRatio, quiet)}
	if withStatsSignal {
		d.stats = newStatsThrottleSignal(quiet)
	}
	return d
}

// observeResponse feeds a single real-time HTTP outcome into the generic signal.
func (d *dualThrottleDetector) observeResponse(r throttleResource, throttled bool, now time.Time) {
	d.resp.observe(r, throttled, now)
}

// observeStatsThrottle records a non-zero poll-delta for the resource. It is a
// no-op when the poll-based signal is not configured (e.g. Blob).
func (d *dualThrottleDetector) observeStatsThrottle(r throttleResource, now time.Time) {
	if d.stats != nil {
		d.stats.observe(r, now)
	}
}

// sustainedThrottling reports whether the real-time response stream alone
// indicates sustained throttling for the resource. This is the fast trigger used
// to switch to reactive AIMD before the next poll.
func (d *dualThrottleDetector) sustainedThrottling(r throttleResource, now time.Time) bool {
	return d.resp.sustainedThrottling(r, now)
}

// quiet reports whether the resource is quiet across every configured signal:
// the response stream has seen no throttle for its quiet window AND (there is no
// poll-based signal OR it too has seen no delta within its quiet window). Only
// when every resource is quiet may the limiter return to proactive equal-share.
func (d *dualThrottleDetector) quiet(r throttleResource, now time.Time) bool {
	if !d.resp.quiet(r, now) {
		return false
	}
	if d.stats == nil {
		return true
	}
	return d.stats.isQuiet(r, now)
}
