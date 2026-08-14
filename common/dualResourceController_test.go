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
	"sync"
	"testing"
	"time"
)

// manualTestClock is a controllable pacerClock for deterministic tests.
type manualTestClock struct {
	mu  sync.Mutex
	now time.Time
}

func newManualTestClock(start time.Time) *manualTestClock { return &manualTestClock{now: start} }

func (c *manualTestClock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.now
}

func (c *manualTestClock) Advance(d time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if d > 0 {
		c.now = c.now.Add(d)
	}
}

// recordingSink captures the most recent rates pushed by the controller.
type recordingSink struct {
	mu   sync.Mutex
	bw   int64
	iops int64
}

func (r *recordingSink) UpdateTargetBytesPerSecond(v int64) {
	r.mu.Lock()
	r.bw = v
	r.mu.Unlock()
}

func (r *recordingSink) UpdateTargetIOPS(v int64) {
	r.mu.Lock()
	r.iops = v
	r.mu.Unlock()
}

func (r *recordingSink) rates() (bw, iops int64) {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.bw, r.iops
}

var _ DualRateSink = (*recordingSink)(nil)

// fakeStatsSource is a controllable ResourceStatsSource for deterministic tests.
type fakeStatsSource struct{ stats ResourceStats }

func (f *fakeStatsSource) PollStats() (ResourceStats, error) { return f.stats, nil }

func TestDualResource_ProactiveEqualShareBothDimensions(t *testing.T) {
	clk := newManualTestClock(time.Unix(0, 0))
	src := &fakeStatsSource{stats: ResourceStats{IopsLimit: 1000, BandwidthLimitBytesPerSec: 100 * 1024 * 1024}}
	sink := &recordingSink{}

	d := newDualResourceControllerWithClock(sink, src, clk, 4, DefaultDualResourceConfig(), true) // 4 active workers

	if _, err := d.Refresh(); err != nil {
		t.Fatalf("refresh: %v", err)
	}
	if d.Mode() != ModeProactive {
		t.Fatalf("expected proactive mode, got %s", d.Mode())
	}
	if got := d.IopsRate(); got != 250 { // 1000 / 4
		t.Fatalf("IOPs share: want 250, got %d", got)
	}
	wantBw := int64(100 * 1024 * 1024 / 4)
	if got := d.BandwidthRate(); got != wantBw {
		t.Fatalf("bandwidth share: want %d, got %d", wantBw, got)
	}
	// The controller must have driven the sink with the same equal-share rates.
	if bw, iops := sink.rates(); bw != wantBw || iops != 250 {
		t.Fatalf("sink not driven: bw=%d iops=%d, want bw=%d iops=250", bw, iops, wantBw)
	}
}

func TestDualResource_ReactiveHalvesOnThrottleDelta(t *testing.T) {
	clk := newManualTestClock(time.Unix(0, 0))
	src := &fakeStatsSource{stats: ResourceStats{IopsLimit: 1000, BandwidthLimitBytesPerSec: 100 * 1024 * 1024}}
	d := newDualResourceControllerWithClock(&recordingSink{}, src, clk, 2, DefaultDualResourceConfig(), true)

	// Prime the baseline (no delta yet).
	if _, err := d.Refresh(); err != nil {
		t.Fatalf("prime: %v", err)
	}

	// Now report a new IOPs throttle delta => reactive AIMD halves IOPs target.
	src.stats.IopsThrottleCount = 5
	backoff, err := d.Refresh()
	if err != nil {
		t.Fatalf("refresh: %v", err)
	}
	if d.Mode() != ModeReactive {
		t.Fatalf("expected reactive mode after throttle delta, got %s", d.Mode())
	}
	if got := d.IopsRate(); got != 500 { // IopsLimit/2
		t.Fatalf("IOPs target after halving: want 500, got %d", got)
	}
	if backoff <= 0 {
		t.Fatal("expected a positive backoff on throttle")
	}
}

// Real-time sustained 429s must switch to reactive AIMD before any poll
// delta is observed (the fast path), independent of the 30s poll.
func TestDualResource_ResponseSignalTriggersReactive(t *testing.T) {
	clk := newManualTestClock(time.Unix(0, 0))
	src := &fakeStatsSource{stats: ResourceStats{IopsLimit: 1000, BandwidthLimitBytesPerSec: 100 * 1024 * 1024}}
	cfg := DefaultDualResourceConfig()
	d := newDualResourceControllerWithClock(&recordingSink{}, src, clk, 2, cfg, true)

	// Prime proactive baseline; no throttles yet.
	if _, err := d.Refresh(); err != nil {
		t.Fatalf("prime: %v", err)
	}
	if d.Mode() != ModeProactive {
		t.Fatalf("want proactive after prime, got %s", d.Mode())
	}

	// Feed a burst of IOPS 429s inside the response window. The gate needs
	// ResponseMinEvents (5) throttles at >= ResponseMinRatio (0.2).
	var lastBackoff time.Duration
	for i := 0; i < cfg.ResponseMinEvents; i++ {
		lastBackoff = d.HandleResponse(ThrottleIops, 0)
		clk.Advance(100 * time.Millisecond)
	}

	if d.Mode() != ModeReactive {
		t.Fatalf("want reactive after sustained 429s, got %s", d.Mode())
	}
	if got := d.IopsRate(); got != 500 { // IopsLimit/2, seeded from lastStats
		t.Fatalf("iops target after fast-path halving: want 500, got %d", got)
	}
	if lastBackoff <= 0 {
		t.Fatal("want positive backoff on sustained response throttle")
	}
	// Bandwidth was never throttled and the fast path must not ramp it.
	if got := d.BandwidthRate(); got != 100*1024*1024/2 {
		t.Fatalf("bandwidth should stay at proactive share, got %d", got)
	}
}

// Retry-After from the response overrides the computed backoff on the fast path.
func TestDualResource_ResponseRetryAfterOverridesBackoff(t *testing.T) {
	clk := newManualTestClock(time.Unix(0, 0))
	src := &fakeStatsSource{stats: ResourceStats{IopsLimit: 1000, BandwidthLimitBytesPerSec: 100 * 1024 * 1024}}
	cfg := DefaultDualResourceConfig()
	d := newDualResourceControllerWithClock(&recordingSink{}, src, clk, 2, cfg, true)
	if _, err := d.Refresh(); err != nil {
		t.Fatalf("prime: %v", err)
	}

	var backoff time.Duration
	for i := 0; i < cfg.ResponseMinEvents; i++ {
		backoff = d.HandleResponse(ThrottleIops, 3.0) // Retry-After: 3s
		clk.Advance(100 * time.Millisecond)
	}
	if backoff != 3*time.Second {
		t.Fatalf("want Retry-After backoff of 3s, got %v", backoff)
	}
}

// Return to proactive requires BOTH the poll delta signal and the
// real-time response signal to be quiet.
func TestDualResource_ReturnToProactiveRequiresBothQuiet(t *testing.T) {
	clk := newManualTestClock(time.Unix(0, 0))
	src := &fakeStatsSource{stats: ResourceStats{IopsLimit: 1000, BandwidthLimitBytesPerSec: 100 * 1024 * 1024}}
	cfg := DefaultDualResourceConfig()
	d := newDualResourceControllerWithClock(&recordingSink{}, src, clk, 2, cfg, true)
	if _, err := d.Refresh(); err != nil {
		t.Fatalf("prime: %v", err)
	}

	// Drive reactive via a stats IOPS delta.
	src.stats.IopsThrottleCount = 5
	if _, err := d.Refresh(); err != nil {
		t.Fatalf("refresh: %v", err)
	}
	if d.Mode() != ModeReactive {
		t.Fatalf("want reactive after stats delta, got %s", d.Mode())
	}

	// Stats now quiet (no further delta), but a fresh sustained 429 burst keeps
	// the response signal hot. A poll must NOT return to proactive.
	for i := 0; i < cfg.ResponseMinEvents; i++ {
		d.HandleResponse(ThrottleIops, 0)
		clk.Advance(100 * time.Millisecond)
	}
	if _, err := d.Refresh(); err != nil {
		t.Fatalf("refresh: %v", err)
	}
	if d.Mode() != ModeReactive {
		t.Fatalf("response signal still hot: want reactive, got %s", d.Mode())
	}

	// Let BOTH signals go quiet past the quiet window, then poll: proactive.
	clk.Advance(cfg.QuietForProactiveReturn + time.Second)
	if _, err := d.Refresh(); err != nil {
		t.Fatalf("refresh: %v", err)
	}
	if d.Mode() != ModeProactive {
		t.Fatalf("both signals quiet: want proactive, got %s", d.Mode())
	}
	if got := d.IopsRate(); got != 500 { // equal share restored: IopsLimit/2 workers
		t.Fatalf("want restored proactive share 500, got %d", got)
	}
}

// A Blob-style controller (pollStatsSignal=false, e.g. NewBlobController) must
// IGNORE poll-based throttle-counter deltas entirely, and switch to reactive
// only on sustained real-time 429/503 responses.
func TestDualResource_BlobIgnoresStatsSignal(t *testing.T) {
	clk := newManualTestClock(time.Unix(0, 0))
	// Blob is bandwidth-only: IopsLimit 0 (unlimited), bandwidth from job rate.
	src := &fakeStatsSource{stats: ResourceStats{BandwidthLimitBytesPerSec: 100 * 1024 * 1024}}
	cfg := DefaultDualResourceConfig()
	d := newDualResourceControllerWithClock(&recordingSink{}, src, clk, 2, cfg, false) // no poll-based stats signal

	if _, err := d.Refresh(); err != nil {
		t.Fatalf("prime: %v", err)
	}
	if d.Mode() != ModeProactive {
		t.Fatalf("want proactive after prime, got %s", d.Mode())
	}

	// A poll-based throttle-counter delta must be ignored for Blob.
	src.stats.BandwidthThrottleCount = 10
	src.stats.IopsThrottleCount = 10
	if _, err := d.Refresh(); err != nil {
		t.Fatalf("refresh: %v", err)
	}
	if d.Mode() != ModeProactive {
		t.Fatalf("Blob must ignore stats deltas: want proactive, got %s", d.Mode())
	}

	// Sustained real-time 429s must still drive it reactive on bandwidth.
	for i := 0; i < cfg.ResponseMinEvents; i++ {
		d.HandleResponse(ThrottleBandwidth, 0)
		clk.Advance(100 * time.Millisecond)
	}
	if d.Mode() != ModeReactive {
		t.Fatalf("Blob must react to sustained responses: want reactive, got %s", d.Mode())
	}
}
