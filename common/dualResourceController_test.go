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
	numofWorkers := 1
	iopsLimit := int64(1000)
	bwLimit := int64(100 * 1024 * 1024)
	perWorkerIops := iopsLimit / int64(numofWorkers)
	perWorkerBw := bwLimit / int64(numofWorkers)

	clk := newManualTestClock(time.Unix(0, 0))
	src := &fakeStatsSource{stats: ResourceStats{IopsLimit: iopsLimit, BandwidthLimitBytesPerSec: bwLimit}}
	sink := &recordingSink{}

	d := newDualResourceControllerWithClock(sink, src, clk, numofWorkers, DefaultDualResourceConfig(), true) // 1 active worker
	t.Logf("setup: workers=%d iopsLimit=%d bwLimit=%d", numofWorkers, src.stats.IopsLimit, src.stats.BandwidthLimitBytesPerSec)

	if _, err := d.Refresh(); err != nil {
		t.Fatalf("refresh: %v", err)
	}
	t.Logf("after refresh: mode=%s iopsRate=%d bwRate=%d", d.Mode(), d.IopsRate(), d.BandwidthRate())
	if d.Mode() != ModeProactive {
		t.Fatalf("expected proactive mode, got %s", d.Mode())
	}
	t.Logf("Running in : mode=%s iopsRate=%d bwRate=%d", d.Mode(), d.IopsRate(), d.BandwidthRate())
	if got := d.IopsRate(); got != perWorkerIops { // 1000 / 1
		t.Fatalf("IOPs share: want %d, got %d", perWorkerIops, got)
	}
	t.Logf("Validated the IOPs in Proactive Mode")
	wantBw := (bwLimit / int64(numofWorkers))
	if got := d.BandwidthRate(); got != wantBw {
		t.Fatalf("bandwidth share: want %d, got %d", wantBw, got)
	}
	t.Logf("Validated the bandwidth in Proactive Mode")
	// The controller must have driven the sink with the same equal-share rates.
	if bw, iops := sink.rates(); bw != perWorkerBw || iops != perWorkerIops {
		t.Fatalf("sink not driven: bw=%d iops=%d, want bw=%d iops=%d", bw, iops, perWorkerBw, perWorkerIops)
	}
}

func TestDualResource_UpdateIopsBwLimits(t *testing.T) {
	numofWorkers := 1
	iopsLimit := int64(1000)
	bwLimit := int64(100 * 1024 * 1024)
	clk := newManualTestClock(time.Unix(0, 0))
	src := &fakeStatsSource{stats: ResourceStats{IopsLimit: iopsLimit, BandwidthLimitBytesPerSec: bwLimit}}
	d := newDualResourceControllerWithClock(&recordingSink{}, src, clk, numofWorkers, DefaultDualResourceConfig(), true)

	// Prime the baseline (no delta yet).
	if _, err := d.Refresh(); err != nil {
		t.Fatalf("prime: %v", err)
	}

	t.Logf("after refresh: mode=%s iopsRate=%d bwRate=%d", d.Mode(), d.IopsRate(), d.BandwidthRate())
	if d.Mode() != ModeProactive {
		t.Fatalf("expected proactive mode, got %s", d.Mode())
	}
	t.Logf("Running in : mode=%s iopsRate=%d bwRate=%d", d.Mode(), d.IopsRate(), d.BandwidthRate())
	src.stats.IopsLimit = iopsLimit * 10
	src.stats.BandwidthLimitBytesPerSec = bwLimit * 10
	if _, err := d.Refresh(); err != nil {
		t.Fatalf("refresh: %v", err)
	}
	t.Logf("after increasing IOPs Rate 10 times: mode=%s iopsRate=%d bwRate=%d", d.Mode(), d.IopsRate(), d.BandwidthRate())
	if d.Mode() != ModeProactive {
		t.Fatalf("expected proactive mode, got %s", d.Mode())
	}
	t.Logf("Running in : mode=%s iopsRate=%d bwRate=%d", d.Mode(), d.IopsRate(), d.BandwidthRate())
	if got := d.IopsRate(); got != (iopsLimit * 10) { // 1000 / 1
		t.Fatalf("IOPs share: want %d, got %d", iopsLimit*10, got)
	}
	if got := d.BandwidthRate(); got != (bwLimit * 10) { // 100 * 1024 * 1024
		t.Fatalf("Bandwidth share: want %d, got %d", bwLimit*10, got)
	}

	src.stats.IopsLimit = iopsLimit / 5
	src.stats.BandwidthLimitBytesPerSec = bwLimit / 5
	if _, err := d.Refresh(); err != nil {
		t.Fatalf("refresh: %v", err)
	}
	t.Logf("after decreasing IOPs Rate 5 times: mode=%s iopsRate=%d bwRate=%d", d.Mode(), d.IopsRate(), d.BandwidthRate())
	if d.Mode() != ModeProactive {
		t.Fatalf("expected proactive mode, got %s", d.Mode())
	}
	t.Logf("Running in : mode=%s iopsRate=%d bwRate=%d", d.Mode(), d.IopsRate(), d.BandwidthRate())
	if got := d.IopsRate(); got != (iopsLimit / 5) { // 1000 / 1
		t.Fatalf("IOPs share: want %d, got %d", iopsLimit/5, got)
	}
	if got := d.BandwidthRate(); got != (bwLimit / 5) { // 100 * 1024 * 1024
		t.Fatalf("Bandwidth share: want %d, got %d", bwLimit/5, got)
	}
}

// Real-time sustained 429s must switch to reactive AIMD before any poll
// delta is observed (the fast path), independent of the 30s poll.
func TestDualResource_ResponseSignalTriggersReactive(t *testing.T) {
	numofWorkers := 1
	iopsLimit := int64(1000)
	bwLimit := int64(100 * 1024 * 1024)
	clk := newManualTestClock(time.Unix(0, 0))
	src := &fakeStatsSource{stats: ResourceStats{IopsLimit: iopsLimit, BandwidthLimitBytesPerSec: bwLimit}}
	cfg := DefaultDualResourceConfig()
	d := newDualResourceControllerWithClock(&recordingSink{}, src, clk, numofWorkers, cfg, true)

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
		t.Logf("429 burst i=%d: mode=%s backoff=%v", i, d.Mode(), lastBackoff)
	}

	if d.Mode() != ModeReactive {
		t.Fatalf("want reactive after sustained 429s, got %s", d.Mode())
	}
	if got := d.IopsRate(); got != iopsLimit/2 { // IopsLimit/2, seeded from lastStats
		t.Fatalf("iops target after fast-path halving: want %d, got %d", iopsLimit/2, got)
	}
	if lastBackoff <= 0 {
		t.Fatal("want positive backoff on sustained response throttle")
	}
	// Bandwidth was never throttled and the fast path must not ramp it.
	if got := d.BandwidthRate(); got != bwLimit {
		t.Fatalf("bandwidth should stay at proactive share, got %d", got)
	}
}

// Real-time sustained bandwidth 429s must switch to reactive AIMD before any
// poll delta is observed (the fast path) and halve the bandwidth target,
// leaving IOPS at its proactive share.
func TestDualResource_ResponseSignalTriggersReactiveBandwidth(t *testing.T) {
	numofWorkers := 1
	iopsLimit := int64(1000)
	bwLimit := int64(100 * 1024 * 1024)
	clk := newManualTestClock(time.Unix(0, 0))
	src := &fakeStatsSource{stats: ResourceStats{IopsLimit: iopsLimit, BandwidthLimitBytesPerSec: bwLimit}}
	cfg := DefaultDualResourceConfig()
	d := newDualResourceControllerWithClock(&recordingSink{}, src, clk, numofWorkers, cfg, true)

	// Prime proactive baseline; no throttles yet.
	if _, err := d.Refresh(); err != nil {
		t.Fatalf("prime: %v", err)
	}
	if d.Mode() != ModeProactive {
		t.Fatalf("want proactive after prime, got %s", d.Mode())
	}

	// Feed a burst of bandwidth 429s inside the response window. The gate needs
	// ResponseMinEvents (5) throttles at >= ResponseMinRatio (0.2).
	var lastBackoff time.Duration
	for i := 0; i < cfg.ResponseMinEvents; i++ {
		lastBackoff = d.HandleResponse(ThrottleBandwidth, 0)
		clk.Advance(100 * time.Millisecond)
		t.Logf("429 burst i=%d: mode=%s backoff=%v", i, d.Mode(), lastBackoff)
	}

	if d.Mode() != ModeReactive {
		t.Fatalf("want reactive after sustained 429s, got %s", d.Mode())
	}
	if got := d.BandwidthRate(); got != bwLimit/2 { // BandwidthLimit/2, seeded from lastStats
		t.Fatalf("bandwidth target after fast-path halving: want %d, got %d", bwLimit/2, got)
	}
	if lastBackoff <= 0 {
		t.Fatal("want positive backoff on sustained response throttle")
	}
	// IOPS was never throttled and the fast path must not ramp it.
	if got := d.IopsRate(); got != iopsLimit/(int64(numofWorkers)) { // IopsLimit/2, seeded from lastStats
		t.Fatalf("iops should stay at proactive share, got %d", got)
	}
}

// Retry-After from the response overrides the computed backoff on the fast path.
func TestDualResource_ResponseRetryAfterOverridesBackoff(t *testing.T) {
	numofWorkers := 1
	clk := newManualTestClock(time.Unix(0, 0))
	src := &fakeStatsSource{stats: ResourceStats{IopsLimit: 1000, BandwidthLimitBytesPerSec: 100 * 1024 * 1024}}
	cfg := DefaultDualResourceConfig()
	d := newDualResourceControllerWithClock(&recordingSink{}, src, clk, numofWorkers, cfg, true)
	if _, err := d.Refresh(); err != nil {
		t.Fatalf("prime: %v", err)
	}

	var backoff time.Duration
	for i := 0; i < cfg.ResponseMinEvents; i++ {
		backoff = d.HandleResponse(ThrottleIops, 3.0) // Retry-After: 3s
		clk.Advance(100 * time.Millisecond)
	}
	t.Logf("final backoff with Retry-After=3s: %v", backoff)
	if backoff != 3*time.Second {
		t.Fatalf("want Retry-After backoff of 3s, got %v", backoff)
	}
}

// Return to proactive requires BOTH the poll delta signal and the
// real-time response signal to be quiet.
func TestDualResource_ReturnToProactiveRequiresBothQuiet(t *testing.T) {
	clk := newManualTestClock(time.Unix(0, 0))
	numofWorkers := 1
	src := &fakeStatsSource{stats: ResourceStats{IopsLimit: 1000, BandwidthLimitBytesPerSec: 100 * 1024 * 1024}}
	cfg := DefaultDualResourceConfig()
	d := newDualResourceControllerWithClock(&recordingSink{}, src, clk, numofWorkers, cfg, true)
	if _, err := d.Refresh(); err != nil {
		t.Fatalf("prime: %v", err)
	}

	// Drive reactive via a stats IOPS delta.
	src.stats.IopsThrottleCount = 5
	if _, err := d.Refresh(); err != nil {
		t.Fatalf("refresh: %v", err)
	}
	t.Logf("after stats delta: mode=%s", d.Mode())
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
	t.Logf("after quiet window: mode=%s iopsRate=%d", d.Mode(), d.IopsRate())
	if d.Mode() != ModeProactive {
		t.Fatalf("both signals quiet: want proactive, got %s", d.Mode())
	}
	if got := d.IopsRate(); got != 1000 { // equal share restored: IopsLimit/numofWorkers
		t.Fatalf("want restored proactive share 1000, got %d", got)
	}
}

// A Blob-style controller (pollStatsSignal=false, e.g. NewBlobController) must
// IGNORE poll-based throttle-counter deltas entirely, and switch to reactive
// only on sustained real-time 429/503 responses.
func TestDualResource_BlobIgnoresStatsSignal(t *testing.T) {
	numofWorkers := 1
	clk := newManualTestClock(time.Unix(0, 0))
	// Blob is bandwidth-only: IopsLimit 0 (unlimited), bandwidth from job rate.
	src := &fakeStatsSource{stats: ResourceStats{BandwidthLimitBytesPerSec: 100 * 1024 * 1024}}
	cfg := DefaultDualResourceConfig()
	d := newDualResourceControllerWithClock(&recordingSink{}, src, clk, numofWorkers, cfg, false) // no poll-based stats signal

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
	t.Logf("blob after stats delta (should stay proactive): mode=%s", d.Mode())
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

// SetActiveWorkerCount updates the number of active workers, which is used to
// compute the equal-share target rates in proactive mode. It does not trigger a
// refresh, so the caller must call Refresh() to update the rates after changing
// the active worker count.
func TestDualResource_MultiWorker(t *testing.T) {
	numofWorkers := 2
	iopsLimit := int64(1000)
	bwLimit := int64(100 * 1024 * 1024)
	perWorkerIops := iopsLimit / int64(numofWorkers)
	perWorkerBw := bwLimit / int64(numofWorkers)

	clk := newManualTestClock(time.Unix(0, 0))
	src := &fakeStatsSource{stats: ResourceStats{IopsLimit: iopsLimit, BandwidthLimitBytesPerSec: bwLimit}}
	sink := &recordingSink{}

	d := newDualResourceControllerWithClock(sink, src, clk, numofWorkers, DefaultDualResourceConfig(), true) // 1 active worker
	t.Logf("setup: workers=%d iopsLimit=%d bwLimit=%d", numofWorkers, src.stats.IopsLimit, src.stats.BandwidthLimitBytesPerSec)

	if _, err := d.Refresh(); err != nil {
		t.Fatalf("refresh: %v", err)
	}
	t.Logf("after refresh: mode=%s iopsRate=%d bwRate=%d", d.Mode(), d.IopsRate(), d.BandwidthRate())
	if d.Mode() != ModeProactive {
		t.Fatalf("expected proactive mode, got %s", d.Mode())
	}
	t.Logf("Running in : mode=%s iopsRate=%d bwRate=%d", d.Mode(), d.IopsRate(), d.BandwidthRate())
	if got := d.IopsRate(); got != perWorkerIops { // 1000 / 1
		t.Fatalf("IOPs share: want %d, got %d", perWorkerIops, got)
	}
	t.Logf("Validated the IOPs in Proactive Mode")
	wantBw := (bwLimit / int64(numofWorkers))
	if got := d.BandwidthRate(); got != wantBw {
		t.Fatalf("bandwidth share: want %d, got %d", wantBw, got)
	}
	t.Logf("Validated the bandwidth in Proactive Mode")
	// The controller must have driven the sink with the same equal-share rates.
	if bw, iops := sink.rates(); bw != perWorkerBw || iops != perWorkerIops {
		t.Fatalf("sink not driven: bw=%d iops=%d, want bw=%d iops=%d", bw, iops, perWorkerBw, perWorkerIops)
	}
	numofWorkers = 4
	d.SetActiveWorkerCount(numofWorkers)
	t.Logf("Worker count got updated to : %d", numofWorkers)
	perWorkerIops = iopsLimit / int64(numofWorkers)
	perWorkerBw = bwLimit / int64(numofWorkers)
	if got := d.IopsRate(); got != perWorkerIops { // 1000 / 4
		t.Fatalf("IOPs share: want %d, got %d with workers:%d", perWorkerIops, got, numofWorkers)
	}
	if got := d.BandwidthRate(); got != perWorkerBw { // 100 * 1024 * 1024 / 4
		t.Fatalf("bandwidth share: want %d, got %d with workers:%d", perWorkerBw, got, numofWorkers)
	}

	t.Logf("Equal share validation after increasing worker count in Proactive Mode is successful")
	numofWorkers = 1
	d.SetActiveWorkerCount(numofWorkers)
	t.Logf("Worker count got updated to : %d", numofWorkers)
	perWorkerIops = iopsLimit / int64(numofWorkers)
	perWorkerBw = bwLimit / int64(numofWorkers)
	if got := d.IopsRate(); got != perWorkerIops { // 1000 / 1
		t.Fatalf("IOPs share: want %d, got %d with workers:%d", perWorkerIops, got, numofWorkers)
	}
	if got := d.BandwidthRate(); got != perWorkerBw { // 100 * 1024 * 1024 / 1
		t.Fatalf("bandwidth share: want %d, got %d with workers:%d", perWorkerBw, got, numofWorkers)
	}

	t.Logf("Equal share validation after decreasing worker count in Proactive Mode is successful")
}

// Reactive AIMD must multiplicatively HALVE both the IOPS and bandwidth targets
// on each fresh poll throttle delta (debounced by the backoff window), driving
// them down toward their configured minimums.
func TestDualResource_ReactiveMultiplicativeDecrease(t *testing.T) {
	const iopsLimit = int64(1000)
	const bwLimit = int64(100 * 1024 * 1024)
	numofWorkers := 1
	clk := newManualTestClock(time.Unix(0, 0))
	src := &fakeStatsSource{stats: ResourceStats{IopsLimit: iopsLimit, BandwidthLimitBytesPerSec: bwLimit}}
	d := newDualResourceControllerWithClock(&recordingSink{}, src, clk, numofWorkers, DefaultDualResourceConfig(), true)

	// Prime the baseline; the first poll never counts as a throttle.
	if _, err := d.Refresh(); err != nil {
		t.Fatalf("prime: %v", err)
	}

	// Each step advances the clock past the debounce window and reports a fresh
	// throttle delta on BOTH dimensions, so both targets must halve once.
	wantIops := iopsLimit
	wantBw := bwLimit
	for step := 1; step <= 3; step++ {
		clk.Advance(5 * time.Second)
		src.stats.IopsThrottleCount = int64(step)
		src.stats.BandwidthThrottleCount = int64(step)
		if _, err := d.Refresh(); err != nil {
			t.Fatalf("refresh step %d: %v", step, err)
		}
		wantIops /= 2
		wantBw /= 2
		t.Logf("step %d: mode=%s iopsRate=%d bwRate=%d", step, d.Mode(), d.IopsRate(), d.BandwidthRate())
		if d.Mode() != ModeReactive {
			t.Fatalf("step %d: want reactive, got %s", step, d.Mode())
		}
		if got := d.IopsRate(); got != wantIops {
			t.Fatalf("step %d: IOPS after halving: want %d, got %d", step, wantIops, got)
		}
		if got := d.BandwidthRate(); got != wantBw {
			t.Fatalf("step %d: bandwidth after halving: want %d, got %d", step, wantBw, got)
		}
	}
}

// Reactive AIMD must additively INCREASE a non-throttled resource by exactly one
// step per poll while the controller is still reactive (held there by throttling
// on the OTHER dimension), ramping it back toward its limit.
func TestDualResource_ReactiveAdditiveIncrease(t *testing.T) {
	const iopsLimit = int64(1000)
	const bwLimit = int64(100 * 1024 * 1024)
	numofWorkers := 1

	clk := newManualTestClock(time.Unix(0, 0))
	src := &fakeStatsSource{stats: ResourceStats{IopsLimit: iopsLimit, BandwidthLimitBytesPerSec: bwLimit}}
	cfg := DefaultDualResourceConfig()
	d := newDualResourceControllerWithClock(&recordingSink{}, src, clk, numofWorkers, cfg, true)

	if _, err := d.Refresh(); err != nil {
		t.Fatalf("prime: %v", err)
	}

	// Establish reactive targets by throttling BOTH dimensions once: IOPS -> 500,
	// bandwidth -> 50 MiB.
	throttleCount := int64(1)
	clk.Advance(5 * time.Second)
	src.stats.IopsThrottleCount = throttleCount
	src.stats.BandwidthThrottleCount = throttleCount
	if _, err := d.Refresh(); err != nil {
		t.Fatalf("seed reactive: %v", err)
	}
	if d.Mode() != ModeReactive {
		t.Fatalf("want reactive after seed, got %s", d.Mode())
	}
	wantBw := bwLimit / 2
	if got := d.BandwidthRate(); got != wantBw {
		t.Fatalf("seed bandwidth: want %d, got %d", wantBw, got)
	}

	// Keep throttling ONLY IOPS on each poll; bandwidth stays quiet and must ramp
	// up by exactly BandwidthStep per Refresh.
	for step := 1; step <= 3; step++ {
		clk.Advance(10 * time.Second)
		throttleCount++
		src.stats.IopsThrottleCount = throttleCount // fresh IOPS delta holds reactive
		if _, err := d.Refresh(); err != nil {
			t.Fatalf("bw increase step %d: %v", step, err)
		}
		wantBw += cfg.BandwidthStep
		t.Logf("bw step %d: mode=%s bwRate=%d", step, d.Mode(), d.BandwidthRate())
		if got := d.BandwidthRate(); got != wantBw {
			t.Fatalf("bw step %d: additive increase: want %d, got %d", step, wantBw, got)
		}
	}

	// Now hold reactive by throttling ONLY bandwidth; IOPS stays quiet and must
	// ramp up by exactly IopsStep per Refresh.
	wantIops := d.IopsRate()
	for step := 1; step <= 3; step++ {
		clk.Advance(10 * time.Second)
		src.stats.BandwidthThrottleCount++ // fresh bandwidth delta holds reactive
		if _, err := d.Refresh(); err != nil {
			t.Fatalf("iops increase step %d: %v", step, err)
		}
		wantIops += cfg.IopsStep
		t.Logf("iops step %d: mode=%s iopsRate=%d", step, d.Mode(), d.IopsRate())
		if got := d.IopsRate(); got != wantIops {
			t.Fatalf("iops step %d: additive increase: want %d, got %d", step, wantIops, got)
		}
	}
}
