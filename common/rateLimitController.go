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
	"context"
	"math/rand"
	"sync"
	"time"
)

// Control-mode names reported by RateLimitController.Mode() for observability.
const (
	ModeProactive = "proactive"
	ModeReactive  = "reactive"
)

// pacerClock abstracts wall-clock time so mode switching and backoff are
// deterministic under test. Production uses realPacerClock; tests inject a
// manual clock to advance virtual time without sleeping.
type pacerClock interface {
	Now() time.Time
}

type realPacerClock struct{}

func (realPacerClock) Now() time.Time { return time.Now() }

// ResourceStats is the storage-service-agnostic view of a throttled resource's
// limits and cumulative throttle counters that the controller consumes. Concrete
// adapters (e.g. the Azure Files GetShareStats adapter in ste) map their SDK
// response onto this struct, so the controller has no service-specific coupling.
// A 0 limit means "unlimited" for that dimension.
type ResourceStats struct {
	IopsLimit                 int64 // ops/sec; 0 = unlimited
	BandwidthLimitBytesPerSec int64 // bytes/sec; 0 = unlimited
	IopsThrottleCount         int64 // cumulative IOPS throttle counter
	BandwidthThrottleCount    int64 // cumulative bandwidth throttle counter
}

// ResourceStatsSource abstracts the authoritative, poll-based limits/throttle
// signal so the controller has no direct SDK dependency (and tests can drive
// throttle scenarios deterministically). The concrete adapter (Azure Files
// GetShareStats, or a future Azure Blob equivalent) is wired in ste.
type ResourceStatsSource interface {
	PollStats() (ResourceStats, error)
}

// ThrottleKind is the storage-service-agnostic classification of a single HTTP
// response. Callers (e.g. ste for Azure Files) classify their service's 429/503
// bodies into one of these so the controller stays service-neutral.
type ThrottleKind int

const (
	ThrottleNone ThrottleKind = iota
	ThrottleIops
	ThrottleBandwidth
	ThrottleUnknown
)

// RateLimitSink receives the IOPS and bandwidth targets the controller computes.
// The ste rateLimitTokenBucketPacer implements it (UpdateTargetBytesPerSecond +
// UpdateTargetIOPS), so the controller drives enforcement without importing ste
// or owning any token buckets itself. A value of 0 on either dimension means
// "unlimited" for that dimension, matching the pacer's null-pacer semantics.
type RateLimitSink interface {
	UpdateTargetBytesPerSecond(bytesPerSecond int64)
	UpdateTargetIOPS(opsPerSecond int64)
}

// RateLimitConfig tunes the dual-bucket (IOPS + bandwidth) controller.
type RateLimitConfig struct {
	MinIops                 int64
	MinBandwidth            int64
	IopsStep                int64
	BandwidthStep           int64
	QuietForProactiveReturn time.Duration
	BaseBackoff             time.Duration
	MaxBackoff              time.Duration
	JitterFraction          float64

	// Real-time response throttle gate. These tune the sliding-window detector
	// that fuses HTTP 429/503 responses with the poll-based stats signal so the
	// controller can react before the next (~30s) poll without flapping on
	// momentary bursts.
	ResponseWindow    time.Duration
	ResponseMinEvents int
	ResponseMinRatio  float64
}

// DefaultRateLimitConfig returns the recommended defaults.
func DefaultRateLimitConfig() RateLimitConfig {
	return RateLimitConfig{
		MinIops:                 100,
		MinBandwidth:            1 * 1024 * 1024,
		IopsStep:                100,
		BandwidthStep:           1 * 1024 * 1024,
		QuietForProactiveReturn: 60 * time.Second,
		BaseBackoff:             250 * time.Millisecond,
		MaxBackoff:              10 * time.Second,
		JitterFraction:          0.2,
		ResponseWindow:          10 * time.Second,
		ResponseMinEvents:       5,
		ResponseMinRatio:        0.2,
	}
}

func normalizeRateLimitConfig(cfg RateLimitConfig) RateLimitConfig {
	d := DefaultRateLimitConfig()
	if cfg.MinIops <= 0 {
		cfg.MinIops = d.MinIops
	}
	if cfg.MinBandwidth <= 0 {
		cfg.MinBandwidth = d.MinBandwidth
	}
	if cfg.IopsStep <= 0 {
		cfg.IopsStep = d.IopsStep
	}
	if cfg.BandwidthStep <= 0 {
		cfg.BandwidthStep = d.BandwidthStep
	}
	if cfg.QuietForProactiveReturn <= 0 {
		cfg.QuietForProactiveReturn = d.QuietForProactiveReturn
	}
	if cfg.BaseBackoff <= 0 {
		cfg.BaseBackoff = d.BaseBackoff
	}
	if cfg.MaxBackoff <= 0 {
		cfg.MaxBackoff = d.MaxBackoff
	}
	if cfg.JitterFraction < 0 {
		cfg.JitterFraction = d.JitterFraction
	}
	if cfg.ResponseWindow <= 0 {
		cfg.ResponseWindow = d.ResponseWindow
	}
	if cfg.ResponseMinEvents <= 0 {
		cfg.ResponseMinEvents = d.ResponseMinEvents
	}
	if cfg.ResponseMinRatio <= 0 {
		cfg.ResponseMinRatio = d.ResponseMinRatio
	}
	return cfg
}

// ResourceController is the pluggable dual-dimension throttling "brain" that the
// per-share registry and the response policy depend on (instead of a concrete
// type), so each storage service can supply its own control strategy while
// sharing the same wiring. The default implementation is RateLimitController
// (proactive equal-share + reactive AIMD); services select it via the
// per-service constructors (NewAzureFilesController, NewBlobController).
type ResourceController interface {
	// Refresh polls the authoritative stats source, adjusts targets, and returns
	// the backoff the caller should sleep (0 when proactive/quiet).
	Refresh() (time.Duration, error)
	// HandleResponse feeds a single pre-classified real-time throttle outcome and
	// returns the backoff the caller should sleep (0 when not throttling).
	HandleResponse(kind ThrottleKind, retryAfterSec float64) time.Duration
	// Mode reports proactive vs. reactive for observability.
	Mode() string
}

// RateLimitController is the storage-service-agnostic "brain" of the dual-mode
// rate limiter. It does NOT own token buckets; instead it computes IOPS and
// bandwidth targets and pushes them to a RateLimitSink (the ste pacer), switching
// between proactive equal-share and per-resource reactive AIMD based on
// poll-based ResourceStats throttle-counter deltas fused with the real-time HTTP
// 429/503 response stream. Service specifics (GetShareStats mapping, 429/503 body
// classification) live in the adapter/caller, not here.
//
// Concurrency: Refresh (the ~30s poll) and HandleResponse (the per-request fast
// path) may run concurrently; all mutable state is guarded by mu.
type RateLimitController struct {
	mu     sync.Mutex
	clock  pacerClock
	cfg    RateLimitConfig
	rng    *rand.Rand
	source ResourceStatsSource
	sink   RateLimitSink

	// detector fuses the real-time 429/503 response stream with the
	// poll-based stats delta signal, per resource (IOPS and bandwidth).
	detector *rateLimitThrottleDetector

	// pollStatsSignal reports whether the poll-based (e.g. GetShareStats) throttle
	// signal participates in mode decisions. False for services without one (e.g.
	// Blob), where only the real-time HTTP 429/503 signal governs.
	pollStatsSignal bool

	mode              string
	activeWorkerCount int

	// curBw/curIops are the most recently pushed sink targets (bytes/sec and
	// ops/sec). 0 means unlimited on that dimension.
	curBw   int64
	curIops int64

	targetIops int64
	targetBw   int64
	iopsStreak int
	bwStreak   int

	prevIops       int64
	prevBwThrottle int64
	lastThrottleAt time.Time
	primed         bool

	// lastStats caches the most recent poll limits so the fast-path
	// HandleResponse can seed reactive targets without waiting for a fresh poll.
	lastStats ResourceStats

	// Per-resource decrease debounce: a burst of concurrent throttles inside the
	// same congestion window must halve the target only once.
	lastIopsDecreaseAt time.Time
	lastBwDecreaseAt   time.Time
}

// NewRateLimitController creates a dual-resource controller in proactive
// mode, driving sink. Call Refresh every poll interval (~30s), and HandleResponse
// on every relevant HTTP outcome. activeWorkers is the equal-share denominator
// (1 for single-process).
func NewRateLimitController(sink RateLimitSink, source ResourceStatsSource, activeWorkers int, cfg RateLimitConfig) *RateLimitController {
	return newRateLimitControllerWithClock(sink, source, realPacerClock{}, activeWorkers, cfg, true)
}

// Compile-time proof that the shared engine satisfies the pluggable interface.
var _ ResourceController = (*RateLimitController)(nil)

// NewAzureFilesController builds a ResourceController for AzureFiles -> AzureFiles
// copies. It is the shared AIMD engine fed by a GetShareStats-backed
// ResourceStatsSource (dual dimension: IOPS + bandwidth) and Azure Files 429/503
// classification (done in ste). The core engine is unchanged; only the injected
// source/classification/config differ per service.
func NewAzureFilesController(sink RateLimitSink, source ResourceStatsSource, activeWorkers int, cfg RateLimitConfig) ResourceController {
	return NewRateLimitController(sink, source, activeWorkers, cfg)
}

// NewBlobController builds a ResourceController for Blob -> Blob copies. Blob is
// bandwidth-only, so its ResourceStatsSource reports IopsLimit == 0 (IOPS left
// unlimited, so the IOPS AIMD path never engages) and derives the bandwidth
// limit from the job's rate rather than GetShareStats. It also omits the
// poll-based stats throttle signal entirely (withStatsSignal=false), so only the
// generic real-time HTTP 429/503 signal governs mode - GetShareStats-style deltas
// are irrelevant to Blob. It reuses the same shared AIMD engine, so the Blob path
// can be wired in later without changing any core throttling logic.
func NewBlobController(sink RateLimitSink, source ResourceStatsSource, activeWorkers int, cfg RateLimitConfig) ResourceController {
	return newRateLimitControllerWithClock(sink, source, realPacerClock{}, activeWorkers, cfg, false)
}

func newRateLimitControllerWithClock(sink RateLimitSink, source ResourceStatsSource, clock pacerClock, activeWorkers int, cfg RateLimitConfig, pollStatsSignal bool) *RateLimitController {
	cfg = normalizeRateLimitConfig(cfg)
	if activeWorkers < 1 {
		activeWorkers = 1
	}
	return &RateLimitController{
		clock:             clock,
		cfg:               cfg,
		rng:               rand.New(rand.NewSource(time.Now().UnixNano())),
		source:            source,
		sink:              sink,
		mode:              ModeProactive,
		activeWorkerCount: activeWorkers,
		pollStatsSignal:   pollStatsSignal,
		detector: newRateLimitThrottleDetector(
			cfg.ResponseWindow, cfg.ResponseMinEvents, cfg.ResponseMinRatio, cfg.QuietForProactiveReturn, pollStatsSignal,
		),
	}
}

// SetActiveWorkerCount updates the equal-share denominator. Rates are recomputed
// on the next proactive refresh.
func (d *RateLimitController) SetActiveWorkerCount(n int) {
	if n < 1 {
		n = 1
	}
	d.mu.Lock()
	d.activeWorkerCount = n
	d.mu.Unlock()
	d.Refresh() // TBD: Should refresh be called here? It may be better to let the caller decide when to refresh.
}

// Refresh polls the authoritative ResourceStats source, updates mode and target
// rates, and returns the backoff the caller should sleep (0 when proactive/quiet).
// This is the authoritative, poll-paced path (~30s) and is the only path that
// additively increases targets during recovery.
func (d *RateLimitController) Refresh() (time.Duration, error) {
	stats, err := d.source.PollStats()
	if err != nil {
		return 0, err
	}

	d.mu.Lock()
	defer d.mu.Unlock()

	deltaIops := max(stats.IopsThrottleCount-d.prevIops, 0)
	deltaBw := max(stats.BandwidthThrottleCount-d.prevBwThrottle, 0)
	if !d.primed {
		// Only treat as throttle once we have a prior baseline to diff against.
		deltaIops, deltaBw = 0, 0
		d.primed = true
	}
	if !d.pollStatsSignal {
		// Services without a poll-based throttle signal (e.g. Blob) ignore any
		// counter deltas entirely; only the real-time HTTP 429/503 signal drives
		// reactive mode there. The source is still polled for limits (proactive
		// equal-share), just not for throttle counters.
		deltaIops, deltaBw = 0, 0
	}
	d.prevIops = stats.IopsThrottleCount
	d.prevBwThrottle = stats.BandwidthThrottleCount
	d.lastStats = stats

	now := d.clock.Now()

	// Record the authoritative signal so the combined detector can reconcile it
	// with the real-time response stream and gate the return to proactive.
	if deltaIops > 0 {
		d.detector.observeStatsThrottle(resourceIops, now)
	}
	if deltaBw > 0 {
		d.detector.observeStatsThrottle(resourceBandwidth, now)
	}

	// Effective per-resource throttle = authoritative stats delta OR sustained
	// real-time responses. Either signal keeps the resource in reactive AIMD.
	iopsThrottled := deltaIops > 0 || d.detector.sustainedThrottling(resourceIops, now)
	bwThrottled := deltaBw > 0 || d.detector.sustainedThrottling(resourceBandwidth, now)

	if iopsThrottled || bwThrottled {
		return d.applyReactiveLocked(stats, iopsThrottled, bwThrottled, now, 0, true), nil
	}
	d.maybeReturnToProactiveLocked(stats, now)
	return 0, nil
}

// HandleResponse feeds a single pre-classified HTTP throttle outcome into the
// fast, real-time signal. When the response stream becomes sustained-throttling
// for a resource, it switches to reactive AIMD immediately (without waiting for
// the next ~30s poll) and returns the backoff the caller should sleep.
// Non-throttle and not-yet-sustained responses return 0.
//
// kind is classified by the caller (e.g. ste maps Azure Files 429/503 bodies)
// so the controller remains storage-service agnostic. ThrottleUnknown is applied
// conservatively to both resources.
func (d *RateLimitController) HandleResponse(kind ThrottleKind, retryAfterSec float64) time.Duration {
	now := d.clock.Now()

	d.mu.Lock()
	defer d.mu.Unlock()

	switch kind {
	case ThrottleIops:
		d.detector.observeResponse(resourceIops, true, now)
		d.detector.observeResponse(resourceBandwidth, false, now)
	case ThrottleBandwidth:
		d.detector.observeResponse(resourceBandwidth, true, now)
		d.detector.observeResponse(resourceIops, false, now)
	case ThrottleUnknown:
		// Unknown throttle: charge both dimensions conservatively.
		d.detector.observeResponse(resourceIops, true, now)
		d.detector.observeResponse(resourceBandwidth, true, now)
	default: // ThrottleNone: success on both dimensions.
		d.detector.observeResponse(resourceIops, false, now)
		d.detector.observeResponse(resourceBandwidth, false, now)
		return 0
	}

	// Only react once the real-time stream is genuinely sustained; a single 429
	// must not flip modes. The authoritative poll path still catches throttling
	// the response gate misses.
	iopsThrottled := d.detector.sustainedThrottling(resourceIops, now)
	bwThrottled := d.detector.sustainedThrottling(resourceBandwidth, now)
	if !iopsThrottled && !bwThrottled {
		return 0
	}
	// Fast path applies multiplicative decrease only; additive increase stays
	// poll-paced in Refresh.
	return d.applyReactiveLocked(d.lastStats, iopsThrottled, bwThrottled, now, retryAfterSec, false)
}

// setRatesLocked stores and pushes both target rates to the sink.
func (d *RateLimitController) setRatesLocked(bwRate, iopsRate int64) {
	d.setBandwidthRateLocked(bwRate)
	d.setIopsRateLocked(iopsRate)
}

func (d *RateLimitController) setBandwidthRateLocked(bwRate int64) {
	if bwRate < 0 {
		bwRate = 0
	}
	d.curBw = bwRate
	if d.sink != nil {
		d.sink.UpdateTargetBytesPerSecond(bwRate)
	}
}

func (d *RateLimitController) setIopsRateLocked(iopsRate int64) {
	if iopsRate < 0 {
		iopsRate = 0
	}
	d.curIops = iopsRate
	if d.sink != nil {
		d.sink.UpdateTargetIOPS(iopsRate)
	}
}

func (d *RateLimitController) applyProactiveLocked(stats ResourceStats) {
	d.mode = ModeProactive
	d.iopsStreak = 0
	d.bwStreak = 0
	d.targetIops = 0
	d.targetBw = 0
	workers := d.activeWorkerCount
	if workers < 1 {
		workers = 1
	}
	iopsShare := stats.IopsLimit / int64(workers)
	bwShare := stats.BandwidthLimitBytesPerSec / int64(workers)
	d.setRatesLocked(bwShare, iopsShare)
}

// maybeReturnToProactiveLocked restores proactive equal-share only when the
// combined detector reports BOTH resources quiet across BOTH signals (no
// poll delta within the quiet window AND no sustained real-time throttling).
// Until then it stays reactive, letting AIMD ramp back up.
func (d *RateLimitController) maybeReturnToProactiveLocked(stats ResourceStats, now time.Time) {
	if d.mode == ModeProactive {
		d.applyProactiveLocked(stats)
		return
	}
	if d.detector.quiet(resourceIops, now) && d.detector.quiet(resourceBandwidth, now) {
		d.applyProactiveLocked(stats)
	}
	// Otherwise remain reactive: one or both signals not yet quiet.
}

// applyReactiveLocked drives per-resource AIMD from the combined throttle
// decision. Each resource independently multiplicatively decreases when
// throttled (debounced within a congestion window). When allowIncrease is set
// (the poll-paced stats path), a non-throttled resource additively increases
// toward its limit; the per-response fast path passes false so recovery stays
// poll-paced. retryAfterSec, when > 0, overrides the computed backoff with the
// server-provided Retry-After. It returns the backoff to sleep.
func (d *RateLimitController) applyReactiveLocked(stats ResourceStats, iopsThrottled, bwThrottled bool, now time.Time, retryAfterSec float64, allowIncrease bool) time.Duration {
	d.mode = ModeReactive
	d.lastThrottleAt = now
	limitBw := stats.BandwidthLimitBytesPerSec

	// IOPS resource.
	if iopsThrottled {
		d.decreaseIopsLocked(now, stats.IopsLimit)
	} else if allowIncrease && d.targetIops > 0 {
		d.targetIops = min(d.targetIops+d.cfg.IopsStep, stats.IopsLimit)
		d.iopsStreak = 0
		d.setIopsRateLocked(d.targetIops)
	}

	// Bandwidth resource.
	if bwThrottled {
		d.decreaseBandwidthLocked(now, limitBw)
	} else if allowIncrease && d.targetBw > 0 {
		d.targetBw = min(d.targetBw+d.cfg.BandwidthStep, limitBw)
		d.bwStreak = 0
		d.setBandwidthRateLocked(d.targetBw)
	}

	if retryAfterSec > 0 {
		return time.Duration(retryAfterSec * float64(time.Second))
	}
	backoffIops := computeRateLimitBackoff(d.iopsStreak, 0, d.cfg.BaseBackoff, d.cfg.MaxBackoff, d.cfg.JitterFraction, d.rng)
	backoffBw := computeRateLimitBackoff(d.bwStreak, 0, d.cfg.BaseBackoff, d.cfg.MaxBackoff, d.cfg.JitterFraction, d.rng)
	if backoffIops > backoffBw {
		return backoffIops
	}
	return backoffBw
}

// decreaseIopsLocked multiplicatively halves the IOPS target toward MinIops,
// debounced so concurrent throttles inside one congestion window halve once.
func (d *RateLimitController) decreaseIopsLocked(now time.Time, seedLimit int64) {
	window := computeRateLimitBackoff(d.iopsStreak, 0, d.cfg.BaseBackoff, d.cfg.MaxBackoff, 0, nil)
	if !d.lastIopsDecreaseAt.IsZero() && now.Sub(d.lastIopsDecreaseAt) < window {
		return
	}
	if d.targetIops == 0 {
		d.targetIops = max(seedLimit/2, d.cfg.MinIops)
	} else {
		d.targetIops = max(d.targetIops/2, d.cfg.MinIops)
	}
	d.iopsStreak++
	d.lastIopsDecreaseAt = now
	d.setIopsRateLocked(d.targetIops)
}

// decreaseBandwidthLocked mirrors decreaseIopsLocked for the bandwidth target.
func (d *RateLimitController) decreaseBandwidthLocked(now time.Time, seedLimit int64) {
	window := computeRateLimitBackoff(d.bwStreak, 0, d.cfg.BaseBackoff, d.cfg.MaxBackoff, 0, nil)
	if !d.lastBwDecreaseAt.IsZero() && now.Sub(d.lastBwDecreaseAt) < window {
		return
	}
	if d.targetBw == 0 {
		d.targetBw = max(seedLimit/2, d.cfg.MinBandwidth)
	} else {
		d.targetBw = max(d.targetBw/2, d.cfg.MinBandwidth)
	}
	d.bwStreak++
	d.lastBwDecreaseAt = now
	d.setBandwidthRateLocked(d.targetBw)
}

// Mode reports proactive vs. reactive.
func (d *RateLimitController) Mode() string {
	d.mu.Lock()
	defer d.mu.Unlock()
	return d.mode
}

// IopsRate returns the current IOPS target (ops/sec) pushed to the sink.
func (d *RateLimitController) IopsRate() int64 {
	d.mu.Lock()
	defer d.mu.Unlock()
	return d.curIops
}

// BandwidthRate returns the current bandwidth target (bytes/sec) pushed to the sink.
func (d *RateLimitController) BandwidthRate() int64 {
	d.mu.Lock()
	defer d.mu.Unlock()
	return d.curBw
}

// computeRateLimitBackoff returns exponential backoff for the given streak. An
// explicit Retry-After (seconds) takes precedence. When jitterFrac > 0 and
// rng != nil, +/- jitter is applied.
func computeRateLimitBackoff(streak int, retryAfterSec float64, base, maxBackoff time.Duration, jitterFrac float64, rng *rand.Rand) time.Duration {
	if retryAfterSec > 0 {
		return time.Duration(retryAfterSec * float64(time.Second))
	}
	shift := streak
	if shift < 0 {
		shift = 0
	}
	if shift > 30 {
		shift = 30
	}
	backoff := base * time.Duration(int64(1)<<uint(shift))
	if backoff <= 0 || backoff > maxBackoff {
		backoff = maxBackoff
	}
	if jitterFrac > 0 && rng != nil {
		delta := time.Duration(float64(backoff) * jitterFrac * (2*rng.Float64() - 1))
		backoff += delta
		if backoff < 0 {
			backoff = 0
		}
	}
	return backoff
}

// runPollLoop is a convenience driver that calls Refresh every
// pollInterval until ctx is cancelled, sleeping any returned backoff. It is
// optional: callers that already own a polling loop can call
// Refresh directly.
func (d *RateLimitController) runPollLoop(ctx context.Context, pollInterval time.Duration) {
	ticker := time.NewTicker(pollInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			if backoff, err := d.Refresh(); err == nil && backoff > 0 {
				select {
				case <-ctx.Done():
					return
				case <-time.After(backoff):
				}
			}
		}
	}
}
