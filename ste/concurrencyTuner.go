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

package ste

import (
	"github.com/Azure/azure-storage-azcopy/v10/common"
	"sync"
	"sync/atomic"
)

type ConcurrencyTuner interface {
	// GetRecommendedConcurrency is called repeatedly, at intervals decided by the caller,
	// to compute recommended concurrency levels
	GetRecommendedConcurrency(currentMbps int, highCpuUsage bool) (newConcurrency int, reason string)

	// RequestCallbackWhenStable lets interested parties ask the concurrency tuner to call them back when the tuner has reached a stable level
	RequestCallbackWhenStable(callback func()) (callbackAccepted bool)

	// GetFinalState returns the final state of the tuner
	GetFinalState() (finalReason string, finalRecommendedConcurrency int)

	// recordRetry informs the concurrencyTuner that a retry has happened
	recordRetry()
}

type nullConcurrencyTuner struct {
	fixedValue int
}

func (n *nullConcurrencyTuner) GetRecommendedConcurrency(currentMbps int, highCpuUsage bool) (newConcurrency int, reason string) {
	return n.fixedValue, concurrencyReasonFinished
}

func (n *nullConcurrencyTuner) RequestCallbackWhenStable(callback func()) (callbackAccepted bool) {
	return false
}

func (n *nullConcurrencyTuner) GetFinalState() (finalReason string, finalRecommendedConcurrency int) {
	return concurrencyReasonTunerDisabled, n.fixedValue
}

func (n *nullConcurrencyTuner) recordRetry() {
	// noop
}

type autoConcurrencyTuner struct {
	atomicRetryCount int64
	observations     chan struct {
		mbps      int
		isHighCpu bool
	}
	recommendations chan struct {
		value  int
		reason string
	}
	initialConcurrency  int
	maxConcurrency      int
	cpuMonitor          common.CPUMonitor
	callbacksWhenStable chan func()
	finalReason         string
	finalConcurrency    int
	lockFinal           sync.Mutex
	isBenchmarking      bool
}

func NewAutoConcurrencyTuner(initial, max int, isBenchmarking bool) ConcurrencyTuner {
	t := &autoConcurrencyTuner{
		observations: make(chan struct {
			mbps      int
			isHighCpu bool
		}),
		recommendations: make(chan struct {
			value  int
			reason string
		}),
		initialConcurrency:  initial,
		maxConcurrency:      max,
		callbacksWhenStable: make(chan func(), 1000),
		lockFinal:           sync.Mutex{},
		isBenchmarking:      isBenchmarking,
	}
	go t.worker()
	return t
}

// GetRecommendedConcurrency is the public interface of the tuner.
// It imposes no timing constraints, on how frequently it is called, because we want
// to run unit tests very quickly. It's up to the caller (in non-test situations) to
// call at an appropriate frequency such that the currentMbps values are sufficiently accurate
// (E.g. calling continuously doesn't give enough time to measure actual speeds)
func (t *autoConcurrencyTuner) GetRecommendedConcurrency(currentMbps int, highCpuUsage bool) (newConcurrency int, reason string) {
	if currentMbps < 0 {
		return t.initialConcurrency, concurrencyReasonInitial
	} else {
		// push value into worker, and get its result
		t.observations <- struct {
			mbps      int
			isHighCpu bool
		}{currentMbps, highCpuUsage}

		result := <-t.recommendations

		return result.value, result.reason
	}
}

func (t *autoConcurrencyTuner) recordRetry() {
	atomic.AddInt64(&t.atomicRetryCount, 1)
}

const (
	concurrencyReasonNone          = ""
	concurrencyReasonTunerDisabled = "tuner disabled" // used as the final (non-finished) state for null tuner
	concurrencyReasonInitial       = "initial starting point"
	concurrencyReasonSeeking       = "seeking optimum"
	concurrencyReasonBackoff       = "backing off"
	concurrencyReasonHitMax        = "hit max concurrency limit"
	concurrencyReasonHighCpu       = "at optimum, but may be limited by CPU"
	concurrencyReasonAtOptimum     = "at optimum"
	concurrencyReasonFinished      = "tuning already finished (or never started)"
)

func (t *autoConcurrencyTuner) worker() {
	const standardMultiplier = 2
	const boostedMultiplier = standardMultiplier * 2
	const topOfBoostZone = 256 // boosted multiplier applies up to this many connections
	const slowdownFactor = 5
	const minMulitplier = 1.19 // really this is 1.2, but use a little less to make the floating point comparisons robust
	const fudgeFactor = 0.2

	multiplier := float32(boostedMultiplier)
	concurrency := float32(t.initialConcurrency)
	atMax := false
	highCpu := false
	everSawHighCpu := false
	sawHighMultiGbps := false
	probeHigherRegardless := false
	dontBackoffRegardless := false
	multiplierReductionCount := 0
	lastReason := concurrencyReasonNone

	// get initial baseline throughput
	lastSpeed, _ := t.getCurrentSpeed()

	for { // todo, add the conditions here
		rateChangeReason := concurrencyReasonSeeking

		if concurrency >= topOfBoostZone && multiplier > standardMultiplier {
			multiplier = standardMultiplier // don't use boosted multiplier for ever
		}

		// enforce a ceiling
		atMax = concurrency*multiplier > float32(t.maxConcurrency)
		if atMax {
			multiplier = float32(t.maxConcurrency) / concurrency
			rateChangeReason = concurrencyReasonHitMax
		}

		// compute increase
		concurrency = concurrency * multiplier
		desiredSpeedIncrease := lastSpeed * (multiplier - 1) * fudgeFactor // we'd like it to speed up linearly, but we'll accept a _lot_ less, according to fudge factor in the interests of finding best possible speed
		desiredNewSpeed := lastSpeed + desiredSpeedIncrease

		// action the increase and measure its effect
		lastReason = t.setConcurrency(concurrency, rateChangeReason)
		lastSpeed, highCpu = t.getCurrentSpeed()
		if lastSpeed > 11000 {
			sawHighMultiGbps = true
		}
		if highCpu {
			everSawHighCpu = true // this doesn't stop us probing higher concurrency, since sometimes that works even when CPU looks high, but it does change the way we report the result
		}

		if t.isBenchmarking {
			// Be a little more aggressive if we are tuning for benchmarking purposes (as opposed to day to day use)

			// If we are seeing retries (within "normal" concurrency range) then for benchmarking purposes we don't want to back off.
			// (Since if we back off the retries might stop and then they won't be reported on as a limiting factor.)
			sawRetry := atomic.SwapInt64(&t.atomicRetryCount, 0) > 0
			dontBackoffRegardless = sawRetry && concurrency <= 256

			// Workaround for variable throughput when targeting 20 Gbps account limit (concurrency around 64 didn't seem to give stable throughput in some tests)
			// TODO: review this, and look for root cause/better solution
			probeHigherRegardless = sawHighMultiGbps && concurrency >= 32 && concurrency < 128 && multiplier >= standardMultiplier
		}

		// decide what to do based on the measurement
		if lastSpeed > desiredNewSpeed || probeHigherRegardless {
			// Our concurrency change gave the hoped-for speed increase, so loop around and see if another increase will also work,
			// unless already at max
			if atMax {
				break
			}
		} else if dontBackoffRegardless {
			// nothing more we can do
			break
		} else {
			// the new speed didn't work, so we conclude it was too aggressive and back off to where we were before
			concurrency = concurrency / multiplier

			// reduce multiplier to probe more slowly on the next iteration
			if multiplier > standardMultiplier {
				multiplier = standardMultiplier // just back off from our "boosted" multiplier
			} else {
				multiplier = 1 + (multiplier-1)/slowdownFactor // back off to a much smaller multiplier
			}

			// bump multiplier up until its at least enough to influence the connection count by 1
			// (but, to make sure our algorithm terminates, limit how much we do this)
			multiplierReductionCount++
			if multiplierReductionCount <= 2 {
				for int(multiplier*concurrency) == int(concurrency) {
					multiplier += 0.05
				}
			}

			if multiplier < minMulitplier {
				break // no point in tuning any more
			} else {
				lastReason = t.setConcurrency(concurrency, concurrencyReasonBackoff)
				lastSpeed, _ = t.getCurrentSpeed() // must re-measure immediately after backing off
			}
		}
	}

	if atMax {
		// provide no special "we found the best value" result, because actually we possibly didn't find it, we just hit the max,
		// and we've already notified caller of that reason, when we tied using the max
	} else {
		// provide the final value once with a reason why its our final value
		if everSawHighCpu {
			lastReason = t.setConcurrency(concurrency, concurrencyReasonHighCpu)
		} else {
			lastReason = t.setConcurrency(concurrency, concurrencyReasonAtOptimum)
		}
		_, _ = t.getCurrentSpeed() // read from the channel
	}

	t.storeFinalState(lastReason, concurrency)
	t.signalStability()

	// now just provide an "inactive" value for ever
	for {
		_ = t.setConcurrency(concurrency, concurrencyReasonFinished)
		_, _ = t.getCurrentSpeed() // read from the channel
		t.signalStability()        // in case anyone new has "subscribed"
	}
}

func (t *autoConcurrencyTuner) setConcurrency(mbps float32, reason string) string {
	t.recommendations <- struct {
		value  int
		reason string
	}{int(mbps), reason}
	return reason
}

func (t *autoConcurrencyTuner) getCurrentSpeed() (mbps float32, isHighCpu bool) {
	// assume that any necessary time delays, to measure or to wait for stablization,
	// are done by the caller of GetRecommendedConcurrency
	ob := <-t.observations
	return float32(ob.mbps), ob.isHighCpu
}

func (t *autoConcurrencyTuner) signalStability() {
	for {
		select {
		case callback := <-t.callbacksWhenStable:
			callback() // consume and call each callback once
		default:
			return
		}
	}
}

func (t *autoConcurrencyTuner) storeFinalState(reason string, concurrency float32) {
	t.lockFinal.Lock()
	defer t.lockFinal.Unlock()

	t.finalReason = reason
	t.finalConcurrency = int(concurrency)
}

func (t *autoConcurrencyTuner) GetFinalState() (reason string, concurrency int) {
	t.lockFinal.Lock()
	defer t.lockFinal.Unlock()

	return t.finalReason, t.finalConcurrency
}

func (t *autoConcurrencyTuner) RequestCallbackWhenStable(callback func()) (callbackAccepted bool) {
	select {
	case t.callbacksWhenStable <- callback:
		return true
	default:
		return false // channel full
	}
}
