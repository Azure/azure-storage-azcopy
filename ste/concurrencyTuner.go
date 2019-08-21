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
	"github.com/Azure/azure-storage-azcopy/common"
	"sync"
	"time"
)

type ConcurrencyTuner interface {
	// GetRecommendedConcurrency is called repeatedly, at intervals decided by the caller,
	// to compute recommended concurrency levels
	GetRecommendedConcurrency(currentMbps int, highCpuUsage bool) (newConcurrency int, reason string)

	// RequestCallbackWhenStable lets interested parties ask the concurrency tuner to call them back when the tuner has reached a stable level
	RequestCallbackWhenStable(callback func()) (callbackAccepted bool)

	// GetFinalState returns the final state of the tuner
	GetFinalState() (finalReason string, finalRecommendedConcurrency int, tm time.Time)
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

func (n *nullConcurrencyTuner) GetFinalState() (finalReason string, finalRecommendedConcurrency int, tm time.Time) {
	return concurrencyReasonTunerDisabled, 0, time.Time{}
}

type autoConcurrencyTuner struct {
	observations chan struct {
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
	finalTime           time.Time
	lockFinal           sync.Mutex
}

func NewAutoConcurrencyTuner(initial, max int) ConcurrencyTuner {
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

const (
	concurrencyReasonNone          = ""
	concurrencyReasonTunerDisabled = "tuner disabled" // used as the final (non-finished) state for null tuner
	concurrencyReasonInitial       = "initial starting point"
	concurrencyReasonSeeking       = "seeking optimum"
	concurrencyReasonBackoff       = "backing off"
	concurrencyReasonHitMax        = "hit max concurrency limit"
	concurrencyReasonHighCpu       = "high CPU load, so won't increase concurrency further"
	concurrencyReasonAtOptimum     = "at optimum"
	concurrencyReasonFinished      = "tuning already finished (or never started)"
)

func (t *autoConcurrencyTuner) worker() {
	const initialMultiplier = 2
	const slowdownFactor = 5
	const minMulitplier = 1.19 // really this is 1.2, but use a little less to make the floating point comparisons robust
	const fudgeFactor = 0.25

	multiplier := float32(initialMultiplier)
	concurrency := float32(t.initialConcurrency)
	atMax := false
	highCpu := false
	sawHighMultiGbps := false
	lastReason := concurrencyReasonNone

	// get initial baseline throughput
	lastSpeed, _ := t.getCurrentSpeed()

	for { // todo, add the conditions here
		rateChangeReason := concurrencyReasonSeeking

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
		if lastSpeed > 10000 {
			sawHighMultiGbps = true
		}

		// workaround for variable throughput when targeting 20 Gbps account limit (concurrency > 32 and < 256 didn't seem to give stable throughput)
		// TODO: review this, and look for root cause/better solution. Justification for the current approach is that
		//    if link supports multiGb speeds, then 256 conns is probably fine (i.e. not so many that it will cause problems)
		dontBackoffRegardless := sawHighMultiGbps && concurrency >= 32 && concurrency <= 256
		probeHigherRegardless := sawHighMultiGbps && concurrency >= 32 && concurrency < 256 && multiplier == initialMultiplier

		// decide what to do based on the measurement
		if lastSpeed > desiredNewSpeed || probeHigherRegardless {
			// Our concurrency change gave the hoped-for speed increase, so loop around and see if another increase will also work,
			// unless already at max
			if atMax || highCpu {
				break
			}
		} else if dontBackoffRegardless {
			// nothing more we can do
			break
		} else {
			// the new speed didn't work, so we conclude it was too aggressive and back off to where we were before
			concurrency = concurrency / multiplier

			// reduce multiplier to probe more slowly on the next iteration
			multiplier = 1 + (multiplier-1)/slowdownFactor

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
		if highCpu {
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
	t.finalTime = time.Now()
}

func (t *autoConcurrencyTuner) GetFinalState() (reason string, concurrency int, tm time.Time) {
	t.lockFinal.Lock()
	defer t.lockFinal.Unlock()

	return t.finalReason, t.finalConcurrency, t.finalTime
}

func (t *autoConcurrencyTuner) RequestCallbackWhenStable(callback func()) (callbackAccepted bool) {
	select {
	case t.callbacksWhenStable <- callback:
		return true
	default:
		return false // channel full
	}
}
