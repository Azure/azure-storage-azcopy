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

// ConcurrencyTuner is the primary interface to the concurrency tuner
type ConcurrencyTuner interface {
	GetRecommendedConcurrency(currentMbps int) (newConcurrency int, reason string)
}

// ConcurrencyTunerStatsCoordinator supports linkage between stats gathering and concurrency tuning
type ConcurrencyTunerStatsCoordinator interface {
	// RequestCallbackWhenStable lets our stats-gather ask the concurrency tuner to call it back when the tuner has reached a stable level
	RequestCallbackWhenStable(callback func()) (callbackAccepted bool)
}

type nullConcurrencyTuner struct {
	fixedValue int
}

func (n *nullConcurrencyTuner) GetRecommendedConcurrency(currentMbps int) (newConcurrency int, reason string) {
	return n.fixedValue, concurrencyReasonNotActive
}

func (n *nullConcurrencyTuner) RequestCallbackWhenStable(callback func()) (callbackAccepted bool) {
	return false
}

type autoConcurrencyTuner struct {
	mbps        chan int
	concurrency chan struct {
		v int
		s string
	}
	initialConcurrency  int
	maxConcurrency      int
	callbacksWhenStable chan func()
}

func NewAutoConcurrencyTuner(initial, max int) ConcurrencyTuner {
	t := &autoConcurrencyTuner{
		mbps: make(chan int),
		concurrency: make(chan struct {
			v int
			s string
		}),
		initialConcurrency:  initial,
		maxConcurrency:      max,
		callbacksWhenStable: make(chan func(), 1000),
	}
	go t.worker()
	return t
}

// GetRecommendedConcurrency is the public interface of the tuner.
// It imposes no timing constraints, on how frequently it is called, because we want
// to run unit tests very quickly. It's up to the caller (in non-test situations) to
// call at an appropriate frequency such that the currentMbps values are sufficiently accurate
// (E.g. calling continuously doesn't give enough time to measure actual speeds)
func (t *autoConcurrencyTuner) GetRecommendedConcurrency(currentMbps int) (newConcurrency int, reason string) {
	if currentMbps < 0 {
		return t.initialConcurrency, concurrencyReasonInitial
	} else {
		// push value into worker, and get its result
		t.mbps <- currentMbps
		result := <-t.concurrency
		return result.v, result.s
	}
}

const (
	concurrencyReasonInitial   = "initial"
	concurrencyReasonSeeking   = "seeking"
	concurrencyReasonBackoff   = "backing off"
	concurrencyReasonHitMax    = "hit max concurrency limit"
	concurrencyReasonAtOptimum = "at optimum"
	concurrencyReasonNotActive = "not actively tuning"
)

func (t *autoConcurrencyTuner) worker() {
	const initialMultiplier = 2
	const slowdownFactor = 5
	const minMulitplier = 1.19 // really this is 1.2, but use a little less to make the floating point comparisons robust
	const fudgeFactor = 0.25

	multiplier := float32(initialMultiplier)
	concurrency := float32(t.initialConcurrency)
	hitMax := false
	sawHighMultiGbps := false

	// get initial baseline throughput
	lastSpeed := t.getCurrentSpeed()

	for { // todo, add the conditions here
		rateChangeReason := concurrencyReasonSeeking

		// enforce a ceiling
		hitMax = concurrency*multiplier > float32(t.maxConcurrency)
		if hitMax {
			multiplier = float32(t.maxConcurrency) / concurrency
			rateChangeReason = concurrencyReasonHitMax
		}

		// compute increase
		concurrency = concurrency * multiplier
		desiredSpeedIncrease := lastSpeed * (multiplier - 1) * fudgeFactor // we'd like it to speed up linearly, but we'll accept a _lot_ less, according to fudge factor in the interests of finding best possible speed
		desiredNewSpeed := lastSpeed + desiredSpeedIncrease

		// action the increase and measure its effect
		t.setConcurrency(concurrency, rateChangeReason)
		lastSpeed = t.getCurrentSpeed()
		if lastSpeed > 5000 {
			sawHighMultiGbps = true
		}

		// workaround for variable throughput when targeting 20 Gbps account limit (concurrency > 32 and < 256 didn't seem to give stable throughput)
		// TODO: review this, and look for root cause/better solution. Justification for the current approach is that
		//    if link supports multiGb speeds, then 256 conns is probably fine (i.e. not so many that it will cause problems)
		probeHigherRegardless := sawHighMultiGbps && multiplier == initialMultiplier && concurrency >= 32 && concurrency < 256

		// decide what to do based on the measurement
		if lastSpeed > desiredNewSpeed || probeHigherRegardless {
			// Our concurrency change gave the hoped-for speed increase, so loop around and see if another increase will also work,
			// unless already at max
			if hitMax {
				break
			}
		} else {
			// the new speed didn't work, so we conclude it was too aggressive and back off to where we were before
			concurrency = concurrency / multiplier

			// reduce multiplier to probe more slowly on the next iteration
			multiplier = 1 + (multiplier-1)/slowdownFactor

			if multiplier < minMulitplier {
				break // no point in tuning any more
			} else {
				t.setConcurrency(concurrency, concurrencyReasonBackoff)
				lastSpeed = t.getCurrentSpeed() // must re-measure immediately after backing off
			}
		}
	}

	if hitMax {
		// provide no special "we found the best value" result, because actually we possibly didn't find it, we just hit the max,
		// and we've already notified that fact, when we tied using the max
	} else {
		// provide the final value once with a reason that shows we've arrived at what we believe to be the optimal value
		t.setConcurrency(concurrency, concurrencyReasonAtOptimum)
		_ = t.getCurrentSpeed() // read from the channel
		t.signalStability()
	}

	// now just provide an "inactive" value for ever
	for {
		t.signalStability() // in case anyone new has "subscribed"
		t.setConcurrency(concurrency, concurrencyReasonNotActive)
		_ = t.getCurrentSpeed() // read from the channel
	}
}

func (t *autoConcurrencyTuner) setConcurrency(mbps float32, reason string) {
	t.concurrency <- struct {
		v int
		s string
	}{v: int(mbps), s: reason}
}

func (t *autoConcurrencyTuner) getCurrentSpeed() (mbps float32) {
	// assume that any necessary time delays, to measure or to wait for stablization,
	// are done by the caller of GetRecommendedConcurrency
	return float32(<-t.mbps)
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

func (t *autoConcurrencyTuner) RequestCallbackWhenStable(callback func()) (callbackAccepted bool) {
	select {
	case t.callbacksWhenStable <- callback:
		return true
	default:
		return false // channel full
	}
}
