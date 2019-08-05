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

type ConcurrencyTuner interface {
	GetRecommendedConcurrency(currentMbps int) (newConcurrency int, reason string)
}

type nullConcurrencyTuner struct {
	fixedValue int
}

func (n *nullConcurrencyTuner) GetRecommendedConcurrency(currentMbps int) (newConcurrency int, reason string) {
	return n.fixedValue, concurrencyReasonFixed
}

type autoConcurrencyTuner struct {
	mbps        chan int
	concurrency chan struct {
		v int
		s string
	}
	initialConcurrency int
}

func NewAutoConcurrencyTuner() ConcurrencyTuner {
	t := &autoConcurrencyTuner{
		mbps: make(chan int),
		concurrency: make(chan struct {
			v int
			s string
		}),
		initialConcurrency: 16}
	go t.worker()
	return t
}

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
	concurrencyReasonInitial = "initial"
	concurrencyReasonSeeking = "seeking"
	concurrencyReasonBackoff = "backingOff"
	concurrencyReasonStable  = "stable"
	concurrencyReasonFixed   = "fixedRate"
)

func (t *autoConcurrencyTuner) worker() {
	const aggressiveMultiplier = 4
	const standardMultiplier = 2
	const cuttoffForAgqressiveMultiplier = 255
	const slowdownFactor = 5
	const minMulitplier = 1.19 // really this is 1.2, but use a little less to make the floating point comparisons robust
	const fudgeFactor = 0.75

	multiplier := float32(aggressiveMultiplier)
	concurrency := float32(t.initialConcurrency)

	// get initial baseline throughput
	lastSpeed := t.getCurrentSpeed()

	for { // todo, add the conditions here
		concurrency = concurrency * multiplier
		desiredSpeedIncrease := lastSpeed * (multiplier - 1) * fudgeFactor // we'd like it to speed up linearly, but we'll accept a bit less, according to fudge factor
		desiredNewSpeed := lastSpeed + desiredSpeedIncrease

		t.setConcurrency(concurrency, concurrencyReasonSeeking)
		lastSpeed = t.getCurrentSpeed()

		if lastSpeed > desiredNewSpeed {
			// Our concurrency change gave the hoped-for speed increase, so loop around and see if another increase will also work
			// (but first reduce aggression if concurrency is already high)
			if multiplier > standardMultiplier && concurrency >= cuttoffForAgqressiveMultiplier {
				multiplier = standardMultiplier
			}
		} else {
			// it didn't work, so we conclude it was too aggressive and back off to where we were before
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

	// just provide the stable value for ever
	for {
		t.setConcurrency(concurrency, concurrencyReasonStable)
		_ = t.getCurrentSpeed()
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
