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
	chk "gopkg.in/check.v1"
	"math"
)

type concurrencyTunerSuite struct{}

var _ = chk.Suite(&concurrencyTunerSuite{})

type tunerStep struct {
	concurrency     int // the concurrency value recommended by the tuner
	reason          string
	mbpsObserved    int // the speed observed with the new concurrency value
	highCpuObserved bool
}

func (s *concurrencyTunerSuite) noMax() int {
	return math.MaxInt32
}

func (s *concurrencyTunerSuite) TestConcurrencyTuner_LowBW(c *chk.C) {
	steps := []tunerStep{
		{4, concurrencyReasonInitial, 40, false},
		{16, concurrencyReasonSeeking, 100, false},
		{64, concurrencyReasonSeeking, 100, false},
		{16, concurrencyReasonBackoff, 100, false},
		{32, concurrencyReasonSeeking, 100, false},
		{16, concurrencyReasonBackoff, 100, false},
		{19, concurrencyReasonSeeking, 100, false},
		{16, concurrencyReasonAtOptimum, 100, false},
		{16, concurrencyReasonFinished, 100, false}}

	s.runTest(c, steps, s.noMax(), true, false)

}

func (s *concurrencyTunerSuite) TestConcurrencyTuner_VeryLowBandwidth(c *chk.C) {
	steps := []tunerStep{
		{4, concurrencyReasonInitial, 10, false},
		{16, concurrencyReasonSeeking, 11, false},
		{4, concurrencyReasonBackoff, 10, false},
		{8, concurrencyReasonSeeking, 11, false},
		{4, concurrencyReasonBackoff, 10, false},
		{5, concurrencyReasonSeeking, 10, false},
		{4, concurrencyReasonAtOptimum, 10, false},
		{4, concurrencyReasonFinished, 10, false}}

	s.runTest(c, steps, s.noMax(), true, false)

}

func (s *concurrencyTunerSuite) TestConcurrencyTuner_HighBandwidth_PlentyOfCpu(c *chk.C) {
	steps := []tunerStep{
		{4, concurrencyReasonInitial, 400, false},
		{16, concurrencyReasonSeeking, 1000, false},
		{64, concurrencyReasonSeeking, 6000, false},
		{256, concurrencyReasonSeeking, 20000, false},
		{512, concurrencyReasonSeeking, 20000, false},
		{256, concurrencyReasonBackoff, 20000, false},
		{307, concurrencyReasonSeeking, 20000, false},
		{256, concurrencyReasonAtOptimum, 20000, false},
		{256, concurrencyReasonFinished, 20000, false},
	}

	s.runTest(c, steps, s.noMax(), true, false)
}

func (s *concurrencyTunerSuite) TestConcurrencyTuner_HighBandwidth_ConstrainedCpu(c *chk.C) {
	steps := []tunerStep{
		{4, concurrencyReasonInitial, 400, false},
		{16, concurrencyReasonSeeking, 1000, false},
		{64, concurrencyReasonSeeking, 6000, false},
		{256, concurrencyReasonSeeking, 20000, true}, // high CPU doesn't stop it probing higher, but does change the final status
		{512, concurrencyReasonSeeking, 20000, true},
		{256, concurrencyReasonBackoff, 20000, false},
		{307, concurrencyReasonSeeking, 20000, false},
		{256, concurrencyReasonHighCpu, 20000, false}, // different status reported here if we ever saw high CPU, even if we are not seeing it right now
		{256, concurrencyReasonFinished, 20000, false},
	}

	s.runTest(c, steps, s.noMax(), true, false)
}

func (s *concurrencyTunerSuite) TestConcurrencyTuner_CapMaxConcurrency(c *chk.C) {
	steps := []tunerStep{
		{4, concurrencyReasonInitial, 400, false},
		{16, concurrencyReasonSeeking, 1000, false},
		{64, concurrencyReasonSeeking, 4000, false},
		{100, concurrencyReasonHitMax, 8000, false}, // NOT "at optimum"
		{100, concurrencyReasonFinished, 8000, false},
	}

	s.runTest(c, steps, 100, true, false)
}

func (s *concurrencyTunerSuite) TestConcurrencyTuner_OptimalValueNotNearStandardSteps(c *chk.C) {
	steps := []tunerStep{
		{4, concurrencyReasonInitial, 200, false},
		{16, concurrencyReasonSeeking, 800, false},
		{64, concurrencyReasonSeeking, 2000, false},
		{256, concurrencyReasonSeeking, 10000, false},
		{512, concurrencyReasonSeeking, 17500, false},
		{1024, concurrencyReasonSeeking, 20000, false},
		{512, concurrencyReasonBackoff, 17500, false},
		{614, concurrencyReasonSeeking, 18500, false},
		{737, concurrencyReasonSeeking, 19500, false},
		{884, concurrencyReasonSeeking, 19550, false},
		{737, concurrencyReasonAtOptimum, 19500, false},
		{737, concurrencyReasonFinished, 19500, false},
	}

	s.runTest(c, steps, s.noMax(), true, false)

}

func (s *concurrencyTunerSuite) TestConcurrencyTuner_HighBandwidthWorkaround_AppliesWhenBenchmarking(c *chk.C) {
	steps := []tunerStep{
		{4, concurrencyReasonInitial, 2000, false},
		{16, concurrencyReasonSeeking, 8000, false},
		{64, concurrencyReasonSeeking, 11500, false},  // this would cause backoff if not for workaround
		{256, concurrencyReasonSeeking, 11500, false}, // instead it tries higher...
		{64, concurrencyReasonBackoff, 11500, false},  // ... but, with no retries to prevent it backing off, it backs off from the higher value that it tried
	}

	s.runTest(c, steps, s.noMax(), true, false)
}

func (s *concurrencyTunerSuite) TestConcurrencyTuner_HighBandwidthWorkaround_DoesntApplyWhenNotBenchmarking(c *chk.C) {
	steps := []tunerStep{
		{4, concurrencyReasonInitial, 2000, false},
		{16, concurrencyReasonSeeking, 8000, false},
		{64, concurrencyReasonSeeking, 11500, false},
		{16, concurrencyReasonBackoff, 115000, false},
	}

	s.runTest(c, steps, s.noMax(), false, false)
}

func (s *concurrencyTunerSuite) TestConcurrencyTuner__HighBandwidthWorkaround_StaysHighIfSeesRetries(c *chk.C) {
	steps := []tunerStep{
		{4, concurrencyReasonInitial, 2000, false},
		{16, concurrencyReasonSeeking, 8000, false},
		{64, concurrencyReasonSeeking, 11500, false},    // this would cause backoff if not for workaround
		{256, concurrencyReasonSeeking, 11500, false},   // instead it tries higher...
		{256, concurrencyReasonAtOptimum, 11500, false}, // ... and, because there ARE reties, it does not back off
	}

	s.runTest(c, steps, s.noMax(), true, true)
}

func (s *concurrencyTunerSuite) runTest(c *chk.C, steps []tunerStep, maxConcurrency int, isBenchmarking bool, simulateRetries bool) {
	t := NewAutoConcurrencyTuner(4, maxConcurrency, isBenchmarking)
	observedMbps := -1 // there's no observation at first
	observedHighCpu := false

	for _, x := range steps {
		// ask the tuner what do to
		if simulateRetries {
			t.recordRetry() // tell it we got a 503 back from the server
		}
		conc, reason := t.GetRecommendedConcurrency(observedMbps, observedHighCpu)

		// assert that it told us what we expect in this test
		c.Assert(conc, chk.Equals, x.concurrency)
		c.Assert(reason, chk.Equals, x.reason)

		// get the "simulated" throughput that results from the new concurrency
		observedMbps = x.mbpsObserved
		observedHighCpu = x.highCpuObserved
	}
}
