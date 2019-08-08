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
	concurrency  int // the concurrency value recommended by the tuner
	reason       string
	mbpsObserved int // the speed observed with the new concurrency value
}

func (s *concurrencyTunerSuite) noMax() int {
	return math.MaxInt32
}

func (s *concurrencyTunerSuite) TestConcurrencyTuner_LowBandwidth(c *chk.C) {
	steps := []tunerStep{
		{16, concurrencyReasonInitial, 100},
		{64, concurrencyReasonSeeking, 100},
		{16, concurrencyReasonBackoff, 100},
		{25, concurrencyReasonSeeking, 100},
		{16, concurrencyReasonAtOptimum, 100},
		{16, concurrencyReasonNotActive, 100}}

	s.runTest(c, steps, s.noMax())

}

func (s *concurrencyTunerSuite) TestConcurrencyTuner_HighBandwidth(c *chk.C) {
	steps := []tunerStep{
		{16, concurrencyReasonInitial, 1000},
		{64, concurrencyReasonSeeking, 4000},
		{256, concurrencyReasonSeeking, 20000},
		{512, concurrencyReasonSeeking, 20000},
		{256, concurrencyReasonBackoff, 20000},
		{307, concurrencyReasonSeeking, 20000},
		{256, concurrencyReasonAtOptimum, 20000},
		{256, concurrencyReasonNotActive, 20000},
	}

	s.runTest(c, steps, s.noMax())

}

func (s *concurrencyTunerSuite) TestConcurrencyTuner_CapMaxConcurrency(c *chk.C) {
	steps := []tunerStep{
		{16, concurrencyReasonInitial, 1000},
		{64, concurrencyReasonSeeking, 4000},
		{100, concurrencyReasonHitMax, 8000}, // NOT "at optimum"
		{100, concurrencyReasonNotActive, 8000},
	}

	s.runTest(c, steps, 100)
}

func (s *concurrencyTunerSuite) TestConcurrencyTuner_OptimalValueNotNearStandardSteps(c *chk.C) {
	steps := []tunerStep{
		{16, concurrencyReasonInitial, 500},
		{64, concurrencyReasonSeeking, 2000},
		{256, concurrencyReasonSeeking, 8000},
		{512, concurrencyReasonSeeking, 14500},
		{1024, concurrencyReasonSeeking, 20000},
		{512, concurrencyReasonBackoff, 14500},
		{614, concurrencyReasonSeeking, 16800},
		{737, concurrencyReasonSeeking, 19500},
		{884, concurrencyReasonSeeking, 20500},
		{737, concurrencyReasonAtOptimum, 19500},
		{737, concurrencyReasonNotActive, 19500},
	}

	s.runTest(c, steps, s.noMax())

}

func (s *concurrencyTunerSuite) runTest(c *chk.C, steps []tunerStep, maxConcurrency int) {
	t := NewAutoConcurrencyTuner(16, maxConcurrency)
	observedMbps := -1 // there's no observation at first
	for _, x := range steps {
		// ask the tuner what do to
		conc, reason := t.GetRecommendedConcurrency(observedMbps)

		// assert that it told us what we expect in this test
		c.Assert(conc, chk.Equals, x.concurrency)
		c.Assert(reason, chk.Equals, x.reason)

		// get the "simulated" throughput that results from the new concurrency
		observedMbps = x.mbpsObserved
	}
}
