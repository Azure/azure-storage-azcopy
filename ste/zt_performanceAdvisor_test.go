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
	chk "gopkg.in/check.v1"
)

type perfAdvisorSuite struct{}

var _ = chk.Suite(&perfAdvisorSuite{})

func (s *perfAdvisorSuite) TestPerfAdvisor(c *chk.C) {

	none := AdviceType{"NoneUnitTestOnly", ""}

	// abbreviated names for the various advice types, to make the test more concise
	iops := EAdviceType.AccountIOPS()
	throughput := EAdviceType.AccountThroughput()
	otherBusy := EAdviceType.ServerBusy()
	concNotEnoughTime := EAdviceType.ConcurrencyNotEnoughTime()
	concNotTuned := EAdviceType.ConcurrencyNotTuned()
	concHitMax := EAdviceType.ConcurrencyHitUpperLimit()
	netIsBottleneck := EAdviceType.NetworkIsBottleneck()
	netOK := EAdviceType.NetworkNotBottleneck()
	mbpsCapped := EAdviceType.MbpsCapped()
	netErrors := EAdviceType.NetworkErrors()

	// define test cases
	cases := []struct {
		caseName                       string
		serverBusyPercentageIOPS       float32
		serverBusyPercentageThroughput float32
		serverBusyPercentageOther      float32
		networkErrorPercentage         float32
		finalConcurrencyTunerReason    string
		capMbps                        int64 // 0 if no cap
		mbps                           int64
		expectedPrimaryResult          AdviceType
		expectedSecondary1             AdviceType
		expectedSecondary2             AdviceType
		expectedSecondary3             AdviceType
	}{
		// Each row here is a test case.  It starts with a descriptive name, then has
		// all necessary inputs to the advisor, then the expected outputs.
		// E.g:
		// {"thisIsTheCaseName", /* Begin inputs */ 0, 0, 0, 0, concurrencyReasonAtOptimum, 0, 1000, /* Begin expected outputs */ netIsBottleneck, none, none, none},
		// These initial cases test just one thing at a time (below we test some interactions)
		{"simpleBandwidth", 0, 0, 0, 0, concurrencyReasonAtOptimum, 0, 1000, netIsBottleneck, none, none, none},
		{"simpleIOPS     ", 7, 0, 0, 0, concurrencyReasonAtOptimum, 0, 1000, iops, netOK, none, none},
		{"simpleTput     ", 0, 6, 0, 0, concurrencyReasonAtOptimum, 0, 1000, throughput, netOK, none, none},
		{"otherThrottling", 0, 0, 8, 0, concurrencyReasonAtOptimum, 0, 1000, otherBusy, netOK, none, none},
		{"networkErrors  ", 0, 0, 0, 7, concurrencyReasonAtOptimum, 0, 1000, netErrors, netOK, none, none},
		{"cappedMbps     ", 0, 0, 0, 0, concurrencyReasonAtOptimum, 1000, 950, mbpsCapped, netOK, none, none},
		{"concNotTuned   ", 0, 0, 0, 0, concurrencyReasonNone, 0, 1000, concNotTuned, none, none, none},
		{"concHitLimit   ", 0, 0, 0, 0, concurrencyReasonHitMax, 0, 1000, concHitMax, none, none, none},
		{"concOutOfTime1 ", 0, 0, 0, 0, concurrencyReasonSeeking, 0, 1000, concNotEnoughTime, none, none, none},
		{"concOutOfTime2 ", 0, 0, 0, 0, concurrencyReasonBackoff, 0, 1000, concNotEnoughTime, none, none, none},
		{"concOutOfTime3 ", 0, 0, 0, 0, concurrencyReasonInitial, 0, 1000, concNotEnoughTime, none, none, none},

		// these test cases look at combinations
		{"badStatsAndCap1", 8, 7, 7, 7, concurrencyReasonAtOptimum, 1000, 999, iops, throughput, mbpsCapped, netOK}, // note no netError because we ignore those if throttled
		{"badStatsAndCap2", 8, 7, 7, 7, concurrencyReasonSeeking, 1000, 999, iops, throughput, mbpsCapped, netOK},   // netOK not concNotEnoughTime because net is not the bottleneck
		{"combinedThrottl", 2, 2, 2, 0, concurrencyReasonAtOptimum, 0, 1000, otherBusy, netOK, none, none},
	}

	// Run the tests, asserting that for each case, the given inputs produces the expected output
	for _, cs := range cases {
		a := &PerformanceAdvisor{
			networkErrorPercentage:         cs.networkErrorPercentage,
			serverBusyPercentageIOPS:       cs.serverBusyPercentageIOPS,
			serverBusyPercentageThroughput: cs.serverBusyPercentageThroughput,
			serverBusyPercentageOther:      cs.serverBusyPercentageOther,
			iops:                           789, // just informational, not used for computations
			mbps:                           cs.mbps,
			capMbps:                        cs.capMbps,
			finalConcurrencyTunerReason:    cs.finalConcurrencyTunerReason,
			finalConcurrency:               123, // just informational, not used for computations
		}
		obtained := a.GetAdvice()
		expectedCount := 1
		if cs.expectedSecondary1 != none {
			expectedCount++
		}
		if cs.expectedSecondary2 != none {
			expectedCount++
		}
		if cs.expectedSecondary3 != none {
			expectedCount++
		}
		c.Assert(len(obtained), chk.Equals, expectedCount)

		s.assertAdviceMatches(c, cs.caseName, obtained, 0, cs.expectedPrimaryResult)
		s.assertAdviceMatches(c, cs.caseName, obtained, 1, cs.expectedSecondary1)
		s.assertAdviceMatches(c, cs.caseName, obtained, 2, cs.expectedSecondary2)
		s.assertAdviceMatches(c, cs.caseName, obtained, 3, cs.expectedSecondary3)
	}
}

func (s *perfAdvisorSuite) assertAdviceMatches(c *chk.C, caseName string, obtained []common.PerformanceAdvice, index int, expected AdviceType) {
	if expected.code == "NoneUnitTestOnly" {
		return
	}
	adv := obtained[index]
	shouldBePrimary := index == 0
	c.Assert(adv.PriorityAdvice, chk.Equals, shouldBePrimary, chk.Commentf(caseName))
	c.Assert(adv.Code, chk.Equals, expected.code, chk.Commentf(caseName))
}

// TODO: for conciseness, we don't check the Title or Reason of the advice objects that are generated.
//    Should we?
