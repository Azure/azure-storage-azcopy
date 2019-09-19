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
	concCpu := EAdviceType.ConcurrencyHighCpu()
	netIsBottleneck := EAdviceType.NetworkIsBottleneck()
	netOK := EAdviceType.NetworkNotBottleneck()
	mbpsCapped := EAdviceType.MbpsCapped()
	netErrors := EAdviceType.NetworkErrors()
	vmSize := EAdviceType.VMSize()
	smallFilesOrNetwork := EAdviceType.SmallFilesOrNetwork()

	// file sizes
	const normal = 8 * 1024 * 1024
	const small = 32 * 1024

	// define test cases
	cases := []struct {
		caseName                       string
		serverBusyPercentageIOPS       float32
		serverBusyPercentageThroughput float32
		serverBusyPercentageOther      float32
		networkErrorPercentage         float32
		finalConcurrencyTunerReason    string
		avgFileSize                    int64
		capMbps                        int64 // 0 if no cap
		mbps                           int64
		azureVmCores                   int // 0 if not azure VM
		expectedPrimaryResult          AdviceType
		expectedSecondary1             AdviceType
		expectedSecondary2             AdviceType
		expectedSecondary3             AdviceType
	}{
		// Each row here is a test case.  It starts with a descriptive name, then has
		// all necessary inputs to the advisor, then the expected outputs.
		// E.g:
		// {"thisIsTheCaseName", /* Begin inputs */ 0, 0, 0, 0, concurrencyReasonAtOptimum, 0, 1000, 0, /* Begin expected outputs */ netIsBottleneck, none, none, none},
		// These initial cases test just one thing at a time (below we test some interactions)
		{"simpleBandwidth", 0, 0, 0, 0, concurrencyReasonAtOptimum, normal, 0, 100, 0, netIsBottleneck, none, none, none},
		{"concHighCpu    ", 0, 0, 0, 0, concurrencyReasonHighCpu, normal, 0, 100, 0, concCpu, none, none, none}, // only difference from netIsBottleneck is that tuner ran out of CPU
		{"simpleIOPS     ", 7, 0, 0, 0, concurrencyReasonAtOptimum, normal, 0, 1000, 0, iops, netOK, none, none},
		{"simpleTput     ", 0, 6, 0, 0, concurrencyReasonAtOptimum, normal, 0, 1000, 0, throughput, netOK, none, none},
		{"otherThrottling", 0, 0, 8, 0, concurrencyReasonAtOptimum, normal, 0, 1000, 0, otherBusy, netOK, none, none},
		{"networkErrors  ", 0, 0, 0, 7, concurrencyReasonAtOptimum, normal, 0, 1000, 0, netErrors, netOK, none, none},
		{"cappedMbps     ", 0, 0, 0, 0, concurrencyReasonAtOptimum, normal, 1000, 950, 0, mbpsCapped, netOK, none, none},
		{"concNotTuned   ", 0, 0, 0, 0, concurrencyReasonTunerDisabled, normal, 0, 1000, 0, concNotTuned, none, none, none},
		{"concHitLimit   ", 0, 0, 0, 0, concurrencyReasonHitMax, normal, 0, 1000, 0, concHitMax, none, none, none},
		{"concOutOfTime1 ", 0, 0, 0, 0, concurrencyReasonSeeking, normal, 0, 1000, 0, concNotEnoughTime, none, none, none},
		{"concOutOfTime2 ", 0, 0, 0, 0, concurrencyReasonBackoff, normal, 0, 1000, 0, concNotEnoughTime, none, none, none},
		{"concOutOfTime3 ", 0, 0, 0, 0, concurrencyReasonInitial, normal, 0, 1000, 0, concNotEnoughTime, none, none, none},
		{"notVmSize      ", 0, 0, 0, 0, concurrencyReasonAtOptimum, normal, 0, 374, 1, netIsBottleneck, none, none, none},
		{"vmSize1        ", 0, 0, 0, 0, concurrencyReasonAtOptimum, normal, 0, 376, 1, vmSize, none, none, none},
		{"vmSize2        ", 0, 0, 0, 0, concurrencyReasonAtOptimum, normal, 0, 10500, 16, vmSize, none, none, none},
		{"smallFiles     ", 0, 0, 0, 0, concurrencyReasonAtOptimum, small, 0, 10000, 0, smallFilesOrNetwork, none, none, none},

		// these test cases look at combinations
		{"badStatsAndCap1", 8, 7, 7, 7, concurrencyReasonAtOptimum, normal, 1000, 999, 0, iops, throughput, mbpsCapped, netOK}, // note no netError because we ignore those if throttled
		{"badStatsAndCap2", 8, 7, 7, 7, concurrencyReasonSeeking, normal, 1000, 999, 0, iops, throughput, mbpsCapped, netOK},   // netOK not concNotEnoughTime because net is not the bottleneck
		{"combinedThrottl", 0.5, 0.5, 0.5, 0, concurrencyReasonAtOptimum, normal, 0, 1000, 0, otherBusy, netOK, none, none},
		{"notVmSize      ", 0, 8, 0, 0, concurrencyReasonAtOptimum, normal, 0, 10500, 16, throughput, netOK, none, none},
		{"smallFilesOK   ", 0, 8, 0, 0, concurrencyReasonAtOptimum, small, 0, 10500, 0, throughput, netOK, none, none},
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
			azureVmCores:                   cs.azureVmCores,
			azureVmSizeName:                "DS1", // just informational, not used for computations
			avgBytesPerFile:                cs.avgFileSize,
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
		c.Assert(len(obtained), chk.Equals, expectedCount, chk.Commentf(cs.caseName))

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
