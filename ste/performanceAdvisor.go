// Copyright Microsoft <wastore@microsoft.com>
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
	"fmt"
	"github.com/Azure/azure-storage-azcopy/common"
)

// TODO: should this be in ste, common, or a new place of its own?  Reviewers, what do you think?

type AdviceType struct {
	code        string
	description string
}

var EAdviceType = AdviceType{"", ""}

func (AdviceType) AccountIOPS() AdviceType {
	return AdviceType{"AccountIOPS",
		"Approaching max allowed IOPS (IO Operations per second) on the target account"}
}

func (AdviceType) AccountThroughput() AdviceType {
	return AdviceType{"AccountThroughput",
		"Approaching max allowed throughput (Gbps) on the target account"}
}

func (AdviceType) ServerBusy() AdviceType {
	return AdviceType{"ServiceBusy",
		"The service reported that is was busy"} // TODO: reviewers, any suggestion on better wording for this?
}

func (AdviceType) ConcurrencyNotEnoughTime() AdviceType {
	return AdviceType{"ConcurrencyNotEnoughTime",
		"Network bandwidth not measured because the test finished too soon"}
}

func (AdviceType) ConcurrencyNotTuned() AdviceType {
	return AdviceType{"ConcurrencyNotTuned",
		"Network bandwidth not measured because concurrency tuning was disabled"}
}

func (AdviceType) ConcurrencyHitUpperLimit() AdviceType {
	return AdviceType{"ConcurrencyHitUpperLimit",
		"Network bandwidth not measured because concurrency tuning hit its upper concurrency limit"}
}

func (AdviceType) NetworkIsBottleneck() AdviceType {
	return AdviceType{"NetworkIsBottleneck",
		"Network bandwidth appears to be the key factor governing performance."}
}

func (AdviceType) NetworkNotBottleneck() AdviceType {
	return AdviceType{"NetworkNotBottleneck",
		"Performance is not limited by network bandwidth"}
}

func (AdviceType) MbpsCapped() AdviceType {
	return AdviceType{"MbpsCapped",
		"Maximum throughput was limited by a command-line parameter"}
}

func (AdviceType) NetworkErrors() AdviceType {
	return AdviceType{"NetworkErrors",
		"Network errors, such as losses of connections, may have limited throughput"}
}

type PerformanceAdvisor struct {
	networkErrorPercentage         float32
	serverBusyPercentageIOPS       float32
	serverBusyPercentageThroughput float32
	serverBusyPercentageOther      float32
	iops                           int
	mbps                           int64
	capMbps                        int64 // 0 if no cap
	finalConcurrencyTunerReason    string
	finalConcurrency               int
}

func NewPerformanceAdvisor(stats *pipelineNetworkStats, commandLineMbpsCap int64, mbps int64, finalReason string, finalConcurrency int) *PerformanceAdvisor {
	p := &PerformanceAdvisor{
		capMbps:                     commandLineMbpsCap,
		mbps:                        mbps,
		finalConcurrencyTunerReason: finalReason,
		finalConcurrency:            finalConcurrency,
	}

	if stats != nil {
		p.networkErrorPercentage = stats.NetworkErrorPercentage()
		p.serverBusyPercentageIOPS = stats.IOPSServerBusyPercentage()
		p.serverBusyPercentageThroughput = stats.ThroughputServerBusyPercentage()
		p.serverBusyPercentageOther = stats.OtherServerBusyPercentage()
		p.iops = stats.OperationsPerSecond()
		// TODO: mbps = stats.Mbps     Is this how we'll get this?  Want it to have same start time as rest of the stats...
	}

	return p
}

// GetPerfAdvice returns one or many performance advice objects, in priority order, with the highest priority advice first
func (p *PerformanceAdvisor) GetAdvice() []common.PerformanceAdvice {

	const (
		serverBusyThresholdPercent   = 5.0
		networkErrorThresholdPercent = 5.0
	)

	result := make([]common.PerformanceAdvice, 0)

	// helper functions
	alreadyHaveAdvice := func() bool {
		return len(result) > 0
	}

	addAdvice := func(tp AdviceType, reason string, reasonValues ...interface{}) {
		reasonText := fmt.Sprintf(reason, reasonValues...)
		isFirst := !alreadyHaveAdvice()
		result = append(result, common.PerformanceAdvice{
			Code:           tp.code,
			Title:          tp.description,
			Reason:         reasonText,
			PriorityAdvice: isFirst})
	}

	// Note that order matters below - the most important topics are first, and sometimes later ones will
	// suppress their output if one of the more important earlier ones has already produced advice.

	// Server throttling
	haveKnownThrottling := false
	if p.serverBusyPercentageIOPS > serverBusyThresholdPercent {
		haveKnownThrottling = true
		addAdvice(EAdviceType.AccountIOPS(),
			"Throughput was throttled by Service on %.0f%% of operations, due to IO Operations Per Second (IOPS) approaching "+
				"the limit of your target account. The average IOPS measured in this job was %d operations/sec. "+
				" (This message is shown by AzCopy if %.0f%% or more of operations are throttled due to IOPS.)",
			p.serverBusyPercentageIOPS, p.iops, serverBusyThresholdPercent)
	}
	if p.serverBusyPercentageThroughput > serverBusyThresholdPercent {
		haveKnownThrottling = true
		addAdvice(EAdviceType.AccountThroughput(),
			"Throughput was throttled by Service on %.0f%% of operations, due to throughput approaching "+
				"the limit of your target account.  The throughput measured in this job was %d Mega bits/sec. "+
				"(This message is shown by AzCopy if %.0f%% or more of operations are throttled due to throughput.)",
			p.serverBusyPercentageThroughput, p.mbps, serverBusyThresholdPercent)
	}
	if !haveKnownThrottling {
		totalThrottling := p.serverBusyPercentageIOPS + p.serverBusyPercentageIOPS + p.serverBusyPercentageOther
		if totalThrottling > serverBusyThresholdPercent {
			// TODO: reviewers: any improvement on "other reasons" in this text?
			addAdvice(EAdviceType.ServerBusy(),
				"Throughput was throttled by Service on %.0f%% of operations in total. %.0f%% where throttled due to "+
					"account IOPS limits; %.0f%% due to account throughput limits; and %.0f%% for other reasons. "+
					"(This message is shown by AzCopy if %.0f%% or more of operations are throttled in total and "+
					"no other throttling message applies.)",
				totalThrottling, p.serverBusyPercentageIOPS, p.serverBusyPercentageThroughput, p.serverBusyPercentageOther, serverBusyThresholdPercent)
		}

	}

	// Network errors
	// (Don't report these if we were throttled by service. Why? Because when service wants to throttle more aggressively,
	// it drops connections.  We don't want those connection drops, which are actually just severe throttling, to
	// be mistaken for actually network problems. So we only report things that look like network problems in cases
	// where there was no significant throttling reported)
	if !alreadyHaveAdvice() && p.networkErrorPercentage > networkErrorThresholdPercent {
		addAdvice(EAdviceType.NetworkErrors(),
			"Network errors occurred on %.0f%% of operations. "+
				"Network errors include all cases where the HTTP request could not be sent or did not receive a reply. "+
				"(This message is shown by AzCopy if %.0f%% or more of operations result in network errors.)",
			p.networkErrorPercentage, networkErrorThresholdPercent)
	}

	// Mbps cap
	const mbpsThreshold = 0.9
	if p.capMbps > 0 && float32(p.mbps) > mbpsThreshold*float32(p.capMbps) {
		addAdvice(EAdviceType.MbpsCapped(),
			"Throughput has been capped at %d Mbps with a command line parameter, and the measured throughput was "+
				"close to the cap. "+
				"(This message is shown by AzCopy if a command-line cap is set and the measured throughput is "+
				"over %.0f%% of the cap.)", p.capMbps, mbpsThreshold*100)
	}

	// Network available bandwidth
	if alreadyHaveAdvice() {
		addAdvice(EAdviceType.NetworkNotBottleneck(),
			"Throughput appears to be limited by other factor(s), so it is assumed that network bandwidth is not the bottleneck. "+
				"Throughput of %d Mega bits/sec was obtained with %d concurrent connections.",
			p.mbps, p.finalConcurrency)
	} else {
		switch p.finalConcurrencyTunerReason {
		case concurrencyReasonAtOptimum:
			addAdvice(EAdviceType.NetworkIsBottleneck(),
				"No other factors were identified that are limiting performance, so the bottleneck is assumed to be available "+
					"network bandwidth. The available network bandwidth is the portion of the installed bandwidth that "+
					"is not already used by other traffic. Throughput of %d Mega bits/sec was obtained with %d concurrent connections.",
				p.mbps, p.finalConcurrency)
		case concurrencyReasonNone:
			addAdvice(EAdviceType.ConcurrencyNotTuned(),
				"Auto-tuning of concurrency was prevented by an environment variable setting a specific concurrency value. Therefore "+
					"AzCopy cannot tune itself to find the maximum possible throughput.")
		case concurrencyReasonHitMax:
			addAdvice(EAdviceType.ConcurrencyHitUpperLimit(),
				"Auto-tuning of concurrency hit its upper limit before finding maximum throughput.  Therefore the maximum "+
					"possible throughput was not found")
		default:
			addAdvice(EAdviceType.ConcurrencyNotEnoughTime(),
				"The job completed before AzCopy could find the maximum possible throughput.  Try benchmarking with more files "+
					"or files of a larger size, to give AzCopy more time.")
		}
	}

	// TODO: consider how to factor in CPU load - will it be reflected in concurrency tuner results, or separate?

	// TODO: should we also output aka.ms links to the relevant doc pages?  Hard to maintain?

	// TODO: do we need to add a case for files that are too small for HTBB (need to tell difference between premium block block
	//   blob and normal).  Or will 503s tell us enough in those cases?

	return result
}
