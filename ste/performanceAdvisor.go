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
	"bytes"
	"fmt"
	"net/http"
	"runtime"
	"time"

	"github.com/Azure/azure-storage-azcopy/v10/common"
)

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
	return AdviceType{"ConcurrencyNotEnoughTimeToTune",
		"Network bandwidth not measured because the test finished too soon"}
}

func (AdviceType) ConcurrencyNotTuned() AdviceType {
	return AdviceType{"ConcurrencyTuningDisabled",
		"Network bandwidth not measured because concurrency tuning was disabled"}
}

func (AdviceType) ConcurrencyHitUpperLimit() AdviceType {
	return AdviceType{"ConcurrencyHitUpperLimit",
		"Network bandwidth not measured because concurrency tuning hit its upper concurrency limit"}
}

func (AdviceType) ConcurrencyHighCpu() AdviceType {
	return AdviceType{"ConcurrencyTuningHighCpu",
		"Network bandwidth may not be accurately measured because concurrency tuning encountered high CPU usage"}
}

func (AdviceType) NetworkIsBottleneck() AdviceType {
	return AdviceType{"NetworkIsBottleneck",
		"Network bandwidth appears to be the key factor governing performance."}
}

func (AdviceType) NetworkNotBottleneck() AdviceType {
	return AdviceType{"NetworkNotBottleneck",
		"Performance is not limited by network bandwidth"}
}

func (AdviceType) FileShareOrNetwork() AdviceType {
	return AdviceType{"FileShareOrNetwork",
		"Throughput may have been limited by File Share throughput limits, or by the network "}
}
func (AdviceType) MbpsCapped() AdviceType {
	return AdviceType{"MbpsCapped",
		"Maximum throughput was limited by a command-line parameter"}
}

func (AdviceType) NetworkErrors() AdviceType {
	return AdviceType{"NetworkErrors",
		"Network errors, such as losses of connections, may have limited throughput"}
}

func (AdviceType) VMSize() AdviceType {
	return AdviceType{"VMSize",
		"The size of this Azure VM may have limited throughput"}
}

func (AdviceType) SmallFilesOrNetwork() AdviceType {
	return AdviceType{"SmallFilesOrNetwork",
		"Throughput may have been limited by small file size or by the network"}
}

type PerformanceAdvisor struct {
	networkErrorPercentage         float32
	serverBusyPercentageIOPS       float32
	serverBusyPercentageThroughput float32
	serverBusyPercentageOther      float32
	iops                           int
	mbps                           int64
	capMbps                        float64 // 0 if no cap
	finalConcurrencyTunerReason    string
	finalConcurrency               int
	azureVmCores                   int // 0 if not azure VM
	azureVmSizeName                string
	direction                      common.TransferDirection
	avgBytesPerFile                int64

	// Azure files Standard does not appear to return 503's for Server Busy, so our current code can't tell the
	// difference between slow network and slow Service, when connecting to Standard Azure Files accounts,
	// so we use this flag to display a message that hedges our bets between the two possibilities.
	isToAzureFiles bool
}

func NewPerformanceAdvisor(stats *PipelineNetworkStats, commandLineMbpsCap float64, mbps int64, finalReason string, finalConcurrency int, dir common.TransferDirection, avgBytesPerFile int64, isToAzureFiles bool) *PerformanceAdvisor {
	p := &PerformanceAdvisor{
		capMbps:                     commandLineMbpsCap,
		mbps:                        mbps,
		finalConcurrencyTunerReason: finalReason,
		finalConcurrency:            finalConcurrency,
		direction:                   dir,
		avgBytesPerFile:             avgBytesPerFile,
		isToAzureFiles:              isToAzureFiles,
	}

	p.azureVmSizeName = p.getAzureVmSize()
	if p.azureVmSizeName != "" {
		p.azureVmCores = runtime.NumCPU()
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
		serverBusyThresholdPercent   = 1.0
		networkErrorThresholdPercent = 2.0

		// we don't have any API to get exact throughput given a VM size. But a quick look at the documentation
		// suggests that all current gen VMs get around 500 to 1000 Mbps per core.
		// To allow some wriggle room (VM not quite hitting cap, but still affected by that cap) we choose a value a bit less than the min.
		// Then, if a VM is getting LESS than this amount per core, we infer that it's not at its throughput cap.
		// If it's getting more than this amount AND there are no other constraints, then we assume the constraint IS
		// the throughput cap. (It's probably not the network, because if we use this we already know its an Azure VM).
		expectedMinAzureMbpsPerCore = 375

		// files this size, and smaller, won't trigger the HTBB path on standard storage accounts
		htbbThresholdMB = 4
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
			"Throughput has been capped at %f Mbps with a command line parameter, and the measured throughput was "+
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

			isAzureVM := p.azureVmCores > 0
			definitelyBelowAzureVMLimit := p.mbps < int64(p.azureVmCores*expectedMinAzureMbpsPerCore)
			if isAzureVM &&
				p.direction != common.ETransferDirection.S2SCopy() && // VM limits are not relevant in S2S copy
				!definitelyBelowAzureVMLimit {
				// Azure VM size
				// We're not "definitely" below the VM limit, so we _might_ be at it.
				// Can't get any more accurate than "might" without specific throughput limit for the VM we're running in,
				// and we don't have an API for that
				addAdvice(EAdviceType.VMSize(),
					"Throughput may be limited by the size of the Azure VM that's running AzCopy. Check the documented expected "+
						"network bandwidth for this VM size (%s).  If the observed throughput of %d Mbits/sec is close to the documented maximum for the "+
						"VM, then upsize to a larger VM. But if the observed throughput is NOT close to the maximum, and your VM is not "+
						"in the same region as your target account, consider network bandwidth as a possible bottleneck.  (AzCopy displays this "+
						"message on Azure VMs when throughput per core is greater than %d Mbps and no other limiting factors were identified.)",
					p.azureVmSizeName, p.mbps, expectedMinAzureMbpsPerCore)
				// TODO: can we detect if we're in the same region?  And can we do any better than that, because in many
				//   (virtually all?) cases even being in different regions is fine.
			} else {
				// not limited by VM size, so must be file size, network or Azure Files Standard Share
				if p.isToAzureFiles {
					// give output that hedges our bets between network and File Share, because we can't tell which is limiting perf
					addAdvice(EAdviceType.FileShareOrNetwork(),
						"No other factors were identified that are limiting performance, so the bottleneck is assumed to be either "+
							"the throughput of the Azure File Share OR the available network bandwidth. To test whether the File Share or the network is "+
							"the bottleneck, try running a benchmark to Blob Storage over the same network. If that is much faster, then the bottleneck in this "+
							"run was probably the File Share. Check the published Azure File Share throughput targets for more info. In this run throughput "+
							"of %d Mega bits/sec was obtained with %d concurrent connections.",
						p.mbps, p.finalConcurrency)
				} else if p.avgBytesPerFile <= (htbbThresholdMB * 1024 * 1024) {
					addAdvice(EAdviceType.SmallFilesOrNetwork(),
						"The files in this test are relatively small. In such cases AzCopy cannot tell whether performance was limited by "+
							"your network, or by the additional processing overheads associated with small files. To check, run another benchmark using "+
							"files at least 32 MB in size. That will provide a good test of your network speed, unaffected by file size. Then compare that speed "+
							"to the speed measured just now, with the small files.  If throughput is lower in the small-file case, that means file size "+
							"is affecting performance.  (AzCopy shows this message when files are less than or equal to %d MiB in size, and no other performance "+
							"constraints were identified). In this test, throughput of %d Mega bits/sec was obtained with %d concurrent connections.",
						htbbThresholdMB, p.mbps, p.finalConcurrency)
				} else {
					addAdvice(EAdviceType.NetworkIsBottleneck(),
						"No other factors were identified that are limiting performance, so the bottleneck is assumed to be available "+
							"network bandwidth. The available network bandwidth is the portion of the installed bandwidth that "+
							"is not already used by other traffic. Throughput of %d Mega bits/sec was obtained with %d concurrent connections.",
						p.mbps, p.finalConcurrency)
				}
			}

		case concurrencyReasonHighCpu:
			addAdvice(EAdviceType.ConcurrencyHighCpu(),
				"When auto-tuning concurrency, AzCopy experienced high CPU usage so "+
					"AzCopy might not have reached the full capacity of your network. Consider trying a machine or VM with more CPU power. "+
					"(This is an experimental feature so, if you believe AzCopy was mistaken and CPU usage was actually fine, you can turn "+
					"off this message by setting the environment variable %s to false.)", common.EEnvironmentVariable.AutoTuneToCpu().Name)
			// TODO: review whether we still need the environment variable, and adjust this message if we remove it
		case ConcurrencyReasonTunerDisabled:
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
					"to give AzCopy more time. To do so, use the --%s command line parameter. Its default is value %d, so try "+
					"benchmarking with it set to at least %d", common.FileCountParam, common.FileCountDefault,
				common.FileCountDefault*20) // * 20 because default is calibrated for 1 Gbps, and 20 Gbps is a common max storage account speed
		}
	}

	// TODO: consider how to factor in CPU load - will it be reflected in concurrency tuner results, or separate?

	// TODO: should we also output aka.ms links to the relevant doc pages?  Hard to maintain?

	// TODO: do we need to add a case for files that are too small for HTBB (need to tell difference between premium block block
	//   blob and normal).  Or will 503s tell us enough in those cases?

	return result
}

// the Azure Instance Metadata Service lives at a non-routable IP address.
// If we get a reply from it, and the reply looks reasonable, we know we are in Azure.
// No need for proxy settings here, because its a non-routable IP address visible only to Azure VMs.
// And no need to get a signed response from metadata/attested since this is not security-sensitive.
func (p *PerformanceAdvisor) getAzureVmSize() string {
	client := &http.Client{
		Timeout: time.Second * 3, // no point in waiting too long, since when it works, it will be almost instant
	}

	req, err := http.NewRequest("GET", "http://169.254.169.254/metadata/instance/compute/vmSize?api-version=2019-03-11&format=text", nil)
	if err != nil {
		return ""
	}
	req.Header.Add("Metadata", "true")

	resp, err := client.Do(req)
	if err != nil {
		return ""
	}
	defer func() { _ = resp.Body.Close() }()

	buf := bytes.Buffer{}
	n, err := buf.ReadFrom(resp.Body)
	if n == 0 || err != nil {
		return ""
	}

	return buf.String()
}
