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
	"log"
	"runtime"
	"strconv"

	"github.com/Azure/azure-storage-azcopy/v10/common"
)

// ConfiguredInt is an integer which may be optionally configured by user through an environment variable
type ConfiguredInt struct {
	Value             int
	IsUserSpecified   bool
	EnvVarName        string
	DefaultSourceDesc string
}

func (i *ConfiguredInt) GetDescription() string {
	if i.IsUserSpecified {
		return fmt.Sprintf("Based on %s environment variable", i.EnvVarName)
	} else {
		return fmt.Sprintf("Based on %s. Set %s environment variable to override", i.DefaultSourceDesc, i.EnvVarName)
	}
}

// tryNewConfiguredInt populates a ConfiguredInt from an environment variable, or returns nil if env var is not set
func tryNewConfiguredInt(envVar common.EnvironmentVariable) *ConfiguredInt {
	override := common.GetLifecycleMgr().GetEnvironmentVariable(envVar)
	if override != "" {
		val, err := strconv.ParseInt(override, 10, 64)
		if err != nil {
			log.Fatalf("error parsing the env %s %q failed with error %v",
				envVar.Name, override, err)
		}
		return &ConfiguredInt{int(val), true, envVar.Name, ""}
	}
	return nil
}

// ConfiguredBool is a boolean which may be optionally configured by user through an environment variable
type ConfiguredBool struct {
	Value             bool
	IsUserSpecified   bool
	EnvVarName        string
	DefaultSourceDesc string
}

func (b *ConfiguredBool) GetDescription() string {
	if b.IsUserSpecified {
		return fmt.Sprintf("Based on %s environment variable", b.EnvVarName)
	} else {
		return fmt.Sprintf("Based on %s. Set %s environment variable to true or false override", b.DefaultSourceDesc, b.EnvVarName)
	}
}

// tryNewConfiguredBool populates a ConfiguredInt from an environment variable, or returns nil if env var is not set
func tryNewConfiguredBool(envVar common.EnvironmentVariable) *ConfiguredBool {
	override := common.GetLifecycleMgr().GetEnvironmentVariable(envVar)
	if override != "" {
		val, err := strconv.ParseBool(override)
		if err != nil {
			log.Fatalf("error parsing the env %s %q failed with error %v",
				envVar.Name, override, err)
		}
		return &ConfiguredBool{bool(val), true, envVar.Name, ""}
	}
	return nil
}

// ConcurrencySettings stores the set of related numbers that govern concurrency levels in the STE
type ConcurrencySettings struct {

	// InitialMainPoolSize is the initial size of the main goroutine pool that transfers the data
	// (i.e. executes chunkfuncs)
	InitialMainPoolSize int

	// MaxMainPoolSize is a number >= InitialMainPoolSize, representing max size we will grow the main pool to
	MaxMainPoolSize *ConfiguredInt

	// TransferInitiationPoolSize is the size of the auxiliary goroutine pool that initiates transfers
	// (i.e. creates chunkfuncs)
	TransferInitiationPoolSize *ConfiguredInt

	// EnumerationPoolSize is size of auxiliary goroutine pool used in enumerators (only some of which are in fact parallelized)
	EnumerationPoolSize *ConfiguredInt

	// ParallelStatFiles says whether file.Stat calls should be parallelized during enumeration. May help enumeration performance
	// on Linux, but is not necessary and should not be activate on Windows.
	ParallelStatFiles *ConfiguredBool

	// MaxIdleConnections is the max number of idle TCP connections to keep open
	MaxIdleConnections int

	// MaxOpenFiles is the max number of file handles that we should have open at any time
	// Currently (July 2019) this is only used for downloads, which is where we wouldn't
	// otherwise have strict control of the number of open files.
	// For uploads, the number of open files is effectively controlled by
	// TransferInitiationPoolSize, since all the file IO (except retries) happens in
	// transfer initiation.
	MaxOpenDownloadFiles int
	// TODO: consider whether we should also use this (renamed to( MaxOpenFiles) for uploads, somehow (see command above). Is there any actual value in that? Maybe only highly handle-constrained Linux environments?

	// CheckCpuWhenTuning determines whether CPU usage should be taken into account when auto-tuning
	CheckCpuWhenTuning *ConfiguredBool
}

// AutoTuneMainPool says whether the main pool size should by dynamically tuned
func (c ConcurrencySettings) AutoTuneMainPool() bool {
	return c.MaxMainPoolSize.Value > c.InitialMainPoolSize
}

const defaultTransferInitiationPoolSize = 64
const defaultEnumerationPoolSize = 16
const concurrentFilesFloor = 32

// NewConcurrencySettings gets concurrency settings by referring to the
// environment variable AZCOPY_CONCURRENCY_VALUE (if set) and to properties of the
// machine where we are running
func NewConcurrencySettings(maxFileAndSocketHandles int, requestAutoTuneGRs bool) ConcurrencySettings {

	initialMainPoolSize, maxMainPoolSize := getMainPoolSize(runtime.NumCPU(), requestAutoTuneGRs)

	s := ConcurrencySettings{
		InitialMainPoolSize:        initialMainPoolSize,
		MaxMainPoolSize:            maxMainPoolSize,
		TransferInitiationPoolSize: getTransferInitiationPoolSize(),
		EnumerationPoolSize:        GetEnumerationPoolSize(),
		ParallelStatFiles:          GetParallelStatFiles(),
		CheckCpuWhenTuning:         getCheckCpuUsageWhenTuning(),
	}

	s.MaxOpenDownloadFiles = getMaxOpenPayloadFiles(maxFileAndSocketHandles,
		maxMainPoolSize.Value+s.TransferInitiationPoolSize.Value+s.EnumerationPoolSize.Value)

	// Set the max idle connections that we allow. If there are any more idle connections
	// than this, they will be closed, and then will result in creation of new connections
	// later if needed. In AzCopy, they almost always will be needed soon after, so better to
	// keep them open.
	// So set this number high so that that will not happen.
	// (Previously, when using Dial instead of DialContext, there was an added benefit of keeping
	// this value high, which was that, without it being high, all the extra dials,
	// to compensate for the closures, were causing a pathological situation
	// where lots and lots of OS threads get created during the creation of new connections
	// (presumably due to some blocking OS call in dial) and the app hits Go's default
	// limit of 10,000 OS threads, and panics and shuts down.  This has been observed
	// on Windows when this value was set to 500 but there were 1000 to 2000 goroutines in the
	// main pool size.  Using DialContext appears to mitigate that issue, so the value
	// we compute here is really just to reduce unneeded make and break of connections)
	s.MaxIdleConnections = maxMainPoolSize.Value

	return s
}

func getMainPoolSize(numOfCPUs int, requestAutoTune bool) (initial int, max *ConfiguredInt) {

	envVar := common.EEnvironmentVariable.ConcurrencyValue()

	if common.GetLifecycleMgr().GetEnvironmentVariable(envVar) == "AUTO" {
		// Allow user to force auto-tuning from the env var, even when not in benchmark mode
		// Might be handy in some S2S cases, where we know that release 10.2.1 was using too few goroutines
		// This feature will probably remain undocumented for at least one release cycle, while we consider
		// whether to do more in this regard (e.g. make it the default behaviour)
		requestAutoTune = true
	} else if c := tryNewConfiguredInt(envVar); c != nil {
		if requestAutoTune {
			// Tell user that we can't actually auto tune, because configured value takes precedence
			// This case happens when benchmarking with a fixed value from the env var
			common.GetLifecycleMgr().Info(fmt.Sprintf("Cannot auto-tune concurrency because it is fixed by environment variable %s", envVar.Name))
		}
		return c.Value, c // initial and max are same, fixed to the env var
	}

	var initialValue int

	if requestAutoTune {
		initialValue = 4 // deliberately start with a small initial value if we are auto-tuning.  If it's not small enough, then the auto tuning becomes
		// sluggish since, every time it needs to tune downwards, it needs to let a lot of data (num connections * block size) get transmitted,
		// and that is slow over very small links, e.g. 10 Mbps, and produces noticeable time lag when downsizing the connection count.
		// So we start small. (The alternatives, of using small chunk sizes or small file sizes just for the first 200 MB or so, were too hard to orchestrate within the existing app architecture)
	} else if numOfCPUs <= 4 {
		// fix the concurrency value for smaller machines
		initialValue = 32
	} else if 16*numOfCPUs > 300 {
		// for machines that are extremely powerful, fix to 300 (previously this was to avoid running out of file descriptors, but we have another solution to that now)
		initialValue = 300
	} else {
		// for moderately powerful machines, compute a reasonable number
		initialValue = 16 * numOfCPUs
	}

	reason := "number of CPUs"
	maxValue := initialValue
	if requestAutoTune {
		reason = "auto-tuning limit"
		maxValue = 3000 // TODO: what should this be?  Testing indicates that this value is all we're ever likely to need, even in small-files cases
	}

	return initialValue, &ConfiguredInt{maxValue, false, envVar.Name, reason}
}

func getTransferInitiationPoolSize() *ConfiguredInt {
	envVar := common.EEnvironmentVariable.TransferInitiationPoolSize()

	if c := tryNewConfiguredInt(envVar); c != nil {
		return c
	}

	return &ConfiguredInt{defaultTransferInitiationPoolSize, false, envVar.Name, "hard-coded default"}
}

func GetEnumerationPoolSize() *ConfiguredInt {
	envVar := common.EEnvironmentVariable.EnumerationPoolSize()

	if c := tryNewConfiguredInt(envVar); c != nil {
		return c
	}

	return &ConfiguredInt{defaultEnumerationPoolSize, false, envVar.Name, "hard-coded default"}

}

func GetParallelStatFiles() *ConfiguredBool {
	envVar := common.EEnvironmentVariable.ParallelStatFiles()
	if c := tryNewConfiguredBool(envVar); c != nil {
		return c
	}

	return &ConfiguredBool{false, false, envVar.Name, "hard-coded default"}
}

func getCheckCpuUsageWhenTuning() *ConfiguredBool {
	envVar := common.EEnvironmentVariable.AutoTuneToCpu()
	if c := tryNewConfiguredBool(envVar); c != nil {
		return c
	}

	return &ConfiguredBool{true, false, envVar.Name, "hard-coded default"}
}

// getMaxOpenFiles finds a number of concurrently-openable files
// such that we'll have enough handles left, after using some as network handles.
// This is important on Unix, where total handles can be constrained.
func getMaxOpenPayloadFiles(maxFileAndSocketHandles int, concurrentConnections int) int {

	// The value we return from this routine here only governs payload files. It does not govern plan
	// files that azcopy opens as part of its own operations.  So we make a reasonable allowance for
	// how many of those may be opened
	const fileHandleAllowanceForPlanFiles = 300 // 300 plan files = 300 * common.NumOfFilesPerDispatchJobPart = 3million in total

	// make a conservative estimate of total network and file handles known so far
	estimateOfKnownHandles := int(float32(concurrentConnections)*1.1) + fileHandleAllowanceForPlanFiles

	// see what we've got left over for open files
	concurrentFilesLimit := maxFileAndSocketHandles - estimateOfKnownHandles

	// If we get a negative or ridiculously low value, bring it up to some kind of sensible floor
	// (and take our chances of running out of total handles - which is effectively a bet that
	// we were too conservative earlier)
	if concurrentFilesLimit < concurrentFilesFloor {
		concurrentFilesLimit = concurrentFilesFloor
	}
	return concurrentFilesLimit

}
