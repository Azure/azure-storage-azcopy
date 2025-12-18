// Copyright © 2025 Microsoft <wastore@microsoft.com>
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

package azcopy

import (
	"fmt"
	"sync/atomic"
	"time"

	"github.com/Azure/azure-storage-azcopy/v10/common"
	"github.com/Azure/azure-storage-azcopy/v10/jobsAdmin"
)

var _ jobProgressTracker = &transferProgressTracker{}

type transferProgressTracker struct {
	jobID   common.JobID
	fromTo  common.FromTo
	handler CopyHandler
	//jobType      common.JobType
	isCleanupJob bool

	// variables used to calculate progress
	// intervalStartTime holds the last time value when the progress summary was fetched
	// the value of this variable is used to calculate the throughput
	// it gets updated every time the progress summary is fetched
	intervalStartTime        time.Time
	intervalBytesTransferred uint64

	// used to calculate job summary
	jobStartTime time.Time

	atomicSkippedSymlinkCount     uint32
	atomicSkippedSpecialFileCount uint32
	atomicSkippedHardlinkCount    uint32
	atomicFirstPartOrdered        uint32
	atomicScanningStatus          uint32
}

func (tpt *transferProgressTracker) Start() {
	// initialize the times necessary to track progress
	tpt.jobStartTime = time.Now()
	tpt.intervalStartTime = time.Now()
	tpt.intervalBytesTransferred = 0

	var logPathFolder string
	if common.LogPathFolder != "" {
		logPathFolder = fmt.Sprintf("%s%s%s.log", common.LogPathFolder, common.OS_PATH_SEPARATOR, tpt.jobID)
	}
	if tpt.handler != nil {
		tpt.handler.OnStart(JobContext{JobID: tpt.jobID, LogPath: logPathFolder})
	}
}

func (tpt *transferProgressTracker) CheckProgress() (uint32, bool) {
	if tpt.firstPartOrdered() {
		// fetch a job status
		summary := jobsAdmin.GetJobSummary(tpt.jobID)
		summary.IsCleanupJob = tpt.isCleanupJob

		jobDone := summary.JobStatus.IsJobDone()
		totalKnownCount := summary.TotalTransfers

		// if json is not desired, and job is done, then we generate a special end message to conclude the job
		duration := time.Since(tpt.jobStartTime) // report the total run time of the job

		var computeThroughput = func() float64 {
			// compute the average throughput for the last time interval
			bytesInMb := float64(float64(summary.BytesOverWire-tpt.intervalBytesTransferred) / float64(Base10Mega))
			timeElapsed := time.Since(tpt.intervalStartTime).Seconds()

			// reset the interval timer and byte count
			tpt.intervalStartTime = time.Now()
			tpt.intervalBytesTransferred = summary.BytesOverWire

			return common.Iff(timeElapsed != 0, bytesInMb/timeElapsed, 0) * 8
		}
		throughput := computeThroughput()
		summary.SkippedSymlinkCount = tpt.getSkippedSymlinkCount()
		summary.SkippedSpecialFileCount = tpt.getSkippedSpecialFileCount()
		progress := CopyProgress{
			ListJobSummaryResponse: summary,
			Throughput:             throughput,
			ElapsedTime:            duration,
		}

		if common.AzcopyCurrentJobLogger != nil {
			isBenchmark := tpt.fromTo.From() == common.ELocation.Benchmark()
			common.AzcopyCurrentJobLogger.Log(common.LogInfo, GetCopyProgress(progress, isBenchmark))
		}
		if tpt.handler != nil {
			tpt.handler.OnTransferProgress(progress)
		}
		return totalKnownCount, jobDone
	} else {
		return 0, false
	}
}

func (tpt *transferProgressTracker) CompletedEnumeration() bool {
	return atomic.LoadUint32(&tpt.atomicScanningStatus) > 0
}

func (tpt *transferProgressTracker) GetJobID() common.JobID {
	return tpt.jobID
}

func (tpt *transferProgressTracker) GetElapsedTime() time.Duration {
	return time.Since(tpt.jobStartTime)
}

func newTransferProgressTracker(jobID common.JobID, handler CopyHandler, fromTo common.FromTo) *transferProgressTracker {
	return &transferProgressTracker{
		jobID:        jobID,
		handler:      handler,
		isCleanupJob: false, // TODO: when implementing benchmark, set this properly
		fromTo:       fromTo,
		//jobType:      common.EJobType.Copy(), // TODO: when implementing benchmark, set this properly
	}
}

// setFirstPartOrdered sets the value of atomicFirstPartOrdered to 1
func (tpt *transferProgressTracker) setFirstPartOrdered() {
	atomic.StoreUint32(&tpt.atomicFirstPartOrdered, 1)
}

// firstPartOrdered returns the value of atomicFirstPartOrdered.
func (tpt *transferProgressTracker) firstPartOrdered() bool {
	return atomic.LoadUint32(&tpt.atomicFirstPartOrdered) > 0
}

// setScanningComplete sets the value of atomicScanningStatus to 1.
func (tpt *transferProgressTracker) setScanningComplete() {
	atomic.StoreUint32(&tpt.atomicScanningStatus, 1)
}

func (tpt *transferProgressTracker) getSkippedSymlinkCount() uint32 {
	return atomic.LoadUint32(&tpt.atomicSkippedSymlinkCount)
}

func (tpt *transferProgressTracker) getSkippedSpecialFileCount() uint32 {
	return atomic.LoadUint32(&tpt.atomicSkippedSpecialFileCount)
}

func (tpt *transferProgressTracker) getSkippedHardlinkCount() uint32 {
	return atomic.LoadUint32(&tpt.atomicSkippedHardlinkCount)
}

func (tpt *transferProgressTracker) incEnumeration(entityType common.EntityType, symlinkOption common.SymlinkHandlingType, hardlinkHandling common.HardlinkHandlingType) {
	if tpt.fromTo.IsNFS() {
		if entityType == common.EEntityType.Other() {
			atomic.AddUint32(&tpt.atomicSkippedSpecialFileCount, 1)
		} else if entityType == common.EEntityType.Symlink() {
			switch symlinkOption {
			case common.ESymlinkHandlingType.Skip():
				atomic.AddUint32(&tpt.atomicSkippedSymlinkCount, 1)
			}
		} else if entityType == common.EEntityType.Hardlink() {
			switch hardlinkHandling {
			case common.SkipHardlinkHandlingType:
				atomic.AddUint32(&tpt.atomicSkippedHardlinkCount, 1)
			}
		}
	}
}
