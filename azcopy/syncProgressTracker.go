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

var _ jobProgressTracker = &syncProgressTracker{}

type syncProgressTracker struct {
	// NOTE: for the 64 bit atomic functions to work on a 32 bit system, we have to guarantee the right 64-bit alignment
	// so the 64 bit integers are placed first in the struct to avoid future breaks
	// refer to: https://golang.org/pkg/sync/atomic/#pkg-note-BUG
	// incremented by traversers
	atomicSourceFilesScanned      uint64
	atomicDestinationFilesScanned uint64
	atomicScanningStatus          uint32
	atomicFirstPartOrdered        uint32

	atomicSkippedSymlinkCount     uint32
	atomicSkippedSpecialFileCount uint32
	atomicDeletionCount           uint32
	atomicSkippedHardlinkCount    uint32

	// variables used to calculate progress
	// intervalStartTime holds the last time value when the progress summary was fetched
	// the value of this variable is used to calculate the throughput
	// it gets updated every time the progress summary is fetched
	intervalStartTime        time.Time
	intervalBytesTransferred uint64

	// used to calculate job summary
	jobStartTime time.Time

	jobID   common.JobID
	handler SyncHandler
}

func newSyncProgressTracker(jobID common.JobID, handler SyncHandler) *syncProgressTracker {
	return &syncProgressTracker{
		jobID:   jobID,
		handler: handler,
	}
}

func (spt *syncProgressTracker) Start() {
	// initialize the times necessary to track progress
	spt.jobStartTime = time.Now()
	spt.intervalStartTime = time.Now()
	spt.intervalBytesTransferred = 0

	var logPathFolder string
	if common.LogPathFolder != "" {
		logPathFolder = fmt.Sprintf("%s%s%s.log", common.LogPathFolder, common.OS_PATH_SEPARATOR, spt.jobID)
	}
	if spt.handler != nil {
		spt.handler.OnStart(JobContext{JobID: spt.jobID, LogPath: logPathFolder})
	}
}

func (s *syncProgressTracker) CheckProgress() (uint32, bool) {
	duration := time.Since(s.jobStartTime)
	var summary common.ListJobSummaryResponse
	var jobDone bool
	var totalKnownCount uint32
	var throughput float64

	// transfers have begun, so we can start computing throughput
	if s.firstPartOrdered() {
		summary = jobsAdmin.GetJobSummary(s.jobID)
		jobDone = summary.JobStatus.IsJobDone()
		totalKnownCount = summary.TotalTransfers

		var computeThroughput = func() float64 {
			// compute the average throughput for the last time interval
			bytesInMb := float64(float64(summary.BytesOverWire-s.intervalBytesTransferred) / float64(Base10Mega))
			timeElapsed := time.Since(s.intervalStartTime).Seconds()

			// reset the interval timer and byte count
			s.intervalStartTime = time.Now()
			s.intervalBytesTransferred = summary.BytesOverWire

			return common.Iff(timeElapsed != 0, bytesInMb/timeElapsed, 0) * 8
		}
		throughput = computeThroughput()
	}

	if !s.CompletedEnumeration() {
		// if the scanning is not complete, report scanning progress to the user
		scanProgress := SyncScanProgress{
			SourceFilesScanned:      s.getSourceFilesScanned(),
			DestinationFilesScanned: s.getDestinationFilesScanned(),
			Throughput:              common.Iff(s.firstPartOrdered(), &throughput, nil),
			JobID:                   s.jobID,
		}
		if s.handler != nil {
			s.handler.OnScanProgress(scanProgress)
		}
		return totalKnownCount, false
	} else {
		progress := SyncProgress{
			ListJobSummaryResponse:   summary,
			DeleteTotalTransfers:     s.getDeletionCount(),
			DeleteTransfersCompleted: s.getDeletionCount(),
			Throughput:               throughput,
			ElapsedTime:              duration,
		}
		if common.AzcopyCurrentJobLogger != nil {
			common.AzcopyCurrentJobLogger.Log(common.LogInfo, GetSyncProgress(progress))
		}
		if s.handler != nil {
			s.handler.OnTransferProgress(progress)
		}
		return totalKnownCount, jobDone
	}
}

func (spt *syncProgressTracker) CompletedEnumeration() bool {
	return atomic.LoadUint32(&spt.atomicScanningStatus) > 0
}

func (spt *syncProgressTracker) GetJobID() common.JobID {
	return spt.jobID
}

func (spt *syncProgressTracker) GetElapsedTime() time.Duration {
	return time.Since(spt.jobStartTime)
}

func (spt *syncProgressTracker) incSourceEnumeration(entityType common.EntityType, symlinkOption common.SymlinkHandlingType, hardlinkHandling common.HardlinkHandlingType) {
	if entityType == common.EEntityType.File() {
		atomic.AddUint64(&spt.atomicSourceFilesScanned, 1)
	} else if entityType == common.EEntityType.Other() {
		atomic.AddUint32(&spt.atomicSkippedSpecialFileCount, 1)
	} else if entityType == common.EEntityType.Symlink() {
		switch symlinkOption {
		case common.ESymlinkHandlingType.Skip():
			atomic.AddUint32(&spt.atomicSkippedSymlinkCount, 1)
		}
	} else if entityType == common.EEntityType.Hardlink() {
		switch hardlinkHandling {
		case common.SkipHardlinkHandlingType:
			atomic.AddUint32(&spt.atomicSkippedHardlinkCount, 1)
		}
	}
}

func (spt *syncProgressTracker) getSourceFilesScanned() uint64 {
	return atomic.LoadUint64(&spt.atomicSourceFilesScanned)
}

func (spt *syncProgressTracker) getDestinationFilesScanned() uint64 {
	return atomic.LoadUint64(&spt.atomicDestinationFilesScanned)
}

func (spt *syncProgressTracker) incDestEnumeration(entityType common.EntityType, _ common.SymlinkHandlingType, _ common.HardlinkHandlingType) {
	if entityType == common.EEntityType.File() {
		atomic.AddUint64(&spt.atomicDestinationFilesScanned, 1)
	}
}

func (spt *syncProgressTracker) incrementDeletionCount() {
	atomic.AddUint32(&spt.atomicDeletionCount, 1)
}

func (spt *syncProgressTracker) getDeletionCount() uint32 {
	return atomic.LoadUint32(&spt.atomicDeletionCount)
}

// setFirstPartOrdered sets the value of atomicFirstPartOrdered to 1
func (spt *syncProgressTracker) setFirstPartOrdered() {
	atomic.StoreUint32(&spt.atomicFirstPartOrdered, 1)
}

// firstPartOrdered returns the value of atomicFirstPartOrdered.
func (spt *syncProgressTracker) firstPartOrdered() bool {
	return atomic.LoadUint32(&spt.atomicFirstPartOrdered) > 0
}

// setScanningComplete sets the value of atomicScanningStatus to 1.
func (spt *syncProgressTracker) setScanningComplete() {
	atomic.StoreUint32(&spt.atomicScanningStatus, 1)
}

func (spt *syncProgressTracker) getSkippedSymlinkCount() uint32 {
	return atomic.LoadUint32(&spt.atomicSkippedSymlinkCount)
}

func (spt *syncProgressTracker) getSkippedSpecialFileCount() uint32 {
	return atomic.LoadUint32(&spt.atomicSkippedSpecialFileCount)
}

func (spt *syncProgressTracker) getSkippedHardlinkCount() uint32 {
	return atomic.LoadUint32(&spt.atomicSkippedHardlinkCount)
}
