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
	"github.com/Azure/azure-storage-azcopy/v10/common/buildmode"
	"github.com/Azure/azure-storage-azcopy/v10/jobsAdmin"
)

var _ jobProgressTracker = &transferProgressTracker{}

type CopyEnumerationStats struct {
	FilesScanned            uint64
	FoldersScanned          uint64
	FileEnumerationFailed   uint64
	FolderEnumerationFailed uint64
	SkippedArchiveFileCount uint64
	SkippedSymlinkCount     uint32
	SkippedSpecialFileCount uint32
	SkippedHardlinkCount    uint32
	FirstPartOrdered        bool
	ScanningComplete        bool
}

// CopyEnumerationStatsHandler is optional so existing CopyHandler implementations remain compatible.
type CopyEnumerationStatsHandler interface {
	OnEnumerationStats(CopyEnumerationStats)
}

type transferProgressTracker struct {
	atomicSkippedArchiveFileCount atomic.Uint64
	atomicFilesScanned            atomic.Uint64
	atomicFoldersScanned          atomic.Uint64
	atomicFileEnumerationFailed   atomic.Uint64
	atomicFolderEnumerationFailed atomic.Uint64
	jobID                         common.JobID
	fromTo                        common.FromTo
	handler                       CopyHandler
	logger                        common.ILogger
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
	if handler, ok := tpt.handler.(CopyEnumerationStatsHandler); ok {
		handler.OnEnumerationStats(tpt.GetEnumerationStats())
	}

	if tpt.firstPartOrdered() {
		// fetch a job status
		summary := jobsAdmin.GetJobSummary(tpt.jobID, !buildmode.IsMover)
		summary.IsCleanupJob = tpt.isCleanupJob

		jobDone := summary.JobStatus.IsJobDone()
		totalKnownCount := summary.TotalTransfers

		// if json is not desired, and job is done, then we generate a special end message to conclude the job
		duration := time.Since(tpt.jobStartTime) // report the total run time of the job

		var computeThroughput = func() float64 {
			// compute the average throughput for the last time interval
			bytesTransferred := uint64(0)
			if summary.BytesOverWire >= tpt.intervalBytesTransferred {
				bytesTransferred = summary.BytesOverWire - tpt.intervalBytesTransferred
			}
			bytesInMb := float64(bytesTransferred) / float64(Base10Mega)
			timeElapsed := time.Since(tpt.intervalStartTime).Seconds()

			// reset the interval timer and byte count
			tpt.intervalStartTime = time.Now()
			tpt.intervalBytesTransferred = summary.BytesOverWire

			if timeElapsed <= 0 {
				return 0
			}
			return bytesInMb / timeElapsed * 8
		}
		throughput := computeThroughput()
		summary.SkippedSymlinkCount = tpt.getSkippedSymlinkCount()
		summary.SkippedSpecialFileCount = tpt.getSkippedSpecialFileCount()
		summary.SkippedHardlinkCount = tpt.getSkippedHardlinkCount()
		summary.SkippedArchiveFileCount = tpt.getSkippedArchiveFileCount()
		progress := CopyProgress{
			ListJobSummaryResponse: summary,
			Throughput:             throughput,
			ElapsedTime:            duration,
		}

		isBenchmark := tpt.fromTo.From() == common.ELocation.Benchmark()
		logJobProgress(tpt.jobID, tpt.logger, GetCopyProgress(progress, isBenchmark))
		if tpt.handler != nil {
			tpt.handler.OnTransferProgress(progress)
		}
		return totalKnownCount, jobDone
	} else {
		return 0, tpt.CompletedEnumeration()
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

func newTransferProgressTracker(jobID common.JobID, handler CopyHandler, fromTo common.FromTo, loggers ...common.ILogger) *transferProgressTracker {
	var logger common.ILogger
	if len(loggers) > 0 {
		logger = loggers[0]
	}
	return &transferProgressTracker{
		jobID:        jobID,
		handler:      handler,
		isCleanupJob: false, // TODO: when implementing benchmark, set this properly
		fromTo:       fromTo,
		logger:       logger,
		//jobType:      common.EJobType.Copy(), // TODO: when implementing benchmark, set this properly
	}
}

func logJobProgress(jobID common.JobID, logger common.ILogger, message string) {
	if logger != nil {
		logger.Log(common.LogInfo, message)
		return
	}
	if jobsAdmin.JobsAdmin != nil {
		if manager, ok := jobsAdmin.JobsAdmin.JobMgr(jobID); ok {
			manager.Log(common.LogInfo, message)
		}
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

func (tpt *transferProgressTracker) incSkippedArchive() {
	tpt.atomicSkippedArchiveFileCount.Add(1)
}

func (tpt *transferProgressTracker) getSkippedArchiveFileCount() uint64 {
	return tpt.atomicSkippedArchiveFileCount.Load()
}

func (tpt *transferProgressTracker) GetEnumerationStats() CopyEnumerationStats {
	return CopyEnumerationStats{
		FilesScanned:            tpt.atomicFilesScanned.Load(),
		FoldersScanned:          tpt.atomicFoldersScanned.Load(),
		FileEnumerationFailed:   tpt.atomicFileEnumerationFailed.Load(),
		FolderEnumerationFailed: tpt.atomicFolderEnumerationFailed.Load(),
		SkippedArchiveFileCount: tpt.getSkippedArchiveFileCount(),
		SkippedSymlinkCount:     tpt.getSkippedSymlinkCount(),
		SkippedSpecialFileCount: tpt.getSkippedSpecialFileCount(),
		SkippedHardlinkCount:    tpt.getSkippedHardlinkCount(),
		FirstPartOrdered:        tpt.firstPartOrdered(),
		ScanningComplete:        tpt.CompletedEnumeration(),
	}
}

// incEnumerationFailure mirrors the sync tracker: a failed entity still counts as scanned.
func (tpt *transferProgressTracker) incEnumerationFailure(entityType common.EntityType) {
	if entityType == common.EEntityType.Folder() {
		tpt.atomicFolderEnumerationFailed.Add(1)
		tpt.atomicFoldersScanned.Add(1)
	} else {
		tpt.atomicFileEnumerationFailed.Add(1)
		tpt.atomicFilesScanned.Add(1)
	}
}

func (tpt *transferProgressTracker) incEnumeration(entityType common.EntityType, symlinkOption common.SymlinkHandlingType, hardlinkHandling common.HardlinkHandlingType) {
	switch entityType {
	case common.EEntityType.File(), common.EEntityType.Hardlink(), common.EEntityType.Symlink():
		tpt.atomicFilesScanned.Add(1)
	case common.EEntityType.Folder():
		tpt.atomicFoldersScanned.Add(1)
	case common.EEntityType.Other():
		if tpt.fromTo.IsNFS() || tpt.fromTo.From() == common.ELocation.S3() {
			tpt.atomicFilesScanned.Add(1)
		}
	}

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
