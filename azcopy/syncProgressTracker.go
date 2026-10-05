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

var _ jobProgressTracker = &syncProgressTracker{}

type SyncEnumerationStats struct {
	SourceFilesScanned                  uint64
	DestinationFilesScanned             uint64
	SourceFoldersScanned                uint64
	DestinationFoldersScanned           uint64
	SourceFilesTransferNotRequired      uint64
	SourceFoldersTransferNotRequired    uint64
	SourceFileEnumerationFailed         uint64
	SourceFolderEnumerationFailed       uint64
	DestinationFolderEnumerationFailed  uint64
	DestinationFolderEnumerationSkipped uint64
	SkippedArchiveFileCount             uint64
	FirstPartOrdered                    bool
	ScanningComplete                    bool
	DeletionCount                       uint32
	SkippedSymlinkCount                 uint32
	SkippedSpecialFileCount             uint32
	SkippedHardlinkCount                uint32
}

// SyncEnumerationStatsHandler is optional so existing SyncHandler implementations remain compatible.
type SyncEnumerationStatsHandler interface {
	OnEnumerationStats(SyncEnumerationStats)
}

type syncProgressTracker struct {
	// NOTE: for the 64 bit atomic functions to work on a 32 bit system, we have to guarantee the right 64-bit alignment
	// so the 64 bit integers are placed first in the struct to avoid future breaks
	// refer to: https://golang.org/pkg/sync/atomic/#pkg-note-BUG
	// incremented by traversers
	atomicSourceFilesScanned                  uint64
	atomicDestinationFilesScanned             uint64
	atomicSourceFoldersScanned                uint64
	atomicDestinationFoldersScanned           uint64
	atomicSourceFilesTransferNotRequired      uint64
	atomicSourceFoldersTransferNotRequired    uint64
	atomicSourceFileEnumerationFailed         uint64
	atomicSourceFolderEnumerationFailed       uint64
	atomicDestinationFolderEnumerationFailed  uint64
	atomicDestinationFolderEnumerationSkipped uint64
	atomicSkippedArchiveFileCount             uint64
	atomicScanningStatus                      uint32
	atomicFirstPartOrdered                    uint32

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
	fromTo  common.FromTo
	logger  common.ILogger
}

func newSyncProgressTracker(jobID common.JobID, handler SyncHandler, fromTo ...common.FromTo) *syncProgressTracker {
	var direction common.FromTo
	if len(fromTo) > 0 {
		direction = fromTo[0]
	}
	return &syncProgressTracker{
		jobID:   jobID,
		handler: handler,
		fromTo:  direction,
	}
}

// setFromTo configures source classification before enumeration or reporting starts.
func (spt *syncProgressTracker) setFromTo(fromTo common.FromTo) {
	spt.fromTo = fromTo
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
	snapshot := s.GetEnumerationStats()
	var summary common.ListJobSummaryResponse
	var jobDone bool
	var totalKnownCount uint32
	var throughput float64

	// transfers have begun, so we can start computing throughput
	if snapshot.FirstPartOrdered {
		summary = jobsAdmin.GetJobSummary(s.jobID, !buildmode.IsMover)
		jobDone = summary.JobStatus.IsJobDone()
		totalKnownCount = summary.TotalTransfers

		var computeThroughput = func() float64 {
			// compute the average throughput for the last time interval
			bytesTransferred := uint64(0)
			if summary.BytesOverWire >= s.intervalBytesTransferred {
				bytesTransferred = summary.BytesOverWire - s.intervalBytesTransferred
			}
			bytesInMb := float64(bytesTransferred) / float64(Base10Mega)
			timeElapsed := time.Since(s.intervalStartTime).Seconds()

			// reset the interval timer and byte count
			s.intervalStartTime = time.Now()
			s.intervalBytesTransferred = summary.BytesOverWire

			if timeElapsed <= 0 {
				return 0
			}
			return bytesInMb / timeElapsed * 8
		}
		throughput = computeThroughput()
	} else if snapshot.ScanningComplete {
		summary.JobID = s.jobID
		summary.JobStatus = common.EJobStatus.Completed()
		jobDone = true
	}
	summary.SkippedSymlinkCount = snapshot.SkippedSymlinkCount
	summary.SkippedHardlinkCount = snapshot.SkippedHardlinkCount
	summary.SkippedSpecialFileCount = snapshot.SkippedSpecialFileCount
	summary.SkippedArchiveFileCount = snapshot.SkippedArchiveFileCount

	if handler, ok := s.handler.(SyncEnumerationStatsHandler); ok {
		handler.OnEnumerationStats(snapshot)
	}

	if !snapshot.ScanningComplete {
		// if the scanning is not complete, report scanning progress to the user
		var scanThroughput *float64
		if snapshot.FirstPartOrdered {
			scanThroughput = &throughput
		}
		scanProgress := SyncScanProgress{
			MoverSyncStats:          snapshot,
			SourceFilesScanned:      snapshot.SourceFilesScanned,
			DestinationFilesScanned: snapshot.DestinationFilesScanned,
			Throughput:              scanThroughput,
			JobID:                   s.jobID,
		}
		if s.handler != nil {
			s.handler.OnScanProgress(scanProgress)
		}
		return totalKnownCount, false
	} else {
		progress := SyncProgress{
			MoverSyncStats:           snapshot,
			ListJobSummaryResponse:   summary,
			DeleteTotalTransfers:     snapshot.DeletionCount,
			DeleteTransfersCompleted: snapshot.DeletionCount,
			Throughput:               throughput,
			ElapsedTime:              duration,
		}
		logJobProgress(s.jobID, s.logger, GetSyncProgress(progress))
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
	switch entityType {
	case common.EEntityType.File(), common.EEntityType.Hardlink():
		atomic.AddUint64(&spt.atomicSourceFilesScanned, 1)
		if entityType == common.EEntityType.Hardlink() && spt.fromTo.IsNFS() && hardlinkHandling == common.SkipHardlinkHandlingType {
			atomic.AddUint32(&spt.atomicSkippedHardlinkCount, 1)
		}
	case common.EEntityType.Folder():
		atomic.AddUint64(&spt.atomicSourceFoldersScanned, 1)
	case common.EEntityType.Symlink():
		atomic.AddUint64(&spt.atomicSourceFilesScanned, 1)
		if spt.fromTo.IsNFS() && symlinkOption == common.ESymlinkHandlingType.Skip() {
			atomic.AddUint32(&spt.atomicSkippedSymlinkCount, 1)
		}
	case common.EEntityType.Other():
		if spt.fromTo.IsNFS() {
			atomic.AddUint32(&spt.atomicSkippedSpecialFileCount, 1)
			atomic.AddUint64(&spt.atomicSourceFilesScanned, 1)
		} else if spt.fromTo.From() == common.ELocation.S3() {
			atomic.AddUint64(&spt.atomicSkippedArchiveFileCount, 1)
			atomic.AddUint64(&spt.atomicSourceFilesScanned, 1)
		}
	}
}

func (spt *syncProgressTracker) getSourceFilesScanned() uint64 {
	return atomic.LoadUint64(&spt.atomicSourceFilesScanned)
}

func (spt *syncProgressTracker) getDestinationFilesScanned() uint64 {
	return atomic.LoadUint64(&spt.atomicDestinationFilesScanned)
}

func (spt *syncProgressTracker) incDestEnumeration(entityType common.EntityType, symlinkOption common.SymlinkHandlingType, hardlinkHandling common.HardlinkHandlingType) {
	if entityType == common.EEntityType.File() {
		atomic.AddUint64(&spt.atomicDestinationFilesScanned, 1)
	} else if entityType == common.EEntityType.Folder() {
		atomic.AddUint64(&spt.atomicDestinationFoldersScanned, 1)
	} else if entityType == common.EEntityType.Symlink() {
		if symlinkOption == common.ESymlinkHandlingType.Preserve() {
			atomic.AddUint64(&spt.atomicDestinationFilesScanned, 1)
		}
	} else if entityType == common.EEntityType.Hardlink() {
		if hardlinkHandling == common.EHardlinkHandlingType.Preserve() {
			atomic.AddUint64(&spt.atomicDestinationFilesScanned, 1)
		}
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

func (spt *syncProgressTracker) incrementSkippedSymlinkCount() {
	atomic.AddUint32(&spt.atomicSkippedSymlinkCount, 1)
}

func (spt *syncProgressTracker) getSkippedSpecialFileCount() uint32 {
	return atomic.LoadUint32(&spt.atomicSkippedSpecialFileCount)
}

func (spt *syncProgressTracker) getSkippedHardlinkCount() uint32 {
	return atomic.LoadUint32(&spt.atomicSkippedHardlinkCount)
}

func (spt *syncProgressTracker) incSourceEnumerationFailure(entityType common.EntityType) {
	if entityType == common.EEntityType.Folder() {
		spt.incSourceFolderFailed()
		atomic.AddUint64(&spt.atomicSourceFoldersScanned, 1)
	} else {
		spt.incSourceFileFailed()
		atomic.AddUint64(&spt.atomicSourceFilesScanned, 1)
	}
}

func (spt *syncProgressTracker) incNotTransferred(entityType common.EntityType) {
	switch entityType {
	case common.EEntityType.File(), common.EEntityType.Hardlink(), common.EEntityType.Symlink():
		spt.incSourceFileNotTransferred()
	case common.EEntityType.Folder():
		spt.incSourceFolderNotTransferred()
	}
}

func (spt *syncProgressTracker) IncrementDestinationFolderEnumerationFailed() {
	spt.incDestinationFolderFailed()
}

func (spt *syncProgressTracker) IncrementDestinationFolderEnumerationSkipped() {
	spt.incDestinationFolderSkipped()
}

func (spt *syncProgressTracker) incSourceFolderFailed() {
	atomic.AddUint64(&spt.atomicSourceFolderEnumerationFailed, 1)
}

func (spt *syncProgressTracker) incSourceFileFailed() {
	atomic.AddUint64(&spt.atomicSourceFileEnumerationFailed, 1)
}

func (spt *syncProgressTracker) incDestinationFolderFailed() {
	atomic.AddUint64(&spt.atomicDestinationFolderEnumerationFailed, 1)
}

func (spt *syncProgressTracker) incDestinationFolderSkipped() {
	atomic.AddUint64(&spt.atomicDestinationFolderEnumerationSkipped, 1)
}

func (spt *syncProgressTracker) incSourceFileNotTransferred() {
	atomic.AddUint64(&spt.atomicSourceFilesTransferNotRequired, 1)
}

func (spt *syncProgressTracker) incSourceFolderNotTransferred() {
	atomic.AddUint64(&spt.atomicSourceFoldersTransferNotRequired, 1)
}

func (spt *syncProgressTracker) Stats() MoverSyncStats {
	return spt.GetEnumerationStats()
}

func (spt *syncProgressTracker) GetEnumerationStats() SyncEnumerationStats {
	return SyncEnumerationStats{
		SourceFilesScanned:                  spt.getSourceFilesScanned(),
		DestinationFilesScanned:             spt.getDestinationFilesScanned(),
		SourceFoldersScanned:                atomic.LoadUint64(&spt.atomicSourceFoldersScanned),
		DestinationFoldersScanned:           atomic.LoadUint64(&spt.atomicDestinationFoldersScanned),
		SourceFilesTransferNotRequired:      atomic.LoadUint64(&spt.atomicSourceFilesTransferNotRequired),
		SourceFoldersTransferNotRequired:    atomic.LoadUint64(&spt.atomicSourceFoldersTransferNotRequired),
		SourceFileEnumerationFailed:         atomic.LoadUint64(&spt.atomicSourceFileEnumerationFailed),
		SourceFolderEnumerationFailed:       atomic.LoadUint64(&spt.atomicSourceFolderEnumerationFailed),
		DestinationFolderEnumerationFailed:  atomic.LoadUint64(&spt.atomicDestinationFolderEnumerationFailed),
		DestinationFolderEnumerationSkipped: atomic.LoadUint64(&spt.atomicDestinationFolderEnumerationSkipped),
		SkippedArchiveFileCount:             atomic.LoadUint64(&spt.atomicSkippedArchiveFileCount),
		FirstPartOrdered:                    spt.firstPartOrdered(),
		ScanningComplete:                    spt.CompletedEnumeration(),
		DeletionCount:                       spt.getDeletionCount(),
		SkippedSymlinkCount:                 spt.getSkippedSymlinkCount(),
		SkippedSpecialFileCount:             spt.getSkippedSpecialFileCount(),
		SkippedHardlinkCount:                spt.getSkippedHardlinkCount(),
	}
}
