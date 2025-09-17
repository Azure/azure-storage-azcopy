package azcopy

import (
	"fmt"
	"sync/atomic"
	"time"

	"github.com/Azure/azure-storage-azcopy/v10/common"
	"github.com/Azure/azure-storage-azcopy/v10/jobsAdmin"
)

var _ JobProgressTracker = &syncProgressTracker{}

type syncProgressTracker struct {
	// NOTE: for the 64 bit atomic functions to work on a 32 bit system, we have to guarantee the right 64-bit alignment
	// so the 64 bit integers are placed first in the struct to avoid future breaks
	// refer to: https://golang.org/pkg/sync/atomic/#pkg-note-BUG
	// incremented by traversers
	atomicSourceFilesScanned      uint64
	atomicDestinationFilesScanned uint64
	atomicSkippedSymlinkCount     uint32
	atomicSkippedSpecialFileCount uint32
	atomicDeletionCount           uint32

	atomicScanningStatus uint32
	// defines whether first part has been ordered or not.
	// 0 means first part is not ordered and 1 means first part is ordered.
	atomicFirstPartOrdered uint32

	// variables used to calculate progress
	// intervalStartTime holds the last time value when the progress summary was fetched
	// the value of this variable is used to calculate the throughput
	// it gets updated every time the progress summary is fetched
	intervalStartTime        time.Time
	intervalBytesTransferred uint64

	// used to calculate job summary
	jobStartTime time.Time

	jobID   common.JobID
	handler SyncJobHandler
}

func newSyncProgressTracker(jobID common.JobID, handler SyncJobHandler) *syncProgressTracker {
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
	spt.handler.OnStart(common.JobContext{JobID: spt.jobID, LogPath: logPathFolder})
}

func (s *syncProgressTracker) CheckProgress() (uint32, bool) {
	duration := time.Since(s.jobStartTime)
	var summary common.ListJobSummaryResponse
	var jobDone bool
	var totalKnownCount uint32
	var throughput float64

	// transfers have begun, so we can start computing throughput
	if s.firstPartOrdered() {
		summary := jobsAdmin.GetJobSummary(s.jobID)
		jobDone = summary.JobStatus.IsJobDone()
		totalKnownCount = summary.TotalTransfers
		var computeThroughput = func() float64 {
			// compute the average throughput for the last time interval
			bytesInMb := float64(float64(summary.BytesOverWire-s.intervalBytesTransferred) / float64(common.Base10Mega))
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
		scanProgress := common.ScanProgress{
			Source:             s.getSourceFilesScanned(),
			Destination:        s.getDestinationFilesScanned(),
			TransferThroughput: common.Iff(s.firstPartOrdered(), &throughput, nil),
		}
		if common.AzcopyCurrentJobLogger != nil {
			common.AzcopyCurrentJobLogger.Log(common.LogInfo, common.GetScanProgressOutputBuilder(scanProgress)(common.EOutputFormat.Text()))
		}
		s.handler.OnScanProgress(scanProgress)
		return totalKnownCount, false
	} else {
		// else report transfer progress to the user
		transferProgress := common.TransferProgress{
			ListJobSummaryResponse:   summary,
			DeleteTotalTransfers:     s.getDeletionCount(),
			DeleteTransfersCompleted: s.getDeletionCount(),
			Throughput:               throughput,
			ElapsedTime:              duration,
			JobType:                  common.EJobType.Sync(),
		}
		if common.AzcopyCurrentJobLogger != nil {
			common.AzcopyCurrentJobLogger.Log(common.LogInfo, common.GetProgressOutputBuilder(transferProgress)(common.EOutputFormat.Text()))
		}
		s.handler.OnTransferProgress(SyncJobProgress{
			ListJobSummaryResponse:   summary,
			DeleteTotalTransfers:     s.getDeletionCount(),
			DeleteTransfersCompleted: s.getDeletionCount(),
			Throughput:               throughput,
			ElapsedTime:              duration,
		})
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

func (spt *syncProgressTracker) incSourceEnumeration(entityType common.EntityType) {
	switch entityType {
	case common.EEntityType.File():
		atomic.AddUint64(&spt.atomicSourceFilesScanned, 1)
	case common.EEntityType.Symlink():
		atomic.AddUint32(&spt.atomicSkippedSymlinkCount, 1)
	case common.EEntityType.Other():
		atomic.AddUint32(&spt.atomicSkippedSpecialFileCount, 1)
	}
}

func (spt *syncProgressTracker) getSourceFilesScanned() uint64 {
	return atomic.LoadUint64(&spt.atomicSourceFilesScanned)
}

func (spt *syncProgressTracker) getDestinationFilesScanned() uint64 {
	return atomic.LoadUint64(&spt.atomicDestinationFilesScanned)
}

func (spt *syncProgressTracker) incDestEnumeration(entityType common.EntityType) {
	switch entityType {
	case common.EEntityType.File():
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
