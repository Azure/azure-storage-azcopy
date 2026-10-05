package azcopy

import (
	"testing"

	"github.com/Azure/azure-storage-azcopy/v10/common"
)

type syncStatsTestHandler struct {
	stats         SyncEnumerationStats
	transfers     int
	scanStats     MoverSyncStats
	transferStats MoverSyncStats
	onStats       func()
}

func (*syncStatsTestHandler) OnStart(JobContext) {}
func (h *syncStatsTestHandler) OnScanProgress(progress SyncScanProgress) {
	h.scanStats = progress.MoverSyncStats
}
func (h *syncStatsTestHandler) OnTransferProgress(progress SyncProgress) {
	h.transfers++
	h.transferStats = progress.MoverSyncStats
}
func (*syncStatsTestHandler) OnComplete(SyncResult) {}
func (h *syncStatsTestHandler) OnEnumerationStats(stats SyncEnumerationStats) {
	h.stats = stats
	if h.onStats != nil {
		h.onStats()
	}
}

func TestSyncProgressMoverEnumerationStats(t *testing.T) {
	handler := &syncStatsTestHandler{}
	tracker := newSyncProgressTracker(common.JobID{}, handler, common.EFromTo.FileNFSLocal())
	for _, entity := range []common.EntityType{common.EEntityType.File(), common.EEntityType.Folder(),
		common.EEntityType.Hardlink(), common.EEntityType.Symlink(), common.EEntityType.Other()} {
		tracker.incSourceEnumeration(entity, common.ESymlinkHandlingType.Skip(), common.SkipHardlinkHandlingType)
		tracker.incNotTransferred(entity)
	}
	tracker.incSourceEnumerationFailure(common.EEntityType.File())
	tracker.incSourceEnumerationFailure(common.EEntityType.Folder())
	tracker.incDestEnumeration(common.EEntityType.File(), common.ESymlinkHandlingType.Skip(), common.DefaultHardlinkHandlingType)
	tracker.incDestEnumeration(common.EEntityType.Folder(), common.ESymlinkHandlingType.Skip(), common.DefaultHardlinkHandlingType)
	tracker.IncrementDestinationFolderEnumerationFailed()
	tracker.IncrementDestinationFolderEnumerationSkipped()
	tracker.Start()
	if _, done := tracker.CheckProgress(); done || handler.scanStats != tracker.Stats() {
		t.Fatal("scan progress did not carry the current Mover counters")
	}
	tracker.setScanningComplete()
	count, done := tracker.CheckProgress()
	expected := SyncEnumerationStats{
		SourceFilesScanned: 5, SourceFoldersScanned: 2,
		DestinationFilesScanned: 1, DestinationFoldersScanned: 1,
		SourceFilesTransferNotRequired: 3, SourceFoldersTransferNotRequired: 1,
		SourceFileEnumerationFailed: 1, SourceFolderEnumerationFailed: 1,
		DestinationFolderEnumerationFailed: 1, DestinationFolderEnumerationSkipped: 1,
		ScanningComplete:    true,
		SkippedSymlinkCount: 1, SkippedSpecialFileCount: 1, SkippedHardlinkCount: 1,
	}
	if !done || count != 0 || handler.transfers != 1 || handler.stats != expected || handler.transferStats != tracker.Stats() {
		t.Fatalf("unexpected completion or statistics: count=%d, done=%v, stats=%+v", count, done, handler.stats)
	}
	if tracker.getSkippedSymlinkCount() != 1 || tracker.getSkippedHardlinkCount() != 1 || tracker.getSkippedSpecialFileCount() != 1 {
		t.Fatal("NFS skipped-object counters were not retained")
	}
}

func TestSyncProgressArchiveStats(t *testing.T) {
	tracker := newSyncProgressTracker(common.JobID{}, nil)
	tracker.setFromTo(common.EFromTo.S3Blob())
	tracker.incSourceEnumeration(common.EEntityType.Other(), common.ESymlinkHandlingType.Skip(), common.DefaultHardlinkHandlingType)
	stats := tracker.GetEnumerationStats()
	if stats.SourceFilesScanned != 1 || stats.SkippedArchiveFileCount != 1 ||
		tracker.Stats().SkippedArchiveFileCount != 1 || tracker.getSkippedSpecialFileCount() != 0 {
		t.Fatalf("archive scans were not distinguished from NFS special files: %+v", stats)
	}
}

func TestTransferProgressCompletesWithoutOrderedParts(t *testing.T) {
	tracker := newTransferProgressTracker(common.JobID{}, nil, common.EFromTo.LocalBlob())
	tracker.setScanningComplete()
	if count, done := tracker.CheckProgress(); count != 0 || !done {
		t.Fatalf("empty enumeration did not complete: count=%d, done=%v", count, done)
	}
}

func TestSyncProgressNamedCounterMutators(t *testing.T) {
	tracker := newSyncProgressTracker(common.JobID{}, nil, common.EFromTo.FileNFSLocal())
	tracker.incSourceFolderFailed()
	tracker.incSourceFileFailed()
	tracker.incDestinationFolderFailed()
	tracker.incDestinationFolderSkipped()
	tracker.incSourceFileNotTransferred()
	tracker.incSourceFolderNotTransferred()
	stats := tracker.Stats()
	if stats.SourceFolderEnumerationFailed != 1 || stats.SourceFileEnumerationFailed != 1 ||
		stats.DestinationFolderEnumerationFailed != 1 || stats.DestinationFolderEnumerationSkipped != 1 ||
		stats.SourceFilesTransferNotRequired != 1 || stats.SourceFoldersTransferNotRequired != 1 {
		t.Fatalf("named counters were not preserved in the Mover snapshot: %+v", stats)
	}
	full := tracker.GetEnumerationStats()
	if full.SourceFilesScanned != 0 || full.SourceFoldersScanned != 0 ||
		stats.DestinationFoldersScanned != 0 || stats.SourceFoldersScanned != 0 {
		t.Fatal("named counter methods must not implicitly count scanned objects")
	}
}

func TestSyncProgressStateSnapshot(t *testing.T) {
	tracker := newSyncProgressTracker(common.JobID{}, nil, common.EFromTo.FileNFSLocal())
	tracker.setFirstPartOrdered()
	tracker.setScanningComplete()
	tracker.incrementDeletionCount()
	tracker.incSourceEnumeration(common.EEntityType.Symlink(), common.ESymlinkHandlingType.Skip(), common.SkipHardlinkHandlingType)
	tracker.incSourceEnumeration(common.EEntityType.Hardlink(), common.ESymlinkHandlingType.Skip(), common.SkipHardlinkHandlingType)
	tracker.incSourceEnumeration(common.EEntityType.Other(), common.ESymlinkHandlingType.Skip(), common.SkipHardlinkHandlingType)
	stats := tracker.GetEnumerationStats()
	if !stats.FirstPartOrdered || !stats.ScanningComplete || stats.DeletionCount != 1 ||
		stats.SkippedSymlinkCount != 1 || stats.SkippedHardlinkCount != 1 || stats.SkippedSpecialFileCount != 1 {
		t.Fatalf("state snapshot lost lifecycle or skipped-object counters: %+v", stats)
	}
	if stats.SourceFilesScanned != 3 || stats.SourceFoldersScanned != 0 {
		t.Fatal("state snapshot must not change enumeration totals")
	}
}

func TestSyncProgressCallbacksShareOneSnapshot(t *testing.T) {
	for _, complete := range []bool{false, true} {
		handler := &syncStatsTestHandler{}
		tracker := newSyncProgressTracker(common.JobID{}, handler, common.EFromTo.FileNFSLocal())
		tracker.incSourceEnumeration(common.EEntityType.Folder(), common.ESymlinkHandlingType.Skip(), common.SkipHardlinkHandlingType)
		handler.onStats = func() { tracker.incSourceFolderNotTransferred() }
		tracker.Start()
		if complete {
			tracker.setScanningComplete()
		}
		tracker.CheckProgress()
		payload := handler.scanStats
		if complete {
			payload = handler.transferStats
		}
		if payload != handler.stats || payload.SourceFoldersScanned != 1 {
			t.Fatalf("callbacks observed different or erased counters: callback=%+v, payload=%+v", handler.stats, payload)
		}
	}
}
