package azcopy

import (
	"sync"
	"testing"

	"github.com/Azure/azure-storage-azcopy/v10/common"
)

func TestTransferProgressArchiveCounter(t *testing.T) {
	tracker := newTransferProgressTracker(common.JobID{}, nil, common.EFromTo.S3Blob())
	var workers sync.WaitGroup
	for i := 0; i < 32; i++ {
		workers.Add(1)
		go func() {
			defer workers.Done()
			tracker.incEnumeration(common.EEntityType.Other(), common.ESymlinkHandlingType.Skip(), common.DefaultHardlinkHandlingType)
		}()
	}
	workers.Wait()
	if count := tracker.getSkippedArchiveFileCount(); count != 32 {
		t.Fatalf("archive increments were lost: got %d, want 32", count)
	}
	if tracker.getSkippedSpecialFileCount() != 0 {
		t.Fatal("archive skips must not increment the NFS special-file counter")
	}
}

func TestTransferProgressCountsSkippedSymlinksOnlyForSourcesWithRealSymlinks(t *testing.T) {
	for _, test := range []struct {
		fromTo  common.FromTo
		skipped uint32
	}{
		{common.EFromTo.LocalBlob(), 1},
		{common.EFromTo.LocalFile(), 1},
		{common.EFromTo.FileNFSFileNFS(), 1},
		{common.EFromTo.FileNFSLocal(), 1},
		{common.EFromTo.BlobLocal(), 0},
		{common.EFromTo.BlobBlob(), 0},
	} {
		t.Run(test.fromTo.String(), func(t *testing.T) {
			tracker := newTransferProgressTracker(common.JobID{}, nil, test.fromTo)
			tracker.incEnumeration(common.EEntityType.Symlink(), common.ESymlinkHandlingType.Skip(), common.DefaultHardlinkHandlingType)
			tracker.incEnumeration(common.EEntityType.Symlink(), common.ESymlinkHandlingType.Preserve(), common.DefaultHardlinkHandlingType)
			if got := tracker.getSkippedSymlinkCount(); got != test.skipped {
				t.Fatalf("skipped symlinks = %d, want %d", got, test.skipped)
			}
			if got := tracker.atomicFilesScanned.Load(); got != 2 {
				t.Fatalf("symlinks must still count as scanned files, got %d", got)
			}
		})
	}
}
