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
			tracker.incSkippedArchive()
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
