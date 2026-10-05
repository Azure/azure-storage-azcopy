package azcopy

import (
	"strings"
	"testing"

	"github.com/Azure/azure-storage-azcopy/v10/common"
)

func TestCopyProgressDisplaysZeroThroughput(t *testing.T) {
	output := GetCopyProgress(CopyProgress{}, false)
	if !strings.Contains(output, "Throughput (Mb/s): 0") {
		t.Fatalf("zero throughput disappeared from copy progress: %q", output)
	}
}

func TestSyncProgressDisplaysZeroThroughput(t *testing.T) {
	output := GetSyncProgress(SyncProgress{})
	if !strings.Contains(output, "Throughput (Mb/s): 0") {
		t.Fatalf("zero throughput disappeared from sync progress: %q", output)
	}
}

func TestCopyProgressCleanupRemainsConcise(t *testing.T) {
	output := GetCopyProgress(CopyProgress{ListJobSummaryResponse: common.ListJobSummaryResponse{
		IsCleanupJob: true, TransfersCompleted: 2, TotalTransfers: 3,
	}}, false)
	if output != "Cleanup 2/3" {
		t.Fatalf("cleanup progress formatting changed: %q", output)
	}
}
