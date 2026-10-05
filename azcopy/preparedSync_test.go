package azcopy

import (
	"encoding/json"
	"testing"

	"github.com/Azure/azure-storage-azcopy/v10/common"
	"github.com/stretchr/testify/require"
)

func TestM3PreparedSyncPublishesExecutionState(t *testing.T) {
	id := common.NewJobID()
	tracker := newSyncProgressTracker(id, nil, common.EFromTo.FileNFSFileNFS())
	tracker.setFirstPartOrdered()
	tracker.setScanningComplete()
	tracker.incrementDeletionCount()
	tracker.incSourceEnumeration(common.EEntityType.Symlink(), common.ESymlinkHandlingType.Skip(), common.SkipHardlinkHandlingType)
	tracker.incSourceEnumeration(common.EEntityType.Other(), common.ESymlinkHandlingType.Skip(), common.SkipHardlinkHandlingType)
	tracker.incSourceEnumeration(common.EEntityType.Hardlink(), common.ESymlinkHandlingType.Skip(), common.SkipHardlinkHandlingType)
	prepared := &PreparedSync{syncer: &syncer{spt: tracker}}
	state := prepared.State()
	require.Equal(t, id, state.JobID)
	require.True(t, state.FirstPartOrdered)
	require.True(t, state.ScanningComplete)
	require.Equal(t, uint32(1), state.DeletionCount)
	require.Equal(t, uint32(1), state.SkippedSymlinkCount)
	require.Equal(t, uint32(1), state.SkippedSpecialFileCount)
	require.Equal(t, uint32(1), state.SkippedHardlinkCount)
}

type moverPayloadTestHandler struct {
	stats    SyncEnumerationStats
	scan     SyncScanProgress
	transfer SyncProgress
}

func (*moverPayloadTestHandler) OnStart(JobContext)                              {}
func (h *moverPayloadTestHandler) OnEnumerationStats(stats SyncEnumerationStats) { h.stats = stats }
func (h *moverPayloadTestHandler) OnScanProgress(progress SyncScanProgress)      { h.scan = progress }
func (h *moverPayloadTestHandler) OnTransferProgress(progress SyncProgress)      { h.transfer = progress }
func (*moverPayloadTestHandler) OnComplete(SyncResult)                           {}

func TestM3ProgressPayloadsPreserveMoverStatistics(t *testing.T) {
	handler := &moverPayloadTestHandler{}
	tracker := newSyncProgressTracker(common.NewJobID(), handler, common.EFromTo.FileFile())
	tracker.incSourceEnumeration(common.EEntityType.Folder(), common.ESymlinkHandlingType.Skip(), common.DefaultHardlinkHandlingType)
	tracker.incNotTransferred(common.EEntityType.File())
	tracker.IncrementDestinationFolderEnumerationFailed()
	tracker.Start()
	tracker.CheckProgress()
	require.Equal(t, uint64(1), handler.stats.SourceFoldersScanned)
	require.Equal(t, uint64(1), handler.scan.SourceFoldersScanned)
	require.Equal(t, uint64(1), handler.scan.SourceFilesTransferNotRequired)
	require.Equal(t, uint64(1), handler.scan.DestinationFolderEnumerationFailed)
	tracker.setScanningComplete()
	tracker.CheckProgress()
	expected := handler.scan.MoverSyncStats
	require.False(t, expected.ScanningComplete)
	expected.ScanningComplete = true
	require.Equal(t, expected, handler.transfer.MoverSyncStats)
}

func TestM3SyncJSONRetainsSummaryAndMoverCounters(t *testing.T) {
	progress := SyncProgress{
		MoverSyncStats:         MoverSyncStats{SkippedSymlinkCount: 3},
		ListJobSummaryResponse: common.ListJobSummaryResponse{SkippedSymlinkCount: 5},
	}
	data, err := json.Marshal(progress)
	require.NoError(t, err)
	var decoded struct {
		SkippedSymlinkCount uint32 `json:"SkippedSymlinkCount,string"`
		MoverSyncStats      struct{ SkippedSymlinkCount uint32 }
	}
	require.NoError(t, json.Unmarshal(data, &decoded))
	require.Equal(t, uint32(5), decoded.SkippedSymlinkCount)
	require.Equal(t, uint32(3), decoded.MoverSyncStats.SkippedSymlinkCount)
}
