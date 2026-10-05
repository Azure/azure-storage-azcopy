package azcopy

import (
	"context"
	"errors"
	"io"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/Azure/azure-storage-azcopy/v10/common"
	"github.com/Azure/azure-storage-azcopy/v10/jobsAdmin"
	"github.com/stretchr/testify/require"
)

func m5HardlinkTemplate() *common.CopyJobPartOrderRequest {
	return &common.CopyJobPartOrderRequest{
		JobID: common.NewJobID(), FromTo: common.EFromTo.FileNFSFileNFS(),
		JobProcessingMode:    common.EJobProcessingMode.NFS(),
		HardlinkHandlingType: common.EHardlinkHandlingType.Preserve(),
	}
}

func TestM5CopyProcessorSeparatesHardlinksAndKeepsFileProperties(t *testing.T) {
	for _, isCopy := range []bool{true, false} {
		t.Run(map[bool]string{true: "copy", false: "indexed-sync"}[isCopy], func(t *testing.T) {
			template := m5HardlinkTemplate()
			var orders []common.CopyJobPartOrderRequest
			first, final := 0, 0
			processor := NewCopyTransferProcessor(isCopy, template, 2, common.ResourceString{}, common.ResourceString{},
				func(bool) { first++ }, func() { final++ }, false, true,
				func(order common.CopyJobPartOrderRequest) common.CopyJobPartOrderResponse {
					orders = append(orders, order)
					return common.CopyJobPartOrderResponse{JobStarted: true}
				})
			for _, transfer := range []common.CopyTransfer{
				{Source: "/anchor", EntityType: common.EEntityType.Hardlink(), SourceSize: 11},
				{Source: "/alias1", EntityType: common.EEntityType.Hardlink(), TargetHardlinkFile: "anchor", SourceSize: 11},
				{Source: "/metadata", EntityType: common.EEntityType.FileProperties()},
				{Source: "/alias2", EntityType: common.EEntityType.Hardlink(), TargetHardlinkFile: "anchor", SourceSize: 11},
				{Source: "/file", EntityType: common.EEntityType.File(), SourceSize: 7},
				{Source: "/alias3", EntityType: common.EEntityType.Hardlink(), TargetHardlinkFile: "anchor", SourceSize: 11},
			} {
				require.NoError(t, processor.ScheduleTransfer(transfer))
			}
			require.Len(t, orders, 1)
			started, err := processor.DispatchFinalPart()
			require.NoError(t, err)
			require.True(t, started)
			require.Len(t, orders, 4)
			require.EqualValues(t, 1, orders[0].Transfers.FilePropertyTransferCount)
			require.EqualValues(t, 1, orders[0].Transfers.HardlinksTransferCount)
			require.Zero(t, orders[0].Transfers.HardlinksConvertedCount)
			require.EqualValues(t, 11, orders[0].Transfers.TotalSizeInBytes)
			require.EqualValues(t, 7, orders[1].Transfers.TotalSizeInBytes)
			for index, order := range orders {
				require.EqualValues(t, index, order.PartNum)
				require.Equal(t, index == len(orders)-1, order.IsFinalPart)
				if index < 2 {
					require.Equal(t, common.EJobPartType.Mixed(), order.JobPartType)
				} else {
					require.Equal(t, common.EJobPartType.Hardlink(), order.JobPartType)
					require.EqualValues(t, len(order.Transfers.List), order.Transfers.HardlinksTransferCount)
					require.Zero(t, order.Transfers.TotalSizeInBytes)
				}
			}
			require.Equal(t, 1, first)
			require.Equal(t, 1, final)
		})
	}
}

func TestM5HardlinksOnlyAndAnchorOnlyFinalPart(t *testing.T) {
	for _, target := range []string{"", "existing-anchor"} {
		template := m5HardlinkTemplate()
		var orders []common.CopyJobPartOrderRequest
		processor := NewCopyTransferProcessor(true, template, 1, common.ResourceString{}, common.ResourceString{},
			nil, nil, false, true, func(order common.CopyJobPartOrderRequest) common.CopyJobPartOrderResponse {
				orders = append(orders, order)
				return common.CopyJobPartOrderResponse{JobStarted: true}
			})
		require.NoError(t, processor.ScheduleTransfer(common.CopyTransfer{
			EntityType: common.EEntityType.Hardlink(), SourceSize: 9, TargetHardlinkFile: target,
		}))
		_, err := processor.DispatchFinalPart()
		require.NoError(t, err)
		require.Len(t, orders, 1)
		require.True(t, orders[0].IsFinalPart)
		require.EqualValues(t, 1, orders[0].Transfers.HardlinksTransferCount)
		if target == "" {
			require.Equal(t, common.EJobPartType.Mixed(), orders[0].JobPartType)
			require.EqualValues(t, 9, orders[0].Transfers.TotalSizeInBytes)
		} else {
			require.Equal(t, common.EJobPartType.Hardlink(), orders[0].JobPartType)
			require.Zero(t, orders[0].Transfers.TotalSizeInBytes)
		}
	}
}

func TestM5HardlinkBatchFailureDoesNotAdvanceToDependentParts(t *testing.T) {
	template := m5HardlinkTemplate()
	calls, final := 0, 0
	processor := NewCopyTransferProcessor(true, template, 2, common.ResourceString{}, common.ResourceString{},
		nil, func() { final++ }, false, true, func(order common.CopyJobPartOrderRequest) common.CopyJobPartOrderResponse {
			calls++
			require.Equal(t, common.EJobPartType.Mixed(), order.JobPartType)
			return common.CopyJobPartOrderResponse{ErrorMsg: "mixed dispatch failed"}
		})
	require.NoError(t, processor.ScheduleTransfer(common.CopyTransfer{EntityType: common.EEntityType.FileProperties()}))
	require.NoError(t, processor.ScheduleTransfer(common.CopyTransfer{
		EntityType: common.EEntityType.Hardlink(), TargetHardlinkFile: "anchor",
	}))
	_, err := processor.DispatchFinalPart()
	require.EqualError(t, err, "mixed dispatch failed")
	require.Equal(t, 1, calls)
	require.Zero(t, final)
}

func TestM5HardlinkFollowRetainsMixedCounts(t *testing.T) {
	template := m5HardlinkTemplate()
	template.HardlinkHandlingType = common.EHardlinkHandlingType.Follow()
	processor := NewCopyTransferProcessor(true, template, 2, common.ResourceString{}, common.ResourceString{},
		nil, nil, false, true, func(common.CopyJobPartOrderRequest) common.CopyJobPartOrderResponse {
			return common.CopyJobPartOrderResponse{JobStarted: true}
		})
	require.NoError(t, processor.ScheduleTransfer(common.CopyTransfer{EntityType: common.EEntityType.Hardlink(), SourceSize: 13}))
	require.NoError(t, processor.ScheduleTransfer(common.CopyTransfer{EntityType: common.EEntityType.FileProperties()}))
	_, err := processor.DispatchFinalPart()
	require.NoError(t, err)
	require.EqualValues(t, 1, template.Transfers.HardlinksConvertedCount)
	require.Zero(t, template.Transfers.HardlinksTransferCount)
	require.EqualValues(t, 1, template.Transfers.FilePropertyTransferCount)
	require.EqualValues(t, 13, template.Transfers.TotalSizeInBytes)
	require.Equal(t, common.EJobPartType.Mixed(), template.JobPartType)
}

func TestM5HardlinkFinalizationWaitsForMixedDispatch(t *testing.T) {
	original := jobsAdmin.ExecuteNewCopyJobPartOrder
	entered, release, completed := make(chan struct{}), make(chan struct{}), make(chan error, 1)
	var releaseOnce sync.Once
	unblock := func() { releaseOnce.Do(func() { close(release) }) }
	orders := make(chan common.CopyJobPartOrderRequest, 3)
	jobsAdmin.ExecuteNewCopyJobPartOrder = func(order common.CopyJobPartOrderRequest) common.CopyJobPartOrderResponse {
		if order.PartNum == 0 {
			close(entered)
			<-release
		}
		orders <- order
		return common.CopyJobPartOrderResponse{JobStarted: true}
	}
	template := m5HardlinkTemplate()
	processor := NewCopyTransferProcessor(true, template, 2, common.ResourceString{}, common.ResourceString{}, nil, nil, false, false, nil)
	processor.dispatchCh = make(chan dispatchItem, 1)
	processor.dispatchDone = make(chan struct{})
	t.Cleanup(func() {
		unblock()
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		require.NoError(t, processor.AbortAndWait(ctx))
		jobsAdmin.ExecuteNewCopyJobPartOrder = original
	})
	require.NoError(t, processor.dispatchPart(dispatchItem{partNum: 0}))
	select {
	case <-entered:
	case <-time.After(5 * time.Second):
		t.Fatal("mixed dispatch did not start")
	}
	template.PartNum = 1
	require.NoError(t, processor.ScheduleTransfer(common.CopyTransfer{
		EntityType: common.EEntityType.Hardlink(), TargetHardlinkFile: "anchor",
	}))
	go func() { _, err := processor.DispatchFinalPart(); completed <- err }()
	select {
	case <-completed:
		t.Fatal("hardlink finalization passed a blocked mixed part")
	case <-time.After(20 * time.Millisecond):
	}
	require.Empty(t, orders)
	unblock()
	select {
	case err := <-completed:
		require.NoError(t, err)
	case <-time.After(5 * time.Second):
		t.Fatal("finalization did not finish")
	}
	require.Equal(t, common.EJobPartType.Mixed(), (<-orders).JobPartType)
	final := <-orders
	require.Equal(t, common.EJobPartType.Hardlink(), final.JobPartType)
	require.True(t, final.IsFinalPart)
}

func TestM5AbortPreventsBufferedHardlinkDispatch(t *testing.T) {
	processor := NewCopyTransferProcessor(true, m5HardlinkTemplate(), 2, common.ResourceString{}, common.ResourceString{},
		nil, nil, false, true, func(common.CopyJobPartOrderRequest) common.CopyJobPartOrderResponse {
			t.Error("aborted processor dispatched a hardlink")
			return common.CopyJobPartOrderResponse{}
		})
	require.NoError(t, processor.ScheduleTransfer(common.CopyTransfer{
		EntityType: common.EEntityType.Hardlink(), TargetHardlinkFile: "anchor",
	}))
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	require.NoError(t, processor.AbortAndWait(ctx))
	_, err := processor.DispatchFinalPart()
	require.ErrorIs(t, err, context.Canceled)
}

func TestM5HardlinkPreserveValidationKeepsLegacyContracts(t *testing.T) {
	var validateNFS func(common.FromTo, common.PreservePermissionsOption, bool, *common.HardlinkHandlingType, common.SymlinkHandlingType) error = PerformNFSSpecificValidation
	preserve := common.EHardlinkHandlingType.Preserve()
	require.NoError(t, validateNFS(common.EFromTo.FileNFSFileNFS(), common.EPreservePermissionsOption.None(), false,
		&preserve, common.ESymlinkHandlingType.Skip()))
	require.Error(t, ValidateHardlinksFlag(preserve, common.EFromTo.BlobBlob()))
	require.Error(t, ValidateHardlinksFlag(preserve, common.EFromTo.FileNFSFileSMB()))
	for _, fromTo := range []common.FromTo{common.EFromTo.FileNFSFileSMB(), common.EFromTo.FileSMBFileNFS()} {
		require.NoError(t, ValidateHardlinksFlag(common.EHardlinkHandlingType.Follow(), fromTo))
		require.NoError(t, ValidateHardlinksFlag(common.EHardlinkHandlingType.Skip(), fromTo))
	}
}

func TestM5CrossProtocolNFSRequiresHardlinkSkip(t *testing.T) {
	for _, test := range []struct {
		name     string
		fromTo   common.FromTo
		expected string
	}{
		{"SMBToNFS", common.EFromTo.FileSMBFileNFS(), "'--hardlinks' must be set to 'skip'"},
		{"NFSToSMB", common.EFromTo.FileNFSFileSMB(), "Hardlinked files are not supported between NFS and SMB"},
	} {
		t.Run(test.name, func(t *testing.T) {
			for _, hardlinks := range []common.HardlinkHandlingType{
				common.EHardlinkHandlingType.Follow(),
				common.EHardlinkHandlingType.Preserve(),
			} {
				err := PerformNFSSpecificValidation(
					test.fromTo,
					common.EPreservePermissionsOption.None(),
					false,
					&hardlinks,
					common.ESymlinkHandlingType.Skip(),
				)
				require.EqualError(t, err, test.expected)
			}

			skip := common.EHardlinkHandlingType.Skip()
			require.NoError(t, PerformNFSSpecificValidation(
				test.fromTo,
				common.EPreservePermissionsOption.None(),
				false,
				&skip,
				common.ESymlinkHandlingType.Skip(),
			))
		})
	}
}

type m5CloseBackend struct{ closes int }

func (*m5CloseBackend) ReadAt([]byte, int64) (int, error)      { return 0, io.EOF }
func (*m5CloseBackend) WriteAt(p []byte, _ int64) (int, error) { return len(p), nil }
func (b *m5CloseBackend) Close() error                         { b.closes++; return errors.New("close failure") }

func TestM5CopyExecutorClosesInodeStoreOnce(t *testing.T) {
	var nilExecutor *transferExecutor
	require.NoError(t, nilExecutor.Close())
	backend := &m5CloseBackend{}
	executor := &transferExecutor{inodeStore: common.NewInodeStoreFromBackend(backend)}
	require.EqualError(t, executor.Close(), "close failure")
	require.NoError(t, executor.Close())
	require.Equal(t, 1, backend.closes)
}

func TestM5CopyDryrunInodeStoreIsDisposableAndIsolated(t *testing.T) {
	folder := ".m5-copy-inodes-" + common.NewJobID().String()
	require.NoError(t, os.Mkdir(folder, 0700))
	previous := common.AzcopyJobPlanFolder
	common.AzcopyJobPlanFolder = folder
	t.Cleanup(func() {
		common.AzcopyJobPlanFolder = previous
		require.NoError(t, os.RemoveAll(folder))
	})

	jobID := common.NewJobID()
	persistent, err := newCopyInodeStore(jobID, common.EHardlinkHandlingType.Preserve(), false)
	require.NoError(t, err)
	t.Cleanup(func() { _ = persistent.Close() })
	_, _, err = persistent.GetOrAdd("inode", "existing-anchor")
	require.NoError(t, err)
	require.NoError(t, persistent.Close())
	before, err := os.ReadDir(folder)
	require.NoError(t, err)
	require.Len(t, before, 1)
	original, err := os.ReadFile(filepath.Join(folder, before[0].Name()))
	require.NoError(t, err)

	for range 2 {
		store, err := newCopyInodeStore(jobID, common.EHardlinkHandlingType.Preserve(), true)
		require.NoError(t, err)
		executor := &transferExecutor{inodeStore: store}
		t.Cleanup(func() { _ = executor.Close() })
		anchor, exists, err := store.GetOrAdd("inode", "dryrun-anchor")
		require.NoError(t, err)
		require.Empty(t, anchor)
		require.False(t, exists)
		require.NoError(t, store.Flush())
		require.NoError(t, executor.Close())
		entries, err := os.ReadDir(folder)
		require.NoError(t, err)
		require.Len(t, entries, 1)
		require.Equal(t, before[0].Name(), entries[0].Name())
		actual, err := os.ReadFile(filepath.Join(folder, entries[0].Name()))
		require.NoError(t, err)
		require.Equal(t, original, actual)
	}
}

func TestM5CopyFreshInodeStoreRejectsExistingJobState(t *testing.T) {
	folder := ".m5-copy-fresh-inodes-" + common.NewJobID().String()
	require.NoError(t, os.Mkdir(folder, 0700))
	previous := common.AzcopyJobPlanFolder
	common.AzcopyJobPlanFolder = folder
	t.Cleanup(func() {
		common.AzcopyJobPlanFolder = previous
		require.NoError(t, os.RemoveAll(folder))
	})

	jobID := common.NewJobID()
	store, err := newCopyInodeStore(jobID, common.EHardlinkHandlingType.Preserve(), false)
	require.NoError(t, err)
	t.Cleanup(func() { _ = store.Close() })
	_, _, err = store.GetOrAdd("inode", "original-anchor")
	require.NoError(t, err)
	require.NoError(t, store.Close())
	entries, err := os.ReadDir(folder)
	require.NoError(t, err)
	require.Len(t, entries, 1)
	path := filepath.Join(folder, entries[0].Name())
	before, err := os.ReadFile(path)
	require.NoError(t, err)

	for range 2 {
		retry, err := newCopyInodeStore(jobID, common.EHardlinkHandlingType.Preserve(), false)
		if retry != nil {
			_ = retry.Close()
		}
		require.ErrorIs(t, err, os.ErrExist)
		require.ErrorContains(t, err, "ResumeJob")
		require.Nil(t, retry)
		after, err := os.ReadFile(path)
		require.NoError(t, err)
		require.Equal(t, before, after)
	}
	resumed, err := common.NewInodeStore(jobID)
	require.NoError(t, err)
	t.Cleanup(func() { _ = resumed.Close() })
	anchor, err := resumed.GetAnchor("inode")
	require.NoError(t, err)
	require.Equal(t, "original-anchor", anchor)
	require.NoError(t, resumed.Close())
}

func TestM5CopyFollowAndSkipDoNotCreateInodeState(t *testing.T) {
	previous := common.AzcopyJobPlanFolder
	common.AzcopyJobPlanFolder = ""
	t.Cleanup(func() { common.AzcopyJobPlanFolder = previous })
	for _, mode := range []common.HardlinkHandlingType{common.EHardlinkHandlingType.Follow(), common.EHardlinkHandlingType.Skip()} {
		for _, dryrun := range []bool{false, true} {
			store, err := newCopyInodeStore(common.NewJobID(), mode, dryrun)
			require.NoError(t, err)
			require.Nil(t, store)
		}
	}
}

type m5SummaryLifecycle struct {
	common.LifecycleMgr
	output string
}

func (l *m5SummaryLifecycle) Exit(builder common.OutputBuilder, _ common.ExitCode) {
	l.output = builder(common.EOutputFormat.Text())
}

func TestM5JobSummarySeparatesHardlinkAndFileCounts(t *testing.T) {
	lifecycle := &m5SummaryLifecycle{}
	PrintJobProgressSummary(common.ListJobSummaryResponse{
		TransfersCompleted: 20, FoldersCompleted: 2, HardlinksCompleted: 3,
		TransfersFailed: 10, FoldersFailed: 1, HardlinksFailed: 2,
		TransfersSkipped: 8, FoldersSkipped: 1, HardlinksSkipped: 3,
	}, common.EOutputFormat.Text(), lifecycle)
	require.Contains(t, lifecycle.output, "Number of File Transfers Completed: 15")
	require.Contains(t, lifecycle.output, "Number of File Transfers Failed: 7")
	require.Contains(t, lifecycle.output, "Number of File Transfers Skipped: 4")
	require.Contains(t, lifecycle.output, "Number of Hardlinks Completed: 3")
	require.Contains(t, lifecycle.output, "Number of Hardlinks Failed: 2")
	require.Contains(t, lifecycle.output, "Number of Hardlinks Skipped: 3")
}
