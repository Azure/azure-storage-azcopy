package azcopy

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	"github.com/Azure/azure-storage-azcopy/v10/common"
	"github.com/stretchr/testify/require"
)

func TestCheckpoint5PreserveRejectsMoverSyncPaths(t *testing.T) {
	for _, streaming := range []bool{false, true} {
		_, err := newCookedSyncOptions(
			"https://source.file.core.windows.net/share", "https://destination.file.core.windows.net/share",
			SyncOptions{
				FromTo: common.EFromTo.FileNFSFileNFS(), Hardlinks: common.PreserveHardlinkHandlingType,
				UseSyncOrchestrator: !streaming, UseStreamingMergeJoin: streaming,
			})
		require.ErrorContains(t, err, "hardlink preservation requires standard indexed sync")
	}
	_, err := newCookedSyncOptions(
		"https://source.file.core.windows.net/share", "https://destination.file.core.windows.net/share",
		SyncOptions{FromTo: common.EFromTo.FileNFSFileNFS(), Hardlinks: common.PreserveHardlinkHandlingType})
	require.NoError(t, err)
}

func TestCheckpoint5PreparedSyncClosesOnlySafeResources(t *testing.T) {
	file, err := os.Create(filepath.Join(t.TempDir(), "inode-state"))
	require.NoError(t, err)
	store := common.NewInodeStoreFromBackend(file)
	prepared := &PreparedSync{syncer: &syncer{inodeStore: store}, running: true}
	require.Error(t, prepared.Close())
	require.Same(t, store, prepared.syncer.inodeStore)
	prepared.running, prepared.retainResources = false, true
	require.Error(t, prepared.Close())
	require.Same(t, store, prepared.syncer.inodeStore)
	prepared.retainResources = false
	require.NoError(t, prepared.Close())
	require.Nil(t, prepared.syncer.inodeStore)
	require.NoError(t, prepared.Close())
	require.ErrorContains(t, prepared.Enumerate(context.Background()), "closed")
}

func TestCheckpoint5PreservedLinkCounters(t *testing.T) {
	tracker := newSyncProgressTracker(common.NewJobID(), nil, common.EFromTo.FileNFSFileNFS())
	for _, entity := range []common.EntityType{common.EEntityType.Hardlink(), common.EEntityType.Symlink()} {
		tracker.incSourceEnumeration(entity, common.ESymlinkHandlingType.Preserve(), common.PreserveHardlinkHandlingType)
		tracker.incDestEnumeration(entity, common.ESymlinkHandlingType.Preserve(), common.PreserveHardlinkHandlingType)
	}
	require.Equal(t, uint64(2), tracker.getSourceFilesScanned())
	require.Equal(t, uint64(2), tracker.getDestinationFilesScanned())
	require.Zero(t, tracker.getSkippedHardlinkCount())
	require.Zero(t, tracker.getSkippedSymlinkCount())
}

func TestCheckpoint5SyncOrdersPersistOperationIdentity(t *testing.T) {
	for _, command := range []string{"", "sc source destination", "s source destination", "copy display-only"} {
		t.Run(command, func(t *testing.T) {
			var orders []common.CopyJobPartOrderRequest
			job := &syncer{
				opts: &cookedSyncOptions{
					dryrun: true, hardlinks: common.PreserveHardlinkHandlingType,
					dryrunJobPartOrderHandler: func(order common.CopyJobPartOrderRequest) common.CopyJobPartOrderResponse {
						orders = append(orders, order)
						return common.CopyJobPartOrderResponse{JobStarted: true}
					},
				},
				spt: newSyncProgressTracker(common.NewJobID(), nil),
			}

			template := m5HardlinkTemplate()
			template.CommandString = command
			processor := job.newSyncTransferProcessor(1, template)
			require.NoError(t, processor.ScheduleTransfer(common.CopyTransfer{EntityType: common.EEntityType.File()}))
			require.NoError(t, processor.ScheduleTransfer(common.CopyTransfer{
				EntityType: common.EEntityType.Hardlink(), TargetHardlinkFile: "anchor",
			}))
			_, err := processor.DispatchFinalPart()
			require.NoError(t, err)
			require.NotEmpty(t, orders)
			for _, order := range orders {
				require.True(t, order.IsSyncJob)
				require.Equal(t, command, order.CommandString)
			}
		})
	}
}

func TestCheckpoint5DeletionPreservesExplicitSourceSymlinkFollowing(t *testing.T) {
	for _, deletion := range []common.DeleteDestination{common.EDeleteDestination.False(), common.EDeleteDestination.True(), common.EDeleteDestination.Prompt()} {
		for _, symlinks := range []common.SymlinkHandlingType{common.ESymlinkHandlingType.Skip(), common.ESymlinkHandlingType.Follow(), common.ESymlinkHandlingType.Preserve()} {
			options := &cookedSyncOptions{fromTo: common.EFromTo.LocalFileNFS()}
			require.NoError(t, options.applyDefaultsAndInferOptions(SyncOptions{DeleteDestination: deletion, Symlinks: symlinks}))
			expectedSource, expectedDestination := symlinks, symlinks
			if deletion != common.EDeleteDestination.False() {
				expectedDestination = common.ESymlinkHandlingType.Preserve()
				if symlinks == common.ESymlinkHandlingType.Skip() {
					expectedSource = common.ESymlinkHandlingType.Preserve()
				}
			}
			require.Equal(t, expectedSource, options.srcSymLinkTracker)
			require.Equal(t, expectedDestination, options.destSymlinks)
			require.Equal(t, symlinks, options.symlinks)
		}
	}
}
