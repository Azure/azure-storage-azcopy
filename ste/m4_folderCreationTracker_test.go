package ste

import (
	"sync/atomic"
	"testing"
	"time"

	"github.com/Azure/azure-storage-azcopy/v10/common"
	"github.com/stretchr/testify/require"
)

func TestM4FolderCacheWithoutProperties(t *testing.T) {
	tracker := NewFolderCreationTracker(common.EFolderPropertiesOption.NoFolders(), nil, common.EFromTo.LocalFile())
	_, ok := tracker.(*jpptFolderTracker)
	require.True(t, ok)
	calls := 0
	create := func() error {
		calls++
		return common.FolderCreationErrorAlreadyExists{}
	}
	require.NoError(t, tracker.CreateFolder("https://example.file.core.windows.net/share/%66older?sig=first", create))
	require.NoError(t, tracker.CreateFolder("https://example.file.core.windows.net/share/folder?sig=second", create))
	require.Equal(t, 1, calls)
	_, isNull := NewFolderCreationTracker(common.EFolderPropertiesOption.NoFolders(), nil, common.EFromTo.LocalBlob()).(*nullFolderTracker)
	require.True(t, isNull)
}

func TestM4FolderExistingStateSurvivesUnregistration(t *testing.T) {
	mapped := true
	transfer := &JobPartPlanTransfer{atomicTransferStatus: common.ETransferStatus.FolderExisted()}
	tracker := NewFolderCreationTracker(common.EFolderPropertiesOption.AllFolders(), func(JpptFolderIndex) *JobPartPlanTransfer {
		require.True(t, mapped, "tracker retained an unmapped plan reference")
		return transfer
	}).(*jpptFolderTracker)
	const key = "https://example.file.core.windows.net/share/folder"
	tracker.RegisterPropertiesTransfer(key, 1, 0)
	require.False(t, tracker.ShouldSetProperties(key, common.EOverwriteOption.False(), nil))
	tracker.StopTracking(key)
	mapped = false
	require.NotContains(t, tracker.contents, key)
	require.NoError(t, tracker.CreateFolder(key+"?sig=ignored", func() error {
		t.Fatal("known pre-existing folder was recreated after properties completion")
		return nil
	}))
}

func TestM4FolderUnlockedCreatePrefersActualCreation(t *testing.T) {
	transfer := &JobPartPlanTransfer{atomicTransferStatus: common.ETransferStatus.NotStarted()}
	tracker := NewFolderCreationTrackerInt(common.EFolderPropertiesOption.AllFolders(), func(JpptFolderIndex) *JobPartPlanTransfer {
		return transfer
	}, false).(*jpptFolderTracker)
	tracker.RegisterPropertiesTransfer("folder", 0, 0)
	entered, release, done := make(chan struct{}), make(chan struct{}), make(chan error, 1)
	go func() {
		done <- tracker.CreateFolder("folder", func() error {
			close(entered)
			<-release
			return nil
		})
	}()
	<-entered
	require.NoError(t, tracker.CreateFolder("folder", func() error { return common.FolderCreationErrorAlreadyExists{} }))
	close(release)
	select {
	case err := <-done:
		require.NoError(t, err)
	case <-time.After(time.Second):
		t.Fatal("unlocked creation did not finish")
	}
	require.True(t, tracker.ShouldSetProperties("folder", common.EOverwriteOption.False(), nil))
	require.Equal(t, common.ETransferStatus.FolderCreated(), transfer.TransferStatus())
}

func TestM4FolderUnregistrationDuringUnlockedCreation(t *testing.T) {
	var mapped atomic.Bool
	mapped.Store(true)
	transfer := &JobPartPlanTransfer{atomicTransferStatus: common.ETransferStatus.NotStarted()}
	tracker := NewFolderCreationTrackerInt(common.EFolderPropertiesOption.AllFolders(), func(JpptFolderIndex) *JobPartPlanTransfer {
		require.True(t, mapped.Load(), "creation reused a released plan index")
		return transfer
	}, false).(*jpptFolderTracker)
	tracker.RegisterPropertiesTransfer("folder", 1, 0)
	entered, release, done := make(chan struct{}), make(chan struct{}), make(chan error, 1)
	go func() {
		done <- tracker.CreateFolder("folder", func() error {
			close(entered)
			<-release
			return nil
		})
	}()
	<-entered
	tracker.StopTracking("folder")
	mapped.Store(false)
	close(release)
	select {
	case err := <-done:
		require.NoError(t, err)
	case <-time.After(time.Second):
		t.Fatal("creation retained or blocked on a released plan")
	}
	require.Nil(t, tracker.contents["folder"].Index)
}
