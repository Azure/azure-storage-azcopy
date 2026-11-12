package ste

import (
	"sync"
	"testing"

	"github.com/Azure/azure-storage-azcopy/v10/common"
	"github.com/Azure/azure-storage-azcopy/v10/common/buildmode"
	"github.com/stretchr/testify/assert"
)

func TestFolderCreationTracker_lockPolicy(t *testing.T) {
	tests := []struct {
		fromTo            common.FromTo
		unlockedWhenMover bool
	}{
		{common.EFromTo.LocalFileSMB(), true},
		{common.EFromTo.LocalFileNFS(), true},
		{common.EFromTo.LocalBlob(), true},
		{common.EFromTo.LocalBlobFS(), true},
		{common.EFromTo.BlobFSFile(), true},
		{common.FromToValue(common.ELocation.BlobFS(), common.ELocation.FileNFS()), true},
		{common.EFromTo.BlobFSBlob(), true},
		{common.EFromTo.BlobFSBlobFS(), true},
		{common.EFromTo.FileSMBFileSMB(), true},
		{common.EFromTo.FileSMBFileNFS(), true},
		{common.EFromTo.FileNFSFileSMB(), true},
		{common.EFromTo.FileNFSFileNFS(), true},
		{common.EFromTo.FileBlob(), false},
		{common.EFromTo.FileBlobFS(), false},
		{common.FromToValue(common.ELocation.FileNFS(), common.ELocation.Blob()), false},
		{common.FromToValue(common.ELocation.FileNFS(), common.ELocation.BlobFS()), false},
		{common.EFromTo.BlobFile(), false},
		{common.EFromTo.BlobBlob(), false},
		{common.EFromTo.FileSMBLocal(), false},
		{common.EFromTo.FileNFSLocal(), false},
		{common.EFromTo.BlobFSLocal(), false},
	}

	for _, tt := range tests {
		t.Run(tt.fromTo.String(), func(t *testing.T) {
			for _, fpo := range []common.FolderPropertyOption{
				common.EFolderPropertiesOption.AllFolders(),
				common.EFolderPropertiesOption.AllFoldersExceptRoot(),
			} {
				tracker := NewFolderCreationTracker(fpo, &JobPartPlanHeader{FromTo: tt.fromTo}).(*jpptFolderTracker)
				assert.Equal(t, !buildmode.IsMover || !tt.unlockedWhenMover, tracker.lockFolderCreation)
			}
			tracker := NewFolderCreationTracker(common.EFolderPropertiesOption.NoFolders(), &JobPartPlanHeader{FromTo: tt.fromTo})
			assert.IsType(t, &nullFolderTracker{}, tracker)
		})
	}
}

func TestFolderCreationTracker_fileToFileCreationOutsideLock(t *testing.T) {
	if !buildmode.IsMover {
		t.Skip("concurrent File-to-File creation is specific to Mover")
	}

	for _, fromTo := range []common.FromTo{
		common.EFromTo.FileSMBFileSMB(),
		common.EFromTo.FileSMBFileNFS(),
		common.EFromTo.FileNFSFileSMB(),
		common.EFromTo.FileNFSFileNFS(),
	} {
		t.Run(fromTo.String(), func(t *testing.T) {
			tracker := NewFolderCreationTracker(common.EFolderPropertiesOption.AllFolders(), &JobPartPlanHeader{FromTo: fromTo}).(*jpptFolderTracker)
			tracker.plan = &mockedJPPH{
				status: []*JobPartPlanTransfer{
					{atomicTransferStatus: common.ETransferStatus.NotStarted()},
					{atomicTransferStatus: common.ETransferStatus.NotStarted()},
				},
			}

			err := tracker.CreateFolder("created-before-registration", func() error {
				if !tracker.mu.TryLock() {
					t.Error("directory creation must not hold the shared tracker lock")
					return nil
				}
				tracker.mu.Unlock()
				tracker.RegisterPropertiesTransfer("registered-before-creation", 0)
				return nil
			})
			assert.NoError(t, err)
			assert.True(t, tracker.IsFolderAlreadyCreated("created-before-registration"))
			tracker.RegisterPropertiesTransfer("created-before-registration", 1)
			assert.True(t, tracker.IsFolderAlreadyCreated("created-before-registration"))

			var wg sync.WaitGroup
			for i := 0; i < 50; i++ {
				wg.Add(1)
				go func() {
					defer wg.Done()
					assert.NoError(t, tracker.CreateFolder("registered-before-creation", func() error { return nil }))
					assert.NoError(t, tracker.CreateFolder("unregistered", func() error { return nil }))
				}()
			}
			wg.Wait()
			assert.True(t, tracker.IsFolderAlreadyCreated("registered-before-creation"))
			assert.True(t, tracker.IsFolderAlreadyCreated("unregistered"))
		})
	}
}
