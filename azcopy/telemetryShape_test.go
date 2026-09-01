package azcopy

import (
	"context"
	"testing"

	"github.com/Azure/azure-storage-azcopy/v10/common"
	"github.com/Azure/azure-storage-azcopy/v10/traverser"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestSourceShapeTracker(t *testing.T) {
	tracker := newSourceShapeTracker(common.ELocation.Blob(), common.ESymlinkHandlingType.Preserve(), common.EHardlinkHandlingType.Follow())
	tracker.addScannedScope("one")
	tracker.addScannedScope("two")

	objects := []traverser.StoredObject{
		{EntityType: common.EEntityType.File(), Size: 0, RelativePath: "root.txt", ContainerName: "one"},
		{EntityType: common.EEntityType.File(), Size: 512, RelativePath: "a/file.txt", ContainerName: "one"},
		{EntityType: common.EEntityType.Hardlink(), Size: 8 * 1024, RelativePath: "a/b/file.txt", ContainerName: "two"},
		{EntityType: common.EEntityType.Symlink(), Size: 2 * 1024 * 1024, RelativePath: "a/b/c/link", ContainerName: "two"},
		{EntityType: common.EEntityType.File(), Size: 32 * 1024 * 1024, RelativePath: "deep/a/b/c/d/file.bin", ContainerName: "two"},
		{EntityType: common.EEntityType.Folder(), Size: 0, RelativePath: "ignored"},
	}
	for _, object := range objects {
		assert.NoError(t, tracker.recordScanned(object))
	}
	tracker.accountScope = true
	tracker.recordScheduled(objects[0])
	tracker.recordScheduled(objects[3])

	summary := tracker.snapshot()
	assert.Equal(t, int64(5), summary.ObjectsScanned)
	assert.Equal(t, int64(35660288), summary.BytesScanned)
	assert.Equal(t, 7132057.6, summary.AverageObjectSizeBytes)
	assert.Equal(t, int64(16*1024), summary.ObjectSizeP50BytesApprox)
	assert.Equal(t, int64(256*1024*1024), summary.ObjectSizeP90BytesApprox)
	assert.Equal(t, int64(256*1024*1024), summary.ObjectSizeP95BytesApprox)
	assert.Equal(t, int64(3), summary.ObjectsUnder1MiB)
	assert.Equal(t, 60.0, summary.ObjectsUnder1MiBRatioPct)
	assert.Equal(t, int64(5), summary.MaxDirectoryDepth)
	assert.Equal(t, int64(2), summary.ContainersScanned)
	assert.Equal(t, int64(2), summary.ContainersTouched)
	assert.Zero(t, summary.BucketsScanned)
}

func TestSourceShapeTrackerEmptyAndDirectBucket(t *testing.T) {
	tracker := newSourceShapeTracker(common.ELocation.S3(), common.ESymlinkHandlingType.Skip(), common.EHardlinkHandlingType.Follow())
	tracker.addScannedScope(directSourceScopeKey)
	assert.NoError(t, tracker.recordScanned(traverser.StoredObject{EntityType: common.EEntityType.Symlink(), Size: 10}))
	tracker.recordScheduled(traverser.StoredObject{EntityType: common.EEntityType.File()})

	summary := tracker.snapshot()
	assert.Zero(t, summary.ObjectsScanned)
	assert.Zero(t, summary.ObjectSizeP50BytesApprox)
	assert.Zero(t, summary.BytesScanned)
	assert.Zero(t, summary.AverageObjectSizeBytes)
	assert.Zero(t, summary.ObjectsUnder1MiBRatioPct)
	assert.Equal(t, int64(1), summary.BucketsScanned)
	assert.Equal(t, int64(1), summary.BucketsTouched)
}

func TestSourceShapeTrackerDerivesAccountScopesWithoutPreEnumeration(t *testing.T) {
	tracker := newSourceShapeTracker(common.ELocation.Blob(), common.ESymlinkHandlingType.Skip(), common.EHardlinkHandlingType.Follow())
	accountTraverser := traverser.NewBlobAccountTraverser(nil, "", context.Background(), traverser.InitResourceTraverserOptions{})

	require.NoError(t, tracker.initializeScopes(accountTraverser))
	for _, object := range []traverser.StoredObject{
		{EntityType: common.EEntityType.File(), ContainerName: "one"},
		{EntityType: common.EEntityType.File(), ContainerName: "one"},
		{EntityType: common.EEntityType.File(), ContainerName: "two"},
		{EntityType: common.EEntityType.File()},
	} {
		require.NoError(t, tracker.recordScanned(object))
		tracker.recordScheduled(object)
	}

	summary := tracker.snapshot()
	assert.Equal(t, int64(2), summary.ContainersScanned)
	assert.Equal(t, int64(2), summary.ContainersTouched)
}

func TestRelativePathDepth(t *testing.T) {
	assert.Equal(t, int64(0), relativePathDepth(""))
	assert.Equal(t, int64(0), relativePathDepth("file.txt"))
	assert.Equal(t, int64(2), relativePathDepth("a/b/file.txt"))
	assert.Equal(t, int64(2), relativePathDepth(`a\b\file.txt`))
}

func TestTransferScheduledObserver(t *testing.T) {
	template := &common.CopyJobPartOrderRequest{
		Fpo:                 common.EFolderPropertiesOption.NoFolders(),
		SymlinkHandlingType: common.ESymlinkHandlingType.Skip(),
	}
	processor := NewCopyTransferProcessor(false, template, 10, common.ResourceString{}, common.ResourceString{}, nil, nil, false, false, nil)
	var scheduled []traverser.StoredObject
	processor.SetTransferScheduledObserver(func(object traverser.StoredObject) {
		scheduled = append(scheduled, object)
	})

	file := traverser.StoredObject{EntityType: common.EEntityType.File(), RelativePath: "file.txt", ContainerName: "one"}
	folder := traverser.StoredObject{EntityType: common.EEntityType.Folder(), RelativePath: "folder", ContainerName: "two"}
	assert.NoError(t, processor.ScheduleSyncRemoveSetPropertiesTransfer(file))
	assert.NoError(t, processor.ScheduleSyncRemoveSetPropertiesTransfer(folder))
	assert.Equal(t, []traverser.StoredObject{file}, scheduled)
	assert.Len(t, processor.dispatcher.PendingTransfers.List, 1)
}
