package cmd

import (
	"strconv"
	"testing"
	"time"

	"github.com/Azure/azure-sdk-for-go/sdk/azcore/to"
	"github.com/Azure/azure-storage-azcopy/v10/common"
	"github.com/stretchr/testify/require"
)

func TestAMLFSCommandOptions(t *testing.T) {
	oldNFS := common.IsNFSCopy()
	common.SetNFSFlag(false)
	t.Cleanup(func() { common.SetNFSFlag(oldNFS) })
	source := t.TempDir()
	destination := "https://account.blob.core.windows.net/container"

	for _, value := range []string{"", "standard", "amlfs", "invalid"} {
		t.Run(value, func(t *testing.T) {
			raw := getDefaultCopyRawInput(source, destination)
			raw.preservePOSIXProperties = true
			raw.posixPropertiesStyle = value
			copyOptions, err := raw.toOptions()
			if value == "invalid" {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
				want := common.StandardPosixPropertiesStyle
				if value == "amlfs" {
					want = common.AMLFSPosixPropertiesStyle
				}
				require.Equal(t, want, copyOptions.PosixPropertiesStyle())
				require.Equal(t, want.String(), copyOptions.ToStringMap()["posixPropertiesStyle"])
			}

			syncOptions, err := CookRawSyncCmdArgs(RawMoverSyncCmdArgs{
				Src: source, Dst: destination, FromTo: "LocalBlob",
				PreservePOSIXProperties: true, PosixPropertiesStyle: value,
				Recursive: true, DeleteDestination: "false",
				Md5ValidationOption:  common.DefaultHashValidationOption.String(),
				CompareHash:          common.ESyncHashType.None().String(),
				LocalHashStorageMode: common.EHashStorageMode.Default().String(),
			})
			if value == "invalid" {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
				require.Equal(t, copyOptions.PosixPropertiesStyle(), syncOptions.posixPropertiesStyle)
				require.Equal(t, copyOptions.PosixPropertiesStyle().String(), syncOptions.ToStringMap()["posixPropertiesStyle"])
			}
		})
	}
}

func TestAMLFSValidation(t *testing.T) {
	require.NoError(t, validatePosixPropertiesStyle(common.StandardPosixPropertiesStyle, false, common.EFromTo.LocalFileNFS()))
	require.Error(t, validatePosixPropertiesStyle(common.AMLFSPosixPropertiesStyle, false, common.EFromTo.LocalBlob()))
	require.Error(t, validatePosixPropertiesStyle(common.AMLFSPosixPropertiesStyle, true, common.EFromTo.LocalFileNFS()))
	require.Error(t, validatePosixPropertiesStyle(common.PosixPropertiesStyle(2), true, common.EFromTo.LocalBlob()))
	require.NoError(t, validatePosixPropertiesStyle(common.AMLFSPosixPropertiesStyle, true, common.EFromTo.BlobBlob()))
}

func TestAMLFSIncrementalComparison(t *testing.T) {
	oldNFS := common.IsNFSCopy()
	common.SetNFSFlag(false)
	t.Cleanup(func() { common.SetNFSFlag(oldNFS) })
	modTime := time.Unix(1767366245, 123456789)
	metadata := common.Metadata{
		common.POSIXModTimeMeta:     to.Ptr(modTime.Format(common.AMLFS_MOD_TIME_LAYOUT)),
		common.POSIXModTimeNanoMeta: to.Ptr(strconv.FormatInt(modTime.UnixNano(), 10)),
		common.POSIXCTimeMeta:       to.Ptr(strconv.FormatInt(modTime.UnixNano(), 10)),
	}
	source := StoredObject{
		entityType: common.EEntityType.File(), size: 100,
		lastWriteTime: modTime, changeTime: modTime,
	}
	destination := StoredObject{entityType: common.EEntityType.File(), size: 100}
	destination.tryUpdateTimestampsFromMetadata(metadata)
	comparator := syncDestinationComparator{orchestratorOptions: &SyncOrchestratorOptions{metaDataOnlySync: true}}

	dataChanged, metadataChanged := comparator.compareSourceAndDestinationObject(source, destination)
	require.False(t, dataChanged, "an unchanged AMLFS file must not transfer again")
	require.False(t, metadataChanged)

	source.lastWriteTime = modTime.Add(time.Nanosecond)
	dataChanged, _ = comparator.compareSourceAndDestinationObject(source, destination)
	require.True(t, dataChanged, "same-size, same-second content edits must be copied")

	source.lastWriteTime = modTime
	source.changeTime = modTime.Add(time.Nanosecond)
	dataChanged, metadataChanged = comparator.compareSourceAndDestinationObject(source, destination)
	require.False(t, dataChanged)
	require.True(t, metadataChanged, "chmod/chown changes must still schedule a metadata-only transfer")

	delete(metadata, common.POSIXModTimeNanoMeta)
	destination.tryUpdateTimestampsFromMetadata(metadata)
	source.lastWriteTime = modTime.Truncate(time.Second)
	dataChanged, _ = comparator.compareSourceAndDestinationObject(source, destination)
	require.True(t, dataChanged, "external AMLFS blobs without precise timestamps require conservative comparison")

	metadata[common.POSIXModTimeMeta] = to.Ptr(strconv.FormatInt(source.lastWriteTime.UnixNano(), 10))
	source.changeTime = modTime
	destination.tryUpdateTimestampsFromMetadata(metadata)
	dataChanged, metadataChanged = comparator.compareSourceAndDestinationObject(source, destination)
	require.False(t, dataChanged, "Standard metadata behavior must be unchanged")
	require.False(t, metadataChanged)
}
