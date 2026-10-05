package cmd

import (
	"net/url"
	"sync/atomic"
	"testing"

	"github.com/Azure/azure-storage-azcopy/v10/azcopy"
	"github.com/Azure/azure-storage-azcopy/v10/common"
	"github.com/stretchr/testify/assert"
)

func TestM3SyncResourceStringsPreserveQueries(t *testing.T) {
	args := cookedSyncCmdArgs{
		fromTo: common.EFromTo.BlobBlob(),
		source: common.ResourceString{
			Value: "https://source.blob.core.windows.net/container",
			SAS:   "sig=source%2Bsignature", ExtraQuery: "snapshot=source-snapshot",
		},
		destination: common.ResourceString{
			Value: "https://destination.blob.core.windows.net/container",
			SAS:   "sig=destination%2Bsignature", ExtraQuery: "versionid=destination-version",
		},
	}
	source, destination, err := args.syncResourceStrings()
	assert.NoError(t, err)
	sourceURL, err := url.Parse(source)
	assert.NoError(t, err)
	destinationURL, err := url.Parse(destination)
	assert.NoError(t, err)
	assert.Equal(t, "source+signature", sourceURL.Query().Get("sig"))
	assert.Equal(t, "source-snapshot", sourceURL.Query().Get("snapshot"))
	assert.Equal(t, "destination+signature", destinationURL.Query().Get("sig"))
	assert.Equal(t, "destination-version", destinationURL.Query().Get("versionid"))
}

func TestM3SyncResourceStringsPreserveLocalPath(t *testing.T) {
	args := cookedSyncCmdArgs{
		fromTo:      common.EFromTo.LocalBlob(),
		source:      common.ResourceString{Value: `C:\source folder\child`},
		destination: common.ResourceString{Value: "https://destination.blob.core.windows.net/container"},
	}
	source, _, err := args.syncResourceStrings()
	assert.NoError(t, err)
	assert.Equal(t, args.source.Value, source)

	args.source.SAS = "sig=invalid-local-query"
	_, _, err = args.syncResourceStrings()
	assert.ErrorContains(t, err, "local sync resource")
}

func TestM3SyncResourceStringsReportParseError(t *testing.T) {
	args := cookedSyncCmdArgs{
		fromTo:      common.EFromTo.BlobBlob(),
		source:      common.ResourceString{Value: "https://source.blob.core.windows.net/%invalid"},
		destination: common.ResourceString{Value: "https://destination.blob.core.windows.net/container"},
	}
	_, _, err := args.syncResourceStrings()
	assert.ErrorContains(t, err, "invalid sync source")
	args.source.Value = "https://source.blob.core.windows.net/container"
	args.destination.Value = "https://destination.blob.core.windows.net/%invalid"
	_, _, err = args.syncResourceStrings()
	assert.ErrorContains(t, err, "invalid sync destination")
}

func TestM3SyncCookDoesNotInheritClientJobID(t *testing.T) {
	previousJobID := Client.CurrentJobID
	Client.CurrentJobID = common.NewJobID()
	defer func() { Client.CurrentJobID = previousJobID }()
	raw := rawSyncCmdArgs{
		src:    "https://source.blob.core.windows.net/container",
		dst:    "https://destination.blob.core.windows.net/container",
		fromTo: "BlobBlob", trailingDot: "Enable", deleteDestination: "false",
		compareHash: "None", localHashStorageMode: "HiddenFiles", md5ValidationOption: common.DefaultHashValidationOption.String(),
		hardlinks: "follow", SrcCredName: "source-token", DstCredName: "destination-token",
	}
	args, err := raw.cook()
	assert.NoError(t, err)
	assert.True(t, args.JobId().IsEmpty())
	assert.Equal(t, raw.SrcCredName, args.SrcCredName)
	assert.Equal(t, raw.DstCredName, args.DstCredName)
}

func TestM3SyncEmptyHardlinksUsesLegacyDefault(t *testing.T) {
	for _, direction := range []string{"BlobBlob", "FileFile", "FileNFSFileNFS"} {
		t.Run(direction, func(t *testing.T) {
			raw := rawSyncCmdArgs{
				src:    "https://source.blob.core.windows.net/container",
				dst:    "https://destination.blob.core.windows.net/container",
				fromTo: direction, trailingDot: "Enable", deleteDestination: "false",
				compareHash: "None", localHashStorageMode: "HiddenFiles", md5ValidationOption: common.DefaultHashValidationOption.String(),
			}

			args, err := raw.toCookedOptions()
			assert.NoError(t, err)
			assert.Equal(t, common.DefaultHardlinkHandlingType, args.hardlinks)

			raw.hardlinks = "skip"
			args, err = raw.toCookedOptions()
			assert.NoError(t, err)
			assert.Equal(t, common.EHardlinkHandlingType.Skip(), args.hardlinks)
		})
	}
}

func TestM3SyncHashMetaDirectoryCompatibility(t *testing.T) {
	previous := common.LocalHashDir
	common.LocalHashDir = "legacy-hash-directory"
	defer func() { common.LocalHashDir = previous }()
	raw := rawSyncCmdArgs{
		src:    "https://source.blob.core.windows.net/container",
		dst:    "https://destination.blob.core.windows.net/container",
		fromTo: "BlobBlob", trailingDot: "Enable", deleteDestination: "false",
		compareHash: "None", localHashStorageMode: "HiddenFiles", md5ValidationOption: common.DefaultHashValidationOption.String(),
	}
	args, err := raw.toCookedOptions()
	assert.NoError(t, err)
	assert.Equal(t, common.LocalHashDir, args.hashMetaDir)

	raw.hashMetaDir = "explicit-hash-directory"
	args, err = raw.toCookedOptions()
	assert.NoError(t, err)
	assert.Equal(t, raw.hashMetaDir, args.hashMetaDir)
	assert.Equal(t, "legacy-hash-directory", common.LocalHashDir)
}

func TestM3PreparedSyncStateMirrorsLegacyGetters(t *testing.T) {
	args := cookedSyncCmdArgs{}
	state := azcopy.PreparedSyncState{
		SyncEnumerationStats: azcopy.SyncEnumerationStats{
			SourceFilesScanned: 11, DestinationFilesScanned: 12,
			SourceFoldersScanned: 13, DestinationFoldersScanned: 14,
			SourceFilesTransferNotRequired: 15, SourceFoldersTransferNotRequired: 16,
			SourceFileEnumerationFailed: 17, SourceFolderEnumerationFailed: 18,
			DestinationFolderEnumerationFailed: 19, DestinationFolderEnumerationSkipped: 20,
			SkippedArchiveFileCount: 21,
		},
		FirstPartOrdered: true, ScanningComplete: true, DeletionCount: 22,
		SkippedSymlinkCount: 23, SkippedSpecialFileCount: 24, SkippedHardlinkCount: 25,
	}
	args.applyPreparedState(state)
	assert.Equal(t, uint64(11), args.GetSourceFilesScanned())
	assert.Equal(t, uint64(12), args.GetDestinationFilesScanned())
	assert.Equal(t, uint64(13), args.GetSourceFoldersScanned())
	assert.Equal(t, uint64(14), args.GetDestinationFoldersScanned())
	assert.Equal(t, uint64(15), args.GetSourceFilesTransferredNotRequired())
	assert.Equal(t, uint64(16), args.GetSourceFoldersTransferredNotRequired())
	assert.Equal(t, uint64(17), args.GetSourceFileEnumerationFailed())
	assert.Equal(t, uint64(18), args.GetSourceFolderEnumerationFailed())
	assert.Equal(t, uint64(19), args.GetDestinationFolderEnumerationFailed())
	assert.Equal(t, uint64(20), args.GetDestinationFolderEnumerationSkipped())
	assert.Equal(t, uint64(21), args.GetSkippedArchiveFileCount())
	assert.True(t, args.FirstPartOrdered())
	assert.True(t, args.ScanningComplete())
	assert.Equal(t, uint32(22), args.GetDeletionCount())
	assert.Equal(t, uint32(23), args.GetSymlinkSkipped())
	assert.Equal(t, uint32(24), args.GetSpecialFileSkipped())
	assert.Equal(t, uint32(25), atomic.LoadUint32(&args.atomicSkippedHardlinkCount))
	assert.Equal(t, "22", args.ToStringMap()["deletionCount"])

	args.applyPreparedState(azcopy.PreparedSyncState{})
	assert.False(t, args.FirstPartOrdered())
	assert.False(t, args.ScanningComplete())
	assert.Zero(t, args.GetSourceFilesScanned())
	assert.Zero(t, args.GetSourceFoldersTransferredNotRequired())
	assert.Zero(t, args.GetSourceFileEnumerationFailed())
	assert.Zero(t, args.GetDestinationFolderEnumerationSkipped())
	assert.Zero(t, args.GetSkippedArchiveFileCount())
	assert.Zero(t, args.GetDeletionCount())
	assert.Zero(t, args.GetSymlinkSkipped())
	assert.Zero(t, args.GetSpecialFileSkipped())
	assert.Zero(t, atomic.LoadUint32(&args.atomicSkippedHardlinkCount))
}

func TestM3SyncEnumerationCallbackMirrorsFullState(t *testing.T) {
	args := cookedSyncCmdArgs{}
	handler := cliSyncHandler{cooked: &args}
	stats := azcopy.SyncEnumerationStats{
		SourceFilesScanned: 7, DestinationFilesScanned: 8,
		SourceFilesTransferNotRequired: 9, SkippedArchiveFileCount: 10,
		FirstPartOrdered: true, ScanningComplete: true, DeletionCount: 11,
		SkippedSymlinkCount: 12, SkippedSpecialFileCount: 13, SkippedHardlinkCount: 14,
	}
	handler.OnEnumerationStats(stats)
	assert.Equal(t, uint64(7), args.GetSourceFilesScanned())
	assert.Equal(t, uint64(8), args.GetDestinationFilesScanned())
	assert.Equal(t, uint64(9), args.GetSourceFilesTransferredNotRequired())
	assert.Equal(t, uint64(10), args.GetSkippedArchiveFileCount())
	assert.True(t, args.FirstPartOrdered())
	assert.True(t, args.ScanningComplete())
	assert.Equal(t, uint32(11), args.GetDeletionCount())
	assert.Equal(t, uint32(12), args.GetSymlinkSkipped())
	assert.Equal(t, uint32(13), args.GetSpecialFileSkipped())
	assert.Equal(t, uint32(14), atomic.LoadUint32(&args.atomicSkippedHardlinkCount))

	handler.applySummary(common.ListJobSummaryResponse{}, 0)
	handler.applyMoverStats(stats)
	assert.Equal(t, uint64(7), args.GetSourceFilesScanned())
	assert.Equal(t, uint64(10), args.GetSkippedArchiveFileCount())
	assert.Equal(t, uint32(11), args.GetDeletionCount())
	assert.Equal(t, uint32(12), args.GetSymlinkSkipped())

	handler.OnEnumerationStats(azcopy.SyncEnumerationStats{})
	assert.False(t, args.FirstPartOrdered())
	assert.False(t, args.ScanningComplete())
	assert.Zero(t, args.GetDeletionCount())
	assert.Zero(t, args.GetSymlinkSkipped())
	assert.Zero(t, args.GetSpecialFileSkipped())
	assert.Zero(t, atomic.LoadUint32(&args.atomicSkippedHardlinkCount))
}
