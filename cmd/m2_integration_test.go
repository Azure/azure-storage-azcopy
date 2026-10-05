package cmd

import (
	"testing"
	"time"

	"github.com/Azure/azure-storage-azcopy/v10/common"
	"github.com/Azure/azure-storage-azcopy/v10/traverser"
	"github.com/stretchr/testify/require"
)

func TestM2SyncOptionsAreCopiedPerJob(t *testing.T) {
	settings := NewSyncOrchestratorOptions(100, true, time.Time{}, false, 3)
	original := settings
	options := NewSyncEnumeratorOptions(2, &settings)
	first := options.forJob(common.EFromTo.LocalFileNFS())
	second := options.forJob(common.EFromTo.FileFile())
	require.NotSame(t, options, first)
	require.NotSame(t, first.SyncOrchOptions, second.SyncOrchOptions)
	require.Same(t, &settings, options.SyncOrchOptions)
	require.Equal(t, original, settings)
	require.Equal(t, options.ErrorChannel, first.ErrorChannel)
	require.NotEqual(t, *first.SyncOrchOptions, *second.SyncOrchOptions)
}

func TestM2SyncSchedulingPreservesTransferPaths(t *testing.T) {
	template := &common.CopyJobPartOrderRequest{
		FromTo:              common.EFromTo.S3Blob(),
		Fpo:                 common.EFolderPropertiesOption.NoFolders(),
		SymlinkHandlingType: common.ESymlinkHandlingType.Skip(),
	}

	processor := newCopyTransferProcessor(template, 1000, common.ResourceString{}, common.ResourceString{}, nil, nil, false, false)
	for _, name := range []string{"a.txt", "z.txt"} {
		require.NoError(t, processor.scheduleCopyTransfer(traverser.StoredObject{
			RelativePath: name, EntityType: common.EEntityType.File(), Size: 1,
		}))
	}
	require.Len(t, template.Transfers.List, 2)
	require.Equal(t, "/a.txt", template.Transfers.List[0].Source)
	require.Equal(t, "/a.txt", template.Transfers.List[0].Destination)
	require.Equal(t, "/z.txt", template.Transfers.List[1].Source)
	require.Equal(t, "/z.txt", template.Transfers.List[1].Destination)
}

func TestM2IncludeRootControlsRootPropertyTransfers(t *testing.T) {
	value := "root-value"
	for _, fromTo := range []common.FromTo{
		common.EFromTo.FileFile(), common.EFromTo.FileNFSFileNFS(),
		common.EFromTo.BlobFSBlobFS(), common.EFromTo.LocalFile(),
	} {
		t.Run(fromTo.String(), func(t *testing.T) {
			for _, includeRoot := range []bool{false, true} {
				fpo, _ := NewFolderPropertyOption(fromTo, true, !includeRoot, nil, true, false, false, false, false)
				root := traverser.StoredObject{
					EntityType: common.EEntityType.Folder(),
					Metadata:   common.Metadata{"root-key": &value},
				}
				transfer, scheduled := root.ToNewCopyTransfer(false, "", "", false, fpo,
					common.ESymlinkHandlingType.Skip(), common.DefaultHardlinkHandlingType)
				require.Equal(t, includeRoot, scheduled)
				if includeRoot {
					require.Equal(t, root.Metadata, transfer.Metadata)
				}
				child := root
				child.RelativePath = "child"
				_, scheduled = child.ToNewCopyTransfer(false, "/child", "/child", false, fpo,
					common.ESymlinkHandlingType.Skip(), common.DefaultHardlinkHandlingType)
				require.True(t, scheduled, "excluding the root must not exclude child folder properties")
			}
		})
	}
}
