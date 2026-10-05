package common

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestM5HardlinkEnumsAndTransferClone(t *testing.T) {
	var mode HardlinkHandlingType
	require.NoError(t, mode.Parse("preserve"))
	require.Equal(t, PreserveHardlinkHandlingType, mode)
	require.Equal(t, uint8(0), uint8(DefaultHardlinkHandlingType))
	require.Equal(t, uint8(1), uint8(SkipHardlinkHandlingType))
	require.Equal(t, uint8(2), uint8(mode))
	require.Equal(t, uint8(0), uint8(EJobPartType.Mixed()))
	require.Equal(t, uint8(1), uint8(EJobPartType.Hardlink()))

	source := Transfers{
		List:                   []CopyTransfer{{Source: "link", TargetHardlinkFile: "anchor"}},
		HardlinksTransferCount: 1, HardlinksConvertedCount: 2, FilePropertyTransferCount: 3,
		TotalSizeInBytes: 4, FileTransferCount: 5, FolderTransferCount: 6, SymlinkTransferCount: 7,
	}
	cloned := source.Clone()
	require.Equal(t, source, cloned)
	cloned.List[0].TargetHardlinkFile = "other"
	require.Equal(t, "anchor", source.List[0].TargetHardlinkFile)
}
