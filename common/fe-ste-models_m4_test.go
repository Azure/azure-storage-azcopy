package common

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestM4Schema21TransferStatusValues(t *testing.T) {
	require.Equal(t, int32(3), int32(ETransferStatus.FolderCreated()))
	require.Equal(t, int32(4), int32(ETransferStatus.FolderExisted()))
	require.Equal(t, int32(5), int32(ETransferStatus.Restarted()))
	require.False(t, ETransferStatus.FolderExisted().StatusLocked())
	require.False(t, ETransferStatus.Restarted().StatusLocked())
	require.True(t, ETransferStatus.Success().StatusLocked())
}

func TestM4POSIXStyleParsing(t *testing.T) {
	for _, test := range []struct {
		text string
		want PosixPropertiesStyle
	}{
		{"", StandardPosixPropertiesStyle},
		{"standard", StandardPosixPropertiesStyle},
		{"AMLFS", AMLFSPosixPropertiesStyle},
		{"amlfs", AMLFSPosixPropertiesStyle},
	} {
		var style PosixPropertiesStyle
		require.NoError(t, style.Parse(test.text))
		require.Equal(t, test.want, style)
	}
	var invalid PosixPropertiesStyle
	require.Error(t, invalid.Parse("unsupported"))
}
