package cmd

import (
	"testing"

	"github.com/Azure/azure-storage-azcopy/v10/common"
	"github.com/Azure/azure-storage-azcopy/v10/common/cred"
	"github.com/stretchr/testify/require"
)

func TestM4SyncAdaptersPreservePOSIXStyle(t *testing.T) {
	previousManager := GetCredentialManager
	GetCredentialManager = func() cred.Manager { return nil }
	t.Cleanup(func() { GetCredentialManager = previousManager })
	for _, style := range []string{"", "standard", "amlfs", "invalid"} {
		t.Run(style, func(t *testing.T) {
			raw := rawSyncCmdArgs{
				src:    "https://source.blob.core.windows.net/container",
				dst:    "https://destination.blob.core.windows.net/container",
				fromTo: "BlobBlob", trailingDot: "Enable", deleteDestination: "false",
				compareHash: "None", localHashStorageMode: "HiddenFiles",
				md5ValidationOption:     common.DefaultHashValidationOption.String(),
				preservePOSIXProperties: true, posixPropertiesStyle: style,
				SrcCredName: "source-credential", DstCredName: "destination-credential",
			}
			options, err := raw.toOptions()
			if style == "invalid" {
				require.Error(t, err)
				return
			}
			require.NoError(t, err)
			expected := common.StandardPosixPropertiesStyle
			if style == "amlfs" {
				expected = common.AMLFSPosixPropertiesStyle
			}
			require.Equal(t, expected, options.PosixPropertiesStyle)
			require.True(t, options.PreservePosixProperties)
			require.Equal(t, raw.SrcCredName, options.SrcCredName)
			require.Equal(t, raw.DstCredName, options.DstCredName)
			require.True(t, options.JobID.IsEmpty())

			raw.preservePOSIXProperties = false
			_, err = raw.toOptions()
			if style == "amlfs" {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
			}
		})
	}
}
