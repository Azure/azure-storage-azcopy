package cmd

import (
	"testing"

	"github.com/Azure/azure-storage-azcopy/v10/azcopy"
	"github.com/Azure/azure-storage-azcopy/v10/common"
	"github.com/spf13/cobra"
	"github.com/stretchr/testify/require"
)

func TestM4SMBValidationAcceptsOptionalPOSIXStyle(t *testing.T) {
	fromTo := common.EFromTo.BlobBlob()
	permissions := common.EPreservePermissionsOption.None()
	hardlinks := common.EHardlinkHandlingType.Skip()
	require.NoError(t, performSMBSpecificValidation(fromTo, permissions, false, false, hardlinks))
	require.NoError(t, performSMBSpecificValidation(fromTo, permissions, false, true, hardlinks, common.AMLFSPosixPropertiesStyle))
	require.ErrorContains(t, performSMBSpecificValidation(fromTo, permissions, false, false, hardlinks, common.AMLFSPosixPropertiesStyle), azcopy.POSIXStyleMisuse)
	require.ErrorContains(t, performSMBSpecificValidation(fromTo, permissions, false, true, hardlinks, common.PosixPropertiesStyle(255)), "unsupported POSIX properties style")
	require.ErrorContains(t, performSMBSpecificValidation(fromTo, permissions, false, true, hardlinks,
		common.StandardPosixPropertiesStyle, common.AMLFSPosixPropertiesStyle), "at most one")
}

func TestM4CopyRawOptionsForwardPutMD5AndCheckLength(t *testing.T) {
	previousJobID := Client.CurrentJobID
	Client.CurrentJobID = common.NewJobID()
	t.Cleanup(func() { Client.CurrentJobID = previousJobID })
	raw := rawCopyCmdArgs{
		src: "source.txt", dst: "https://destination.blob.core.windows.net/container",
		fromTo: common.EFromTo.LocalBlob().String(), putMd5: true, CheckLength: true,
	}
	raw.setMandatoryDefaults()
	options, err := raw.toCopyOptions(&cobra.Command{})
	require.NoError(t, err)
	require.True(t, options.PutMd5)
	require.True(t, options.CheckLength)
	require.True(t, options.JobID.IsEmpty())
}

func TestM4CopyAdaptersForwardAMLFSAndNamedCredentials(t *testing.T) {
	raw := rawCopyCmdArgs{
		src:         "https://source.blob.core.windows.net/container",
		dst:         "https://destination.blob.core.windows.net/container",
		fromTo:      common.EFromTo.BlobBlob().String(),
		SrcCredName: "source-credential", DstCredName: "destination-credential",
		preservePOSIXProperties: true,
	}
	raw.setMandatoryDefaults()
	raw.posixPropertiesStyle = "amlfs"
	options, err := raw.toCopyOptions(&cobra.Command{})
	require.NoError(t, err)
	require.Equal(t, common.AMLFSPosixPropertiesStyle, options.PosixPropertiesStyle)
	require.True(t, options.PreservePosixProperties)
	require.Equal(t, raw.SrcCredName, options.SourceCredentialName)
	require.Equal(t, raw.DstCredName, options.DestinationCredentialName)

	cooked, err := raw.toOptions()
	require.NoError(t, err)
	cooked.jobID = common.NewJobID()
	adapted, err := cooked.ToCopyOptions()
	require.NoError(t, err)
	require.Equal(t, common.AMLFSPosixPropertiesStyle, adapted.PosixPropertiesStyle)
	require.True(t, adapted.PreservePosixProperties)
	require.Equal(t, cooked.jobID, adapted.JobID)
	require.Equal(t, raw.SrcCredName, adapted.SourceCredentialName)
	require.Equal(t, raw.DstCredName, adapted.DestinationCredentialName)
}

func TestM4CopyRejectsUnrecognizedPOSIXStyle(t *testing.T) {
	raw := rawCopyCmdArgs{
		src:    "https://source.blob.core.windows.net/container",
		dst:    "https://destination.blob.core.windows.net/container",
		fromTo: common.EFromTo.BlobBlob().String(),
	}
	raw.setMandatoryDefaults()
	raw.posixPropertiesStyle = "unrecognized"
	_, err := raw.toCopyOptions(&cobra.Command{})
	require.Error(t, err)
	_, err = raw.toOptions()
	require.Error(t, err)
}
