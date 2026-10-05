package ste

import (
	"testing"

	"github.com/Azure/azure-sdk-for-go/sdk/azcore/to"
	"github.com/Azure/azure-storage-azcopy/v10/common"
	"github.com/stretchr/testify/require"
)

type amlfsSymlinkTestSource struct {
	ISourceInfoProvider
	metadata common.Metadata
}

func (s *amlfsSymlinkTestSource) GetUNIXProperties() (common.UnixStatAdapter, error) {
	return common.ReadStatFromMetadata(&common.SafeMetadata{Metadata: s.metadata}, 0)
}

func (*amlfsSymlinkTestSource) HasUNIXProperties() bool { return true }

type amlfsSymlinkTestTransfer struct {
	IJobPartTransferMgr
	info TransferInfo
}

func (m *amlfsSymlinkTestTransfer) Info() *TransferInfo       { return &m.info }
func (*amlfsSymlinkTestTransfer) Log(common.LogLevel, string) {}

func TestM4AMLFSSymlinkLinuxValidator(t *testing.T) {
	metadata := common.Metadata{
		common.AMLFSOwnerMeta: to.Ptr("123"), common.AMLFSGroupMeta: to.Ptr("456"),
		common.POSIXModeMeta: to.Ptr("0777"), common.POSIXSymlinkMeta: to.Ptr("true"),
		common.POSIXModTimeMeta: to.Ptr("2026-01-02 03:04:05 +0000"),
	}
	sender := &blobSymlinkSender{
		jptm: &amlfsSymlinkTestTransfer{info: TransferInfo{
			PreservePOSIXProperties: true, PosixPropertiesStyle: common.AMLFSPosixPropertiesStyle,
		}},
		sip:             &amlfsSymlinkTestSource{metadata: metadata},
		metadataToApply: &common.SafeMetadata{Metadata: metadata},
	}
	require.NoError(t, sender.getExtraProperties())
	require.Equal(t, "0777", *sender.metadataToApply.Metadata[common.POSIXModeMeta])
	require.Equal(t, "true", *sender.metadataToApply.Metadata[common.POSIXSymlinkMeta])
	require.NotContains(t, metadata, common.POSIXNlinkMeta, "source metadata must not be modified")
}

func TestM4AMLFSRegularFileFailsLinuxSymlinkValidator(t *testing.T) {
	metadata := common.Metadata{
		common.AMLFSOwnerMeta: to.Ptr("123"), common.POSIXModeMeta: to.Ptr("0755"),
	}
	sender := &blobSymlinkSender{
		jptm: &amlfsSymlinkTestTransfer{info: TransferInfo{
			PreservePOSIXProperties: true, PosixPropertiesStyle: common.AMLFSPosixPropertiesStyle,
		}},
		sip:             &amlfsSymlinkTestSource{metadata: metadata},
		metadataToApply: &common.SafeMetadata{Metadata: metadata},
	}
	require.ErrorContains(t, sender.getExtraProperties(), "GetUNIXProperties did not return symlink properties")
}
