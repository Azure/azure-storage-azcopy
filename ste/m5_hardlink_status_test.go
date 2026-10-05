package ste

import (
	"testing"

	"github.com/Azure/azure-storage-azcopy/v10/common"
	"github.com/stretchr/testify/require"
)

type hardlinkCompletionTestPart struct {
	IJobPartMgr
	message xferDoneMsg
}

func (p *hardlinkCompletionTestPart) SendXferDoneMsg(message xferDoneMsg)           { p.message = message }
func (*hardlinkCompletionTestPart) ReportTransferDone(common.TransferStatus) uint32 { return 1 }

func TestM5HardlinkCompletionCountersRespectHandlingMode(t *testing.T) {
	for _, test := range []struct {
		name     string
		entity   common.EntityType
		mode     common.HardlinkHandlingType
		target   string
		hardlink bool
	}{
		{"preserved-anchor", common.EEntityType.Hardlink(), common.EHardlinkHandlingType.Preserve(), "", true},
		{"preserved-link", common.EEntityType.Hardlink(), common.EHardlinkHandlingType.Preserve(), "anchor", true},
		{"followed-link", common.EEntityType.Hardlink(), common.EHardlinkHandlingType.Follow(), "", false},
		{"metadata-only", common.EEntityType.FileProperties(), common.EHardlinkHandlingType.Preserve(), "", false},
	} {
		t.Run(test.name, func(t *testing.T) {
			part := &hardlinkCompletionTestPart{}
			manager := &jobPartTransferMgr{
				jobPartMgr:          part,
				jobPartPlanTransfer: &JobPartPlanTransfer{atomicTransferStatus: common.ETransferStatus.Success()},
				cancel:              func() {},
				transferInfo: &TransferInfo{
					EntityType: test.entity, HardlinkHandlingType: test.mode,
					TargetHardlinkFilePath: test.target,
				},
			}
			require.Equal(t, uint32(1), manager.ReportTransferDone())
			require.Equal(t, test.hardlink, part.message.IsHardlink)
			require.Equal(t, common.ETransferStatus.Success(), part.message.TransferStatus)
		})
	}
}
