package ste

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"unsafe"

	"github.com/Azure/azure-storage-azcopy/v10/common"
	"github.com/stretchr/testify/require"
)

func createM5Plan(t *testing.T, transfers []common.CopyTransfer, configure ...func(*common.CopyJobPartOrderRequest)) JobPartPlanFileName {
	t.Helper()
	jobID := common.NewJobID()
	dir := filepath.Join(".", "m5-schema-"+jobID.String())
	require.NoError(t, os.Mkdir(dir, 0700))
	previous := common.AzcopyJobPlanFolder
	common.AzcopyJobPlanFolder = dir
	t.Cleanup(func() {
		common.AzcopyJobPlanFolder = previous
		require.NoError(t, os.RemoveAll(dir))
	})
	name := JobPartPlanFileName(fmt.Sprintf(JobPartPlanFileNameFormat, jobID, 0, DataSchemaVersion))
	order := common.CopyJobPartOrderRequest{
		JobID: jobID, IsFinalPart: true, FromTo: common.EFromTo.FileNFSFileNFS(),
		SourceRoot:           common.ResourceString{Value: "https://source.file.core.windows.net/share/root"},
		DestinationRoot:      common.ResourceString{Value: "https://destination.file.core.windows.net/share/root"},
		HardlinkHandlingType: common.EHardlinkHandlingType.Preserve(),
		JobPartType:          common.EJobPartType.Hardlink(),
		PosixPropertiesStyle: common.AMLFSPosixPropertiesStyle,
		Transfers:            common.Transfers{List: transfers},
	}
	for _, change := range configure {
		change(&order)
	}
	name.Create(order)
	return name
}

func TestM5HardlinkPlanRoundTripAndOffsets(t *testing.T) {
	target := "anchors/" + strings.Repeat("x", 18000)
	name := createM5Plan(t, []common.CopyTransfer{
		{Source: strings.Repeat("s", 18000), Destination: "link", EntityType: common.EEntityType.Hardlink(),
			ContentType: "application/octet-stream", SourceSize: 4096, TargetHardlinkFile: target},
		{Source: "second", Destination: "other/link", EntityType: common.EEntityType.Hardlink(),
			TargetHardlinkFile: "anchors/second"},
	})
	mapped := name.Map()
	defer mapped.Unmap()
	plan := mapped.Plan()
	require.Equal(t, common.Version(22), plan.Version)
	require.Equal(t, common.EJobPartType.Hardlink(), plan.JobPartType)
	require.Equal(t, common.EHardlinkHandlingType.Preserve(), plan.HardlinkHandling)
	require.Equal(t, common.AMLFSPosixPropertiesStyle, plan.PosixPropertiesStyle)
	require.Equal(t, unsafe.Offsetof(plan.SymlinkHandling)+unsafe.Sizeof(plan.SymlinkHandling), unsafe.Offsetof(plan.HardlinkHandling))
	require.Equal(t, unsafe.Offsetof(plan.HardlinkHandling)+unsafe.Sizeof(plan.HardlinkHandling), unsafe.Offsetof(plan.JobPartType))

	first := plan.Transfer(0)
	require.Zero(t, first.SourceSize, "preserved non-anchor links do not copy file data")
	require.Greater(t, unsafe.Offsetof(first.TargetHardlinkFilePathLength), unsafe.Offsetof(first.errorMessage))
	first.SetErrorCode(123, true)
	first.SetErrorMessage("hardlink diagnostic retained", true)
	require.Equal(t, int32(123), first.ErrorCode())
	require.Equal(t, "hardlink diagnostic retained", first.ErrorMessage())
	headers, _, _, _, _, _, _, _, entity, _, _, _, decodedTarget := plan.TransferSrcPropertiesAndMetadataWithHardlink(0)
	require.Equal(t, target, decodedTarget)
	require.Equal(t, "application/octet-stream", headers.ContentType)
	require.Equal(t, common.EEntityType.Hardlink(), entity)
	_, _, _, _, _, _, _, _, _, _, _, _, secondTarget := plan.TransferSrcPropertiesAndMetadataWithHardlink(1)
	require.Equal(t, "anchors/second", secondTarget)
	src, dst := plan.TransferSrcDstRelatives(1)
	require.Equal(t, "second", src)
	require.Equal(t, "other/link", dst)
	legacyHeaders, _, _, _, _, _, _, _, _, _, _, _ := plan.TransferSrcPropertiesAndMetadata(0)
	require.Equal(t, headers, legacyHeaders)
}

func TestM5RejectsSchema21Header(t *testing.T) {
	name := createM5Plan(t, nil)
	mapped := name.Map()
	mapped.Plan().Version = 21
	mapped.Unmap()
	require.PanicsWithError(t, "job part plan header schema 21 is unsupported; this binary requires schema 22", func() { name.Map() })
}

func TestM5SyncIdentityIsPersistedNotParsedFromCommand(t *testing.T) {
	for _, command := range []string{"", "sc source destination", "s source destination", "sync source destination", "copy source destination"} {
		for _, isSync := range []bool{false, true} {
			t.Run(fmt.Sprintf("%q/sync=%v", command, isSync), func(t *testing.T) {
				name := createM5Plan(t, []common.CopyTransfer{{Source: "link", Destination: "link", EntityType: common.EEntityType.Hardlink()}},
					func(order *common.CopyJobPartOrderRequest) {
						order.CommandString = command
						order.IsSyncJob = isSync
					})
				mapped := name.Map()
				defer mapped.Unmap()
				require.Equal(t, isSync, mapped.Plan().IsSyncJob)
				require.Equal(t, command, mapped.Plan().CommandString(), "display command must not be rewritten")
				manager := &jobPartTransferMgr{jobPartMgr: &jobPartMgr{planMMF: mapped}}
				require.Equal(t, isSync, manager.Info().IsSyncJob)
				require.Equal(t, isSync, senderIsSyncJob(manager))
			})
		}
	}
}
