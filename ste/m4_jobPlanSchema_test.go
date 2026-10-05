package ste

import (
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"unsafe"

	"github.com/Azure/azure-storage-azcopy/v10/common"
	"github.com/stretchr/testify/require"
)

func createM4TestPlan(t *testing.T) JobPartPlanFileName {
	t.Helper()
	jobID := common.NewJobID()
	directory := filepath.Join(".", "m4-schema-"+jobID.String())
	require.NoError(t, os.Mkdir(directory, 0700))
	previous := common.AzcopyJobPlanFolder
	common.AzcopyJobPlanFolder = directory
	t.Cleanup(func() {
		common.AzcopyJobPlanFolder = previous
		require.NoError(t, os.RemoveAll(directory))
	})
	name := JobPartPlanFileName(fmt.Sprintf(JobPartPlanFileNameFormat, jobID, 0, DataSchemaVersion))
	name.Create(common.CopyJobPartOrderRequest{
		JobID: jobID, IsFinalPart: true, FromTo: common.EFromTo.LocalBlob(),
		SourceRoot: common.ResourceString{Value: "source"}, DestinationRoot: common.ResourceString{Value: "destination"},
		PreservePOSIXProperties: true, PosixPropertiesStyle: common.AMLFSPosixPropertiesStyle,
		S2SGetPropertiesInBackend: true, S2SSourceChangeValidation: false, DestLengthValidation: true,
		SymlinkHandlingType: common.ESymlinkHandlingType.Preserve(),
		Transfers: common.Transfers{List: []common.CopyTransfer{{
			Source: "file", Destination: "file", EntityType: common.EEntityType.File(),
		}}},
	})
	return name
}

func TestM5Schema22PreservesPOSIXRoundTrip(t *testing.T) {
	name := createM4TestPlan(t)
	mapped := name.Map()
	defer func() {
		if mapped != nil {
			mapped.Unmap()
		}
	}()
	plan := mapped.Plan()
	require.Equal(t, common.Version(22), plan.Version)
	require.Equal(t, common.AMLFSPosixPropertiesStyle, plan.PosixPropertiesStyle)
	require.True(t, plan.PreservePOSIXProperties)
	require.True(t, plan.S2SGetPropertiesInBackend)
	require.False(t, plan.S2SSourceChangeValidation)
	require.True(t, plan.DestLengthValidation)
	require.Equal(t, common.ESymlinkHandlingType.Preserve(), plan.SymlinkHandling)
	require.Equal(t, unsafe.Offsetof(plan.PreservePOSIXProperties)+unsafe.Sizeof(plan.PreservePOSIXProperties), unsafe.Offsetof(plan.PosixPropertiesStyle))
	require.Equal(t, unsafe.Offsetof(plan.PosixPropertiesStyle)+unsafe.Sizeof(plan.PosixPropertiesStyle), unsafe.Offsetof(plan.S2SGetPropertiesInBackend))
	plan.Transfer(0).SetTransferStatus(common.ETransferStatus.FolderExisted(), true)
	mapped.Unmap()
	mapped = nil
	mapped = name.Map()
	require.Equal(t, common.ETransferStatus.FolderExisted(), mapped.Plan().Transfer(0).TransferStatus())
	mapped.Unmap()
	mapped = nil
}

func TestM4RejectsSchema20Header(t *testing.T) {
	name := createM4TestPlan(t)
	mapped := name.Map()
	mapped.Plan().Version = 20
	mapped.Unmap()
	require.PanicsWithError(t, "job part plan header schema 20 is unsupported; this binary requires schema 22", func() {
		name.Map()
	})
}
