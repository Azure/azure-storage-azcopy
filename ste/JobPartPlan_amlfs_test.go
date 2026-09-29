package ste

import (
	"fmt"
	"testing"

	"github.com/Azure/azure-storage-azcopy/v10/common"
	"github.com/stretchr/testify/require"
)

func TestAMLFSPlanPersistence(t *testing.T) {
	oldFolder := common.AzcopyJobPlanFolder
	common.AzcopyJobPlanFolder = t.TempDir()
	t.Cleanup(func() { common.AzcopyJobPlanFolder = oldFolder })
	for _, style := range []common.PosixPropertiesStyle{common.StandardPosixPropertiesStyle, common.AMLFSPosixPropertiesStyle} {
		t.Run(style.String(), func(t *testing.T) {
			order := common.CopyJobPartOrderRequest{
				JobID: common.NewJobID(), FromTo: common.EFromTo.LocalBlob(),
				PreservePOSIXProperties: true, PosixPropertiesStyle: style,
			}
			name := JobPartPlanFileName(fmt.Sprintf(JobPartPlanFileNameFormat, order.JobID, 0, DataSchemaVersion))
			name.Create(order)
			mapped := name.Map()
			require.Equal(t, style, mapped.Plan().PosixPropertiesStyle)
			require.True(t, mapped.Plan().PreservePOSIXProperties)
			require.Equal(t, common.Version(20), mapped.Plan().Version)
			mapped.Unmap()
			reopened := name.Map()
			defer reopened.Unmap()
			require.Equal(t, style, reopened.Plan().PosixPropertiesStyle)
		})
	}
}
