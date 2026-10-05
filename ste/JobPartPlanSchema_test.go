package ste

import (
	"fmt"
	"testing"

	"github.com/Azure/azure-storage-azcopy/v10/common"
	"github.com/stretchr/testify/require"
)

func TestM1JobPlanSchema(t *testing.T) {
	jobID := common.NewJobID()
	name := func(version common.Version) JobPartPlanFileName {
		return JobPartPlanFileName(fmt.Sprintf(JobPartPlanFileNameFormat, jobID, 0, version))
	}
	for _, legacy := range []common.Version{19, 20} {
		_, _, err := name(legacy).Parse()
		require.ErrorContains(t, err, "data schema version")
	}

	require.Equal(t, common.Version(21), DataSchemaVersion)
	parsedID, part, err := name(DataSchemaVersion).Parse()
	require.NoError(t, err)
	require.Equal(t, jobID, parsedID)
	require.Zero(t, part)
}
