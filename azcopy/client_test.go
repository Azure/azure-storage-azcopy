package azcopy

import (
	"github.com/Azure/azure-storage-azcopy/v10/common"
	"github.com/stretchr/testify/assert"
	"os"
	"testing"
)

func TestInitialization(t *testing.T) {
	a := assert.New(t)

	_, err := NewClient()

	a.Nil(err)
	a.NotEmpty(common.AzcopyJobPlanFolder)
	a.NotEmpty(common.LogPathFolder)
}

func TestClientInitialization_CustomLocations(t *testing.T) {
	a := assert.New(t)
	tempLogDir := t.TempDir()
	tempPlanDir := t.TempDir()

	t.Setenv(common.EEnvironmentVariable.LogLocation().Name, tempLogDir)
	t.Setenv(common.EEnvironmentVariable.JobPlanLocation().Name, tempPlanDir)

	_, err := NewClient()
	a.Nil(err)

	// Verify paths are the custom ones
	a.Equal(tempLogDir, common.LogPathFolder)
	a.Equal(tempPlanDir, common.AzcopyJobPlanFolder)

	// Verify directories were created
	logInfo, err := os.Stat(common.LogPathFolder)
	a.Nil(err)
	a.True(logInfo.IsDir())

	planInfo, err := os.Stat(common.AzcopyJobPlanFolder)
	a.Nil(err)
	a.True(planInfo.IsDir())
}
