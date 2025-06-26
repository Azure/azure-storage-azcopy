package azcopy

import (
	"github.com/Azure/azure-storage-azcopy/v10/common"
	"github.com/Azure/azure-storage-azcopy/v10/jobsAdmin"
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestInitialization(t *testing.T) {
	a := assert.New(t)

	_, err := NewClient()

	a.Nil(err)
	a.NotNil(jobsAdmin.JobsAdmin)
	a.NotEmpty(common.AzcopyJobPlanFolder)
}
