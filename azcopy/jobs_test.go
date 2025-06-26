package azcopy

import (
	"github.com/Azure/azure-sdk-for-go/sdk/azcore/to"
	"github.com/Azure/azure-storage-azcopy/v10/common"
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestJobsClean(t *testing.T) {
	a := assert.New(t)

	c, err := NewClient()
	a.Nil(err)

	result, err := c.CleanJobs(CleanJobs{WithStatus: to.Ptr(common.EJobStatus.All())})
	a.GreaterOrEqual(result.Count, 0)
}
