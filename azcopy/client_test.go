package azcopy

import (
	"github.com/Azure/azure-storage-azcopy/v10/common"
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestInitialization(t *testing.T) {
	a := assert.New(t)

	_, err := NewClient()

	a.Nil(err)
	a.NotEmpty(common.AzcopyJobPlanFolder)
	a.NotEmpty(common.LogPathFolder)
}
