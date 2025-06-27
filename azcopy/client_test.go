package azcopy

import (
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestInitialization(t *testing.T) {
	a := assert.New(t)

	c, err := NewClient()

	a.Nil(err)
	a.NotEmpty(c.JobPlanFolder)
	a.NotEmpty(c.LogPathFolder)
}
