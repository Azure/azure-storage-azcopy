package azcopy

import (
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestInitialization(t *testing.T) {
	a := assert.New(t)

	_, err := NewClient()

	a.Nil(err)
}
