package cmd

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestToReversedString(t *testing.T) {
	a := assert.New(t)
	traverser := &benchmarkTraverser{}
	a.Equal("1", traverser.toReversedString(1))
	a.Equal("01", traverser.toReversedString(10))
	a.Equal("54321", traverser.toReversedString(12345))
}
