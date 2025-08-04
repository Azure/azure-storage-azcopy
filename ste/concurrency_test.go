package ste

import (
	"github.com/stretchr/testify/assert"
	"testing"
)

const (
	minConcurrency = 32
	maxConcurrency = 300
)

func TestConcurrencyValue(t *testing.T) {
	a := assert.New(t)
	// weak machines
	for i := 1; i < 5; i++ {
		min, max := getMainPoolSize(i)
		a.Equal(minConcurrency, min)
		a.Equal(minConcurrency, max.Value)
	}

	// moderately powerful machines
	for i := 5; i < 19; i++ {
		min, max := getMainPoolSize(i)
		a.Equal(16*i, min)
		a.Equal(16*i, max.Value)
	}

	// powerful machines
	for i := 19; i < 24; i++ {
		min, max := getMainPoolSize(i)
		a.Equal(maxConcurrency, min)
		a.Equal(maxConcurrency, max.Value)
	}
}
