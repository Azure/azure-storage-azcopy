package ste

import (
	chk "gopkg.in/check.v1"
)

type mainTestSuite struct{}

var _ = chk.Suite(&mainTestSuite{})

const (
	minConcurrency = 32
	maxConcurrency = 300
)

func (s *mainTestSuite) TestConcurrencyValue(c *chk.C) {
	// weak machines
	for i := 1; i < 5; i++ {
		min, max := getMainPoolSize(i, false)
		c.Assert(min, chk.Equals, minConcurrency)
		c.Assert(max.Value, chk.Equals, minConcurrency)
	}

	// moderately powerful machines
	for i := 5; i < 19; i++ {
		min, max := getMainPoolSize(i, false)
		c.Assert(min, chk.Equals, 16*i)
		c.Assert(max.Value, chk.Equals, 16*i)
	}

	// powerful machines
	for i := 19; i < 24; i++ {
		min, max := getMainPoolSize(i, false)
		c.Assert(min, chk.Equals, maxConcurrency)
		c.Assert(max.Value, chk.Equals, maxConcurrency)
	}
}
