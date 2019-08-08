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
		v := getMainPoolSize(i)
		c.Assert(v.Value, chk.Equals, minConcurrency)
	}

	// moderately powerful machines
	for i := 5; i < 19; i++ {
		v := getMainPoolSize(i)
		c.Assert(v.Value, chk.Equals, 16*i)
	}

	// powerful machines
	for i := 19; i < 24; i++ {
		v := getMainPoolSize(i)
		c.Assert(v.Value, chk.Equals, maxConcurrency)
	}
}
