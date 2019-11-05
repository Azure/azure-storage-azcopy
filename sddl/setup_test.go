package sddl

import (
	"testing"

	chk "gopkg.in/check.v1"
)

type GoSDDLTestSuite struct{}

var _ = chk.Suite(&GoSDDLTestSuite{})

func Test(t *testing.T) { chk.TestingT(t) }

func testPanic(panicFunc func(), c *chk.C) {
	defer func() {
		c.Assert(recover(), chk.NotNil)
	}()

	panicFunc()
}
