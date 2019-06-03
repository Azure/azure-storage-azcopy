package common

import (
	chk "gopkg.in/check.v1"
)

type pathHandlerSuite struct{}

var _ = chk.Suite(&pathHandlerSuite{})

func (p *pathHandlerSuite) TestShortToLong(c *chk.C) {
	if OS_PATH_SEPARATOR == `\` {
		c.Assert(ToExtendedPath(`C:\myPath`), chk.Equals, `\\?\C:\myPath`)
		c.Assert(ToExtendedPath(`\\myHost\myPath`), chk.Equals, `\\?\UNC\myHost\myPath`)
		c.Assert(ToExtendedPath(`\\?\C:\myPath`), chk.Equals, `\\?\C:\myPath`)
		c.Assert(ToExtendedPath(`\\?\UNC\myHost\myPath`), chk.Equals, `\\?\UNC\myHost\myPath`)
	} else {
		c.Skip("Test only pertains to Windows.")
	}
}

func (p *pathHandlerSuite) TestLongToShort(c *chk.C) {
	if OS_PATH_SEPARATOR == `\` {
		c.Assert(ToShortPath(`\\?\C:\myPath`), chk.Equals, `C:\myPath`)
		c.Assert(ToShortPath(`\\?\UNC\myHost\myPath`), chk.Equals, `\\myHost\myPath`)
		c.Assert(ToShortPath(`\\myHost\myPath`), chk.Equals, `\\myHost\myPath`)
		c.Assert(ToShortPath(`C:\myPath`), chk.Equals, `C:\myPath`)
	} else {
		c.Skip("Test only pertains to Windows.")
	}
}
