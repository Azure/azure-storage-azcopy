package sddl

import (
	"runtime"

	chk "gopkg.in/check.v1"
)

func (s *GoSDDLTestSuite) TestParseShorthandSID(c *chk.C) {
	sid, err := ParseSID("LA")

	c.Assert(err, chk.IsNil)
	c.Assert(sid, chk.DeepEquals, SID{Shorthand: "LA"})

	// Test toPortable
	if runtime.GOOS == "windows" {
		pSID, err := sid.ToPortable()

		c.Assert(err, chk.IsNil)
		c.Assert(pSID, chk.Not(chk.DeepEquals), sid)
	}
}

func (s *GoSDDLTestSuite) TestParseLongformSID(c *chk.C) {
	sid, err := ParseSID("S-1-5-32-544")

	c.Assert(err, chk.IsNil)
	c.Assert(sid, chk.DeepEquals, SID{Revision: 1, IdentifierAuthority: 5, Subauthorities: []int64{32, 544}})
}

func (s *GoSDDLTestSuite) TestSIDToString(c *chk.C) {
	toStringTests := []struct {
		input  SID
		output string
	}{
		{
			input:  SID{Shorthand: "IN"},
			output: "IN", // invalid shortform SIDs fail to become portable on windows, and default to their short form.
		},
		{
			input:  SID{Revision: 1, IdentifierAuthority: 5, Subauthorities: []int64{32, 544}},
			output: "S-1-5-32-544",
		},
	}

	if runtime.GOOS == "windows" {
		dst, err := SID{Shorthand: "LA"}.ToPortable() // This basically directly calls StringToSID

		if err == nil {
			c.Assert(err, chk.IsNil)

			toStringTests = append(toStringTests,
				struct {
					input  SID
					output string
				}{
					input:  SID{Shorthand: "LA"},
					output: dst.String(),
				},
			)

			c.Log("Testing non-portable \"LA\" SID to portable SID")
		}
	}

	for _, v := range toStringTests {
		str := v.input.String()

		c.Assert(str, chk.Equals, v.output)
	}
}
