package sddl

import (
	chk "gopkg.in/check.v1"
)

func (s *GoSDDLTestSuite) TestACLFlagsToString(c *chk.C) {
	toStringTests := []struct {
		input  ACLFlags
		result string
	}{
		{ // test single flag
			input:  EACLFlags.SDDL_PROTECTED(),
			result: "P",
		},
		{ // test multiple flags
			input:  EACLFlags.SDDL_PROTECTED() | EACLFlags.SDDL_AUTO_INHERIT_REQ(),
			result: "PAR",
		},
		{ // test no flags
			result: "",
		},
		{ // test some fake flags
			// This won't fail because we only worry about flags we know about.
			// On one hand, this feels like odd behavior. On the other hand, I'm not concerned by it.
			// Why should we check every bit of the input?
			input:  1 << 7,
			result: "",
		},
	}

	for _, v := range toStringTests {
		c.Assert(v.input.String(), chk.Equals, v.result)
	}
}

func (s *GoSDDLTestSuite) TestParseACLFlags(c *chk.C) {
	parsingTests := []struct {
		input       string
		result      ACLFlags
		errExpected bool
	}{
		{
			input:  "P", // test single flag
			result: EACLFlags.SDDL_PROTECTED(),
		},
		{
			input:  "PAR", // test multiple flags
			result: EACLFlags.SDDL_PROTECTED() | EACLFlags.SDDL_AUTO_INHERIT_REQ(),
		},
		{
			input:  "NO_ACCESS_CONTROL", // test a really long flag
			result: EACLFlags.SDDL_NULL_ACL(),
		},
		{
			input:       "",    // test no flags
			errExpected: false, // explicit no err expected
		},
		{
			input:  "par", // test lowercase flags
			result: EACLFlags.SDDL_PROTECTED() | EACLFlags.SDDL_AUTO_INHERIT_REQ(),
		},
		{
			input:       "invalidflag", // test an invalid flag
			errExpected: true,
		},
	}

	for _, v := range parsingTests {
		flags, err := ParseACLFlags(v.input)
		if v.errExpected {
			c.Assert(err, chk.NotNil)
		} else {
			c.Assert(err, chk.IsNil)
			c.Assert(flags, chk.Equals, v.result)
		}
	}
}
