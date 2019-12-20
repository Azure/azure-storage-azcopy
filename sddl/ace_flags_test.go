package sddl

import (
	chk "gopkg.in/check.v1"
)

func (s *GoSDDLTestSuite) TestParseACEFlags(c *chk.C) {
	parsingTests := []struct {
		input       string
		result      ACEFlags
		errExpected bool
	}{
		{ // test a single flag
			input:  "CI",
			result: EACEFlags.SDDL_CONTAINER_INHERIT(),
		},
		{ // test multiple flags
			input:  "CINP",
			result: EACEFlags.SDDL_CONTAINER_INHERIT() | EACEFlags.SDDL_NO_PROPOGATE(),
		},
		{ // test multiple flags with an overlap in between
			input:  "IOID", // overlaps with object_inherit. This should not happen.
			result: EACEFlags.SDDL_INHERIT_ONLY() | EACEFlags.SDDL_INHERITED(),
		},
		{ // test non-existant flag
			input:       "nonexist",
			errExpected: true,
		},
		{ // test no flags
			input:  "",
			result: EACEFlags.NO_FLAGS(),
		},
		{ // test lowercase flags
			input:  "cinp",
			result: EACEFlags.SDDL_CONTAINER_INHERIT() | EACEFlags.SDDL_NO_PROPOGATE(),
		},
		{ // test multiple lowercase flags with an overlap in between
			input:  "ioid", // overlaps with object_inherit. This should not happen.
			result: EACEFlags.SDDL_INHERIT_ONLY() | EACEFlags.SDDL_INHERITED(),
		},
	}

	for _, v := range parsingTests {
		aceFlags, err := ParseACEFlags(v.input)

		if v.errExpected {
			c.Assert(err, chk.NotNil)
		} else {
			c.Assert(err, chk.IsNil)
			c.Assert(aceFlags, chk.Equals, v.result)
		}
	}
}

// If the parse test fails, THIS TEST IS UNRELIABLE.
// No existing test cases would/SHOULD overlap accidentally.
// This test RELIES upon parsing if we can't find the flag on an even boundary.
// This is because of the way we re-construct ACE flags.
// using a range over a map results in inconsistent ordering.
// THIS IS OK, though. There is no need for two different parsers within this library, just for testing.
// That would be bug-prone.
func (s *GoSDDLTestSuite) TestACEFlagsToString(c *chk.C) {
	toStringTests := []struct {
		input  ACEFlags
		result string // Contains all of these shorthand flags
	}{
		{ // test single flag
			input:  EACEFlags.SDDL_CONTAINER_INHERIT(),
			result: "CI",
		},
		{ // test a concatenation of flags
			input:  EACEFlags.SDDL_CONTAINER_INHERIT() | EACEFlags.SDDL_NO_PROPOGATE(),
			result: "CINP",
		},
		{ // test a nonexistant flag
			input:  1 << 7,
			result: "",
		},
	}

	for _, v := range toStringTests {
		output := v.input.String()

		c.Assert(output, chk.Equals, v.result)
	}
}
