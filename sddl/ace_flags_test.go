package sddl

import (
	"strings"

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
		result []string // Contains all of these shorthand flags
	}{
		{ // test single flag
			input:  EACEFlags.SDDL_CONTAINER_INHERIT(),
			result: []string{"CI"},
		},
		{ // test a concatenation of flags
			input:  EACEFlags.SDDL_CONTAINER_INHERIT() | EACEFlags.SDDL_NO_PROPOGATE(),
			result: []string{"CI", "NP"},
		},
		{ // test a nonexistant flag
			input:  1 << 7,
			result: []string{},
		},
	}

	for _, v := range toStringTests {
		output := v.input.String()

		if len(v.result) == 0 {
			c.Assert(output, chk.Equals, "")
		} else {
			for _, r := range v.result {
				// It should always be on the two-character boundary to ensure we don't get a weird overlap issue.
				idx := strings.Index(output, r)
				c.Assert(idx, chk.Not(chk.Equals), -1) // the string is not present if this triggers

				// If it's not on an even boundary, we just need to parse and check, as we're experiencing overlapping.
				if idx%2 != 0 {
					f, err := ParseACEFlags(output)
					c.Assert(err, chk.IsNil)
					c.Assert(f, chk.Equals, v.input)
					continue
				}
			}
		}
	}
}
