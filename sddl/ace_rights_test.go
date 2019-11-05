package sddl

import (
	"reflect"
	"strings"

	chk "gopkg.in/check.v1"
)

func (s *GoSDDLTestSuite) TestParseACERights(c *chk.C) {
	parseTests := []struct {
		input     string
		result    ACERights
		expectErr bool
	}{
		{ // test single right
			input:  "CC",
			result: EACERights.SDDL_CREATE_CHILD(),
		},
		{ // test multiple rights
			input:  "SWLC",
			result: EACERights.SDDL_SELF_WRITE() | EACERights.SDDL_LIST_CHILDREN(),
		},
		{ // test multiple rights with overlap
			input:  "CCRC", // CR overlaps in center
			result: EACERights.SDDL_CREATE_CHILD() | EACERights.SDDL_READ_CONTROL(),
		},
		{ // test multiple lowercase rights with overlap
			input:  "ccrc",
			result: EACERights.SDDL_CREATE_CHILD() | EACERights.SDDL_READ_CONTROL(),
		},
		{}, // test no rights
		{ // test nonexistant rights
			input:     "nonexist",
			expectErr: true,
		},
	}

	for _, v := range parseTests {
		rights, err := ParseACERights(v.input)

		if v.expectErr {
			c.Assert(err, chk.NotNil)
		} else {
			c.Assert(err, chk.IsNil)
			c.Assert(rights, chk.Equals, v.result)
		}
	}
}

func (s *GoSDDLTestSuite) TestACERightsShorthands(c *chk.C) {
	// Get an available list of rights to the user
	rType := reflect.TypeOf(EACERights)
	rMethods := make(map[string]func(eACERights) ACERights)

	for i := 0; i < rType.NumMethod(); i++ {
		f := rType.Method(i).Func
		rMethods[rType.Method(i).Name] = f.Interface().(func(eACERights) ACERights)
	}

	// reverse the shorthands map for checking
	rights := make(map[ACERights]string)
	for k, v := range shorthandACERights {
		rights[v] = k
	}

	// check against that map to see all shorthand entries
	for k, v := range rMethods {
		if k == "NO_RIGHTS" {
			// skip no rights as it's not meant to be ToString'd anyway, and returns an empty one.
			continue
		}

		shorthand, ok := rights[v(EACERights)]
		c.Log("testing " + k + " (" + shorthand + ") for existence in shorthand table")
		c.Assert(ok, chk.Equals, true)
	}
}

// If the parse test fails, THIS TEST IS UNRELIABLE.
// No existing test cases would/SHOULD overlap accidentally.
// This test RELIES upon parsing if we can't find the flag on an even boundary.
// This is because of the way we re-construct ACE rights.
// using a range over a map results in inconsistent ordering.
// THIS IS OK, though. There is no need for two different parsers within this library, just for testing.
// That would be bug-prone.
func (s *GoSDDLTestSuite) TestACERightsToString(c *chk.C) {
	toStringTests := []struct {
		input  ACERights
		result []string
	}{
		{ // single right
			input:  EACERights.SDDL_CREATE_CHILD(),
			result: []string{"CC"},
		},
		{ // multiple rights
			input:  EACERights.SDDL_WRITE_OWNER() | EACERights.SDDL_WRITE_DAC(),
			result: []string{"WO", "WD"},
		},
		{ // test a nonexistant right
			input: 1 << 31,
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
					rights, err := ParseACERights(output)
					c.Assert(err, chk.IsNil)
					c.Assert(rights, chk.Equals, v.input)
					continue
				}
			}
		}
	}
}
