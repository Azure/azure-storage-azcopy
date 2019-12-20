package sddl

import (
	"reflect"

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

// Because all combinable structs are now consistently regenerated, checking exact order is safe.
func (s *GoSDDLTestSuite) TestACERightsToString(c *chk.C) {
	toStringTests := []struct {
		input  ACERights
		result string
	}{
		{ // single right
			input:  EACERights.SDDL_CREATE_CHILD(),
			result: "CC",
		},
		{ // multiple rights
			input:  EACERights.SDDL_WRITE_OWNER() | EACERights.SDDL_WRITE_DAC(),
			result: "WDWO",
		},
		{ // test a nonexistant right
			input: 1 << 31,
		},
	}

	for _, v := range toStringTests {
		output := v.input.String()

		c.Assert(output, chk.Equals, v.result)
	}
}
