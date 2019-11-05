package sddl

import (
	"reflect"

	chk "gopkg.in/check.v1"
)

func (s *GoSDDLTestSuite) TestParseACEResourceAttributeValuesType(c *chk.C) {
	parseTests := []struct {
		input     string
		result    ACEResourceAttributeValuesType
		expectErr bool
	}{
		{ // Parse an actual type
			input:  "TU",
			result: EACEResourceAttributeValuesType.SDDL_UINT(),
		},
		{ // Parse an actual lowercase type
			input:  "tu",
			result: EACEResourceAttributeValuesType.SDDL_UINT(),
		},
		{ // Parse a fake type
			input:     "nonreal",
			expectErr: true,
		},
	}

	for _, v := range parseTests {
		t, err := ParseACEResourceAttributeValuesType(v.input)

		if v.expectErr {
			c.Assert(err, chk.NotNil)
		} else {
			c.Assert(err, chk.IsNil)
			c.Assert(t, chk.Equals, v.result)
		}
	}
}

func (s *GoSDDLTestSuite) TestACEResourceAttributeValuesTypeToString(c *chk.C) {
	// Get all types available to the enum base
	rType := reflect.TypeOf(EACEResourceAttributeValuesType)
	rMethods := make([]func(eACEResourceAttributeValuesType) ACEResourceAttributeValuesType, 0)

	for i := 0; i < rType.NumMethod(); i++ {
		f := rType.Method(0).Func
		rMethods = append(rMethods, f.Interface().(func(valuesType eACEResourceAttributeValuesType) ACEResourceAttributeValuesType))
	}

	// Iterate over the functions and stringify them.
	for _, v := range rMethods {
		str := v(EACEResourceAttributeValuesType).String()
		c.Assert(v(EACEResourceAttributeValuesType), chk.Equals, shorthandACEResourceAttributeValuesTypes[str])
	}

	// Test a nonexistant attribute type
	// It should panic.
	testPanic(func() {
		_ = ACEResourceAttributeValuesType(1 << 7).String()
	}, c)
}
