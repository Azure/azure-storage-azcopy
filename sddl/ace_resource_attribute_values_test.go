package sddl

import (
	"strings"

	chk "gopkg.in/check.v1"
)

func (s *GoSDDLTestSuite) TestACEResourceAttributeValuesToString(c *chk.C) {
	toStringTests := []struct {
		input       ACEResourceAttributeValues
		output      string
		expectPanic bool
	}{
		{ // Valid example taken from https://docs.microsoft.com/en-us/windows/win32/secauthz/ace-strings
			input: ACEResourceAttributeValues{
				Name:           "Project",
				Type:           EACEResourceAttributeValuesType.SDDL_WSTRING(),
				AttributeFlags: 0,
				Items:          []string{`"Windows"`, `"SQL"`},
			},
			output: `("Project",TS,0,"Windows","SQL")`,
		},
		{ // Valid example taken from https://docs.microsoft.com/en-us/windows/win32/secauthz/ace-strings
			input: ACEResourceAttributeValues{
				Name:           "Secrecy",
				Type:           EACEResourceAttributeValuesType.SDDL_UINT(),
				AttributeFlags: 0,
				Items:          []string{`3`},
			},
			output: `("Secrecy",TU,0,3)`,
		},
		{ // Invalid resource attributes type
			input: ACEResourceAttributeValues{
				Name:           "test",
				Type:           255,
				AttributeFlags: 0,
				Items:          []string{`3`},
			},
			expectPanic: true,
		},
	}

	for _, v := range toStringTests {
		if v.expectPanic {
			testPanic(func() {
				_ = v.input.StringifyResourceAttribute()
			}, c)
		} else {
			str := v.input.StringifyResourceAttribute()
			c.Assert(str, chk.Equals, v.output)
		}
	}
}

func (s *GoSDDLTestSuite) TestParseACEResourceAttributeValues(c *chk.C) {
	parseTests := []struct {
		input       string
		output      ACEResourceAttributeValues
		expectErr   bool
		errContains string
	}{
		{ // Valid example taken from https://docs.microsoft.com/en-us/windows/win32/secauthz/ace-strings
			input: `("Project",TS,0,"Windows","SQL")`,
			output: ACEResourceAttributeValues{
				Name:           "Project",
				Type:           EACEResourceAttributeValuesType.SDDL_WSTRING(),
				AttributeFlags: 0,
				Items:          []string{`"Windows"`, `"SQL"`},
			},
		},
		{ // Valid example taken from https://docs.microsoft.com/en-us/windows/win32/secauthz/ace-strings
			input: `("Secrecy",TU,0,3)`,
			output: ACEResourceAttributeValues{
				Name:           "Secrecy",
				Type:           EACEResourceAttributeValuesType.SDDL_UINT(),
				AttributeFlags: 0,
				Items:          []string{`3`},
			},
		},
		{ // Test an invalid resource attribute missing a parenthesis
			input:       `("Secrecy", TU, 0, 3`,
			expectErr:   true,
			errContains: "lacks surrounding parentheses",
		},
		{ // Test meta characters within strings and spaces outside strings
			input: `(",()", TU, 0, 3)`,
			output: ACEResourceAttributeValues{
				Name:           ",()",
				Type:           EACEResourceAttributeValuesType.SDDL_UINT(),
				AttributeFlags: 0,
				Items:          []string{`3`},
			},
		},
		{ // test non-contiguous data
			input:       `("test", T U, 0, 3)`,
			expectErr:   true,
			errContains: "contiguous",
		},
		{ // test lowercase resource attribute types
			input: `("test", tu, 0, 3)`,
			output: ACEResourceAttributeValues{
				Name:           "test",
				Type:           EACEResourceAttributeValuesType.SDDL_UINT(),
				AttributeFlags: 0,
				Items:          []string{`3`},
			},
		},
	}

	for _, v := range parseTests {
		attrib, err := ParseACEResourceAttributeValues(v.input)

		if v.expectErr {
			c.Assert(err, chk.NotNil)
			if v.errContains != "" && !strings.Contains(err.Error(), v.errContains) {
				c.Errorf("expected %s within err: %s", v.errContains, err.Error())
			}
		} else {
			c.Assert(err, chk.IsNil)
			c.Assert(attrib, chk.DeepEquals, v.output)
		}
	}
}
