package sddl

import (
	chk "gopkg.in/check.v1"
	"reflect"
)

func (s *GoSDDLTestSuite) TestACETypeParse(c *chk.C) {
	// Attempt to parse all real types
	for k, v := range reverseShorthandACEType {
		aceType, err := ParseACEType(k)
		c.Assert(err, chk.IsNil)
		c.Assert(aceType, chk.Equals, v)
	}

	// Parse a fake type, and ensure we error out
	_, err := ParseACEType("notrealtype")
	c.Assert(err, chk.NotNil)
}

func (s *GoSDDLTestSuite) TestACETypeToString(c *chk.C) {
	// Get all types available to the enum base
	rType := reflect.TypeOf(EACEType)
	rMethods := make([]func(eACEType) ACEType, 0)

	for i := 0; i < rType.NumMethod(); i++ {
		f := rType.Method(i).Func
		rMethods = append(rMethods, f.Interface().(func(eACEType) ACEType))
	}

	// Iterate over the functions and stringify them.
	for _, v := range rMethods {
		str := v(EACEType).String()
		c.Assert(v(EACEType), chk.Equals, reverseShorthandACEType[str])
	}

	// test an invalid type
	x := ACEType(255)
	testPanic(func() { // this function is expected to panic because 255 is invalid
		_ = x.String()
	}, c)
}
