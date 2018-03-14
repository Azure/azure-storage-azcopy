package common

import (
	"fmt"
	"reflect"
	"strconv"
)

type EnumHelper struct{}
type EnumValueComparer func(enumValue interface{}) bool

func (EnumHelper) String(enumType reflect.Type, underlyingType reflect.Type, valueComparer EnumValueComparer) string {
	// Pass 1 argument that is a zero-value of t
	args := [1]reflect.Value{reflect.Zero(enumType)}

	// Call enum methods looking for one that returns the same value we have
	for m := 0; m < enumType.NumMethod(); m++ {
		method := enumType.Method(m)
		if method.Type.NumIn() != 1 || method.Type.NumOut() != 1 || method.Type.Out(0) != enumType {
			// Only try methods that take 1 arg (the receiver) and return just 1 value whose type matches the enum's type
			continue
		}
		// Call the enum method, convert the result to an EnumInt32 and compare its value to the passed-in value.
		value := method.Func.Call(args[:])[0].Convert(underlyingType).Interface()
		if valueComparer(value) {
			return method.Name // We found a match, return the method's name (the enum's symbol)
		}
	}
	return ""
}

func (EnumHelper) Parse(enumType reflect.Type, underlyingType reflect.Type, s string) (interface{}, error) {
	// Look for a method name that matches the string we're trying to parse (case-sensitive)
	if m, found := enumType.MethodByName(s); found {
		// Pass 1 argument that is a zero-value of t.
		args := [1]reflect.Value{reflect.Zero(enumType)}

		// Call the enum type's method passing in the arg receiver; the returned t is converted to an EnumInt32
		// The caller must convert this to their exact type
		return m.Func.Call(args[:])[0].Convert(underlyingType).Interface(), nil
	}
	return nil, fmt.Errorf("Couldn't parse symbol %q into an instance of %q", s, enumType.Name())
}

/**********************************************************************************************************************/

type EnumInt32 struct {
	Value int32 // The enum's value
}

func (e EnumInt32) String(enumType reflect.Type) string {
	if s := (EnumHelper{}).String(enumType, reflect.TypeOf(e),
		func(value interface{}) bool { return value.(EnumInt32).Value == e.Value }); s != "" {
		return s
	}
	return strconv.FormatInt(int64(e.Value), 10) // No match, return the number as a string
}

func (e EnumInt32) Parse(enumType reflect.Type, s string, strict bool) (EnumInt32, error) {
	v, err := (EnumHelper{}).Parse(enumType, reflect.TypeOf(e), s)
	switch {
	case err == nil:
		return v.(EnumInt32), nil
	case strict:
		return EnumInt32{}, err
	}
	// strict is off: Try to parse s as a base 10 string of digits into an int32 & return its value
	i, parseErr := strconv.ParseInt(s, 10, 32)
	if parseErr != nil {
		parseErr = err
	}
	return EnumInt32{Value: int32(i)}, parseErr
}

/**********************************************************************************************************************/

type EnumUint32 struct {
	Value uint32 // The enum's value
}

func (e EnumUint32) String(enumType reflect.Type) string {
	if s := (EnumHelper{}).String(enumType, reflect.TypeOf(e),
		func(value interface{}) bool { return value.(EnumUint32).Value == e.Value }); s != "" {
		return s
	}
	return strconv.FormatUint(uint64(e.Value), 10) // No match, return the number as a string
}

func (e EnumUint32) Parse(enumType reflect.Type, s string, strict bool) (EnumUint32, error) {
	v, err := (EnumHelper{}).Parse(enumType, reflect.TypeOf(e), s)
	switch {
	case err == nil:
		return v.(EnumUint32), nil
	case strict:
		return EnumUint32{}, err
	}
	// strict is off: Try to parse s as a base 10 string of digits into an int32 & return its value
	i, parseErr := strconv.ParseUint(s, 10, 32)
	if parseErr != nil {
		parseErr = err
	}
	return EnumUint32{Value: uint32(i)}, parseErr
}

/**********************************************************************************************************************/

type EnumInt16 struct {
	Value int16 // The enum's value
}

func (e EnumInt16) String(enumType reflect.Type) string {
	if s := (EnumHelper{}).String(enumType, reflect.TypeOf(e),
		func(value interface{}) bool { return value.(EnumInt16).Value == e.Value }); s != "" {
		return s
	}
	return strconv.FormatInt(int64(e.Value), 10) // No match, return the number as a string
}

func (e EnumInt16) Parse(enumType reflect.Type, s string, strict bool) (EnumInt16, error) {
	v, err := (EnumHelper{}).Parse(enumType, reflect.TypeOf(e), s)
	switch {
	case err == nil:
		return v.(EnumInt16), nil
	case strict:
		return EnumInt16{}, err
	}
	// strict is off: Try to parse s as a base 10 string of digits into an int32 & return its value
	i, parseErr := strconv.ParseInt(s, 10, 16)
	if parseErr != nil {
		parseErr = err
	}
	return EnumInt16{Value: int16(i)}, parseErr
}

/**********************************************************************************************************************/

type EnumUint16 struct {
	Value uint16 // The enum's value
}

func (e EnumUint16) String(enumType reflect.Type) string {
	if s := (EnumHelper{}).String(enumType, reflect.TypeOf(e),
		func(value interface{}) bool { return value.(EnumUint16).Value == e.Value }); s != "" {
		return s
	}
	return strconv.FormatUint(uint64(e.Value), 10) // No match, return the number as a string
}

func (e EnumUint16) Parse(enumType reflect.Type, s string, strict bool) (EnumUint16, error) {
	v, err := (EnumHelper{}).Parse(enumType, reflect.TypeOf(e), s)
	switch {
	case err == nil:
		return v.(EnumUint16), nil
	case strict:
		return EnumUint16{}, err
	}
	// strict is off: Try to parse s as a base 10 string of digits into an int32 & return its value
	i, parseErr := strconv.ParseInt(s, 10, 16)
	if parseErr != nil {
		parseErr = err
	}
	return EnumUint16{Value: uint16(i)}, parseErr
}

/**********************************************************************************************************************/

type EnumInt8 struct {
	Value int8 // The enum's value
}

func (e EnumInt8) String(enumType reflect.Type) string {
	if s := (EnumHelper{}).String(enumType, reflect.TypeOf(e),
		func(value interface{}) bool { return value.(EnumInt8).Value == e.Value }); s != "" {
		return s
	}
	return strconv.FormatInt(int64(e.Value), 10) // No match, return the number as a string
}

func (e EnumInt8) Parse(enumType reflect.Type, s string, strict bool) (EnumInt8, error) {
	v, err := (EnumHelper{}).Parse(enumType, reflect.TypeOf(e), s)
	switch {
	case err == nil:
		return v.(EnumInt8), nil
	case strict:
		return EnumInt8{}, err
	}
	// strict is off: Try to parse s as a base 10 string of digits into an int32 & return its value
	i, parseErr := strconv.ParseInt(s, 10, 16)
	if parseErr != nil {
		parseErr = err
	}
	return EnumInt8{Value: int8(i)}, parseErr
}

/**********************************************************************************************************************/

type EnumUint8 struct {
	Value uint8 // The enum's value
}

func (e EnumUint8) String(enumType reflect.Type) string {
	if s := (EnumHelper{}).String(enumType, reflect.TypeOf(e),
		func(value interface{}) bool { return value.(EnumUint8).Value == e.Value }); s != "" {
		return s
	}
	return strconv.FormatUint(uint64(e.Value), 10) // No match, return the number as a string
}

func (e EnumUint8) Parse(enumType reflect.Type, s string, strict bool) (EnumUint8, error) {
	v, err := (EnumHelper{}).Parse(enumType, reflect.TypeOf(e), s)
	switch {
	case err == nil:
		return v.(EnumUint8), nil
	case strict:
		return EnumUint8{}, err
	}
	// strict is off: Try to parse s as a base 10 string of digits into an int32 & return its value
	i, parseErr := strconv.ParseInt(s, 10, 16)
	if parseErr != nil {
		parseErr = err
	}
	return EnumUint8{Value: uint8(i)}, parseErr
}

/**********************************************************************************************************************/

type EnumString struct {
	Value string // The enum's value
}

func (e EnumString) String(enumType reflect.Type) string {
	if s := (EnumHelper{}).String(enumType, reflect.TypeOf(e),
		func(value interface{}) bool { return value.(EnumString).Value == e.Value }); s != "" {
		return s
	}
	return e.Value // No match, return the string as a string
}

func (e EnumString) Parse(enumType reflect.Type, s string, strict bool) (EnumString, error) {
	v, err := (EnumHelper{}).Parse(enumType, reflect.TypeOf(e), s)
	switch {
	case err == nil:
		return v.(EnumString), err
	case strict:
		return EnumString{}, err
	}
	// strict is off: Parse s as itself & return its value
	return EnumString{Value: s}, nil // Just return an EnumString with the passed-in string
}

/**********************************************************************************************************************/

// uncomment the code below to test it out

type Color EnumInt32

func (Color) None() Color  { return Color{0} }
func (Color) Red() Color   { return Color{1} }
func (Color) Green() Color { return Color{2} }
func (Color) Blue() Color  { return Color{3} }

func (c Color) String() string {
	return EnumInt32(c).String(reflect.TypeOf(c))
}

func (c Color) Parse(s string) (Color, error) {
	e, err := EnumInt32{}.Parse(reflect.TypeOf(c), s, false)
	return Color(e), err
}

type SASProtocol EnumString

func (SASProtocol) None() SASProtocol         { return SASProtocol{""} }
func (SASProtocol) Https() SASProtocol        { return SASProtocol{"https"} }
func (SASProtocol) HttpsAndHttp() SASProtocol { return SASProtocol{"https,http"} }

func (p SASProtocol) String() string {
	return EnumString(p).String(reflect.TypeOf(p))
}

func (p SASProtocol) Parse(s string) (SASProtocol, error) {
	e, err := EnumString{}.Parse(reflect.TypeOf(p), s, true)
	return SASProtocol(e), err
}
