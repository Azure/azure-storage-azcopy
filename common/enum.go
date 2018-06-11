package common

import (
	"fmt"
	"reflect"
	"strconv"
	"strings"
)

type EnumHelper struct{}
type EnumSymbolInfo func(enumSymbolName string, enumSymbolValue interface{}) (stop bool)

func (EnumHelper) isValidEnumSymbolMethod(enumType reflect.Type, m reflect.Method) bool {
	// A symbol method must take 1 arg (the receiver) and return 1 value whose type matches the enum's type
	return m.Type.NumIn() == 1 && m.Type.NumOut() == 1 && m.Type.Out(0) == enumType
}

func (EnumHelper) findMethod(enumType reflect.Type, methodName string, caseInsensitive bool) (reflect.Method, bool) {
	if !caseInsensitive {
		return enumType.MethodByName(methodName) // Look up the method by exact name and case
	}
	methodName = strings.ToLower(methodName)    // lowercase the passed method name
	for m := 0; m < enumType.NumMethod(); m++ { // Iterate through all the methods matching their lowercase equivalents
		method := enumType.Method(m)
		if strings.ToLower(method.Name) == methodName {
			return method, true
		}
	}
	return reflect.Method{}, false
}

func (EnumHelper) EnumSymbols(enumType reflect.Type, esi EnumSymbolInfo) {
	// Pass 1 argument that is a zero-value of t
	args := [1]reflect.Value{reflect.Zero(enumType)}

	// Call enum methods looking for one that returns the same value we have
	for m := 0; m < enumType.NumMethod(); m++ {
		method := enumType.Method(m)
		if !(EnumHelper{}).isValidEnumSymbolMethod(enumType, method) {
			continue
		}
		// Call the enum method, convert the result to the enumType interface
		value := method.Func.Call(args[:])[0].Convert(enumType).Interface()
		// Pass the symbol name & value to the callback; stop enumeration if the callback returns true
		if esi(method.Name, value) {
			return
		}
	}
}

func (EnumHelper) String(enumValue interface{}, enumType reflect.Type) string {
	symbolResult := ""
	// Enumerate symbols; if symbol's value matches enumValue, return symbol's name & stop enumeration
	EnumHelper{}.EnumSymbols(enumType, func(symbol string, value interface{}) bool {
		if value == enumValue {
			symbolResult = symbol
			return true
		}
		return false
	})
	return symbolResult // Returns "" if no matching symbol found
}

func (EnumHelper) StringInteger(intValue interface{}, enumType reflect.Type) string {
	if symbolName := (EnumHelper{}).String(intValue, enumType); symbolName != "" {
		return symbolName // Returns matching symbol (if found)
	}
	return fmt.Sprintf("%d", intValue) // No match, return the number as a string
}

func (EnumHelper) StringIntegerFlags(intValue uint64, enumType reflect.Type, intBase int, ifaceToUint64 func(ival interface{}) uint64) string {
	// Call flag's methods that return a flag
	// if flag == 0, return symbol/method that returns 0
	// else skip any method/symbol that returns 0; concatenate to string any method whose return value & f == method's return value
	// return string
	bitsFound := uint64(0)
	symbolNames := strings.Builder{}
	EnumHelper{}.EnumSymbols(enumType, func(symbolName string, symbolValue interface{}) bool {
		symVal := ifaceToUint64(symbolValue)
		if intValue == 0 && symVal == 0 {
			symbolNames.WriteString(symbolName) // We found a match, return the method's name (the enum's symbol)
			return true                         // Stop
		}
		if symVal != 0 && (intValue&symVal == symVal) {
			bitsFound |= symVal
			if symbolNames.Len() > 0 {
				symbolNames.WriteString(", ")
			}
			symbolNames.WriteString(symbolName)
		}
		return false // Continue symbol enumeration
	})
	if bitsFound != intValue {
		// Some bits in the original value were not accounted for, append the remaining decimal value
		if symbolNames.Len() > 0 {
			symbolNames.WriteString(", ")
		}
		symbolNames.WriteString(strconv.FormatUint(intValue^bitsFound, intBase))
	}
	return symbolNames.String() // Returns matching symbol (if found)
}

func (EnumHelper) Parse(enumTypePtr reflect.Type, s string, caseInsensitive bool) (interface{}, error) {
	enumType := enumTypePtr.Elem() // Convert from *T to T
	// Look for a method name that matches the string we're trying to parse
	if method, found := (EnumHelper{}).findMethod(enumType, s, caseInsensitive); found {
		// Pass 1 argument that is a zero-value of t.
		args := [1]reflect.Value{reflect.Zero(enumType)}

		// Call the enum type's method passing in the arg receiver; the returned t is converted to an EnumInt32
		// The caller must convert this to their exact type
		return method.Func.Call(args[:])[0].Convert(enumType).Interface(), nil
	}
	return nil, fmt.Errorf("couldn't parse %q into an instance of %q", s, enumType.Name())
}

func (EnumHelper) ParseIntegerFlags(enumTypePtr reflect.Type, s string, caseInsensitive bool, ifaceToUint64 func(ival interface{}) uint64) (uint64, error) {
	val := uint64(0)
	for _, f := range strings.Split(s, ",") {
		f = strings.TrimSpace(f)
		v, err := EnumHelper{}.Parse(enumTypePtr, f, caseInsensitive)
		if err == nil {
			val |= ifaceToUint64(v) // Symbol found, OR its value
		} else {
			// strict is off: Try to parse f as a base 10 string of digits into a uint64 & return its value
			i, err := strconv.ParseUint(f, 0, int(enumTypePtr.Elem().Size())*8)
			if err == nil {
				val |= i // Successful parse, OR its value
			} else {
				return 0, fmt.Errorf("couldn't parse %q into an instance of %q", f, enumTypePtr.Elem().Name())
			}
		}
	}
	return val, nil
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
/*
type Color2 int32

const (
	ColorNone  Color2 = 0
	ColorRed   Color2 = 1
	ColorGreen Color2 = 2
	ColorBlue  Color2 = 3
)

func (c Color2) String() string {
	switch c {
	case ColorNone:
		return "None"
	case ColorRed:
		return "Red"
	case ColorGreen:
		return "Green"
	case ColorBlue:
		return "Blue"
	default:
		return "Unknown"
	}
}

func (c *Color2) Parse(cs string) error {
	*c = ColorNone // Default unless overridden
	switch cs {
	case "Red":
		*c = ColorRed
	case "Green":
		*c = ColorGreen
	case "Blue":
		*c = ColorBlue
	default:
		return fmt.Errorf("couldn't parse %q into a Color")
	}
	return nil
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

var EColor = Color(0).None()

type Color uint8

func (Color) None() Color  { return Color(0) }
func (Color) Red() Color   { return Color(1) }
func (Color) Green() Color { return Color(2) }
func (Color) Blue() Color  { return Color(3) }
func (c Color) String() string {
	// Calls Color’s methods that return a Color
	// If returned value matches c’s value, return method’s name
	// Return c’s value as string
	return EnumHelper{}.StringInteger(c, reflect.TypeOf(c))
}

func (c *Color) Parse(s string) error {
	// Finds a Color method named s (optionally case-insensitive).
	// If found, calls it and sets c to its value & returns
	// If strict, return error
	// Parses s as integer; if OK, set c to int & returns; else returns error
	val, err := EnumHelper{}.Parse(reflect.TypeOf(c), s, true)
	if err == nil {
		*c = val.(Color)
	}
	//return err	// For strict parsing, uncomment this line
	// strict is off: Try to parse s as a base 10 string of digits into an int32 & return its value
	i, parseErr := strconv.ParseUint(s, 0, int(reflect.TypeOf(*c).Size()) * 8)
	if parseErr == nil {
		*c = Color(i)
		err = nil
	}
	return err
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

var ESASProtocol = SASProtocol("").None()

type SASProtocol string

func (SASProtocol) None() SASProtocol         { return SASProtocol("") }
func (SASProtocol) Https() SASProtocol        { return SASProtocol("https") }
func (SASProtocol) HttpsAndHttp() SASProtocol { return SASProtocol("https,http") }

func (p SASProtocol) String() string {
	return EnumHelper{}.String(p, reflect.TypeOf(p))
}

func (p *SASProtocol) Parse(s string) error {
	v, err := EnumHelper{}.Parse(reflect.TypeOf(p), s, false)
	if err == nil {
		*p = v.(SASProtocol)
	}
	return err
}
*/
