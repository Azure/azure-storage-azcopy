package sddl

import (
	"fmt"
	"strings"
)

// ACEFlags is a struct that defines an enumeration of potential ACE string flags.
// Check https://docs.microsoft.com/en-us/windows/win32/secauthz/ace-strings for a description of these flags.
// Note that these are different than the ACLFlags type, and are not interchangeable.
// Furthermore, note that ACEFlags can be combined. Simply concatenate them for parsing, or use a binary OR on the existing flags.
type ACEFlags uint8
type eACEFlags uint8

func (f ACEFlags) Add(f2 ACEFlags) ACEFlags {
	return f | f2
}

func (f ACEFlags) Sub(f2 ACEFlags) ACEFlags {
	return f ^ f2
}

func (f ACEFlags) Contains(f2 ACEFlags) bool {
	return f&f2 != 0
}

func (f ACEFlags) ToList() []string {
	result := make([]string, 0)

	for k, v := range shorthandACEFlags {
		if f.Contains(v) {
			result = append(result, k)
		}
	}

	return result
}

const EACEFlags eACEFlags = 0

var shorthandACEFlags = map[string]ACEFlags{
	"CI": EACEFlags.SDDL_CONTAINER_INHERIT(),
	"OI": EACEFlags.SDDL_OBJECT_INHERIT(),
	"NP": EACEFlags.SDDL_NO_PROPOGATE(),
	"IO": EACEFlags.SDDL_INHERIT_ONLY(),
	"ID": EACEFlags.SDDL_INHERITED(),
	"SA": EACEFlags.SDDL_AUDIT_SUCCESS(),
	"FA": EACEFlags.SDDL_AUDIT_FAILURE(),
}

// ""
func (eACEFlags) NO_FLAGS() ACEFlags {
	return 0 // empty
}

// "CI"
func (eACEFlags) SDDL_CONTAINER_INHERIT() ACEFlags {
	return 1
}

// "OI"
func (eACEFlags) SDDL_OBJECT_INHERIT() ACEFlags {
	return 1 << 1
}

// "NP"
func (eACEFlags) SDDL_NO_PROPOGATE() ACEFlags {
	return 1 << 2
}

// "IO"
func (eACEFlags) SDDL_INHERIT_ONLY() ACEFlags {
	return 1 << 3
}

// "ID"
func (eACEFlags) SDDL_INHERITED() ACEFlags {
	return 1 << 4
}

// "SA"
func (eACEFlags) SDDL_AUDIT_SUCCESS() ACEFlags {
	return 1 << 5
}

// "FA"
func (eACEFlags) SDDL_AUDIT_FAILURE() ACEFlags {
	return 1 << 6
}

func (f ACEFlags) String() string {
	output := ""

	for k, v := range shorthandACEFlags {
		if f&v != 0 { // If the flag contains this flag, then add the shorthand string.
			output += k
		}
	}

	return output
}

func ParseACEFlags(input string) (ACEFlags, error) {
	out := ACEFlags(0)

	runningString := ""
	for _, v := range strings.Split(strings.ToUpper(input), "") {
		runningString += v

		if flag, ok := shorthandACEFlags[runningString]; ok {
			out |= flag
			runningString = ""
		}
	}

	if runningString != "" {
		return out, fmt.Errorf("%s is an invalid ACE flag string", runningString)
	}

	return out, nil
}
