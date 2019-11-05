package sddl

import (
	"fmt"
	"strings"
)

// ACERights defines a enumeration of rights that can be assigned in an ACE string.
// Check https://docs.microsoft.com/en-us/windows/win32/secauthz/ace-strings for a description of these rights.
// Note that these rights can be combined. Simply concatenate them for parsing, or use a binary OR on the existing flags.
type ACERights uint32
type eACERights uint32

func (r ACERights) Add(r2 ACERights) ACERights {
	return r | r2
}

func (r ACERights) Sub(r2 ACERights) ACERights {
	return r ^ r2
}

func (r ACERights) Contains(r2 ACERights) bool {
	return r&r2 != 0
}

func (r ACERights) ToList() []string {
	result := make([]string, 0)

	for k, v := range shorthandACERights {
		if r.Contains(v) {
			result = append(result, k)
		}
	}

	return result
}

const EACERights eACERights = 0

var shorthandACERights = map[string]ACERights{
	"GA": EACERights.SDDL_GENERIC_ALL(),
	"GR": EACERights.SDDL_GENERIC_READ(),
	"GW": EACERights.SDDL_GENERIC_WRITE(),
	"GX": EACERights.SDDL_GENERIC_EXECUTE(),

	"RC": EACERights.SDDL_READ_CONTROL(),
	"SD": EACERights.SDDL_STANDARD_DELETE(),
	"WD": EACERights.SDDL_WRITE_DAC(),
	"WO": EACERights.SDDL_WRITE_OWNER(),

	"RP": EACERights.SDDL_READ_PROPERTY(),
	"WP": EACERights.SDDL_WRITE_PROPERTY(),
	"CC": EACERights.SDDL_CREATE_CHILD(),
	"DC": EACERights.SDDL_DELETE_CHILD(),
	"LC": EACERights.SDDL_LIST_CHILDREN(),
	"SW": EACERights.SDDL_SELF_WRITE(),
	"LO": EACERights.SDDL_LIST_OBJECT(),
	"DT": EACERights.SDDL_DELETE_TREE(),
	"CR": EACERights.SDDL_CONTROL_ACCESS(),

	"FA": EACERights.SDDL_FILE_ALL(),
	"FR": EACERights.SDDL_FILE_READ(),
	"FW": EACERights.SDDL_FILE_WRITE(),
	"FX": EACERights.SDDL_FILE_EXECUTE(),

	"KA": EACERights.SDDL_KEY_ALL(),
	"KR": EACERights.SDDL_KEY_READ(),
	"KW": EACERights.SDDL_KEY_WRITE(),
	"KX": EACERights.SDDL_KEY_EXECUTE(),

	"NR": EACERights.SDDL_NO_READ_UP(),
	"NW": EACERights.SDDL_NO_WRITE_UP(),
	"NX": EACERights.SDDL_NO_EXECUTE_UP(),
}

func (r ACERights) String() string {
	output := ""

	for k, v := range shorthandACERights {
		if r&v != 0 {
			output += k
		}
	}

	return output
}

func ParseACERights(input string) (ACERights, error) {
	output := ACERights(0)

	runningString := ""
	for _, v := range strings.Split(strings.ToUpper(input), "") {
		runningString += v

		if r, ok := shorthandACERights[runningString]; ok {
			runningString = ""
			output |= r
		}
	}

	if runningString != "" {
		return output, fmt.Errorf("%s is not a valid ACE right", runningString)
	}

	return output, nil
}

func (eACERights) NO_RIGHTS() ACERights {
	return 0
}

// ======== GENERIC ACCESS RIGHTS ===========

// "GA"
func (eACERights) SDDL_GENERIC_ALL() ACERights {
	return 1
}

// "GR"
func (eACERights) SDDL_GENERIC_READ() ACERights {
	return 1 << 1
}

// "GW"
func (eACERights) SDDL_GENERIC_WRITE() ACERights {
	return 1 << 2
}

// "GX"
func (eACERights) SDDL_GENERIC_EXECUTE() ACERights {
	return 1 << 3
}

// ========== STANDARD ACCESS RIGHTS ==========

// "RC"
func (eACERights) SDDL_READ_CONTROL() ACERights {
	return 1 << 4
}

// "SD"
func (eACERights) SDDL_STANDARD_DELETE() ACERights {
	return 1 << 5
}

// "WD"
func (eACERights) SDDL_WRITE_DAC() ACERights {
	return 1 << 6
}

// "WO"
func (eACERights) SDDL_WRITE_OWNER() ACERights {
	return 1 << 7
}

// ========== DIRECTORY SERVICE OBJECT ACCESS RIGHTS ==========

// "RP"
func (eACERights) SDDL_READ_PROPERTY() ACERights {
	return 1 << 8
}

// "WP"
func (eACERights) SDDL_WRITE_PROPERTY() ACERights {
	return 1 << 9
}

// "CC"
func (eACERights) SDDL_CREATE_CHILD() ACERights {
	return 1 << 10
}

// "DC"
func (eACERights) SDDL_DELETE_CHILD() ACERights {
	return 1 << 11
}

// "LC"
func (eACERights) SDDL_LIST_CHILDREN() ACERights {
	return 1 << 12
}

// "SW"
func (eACERights) SDDL_SELF_WRITE() ACERights {
	return 1 << 13
}

// "LO"
func (eACERights) SDDL_LIST_OBJECT() ACERights {
	return 1 << 14
}

// "DT"
func (eACERights) SDDL_DELETE_TREE() ACERights {
	return 1 << 15
}

// "CR"
func (eACERights) SDDL_CONTROL_ACCESS() ACERights {
	return 1 << 16
}

// =========== FILE ACCESS RIGHTS ==========

// "FA"
func (eACERights) SDDL_FILE_ALL() ACERights {
	return 1 << 17
}

// "FR"
func (eACERights) SDDL_FILE_READ() ACERights {
	return 1 << 18
}

// "FW"
func (eACERights) SDDL_FILE_WRITE() ACERights {
	return 1 << 19
}

// "FX"
func (eACERights) SDDL_FILE_EXECUTE() ACERights {
	return 1 << 20
}

// ========= REGISTRY KEY ACCESS RIGHTS =========

// "KA"
func (eACERights) SDDL_KEY_ALL() ACERights {
	return 1 << 21
}

// "KR"
func (eACERights) SDDL_KEY_READ() ACERights {
	return 1 << 22
}

// "KW"
func (eACERights) SDDL_KEY_WRITE() ACERights {
	return 1 << 23
}

// "KX"
func (eACERights) SDDL_KEY_EXECUTE() ACERights {
	return 1 << 24
}

// ========= MANDATORY LABEL RIGHTS =========

// "NR"
func (eACERights) SDDL_NO_READ_UP() ACERights {
	return 1 << 25
}

// "NW"
func (eACERights) SDDL_NO_WRITE_UP() ACERights {
	return 1 << 26
}

// "NX"
func (eACERights) SDDL_NO_EXECUTE_UP() ACERights {
	return 1 << 27
}
