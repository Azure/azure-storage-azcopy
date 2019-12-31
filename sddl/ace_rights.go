package sddl

import (
	"fmt"
	"regexp"
	"strconv"
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
	return r &^ r2 // bitclear
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
	"AS": EACERights.SDDL_ACCESS_SYSTEM_SECURITY(),

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
	// Just return the hex; it's valid.
	// This works past the issue of overlaps with mandatory labels and keys.
	return "0x" + strings.ToUpper(strconv.FormatUint(uint64(r), 16))
}

var hexadecimalRegex = regexp.MustCompile(`(?i)0x[A-F0-9]+`)

func ParseACERights(input string) (ACERights, error) {
	output := ACERights(0)

	if hexadecimalRegex.MatchString(input) {
		// TODO: parse the hexadecimal
		// TODO: Map all ace rights to their original format in the windows DLL!
		tmpOut, err := strconv.ParseUint(input[2:], 16, 32) // this will break input down to the hexadecimal

		if err != nil {
			return output, err
		}

		return ACERights(tmpOut), nil
	}

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

// The following magic numbers are modeled after Windows' implementation, as hexadecimal literal access masks are supported.

// ========== GENERIC ACCESS RIGHTS ==========

// Generic access rights are defined here:
// https://docs.microsoft.com/en-us/windows/win32/secauthz/access-mask-format
// Bits 31-28, GR, GW, GE, GA

func (eACERights) SDDL_GENERIC_READ() ACERights {
	return 1 << 31
}

func (eACERights) SDDL_GENERIC_WRITE() ACERights {
	return 1 << 30
}

func (eACERights) SDDL_GENERIC_EXECUTE() ACERights {
	return 1 << 29
}

func (eACERights) SDDL_GENERIC_ALL() ACERights {
	return 1 << 28
}

// Bit 24 is AS
func (eACERights) SDDL_ACCESS_SYSTEM_SECURITY() ACERights {
	return 1 << 24
}

// The following values come from queries to
// https://www.magnumdb.com
// The queries are above the function.
// Then, log2(x) is used to find how far to push.

// =========== STANDARD ACCESS RIGHTS ===========

// filename:Winnt.h AND READ_CONTROL
func (eACERights) SDDL_READ_CONTROL() ACERights {
	return 1 << 17
}

// filename:Winnt.h AND DELETE
func (eACERights) SDDL_STANDARD_DELETE() ACERights {
	return 1 << 16
}

// filename:Winnt.h AND WRITE_DAC
func (eACERights) SDDL_WRITE_DAC() ACERights {
	return 1 << 18
}

// filename:Winnt.h AND WRITE_OWNER
func (eACERights) SDDL_WRITE_OWNER() ACERights {
	return 1 << 19
}

// ========== DIRECTORY SERVICE OBJECT ACCESS RIGHTS ==========

// filename:iads.h AND ADS_RIGHT_DS_READ_PROP
func (eACERights) SDDL_READ_PROPERTY() ACERights {
	return 1 << 4
}

// filename:iads.h AND ADS_RIGHT_DS_WRITE_PROP
func (eACERights) SDDL_WRITE_PROPERTY() ACERights {
	return 1 << 5
}

// filename:iads.h AND ADS_RIGHT_DS_CREATE_CHILD
func (eACERights) SDDL_CREATE_CHILD() ACERights {
	return 1
}

// filename:iads.h AND ADS_RIGHT_DS_DELETE_CHILD
func (eACERights) SDDL_DELETE_CHILD() ACERights {
	return 1 << 1
}

// filename:iads.h AND ADS_RIGHT_ACTRL_DS_LIST
func (eACERights) SDDL_LIST_CHILDREN() ACERights {
	return 1 << 2
}

// filename:iads.h AND ADS_RIGHT_DS_SELF
func (eACERights) SDDL_SELF_WRITE() ACERights {
	return 1 << 3
}

// filename:iads.h AND ADS_RIGHT_DS_LIST_OBJECT
func (eACERights) SDDL_LIST_OBJECT() ACERights {
	return 1 << 7
}

// filename:iads.h AND ADS_RIGHT_DS_DELETE_TREE
func (eACERights) SDDL_DELETE_TREE() ACERights {
	return 1 << 6
}

// filename:iads.h AND ADS_RIGHT_DS_CONTROL_ACCESS
func (eACERights) SDDL_CONTROL_ACCESS() ACERights {
	return 1 << 8
}

// =========== FILE ACCESS RIGHTS ===========

// Unlike prior sections, raw values will be used here as these aren't push backs.
// I'm _concerned_ for overlaps.

// FILE_ALL_ACCESS
func (eACERights) SDDL_FILE_ALL() ACERights {
	return 2032127
}

// FILE_GENERIC_READ
func (eACERights) SDDL_FILE_READ() ACERights {
	return 1179785
}

// FILE_GENERIC_WRITE
func (eACERights) SDDL_FILE_WRITE() ACERights {
	return 1179926
}

// FILE_GENERIC_EXECUTE
func (eACERights) SDDL_FILE_EXECUTE() ACERights {
	return 1179808
}

// =========== REGISTRY KEY ACCESS RIGHTS ===========

// KEY_ALL_ACCESS
func (eACERights) SDDL_KEY_ALL() ACERights {
	return 983103
}

// KEY_READ
func (eACERights) SDDL_KEY_READ() ACERights {
	return 131097
}

// KEY_WRITE
func (eACERights) SDDL_KEY_WRITE() ACERights {
	return 131078
}

// KEY_EXECUTE
func (eACERights) SDDL_KEY_EXECUTE() ACERights {
	return 131097
}

// =========== MANDATORY LABEL RIGHTS ===========

// These overlap with some directory bits.
// However, they'll never be used in conjunction with each other.
// Therefore, we'll stringify/parse them separately if the stringifier and the parser are told to.

// "NR"
// SYSTEM_MANDATORY_LABEL_NO_READ_UP
func (eACERights) SDDL_NO_READ_UP() ACERights {
	return 1 << 1
}

// "NW"
// SYSTEM_MANDATORY_LABEL_NO_WRITE_UP
func (eACERights) SDDL_NO_WRITE_UP() ACERights {
	return 1
}

// "NX"
// SYSTEM_MANDATORY_LABEL_NO_EXECUTE_UP
func (eACERights) SDDL_NO_EXECUTE_UP() ACERights {
	return 1 << 2
}
