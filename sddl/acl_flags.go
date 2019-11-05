package sddl

import (
	"fmt"
	"strings"
)

// ACLFlags is a struct that defines an enumeration of potential ACL flags.
// Check https://docs.microsoft.com/en-us/windows/win32/secauthz/security-descriptor-string-format for a description of these flags.
// Note that ACE flags can be combined. Simply concatenate them for parsing, or use binary OR on the existing flags.
type ACLFlags uint8
type eACLFlags uint8

func (f ACLFlags) Add(f2 ACLFlags) ACLFlags {
	return f | f2
}

func (f ACLFlags) Sub(f2 ACLFlags) ACLFlags {
	return f ^ f2
}

func (f ACLFlags) Contains(f2 ACLFlags) bool {
	return f&f2 != 0
}

func (f ACLFlags) ToList() []string {
	result := make([]string, 0)

	for k, v := range aclFlagsShorthands {
		if f.Contains(v) {
			result = append(result, k)
		}
	}

	return result
}

// We make the enumeration publicly accessible, but not the underlying type.
// This seems like an awkward design pattern at first sight, but what it means is a couple of things:
// 1) The enumerator isn't EVERY ACLFlags object, it's just one constant available globally
// 2) The enumerator is easy to access, user-facing.
// 3) We don't expose underlying hackiness

// EACLFlags is a enumeration of available SDDL ACL flags.
const EACLFlags = eACLFlags(0)

var aclFlagsShorthands = map[string]ACLFlags{
	"P":                 EACLFlags.SDDL_PROTECTED(),
	"AR":                EACLFlags.SDDL_AUTO_INHERIT_REQ(),
	"AI":                EACLFlags.SDDL_AUTO_INHERITED(),
	"NO_ACCESS_CONTROL": EACLFlags.SDDL_NULL_ACL(),
}

// ""
func (eACLFlags) NO_FLAGS() ACLFlags {
	return 0 // empty
}

// "P"
func (eACLFlags) SDDL_PROTECTED() ACLFlags {
	return 1
}

// "AR"
func (eACLFlags) SDDL_AUTO_INHERIT_REQ() ACLFlags {
	return 1 << 1
}

// "AI"
func (eACLFlags) SDDL_AUTO_INHERITED() ACLFlags {
	return 1 << 2
}

// "NO_ACCESS_CONTROL"
func (eACLFlags) SDDL_NULL_ACL() ACLFlags {
	return 1 << 3
}

// ACLFlags.String() returns the short-hand
func (f ACLFlags) String() string {
	output := ""

	if f.Contains(EACLFlags.SDDL_PROTECTED()) {
		output += "P"
	}

	if f.Contains(EACLFlags.SDDL_AUTO_INHERIT_REQ()) {
		output += "AR"
	}

	if f.Contains(EACLFlags.SDDL_AUTO_INHERITED()) {
		output += "AI"
	}

	if f.Contains(EACLFlags.SDDL_NULL_ACL()) {
		output += "NO_ACCESS_CONTROL"
	}

	return output
}

func ParseACLFlags(input string) (ACLFlags, error) {
	out := ACLFlags(0)

	runningString := ""
	for _, v := range strings.Split(strings.ToUpper(input), "") {
		runningString += v

		if flag, ok := aclFlagsShorthands[runningString]; ok {
			out |= flag
			runningString = ""
		}
	}

	if runningString != "" {
		return out, fmt.Errorf("%s is an invalid ACL flag string", runningString)
	}

	return out, nil
}
