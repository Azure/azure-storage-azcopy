package sddl

import (
	"fmt"
)

// ACEType is a type that describes the potential ACE string types. These overall describe the effect of a particular ACE string.
// Check https://docs.microsoft.com/en-us/windows/win32/secauthz/ace-strings#fields for an in-depth description of these.
// Note that only one ACE type can be used at a time.
type ACEType uint8
type eACEType uint8

const EACEType eACEType = 0

var shorthandACEType = map[ACEType]string{
	EACEType.SDDL_ACCESS_ALLOWED():                 "A",
	EACEType.SDDL_ACCESS_DENIED():                  "D",
	EACEType.SDDL_OBJECT_ACCESS_ALLOWED():          "OA",
	EACEType.SDDL_OBJECT_ACCESS_DENIED():           "OD",
	EACEType.SDDL_AUDIT():                          "AU",
	EACEType.SDDL_ALARM():                          "AL",
	EACEType.SDDL_OBJECT_AUDIT():                   "OU",
	EACEType.SDDL_OBJECT_ALARM():                   "OL",
	EACEType.SDDL_MANDATORY_LABEL():                "ML",
	EACEType.SDDL_CALLBACK_ACCESS_ALLOWED():        "XA",
	EACEType.SDDL_CALLBACK_ACCESS_DENIED():         "XD",
	EACEType.SDDL_RESOURCE_ATTRIBUTE():             "RA",
	EACEType.SDDL_SCOPED_POLICY_ID():               "SP",
	EACEType.SDDL_CALLBACK_AUDIT():                 "XU",
	EACEType.SDDL_CALLBACK_OBJECT_ACCESS_ALLOWED(): "ZA",
}

// construct the reverse of the upper map
var reverseShorthandACEType = func() map[string]ACEType {
	out := map[string]ACEType{}

	for k, v := range shorthandACEType {
		out[v] = k
	}

	return out
}()

func ParseACEType(input string) (ACEType, error) {
	ace, ok := reverseShorthandACEType[input]

	if ok {
		return ace, nil
	} else {
		return 0, fmt.Errorf("invalid ACE type %s", input)
	}
}

func (a ACEType) String() string {
	str, ok := shorthandACEType[a]

	if ok {
		return str
	}

	panic(fmt.Sprintf("%d: invalid ACEType value", uint8(a)))
}

func (eACEType) SDDL_ACCESS_ALLOWED() ACEType {
	return 0
}

func (eACEType) SDDL_ACCESS_DENIED() ACEType {
	return 1
}

func (eACEType) SDDL_OBJECT_ACCESS_ALLOWED() ACEType {
	return 2
}

func (eACEType) SDDL_OBJECT_ACCESS_DENIED() ACEType {
	return 3
}

func (eACEType) SDDL_AUDIT() ACEType {
	return 4
}

func (eACEType) SDDL_ALARM() ACEType {
	return 5
}

func (eACEType) SDDL_OBJECT_AUDIT() ACEType {
	return 6
}

func (eACEType) SDDL_OBJECT_ALARM() ACEType {
	return 7
}

func (eACEType) SDDL_MANDATORY_LABEL() ACEType {
	return 8
}

func (eACEType) SDDL_CALLBACK_ACCESS_ALLOWED() ACEType {
	return 9
}

func (eACEType) SDDL_CALLBACK_ACCESS_DENIED() ACEType {
	return 10
}

func (eACEType) SDDL_RESOURCE_ATTRIBUTE() ACEType {
	return 11
}

func (eACEType) SDDL_SCOPED_POLICY_ID() ACEType {
	return 12
}

func (eACEType) SDDL_CALLBACK_AUDIT() ACEType {
	return 13
}

func (eACEType) SDDL_CALLBACK_OBJECT_ACCESS_ALLOWED() ACEType {
	return 14
}
