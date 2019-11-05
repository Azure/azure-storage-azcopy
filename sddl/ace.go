package sddl

import (
	"errors"
	"reflect"
	"strings"
)

type ACE struct {
	ACEType           ACEType
	ACEFlags          ACEFlags
	ACERights         ACERights
	ObjectGUID        string
	InheritObjectGUID string
	AccountSID        SID
	ResourceAttribute ACEResourceAttribute
}

func ParseACE(input string) (ACE, error) {
	output := ACE{}

	segment := 0
	runningString := ""
	scope := 0
	inString := false
	aceTypePresent := false

	commitACEData := func(data string, part int) (err error) {
		switch part {
		case 0: // ACE type
			output.ACEType, err = ParseACEType(data)
			aceTypePresent = true
		case 1: // ACE flags
			output.ACEFlags, err = ParseACEFlags(data)
		case 2: // ACE rights
			output.ACERights, err = ParseACERights(data)
		case 3: // Object GUID
			output.ObjectGUID = data
		case 4: // inherit object GUID
			output.InheritObjectGUID = data
		case 5: // account SID
			output.AccountSID, err = ParseSID(data)
		case 6: // resource attribute
			output.ResourceAttribute, err = ParseResourceAttribute(data, output.ACEType)
		default:
			err = errors.New("too many ACE data sections")
		}

		return
	}

	if !strings.HasPrefix(input, "(") || !strings.HasSuffix(input, ")") {
		return ACE{}, errors.New("ACE should be surrounded by parentheses")
	}

	input = strings.TrimPrefix(strings.TrimSuffix(input, ")"), "(")

	for k, v := range strings.Split(input, "") {
		switch {
		case inString:
			runningString += v

			if v == `"` && input[k-1] != '\\' {
				if scope == 0 {
					// Commit the data in the string
					err := commitACEData(runningString, segment)

					if err != nil {
						return ACE{}, err
					}
					runningString = ""
					segment++
				}
				inString = false
			}
		case scope > 0:
			runningString += v

			if v == "(" {
				scope++
			} else if v == ")" {
				scope--
			} else if v == `"` {
				inString = true
			}
		case v == " ":
			// string is no longer contiguous, so we should commit the data.
			if runningString != "" {
				err := commitACEData(runningString, segment)

				if err != nil {
					return ACE{}, err
				}
				runningString = ""
				segment++
			}
			// otherwise, do nothing.
		case v == ";":
			// The string has ended, so we should commit the data.
			if runningString != "" {
				err := commitACEData(runningString, segment)

				if err != nil {
					return ACE{}, err
				}
				runningString = ""
			}
			segment++ // Even if we encounter an empty segment, we should increment.
		case v == `"`:
			inString = true
			runningString += v
			if runningString != "" { // There was already data happening before this. This is invalid.
				return output, errors.New("entered parentheses in the middle of currently running string")
			}
		case v == "(" || v == ")":
			scope++
			if runningString != "" { // There was already data happening before this. This is invalid.
				return output, errors.New("entered parentheses in the middle of currently running string")
			}
			runningString += v
		default:
			runningString += v
		}
	}

	if inString {
		return output, errors.New("unclosed string")
	}

	if scope > 0 {
		return output, errors.New("unclosed parentheses")
	}

	// Ensure we commit the last segment of data if present
	if runningString != "" {
		err := commitACEData(runningString, segment)

		if err != nil {
			return ACE{}, err
		}
		runningString = ""
	}

	if !aceTypePresent {
		return output, errors.New("no ACE type present")
	}

	if (output.ACEType == EACEType.SDDL_CALLBACK_ACCESS_ALLOWED() || output.ACEType == EACEType.SDDL_CALLBACK_ACCESS_DENIED() || output.ACEType == EACEType.SDDL_RESOURCE_ATTRIBUTE()) && output.ResourceAttribute == nil {
		return output, errors.New("ACE type specified a resource attribute but no resource attribute was given")
	}

	return output, nil
}

func (a ACE) String() string {
	RAString := ""
	if a.ResourceAttribute != nil {
		RAString = a.ResourceAttribute.StringifyResourceAttribute()
	}

	output := "("

	output += a.ACEType.String() + ";"
	output += a.ACEFlags.String() + ";"
	output += a.ACERights.String() + ";"
	output += a.ObjectGUID + ";"
	output += a.InheritObjectGUID + ";"

	if !reflect.DeepEqual(a.AccountSID, SID{}) {
		output += a.AccountSID.String()
	}

	if RAString != "" {
		output += ";" + RAString
	}
	output += ")"

	return output
}
