package sddl

import (
	"errors"
	"fmt"
	"reflect"
	"strings"
)

// SDDL is a parsed version of a SDDL string.
// Check https://docs.microsoft.com/en-us/windows/win32/secauthz/security-descriptor-string-format for the overarching format.
type SDDL struct {
	Owner, Group SID
	DACLs, SACLs ACLs
}

func (s SDDL) String() string {
	output := ""

	if !reflect.DeepEqual(s.Owner, SID{}) {
		output += "O:" + s.Owner.String()
	}

	if !reflect.DeepEqual(s.Group, SID{}) {
		output += "G:" + s.Group.String()
	}

	if !reflect.DeepEqual(s.DACLs, ACLs{}) {
		output += "D:" + s.DACLs.String()
	}

	if !reflect.DeepEqual(s.SACLs, ACLs{}) {
		output += "S:" + s.SACLs.String()
	}

	return output
}

func ParseSDDL(input string) (sddl SDDL, err error) {
	sddl = SDDL{}

	commitSegment := func(data, segType string) (err error) {
		switch segType {
		case "O":
			sddl.Owner, err = ParseSID(data)

			if err != nil {
				err = fmt.Errorf("in Owner section of SDDL: %s", err.Error())
			}
		case "G":
			sddl.Group, err = ParseSID(data)

			if err != nil {
				err = fmt.Errorf("in Group section of SDDL: %s", err.Error())
			}
		case "D":
			sddl.DACLs, err = ParseACEEntries(data)

			if err != nil {
				err = fmt.Errorf("in DACL section of SDDL: %s", err.Error())
			}
		case "S":
			sddl.SACLs, err = ParseACEEntries(data)

			if err != nil {
				err = fmt.Errorf("in SACL section of SDDL: %s", err.Error())
			}
		default:
			err = errors.New("invalid segment " + segType)
		}
		return
	}

	runningString := ""
	segmentType := ""
	for k, v := range strings.Split(input, "") {
		switch {
		case k+1 == len(input):
			runningString += v

			if segmentType != "" && strings.TrimSpace(runningString) != "" {
				err = commitSegment(strings.TrimSpace(runningString), segmentType)

				if err != nil {
					return
				}
			}
		case v == ":": // Do nothing
		case k+1 > len(input) || (k+1 < len(input) && input[k+1] == ':'):
			// Commit prior data after trimming spaces and establish new segment type
			if segmentType != "" && strings.TrimSpace(runningString) != "" {
				err = commitSegment(strings.TrimSpace(runningString), segmentType)

				if err != nil {
					return
				}
			}

			segmentType = v
			runningString = ""
		default:
			runningString += v
		}
	}

	return
}
