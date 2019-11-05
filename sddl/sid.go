package sddl

import (
	"errors"
	"fmt"
	"regexp"
	"strconv"
	"strings"
)

// SID is a struct that defines the Security Identifier on Windows.
type SID struct {
	Revision            int64
	IdentifierAuthority int64
	Subauthorities      []int64
	Shorthand           string
}

var sidRegex = regexp.MustCompile(`^S-\d+-\d+(-\d+)+$`)

// ParseSID parses a SID.
func ParseSID(input string) (SID, error) {
	output := SID{}

	// This is shorthand format for a SID
	if len(input) == 2 && strings.ToUpper(input) == input {
		output.Shorthand = input
		return output, nil
	}

	if !sidRegex.MatchString(input) {
		return SID{}, errors.New("input does not match SID format")
	}

	workingInput := strings.TrimPrefix(input, "S-")
	index := 0
	runningString := ""
	putItem := func(idx int, data string) error {
		out, err := strconv.ParseInt(data, 10, 64)

		if err != nil {
			return err
		}

		switch idx {
		case 0: // Revision
			output.Revision = out
		case 1: // Identifier authority
			output.IdentifierAuthority = out
		default: // Append to subauthorities
			if out > ((1 << 48) - 1) {
				return errors.New("subauthority field is greater than 48 bits")
			}

			output.Subauthorities = append(output.Subauthorities, out)
		}
		return nil
	}

	for k, v := range strings.Split(workingInput, "") {
		if k+1 == len(workingInput) || v == "-" {
			if k+1 == len(workingInput) && v != "-" {
				runningString += v
			}

			if k+1 == len(workingInput) && v == "-" {
				return SID{}, errors.New("invalid trailing dash")
			}

			err := putItem(index, runningString)

			if err != nil {
				return output, err
			}

			index++
			runningString = ""
		} else {
			runningString += v
		}
	}

	return output, nil
}

// String attempts to make a SID portable before stringifying. If it cannot, it returns the shorthand if present.
func (s SID) String() string {
	workingSID, err := s.ToPortable()

	if err != nil {
		workingSID = s

		if workingSID.Shorthand != "" {
			return workingSID.Shorthand
		}
	}

	output := fmt.Sprintf("S-%d-%d", workingSID.Revision, workingSID.IdentifierAuthority)

	for _, v := range workingSID.Subauthorities {
		output += fmt.Sprintf("-%d", v)
	}

	return output
}

// ToPortable is designed for Windows and will fail on any other OS. This is because in order to make a SID portable, we need windows-exclusive functions.
func (s SID) ToPortable() (SID, error) {
	if s.Shorthand == "" {
		return s, nil
	}

	return getSIDFromShorthand(s.Shorthand)
}
