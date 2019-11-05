package sddl

import (
	"errors"
	"strings"
)

// ACLs is a struct defining the SACL or DACL section of a SDDL string.
// Check https://docs.microsoft.com/en-us/windows/win32/secauthz/security-descriptor-string-format to see the overarching format.
type ACLs struct {
	ACLFlags ACLFlags
	Entries  []ACE
}

func ParseACEEntries(data string) (ACLs, error) {
	output := ACLs{}

	commitData := func(data string) (err error) {
		data = strings.TrimSpace(data)

		// Don't commit the data if there's nothing to commit.
		if data == "" {
			return
		}

		if strings.HasSuffix(data, ")") && strings.HasPrefix(data, "(") {
			var ace ACE

			ace, err = ParseACE(data)

			if err != nil {
				return
			}

			output.Entries = append(output.Entries, ace)
		} else {
			if len(output.Entries) != 0 {
				return errors.New("ACL flags must only be placed at the beginning of the string")
			}

			// commit ACE flags
			var flags ACLFlags
			flags, err = ParseACLFlags(data)

			if err != nil {
				return
			}

			// Because a user could potentially put spaces between their flags, but still have them at the start, we should |= it.
			output.ACLFlags |= flags
		}

		return
	}

	runningString := ""
	scope := 0
	inString := false

	for k, v := range strings.Split(data, "") {
		switch {
		case inString:
			runningString += v
			if v == `"` && data[k-1] != '\\' {
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

			if scope == 0 {
				err := commitData(runningString)

				if err != nil {
					return output, err
				}
				runningString = ""
			}
		case v == "(":
			if runningString != "" {
				err := commitData(runningString)

				if err != nil {
					return output, err
				}

				runningString = ""
			}

			runningString += v
			scope++
		case v == ")":
			return output, errors.New("unexpected parentheses close in ACL")
		case v == `"`:
			runningString += v
			inString = true
		case v == " ":
			if runningString != "" {
				err := commitData(runningString)

				if err != nil {
					return output, err
				}

				runningString = ""
			}
		default:
			runningString += v
		}
	}

	if inString {
		return output, errors.New("string not closed in ACL")
	}

	if scope > 0 {
		return output, errors.New("parentheses not closed in ACL")
	}

	if runningString != "" {
		err := commitData(runningString)

		if err != nil {
			return output, err
		}

		runningString = ""
	}

	return output, nil
}

func (e ACLs) String() string {
	output := e.ACLFlags.String()

	for _, v := range e.Entries {
		output += v.String()
	}

	return output
}
