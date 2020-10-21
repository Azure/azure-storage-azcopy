// Copyright © Microsoft <wastore@microsoft.com>
//
// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files (the "Software"), to deal
// in the Software without restriction, including without limitation the rights
// to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
// copies of the Software, and to permit persons to whom the Software is
// furnished to do so, subject to the following conditions:
//
// The above copyright notice and this permission notice shall be included in
// all copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
// THE SOFTWARE.

package sddl

import (
	"errors"
	"fmt"
	"regexp"
	"sort"
	"strings"
)

var translateSID = OSTranslateSID // this layer of indirection is to support unit testing. TODO: it's ugly to set a global to test. Do something better one day

func IffInt(condition bool, tVal, fVal int) int {
	if condition {
		return tVal
	}
	return fVal
}

// Owner and group SIDs need replacement
type SDDLString struct {
	OwnerSID, GroupSID string
	DACL, SACL         ACLList
}

type ACLList struct {
	Flags      string
	ACLEntries []ACLEntry
}

// field 5 and field 6 will contain SIDs.
// field 5 is a lone SID, but field 6 will contain SIDs under SID(.*)
type ACLEntry struct {
	Sections []string
}

func (s *SDDLString) PortableString() string {
	output := ""

	if s.OwnerSID != "" {
		tx, err := translateSID(s.OwnerSID)

		if err != nil {
			output += "O:" + s.OwnerSID
		} else {
			output += "O:" + tx
		}
	}

	if s.GroupSID != "" {
		tx, err := translateSID(s.GroupSID)

		if err != nil {
			output += "G:" + s.GroupSID
		} else {
			output += "G:" + tx
		}
	}

	if s.DACL.Flags != "" || len(s.DACL.ACLEntries) != 0 {
		output += "D:" + s.DACL.PortableString()
	}

	if s.SACL.Flags != "" || len(s.SACL.ACLEntries) != 0 {
		output += "S:" + s.SACL.PortableString()
	}

	return output
}

var LiteralSIDRegex = regexp.MustCompile(`SID\(.*?\)`)
var StringRegex = regexp.MustCompile(`("")|(".*?[^\\]")`)

// PortableString returns a SDDL that's been ported from non-descript, well known SID strings (such as DU, DA, etc.)
// to domain-specific strings. This allows us to not mix up the admins from one domain to another.
// Azure Files requires that we do this.
func (a *ACLList) PortableString() string {
	output := a.Flags

	for _, v := range a.ACLEntries {
		output += "("

		for k, s := range v.Sections {
			// Append a ; after the last section
			if k > 0 {
				output += ";"
			}

			if k == 5 {
				// This section is a lone SID, so we can make a call to windows and translate it.
				tx, err := translateSID(strings.TrimSpace(s))

				if err != nil {
					output += s
				} else {
					output += tx
				}
			} else if k == 6 {
				// This section will potentially have SIDs unless it's not a conditional ACE.
				// They're identifiable as they're inside a literal SID container. ex "SID(S-1-1-0)"

				workingString := ""
				lastAddPoint := 0
				if v.Sections[0] == "XA" || v.Sections[0] == "XD" || v.Sections[0] == "XU" || v.Sections[0] == "ZA" {
					// We shouldn't do any replacing if we're inside of a string.
					// In order to handle this, we'll handle it as a list of events that occur.

					stringEntries := StringRegex.FindAllStringIndex(s, -1)
					sidEntries := LiteralSIDRegex.FindAllStringIndex(s, -1)
					eventMap := map[int]int{} // 1 = string start, 2 = string end, 3 = SID start, 4 = SID end.
					eventList := make([]int, 0)
					inString := false
					SIDStart := -1
					processSID := false

					// Register string beginnings and ends
					for _, v := range stringEntries {
						eventMap[v[0]] = 1
						eventMap[v[1]] = 2
						eventList = append(eventList, v...)
					}

					// Register SID beginnings and ends
					for _, v := range sidEntries {
						eventMap[v[0]] = 3
						eventMap[v[1]] = 4
						eventList = append(eventList, v...)
					}

					// sort the list
					sort.Ints(eventList)

					// Traverse it.
					// Handle any SIDs outside of strings.
					for _, v := range eventList {
						event := eventMap[v]

						switch event {
						case 1: // String start
							inString = true
							// Add everything prior to this
							workingString += s[lastAddPoint:v]
							lastAddPoint = v
						case 2:
							inString = false
							// Add everything prior to this
							workingString += s[lastAddPoint:v]
							lastAddPoint = v
						case 3:
							processSID = !inString
							SIDStart = v
							// If we're going to process this SID, add everything prior to this.
							if processSID {
								workingString += s[lastAddPoint:v]
								lastAddPoint = v
							}
						case 4:
							if processSID {
								// We have to process the sid string now.
								sidString := strings.TrimSuffix(strings.TrimPrefix(s[SIDStart:v], "SID("), ")")

								tx, err := translateSID(strings.TrimSpace(sidString))

								// It seems like we should probably still add the string if we error out.
								// However, this just gets handled exactly like we're not processing the SID.
								// When the next event happens, we just add everything to the string, including the original SID.
								if err == nil {
									workingString += "SID(" + tx + ")"
									lastAddPoint = v
								}
							}
						}
					}
				}

				if workingString != "" {
					if lastAddPoint != len(s) {
						workingString += s[lastAddPoint:]
					}

					s = workingString
				}

				output += s
			} else {
				output += s
			}
		}

		output += ")"
	}

	return strings.TrimSpace(output)
}

func (a *ACLList) String() string {
	output := a.Flags

	for _, v := range a.ACLEntries {
		output += "("

		for k, s := range v.Sections {
			if k > 0 {
				output += ";"
			}

			output += s
		}

		output += ")"
	}

	return strings.TrimSpace(output)
}

func (s *SDDLString) String() string {
	output := ""

	if s.OwnerSID != "" {
		output += "O:" + s.OwnerSID
	}

	if s.GroupSID != "" {
		output += "G:" + s.GroupSID
	}

	if s.DACL.Flags != "" || len(s.DACL.ACLEntries) != 0 {
		output += "D:" + s.DACL.String()
	}

	if s.SACL.Flags != "" || len(s.SACL.ACLEntries) != 0 {
		output += "S:" + s.SACL.String()
	}

	return output
}

// place an element onto the current ACL
func (s *SDDLString) putACLElement(element string, aclType rune) error {
	var aclEntries *[]ACLEntry
	switch aclType {
	case 'D':
		aclEntries = &s.DACL.ACLEntries
	case 'S':
		aclEntries = &s.SACL.ACLEntries
	default:
		return fmt.Errorf("%s ACL type invalid", string(aclType))
	}

	aclEntriesLength := len(*aclEntries)
	if aclEntriesLength == 0 {
		return errors.New("ACL Entries too short")
	}

	entry := (*aclEntries)[aclEntriesLength-1]
	entry.Sections = append(entry.Sections, element)
	(*aclEntries)[aclEntriesLength-1] = entry
	return nil
}

// create a new ACL
func (s *SDDLString) startACL(aclType rune) error {
	var aclEntries *[]ACLEntry
	switch aclType {
	case 'D':
		aclEntries = &s.DACL.ACLEntries
	case 'S':
		aclEntries = &s.SACL.ACLEntries
	default:
		return fmt.Errorf("%s ACL type invalid", string(aclType))
	}

	*aclEntries = append(*aclEntries, ACLEntry{Sections: make([]string, 0)})

	return nil
}

func (s *SDDLString) setACLFlags(flags string, aclType rune) error {
	var aclFlags *string
	switch aclType {
	case 'D':
		aclFlags = &s.DACL.Flags
	case 'S':
		aclFlags = &s.SACL.Flags
	default:
		return fmt.Errorf("%s ACL type invalid", string(aclType))
	}

	*aclFlags = strings.TrimSpace(flags)

	return nil
}

func ParseSDDL(input string) (sddl SDDLString, err error) {
	scope := 0                     // if scope is 1, we're in an ACE string, if scope is 2, we're in a resource attribute.
	inString := false              // If a quotation mark was found, we've entered a string and should ignore all characters except another quotation mark.
	elementStart := make([]int, 0) // This is the start of the element we're currently analyzing. If the array has more than one element, we're probably under a lower scope.
	awaitingACLFlags := false      // If this is true, a ACL section was just entered, and we're awaiting our first ACE string
	var elementType rune           // We need to keep track of which section of the SDDL string we're in.
	for k, v := range input {
		switch {
		case inString: // ignore characters within a string-- except for the end of a string, and escaped quotes
			if v == '"' && input[k-1] != '\\' {
				inString = false
			}
		case v == '"':
			inString = true
		case v == '(': // this comes before scope == 1 because ACE strings can be multi-leveled. We only care about the bottom level.
			scope++
			if scope == 1 { // only do this if we're in the base of an ACE string-- We don't care about the metadata as much.
				if awaitingACLFlags {
					err := sddl.setACLFlags(input[elementStart[0]:k], elementType)

					if err != nil {
						return sddl, err
					}

					awaitingACLFlags = false
				}
				elementStart = append(elementStart, k+1) // raise the element start scope
				err := sddl.startACL(elementType)

				if err != nil {
					return sddl, err
				}
			}
		case v == ')':
			// (...,...,...,(...))
			scope--
			if scope == 0 {
				err := sddl.putACLElement(input[elementStart[1]:k], elementType)

				if err != nil {
					return sddl, err
				}

				elementStart = elementStart[:1] // lower the element start scope
			}
		case scope == 1: // We're at the top level of an ACE string
			switch v {
			case ';':
				// moving to the next element
				err := sddl.putACLElement(input[elementStart[1]:k], elementType)

				if err != nil {
					return sddl, err
				}

				elementStart[1] = k + 1 // move onto the next bit of the element scope
			}
		case scope == 0: // We're at the top level of a SDDL string
			if k == len(input)-1 || v == ':' { // If we end the string OR start a new section
				if elementType != 0x00 {
					switch elementType {
					case 'O':
						// you are here:
						//       V
						// O:...G:
						//      ^
						//      k-1
						// string separations in go happen [x:y).
						sddl.OwnerSID = strings.TrimSpace(input[elementStart[0]:IffInt(k == len(input)-1, len(input), k-1)])
					case 'G':
						sddl.GroupSID = strings.TrimSpace(input[elementStart[0]:IffInt(k == len(input)-1, len(input), k-1)])
					case 'D', 'S': // These are both parsed WHILE they happen, UNLESS we're awaiting flags.
						if awaitingACLFlags {
							err := sddl.setACLFlags(strings.TrimSpace(input[elementStart[0]:IffInt(k == len(input)-1, len(input), k-1)]), elementType)

							if err != nil {
								return sddl, err
							}
						}
					default:
						return sddl, fmt.Errorf("%s is an invalid SDDL section", string(elementType))
					}
				}

				if v == ':' {
					// set element type to last character
					elementType = rune(input[k-1])

					// await ACL flags
					if elementType == 'D' || elementType == 'S' {
						awaitingACLFlags = true
					}

					// set element start to next character
					if len(elementStart) == 0 { // start the list if it's empty
						elementStart = append(elementStart, k+1)
					} else if len(elementStart) > 1 {
						return sddl, errors.New("elementStart too long for starting a new part of a SDDL")
					} else { // assign the new element start
						elementStart[0] = k + 1
					}
				}
			}
		}
	}

	if scope > 0 || inString {
		return sddl, errors.New("string or scope not fully exited")
	}

	if err == nil {
		if !sanityCheckSDDLParse(input, sddl) {
			return sddl, errors.New("SDDL parsing sanity check failed")
		}
	}

	return
}

var sddlWhitespaceRegex = regexp.MustCompile(`[\x09-\x0D ]`)

func sanityCheckSDDLParse(original string, parsed SDDLString) bool {
	return sddlWhitespaceRegex.ReplaceAllString(original, "") ==
		sddlWhitespaceRegex.ReplaceAllString(parsed.String(), "")
}
