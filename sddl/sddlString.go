package sddl

import (
	"errors"
	"fmt"
	"regexp"
	"sort"
	"strings"
)

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

func (s SDDLString) Compare(other SDDLString) bool {
	matching := true

	s, _ = ParseSDDL(s.PortableString())
	o, _ := ParseSDDL(other.PortableString())

	matching = matching && (s.OwnerSID == o.OwnerSID) // Compare owners
	matching = matching && (s.GroupSID == o.GroupSID)

	// compare flags
	matching = matching && compareFlags(strings.TrimSuffix(s.DACL.Flags, "NO_ACCESS_CONTROL"), strings.TrimSuffix(o.DACL.Flags, "NO_ACCESS_CONTROL"))
	matching = matching && compareFlags(strings.TrimSuffix(s.SACL.Flags, "NO_ACCESS_CONTROL"), strings.TrimSuffix(o.SACL.Flags, "NO_ACCESS_CONTROL"))

	// compare ACEs
	matching = matching && compareACEs(s.DACL.ACLEntries, o.DACL.ACLEntries)
	matching = matching && compareACEs(s.SACL.ACLEntries, o.SACL.ACLEntries)

	return matching
}

func compareFlags(a, b string) bool {
	a = strings.ToUpper(a)
	b = strings.ToUpper(b)

	if len(a) != len(b) {
		// obvious indicator
		return false
	}

	aEntries := make(map[string]bool)

	if len(a)%2 != 0 {
		// this only happens with P (protected). It could also happen with A or D, but we don't use this function for ACE type.
		aidx := strings.IndexByte(a, 'P')
		bidx := strings.IndexByte(b, 'P')

		a = a[:aidx] + a[aidx+1:]
		b = b[:bidx] + b[bidx+1:]
	}

	for i := 0; i < len(a); i += 2 { // flags, outside of NO_ACCESS_CONTROL (which should be trimmed before hitting this) are pairs of two upper-case letters.
		aEntries[a[i:i+2]] = true
	}

	for i := 0; i < len(b); i += 2 {
		str := b[i : i+2]

		if ok := aEntries[str]; ok {
			delete(aEntries, str)
		} else {
			return false
		}
	}

	if len(aEntries) > 0 {
		return false
	}

	return true
}

func compareACEs(a, b []ACLEntry) bool {
	if len(a) != len(b) {
		// obvious indicator
		return false
	}

	aMismatches := make([]ACLEntry, len(a))
	copy(aMismatches, a)

	for _, bACE := range b {
		foundMatch := false

		for k, aACE := range aMismatches {
			aceMatch := true

			if len(aACE.Sections) != len(bACE.Sections) {
				continue // not a match, for sure
			}

			aceMatch = aceMatch && (aACE.Sections[0] == bACE.Sections[0])           // match ace type
			aceMatch = aceMatch && compareFlags(aACE.Sections[1], bACE.Sections[1]) // compare ace flags
			aceMatch = aceMatch && compareFlags(aACE.Sections[2], bACE.Sections[2]) // compare rights
			aceMatch = aceMatch && aACE.Sections[3] == bACE.Sections[3]             // compare object guid
			aceMatch = aceMatch && aACE.Sections[4] == bACE.Sections[4]             // compare inherit object guid
			aceMatch = aceMatch && aACE.Sections[5] == bACE.Sections[5]             // compare SID
			if len(aACE.Sections) == 7 {
				aceMatch = aceMatch && aACE.Sections[6] == bACE.Sections[6] // compare resource attribute (in a naive way, since we don't use them in tests.)
			}

			if aceMatch {
				// delete matches.
				aMismatches = append(aMismatches[:k], aMismatches[k+1:]...)
				foundMatch = true
				break
			}
		}

		if !foundMatch {
			return false
		}
	}

	return true
}
