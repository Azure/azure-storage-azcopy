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
	"strings"
)

var translateSID = OSTranslateSID // this layer of indirection is to support unit testing. TODO: it's ugly to set a global to test. Do something better one day

func IffInt(condition bool, tVal, fVal int) int {
	if condition {
		return tVal
	}
	return fVal
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
