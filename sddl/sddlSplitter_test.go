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

package sddl_test

import (
	"testing"

	chk "gopkg.in/check.v1"

	"github.com/Azure/azure-storage-azcopy/sddl"
)

// Hookup to the testing framework
func Test(t *testing.T) { chk.TestingT(t) }

type sddlTestSuite struct{}

var _ = chk.Suite(&sddlTestSuite{})

func (*sddlTestSuite) TestSDDLSplitting(c *chk.C) {
	tests := []struct {
		input  string
		result sddl.SDDLString
	}{
		{ // Test single section
			input: "G:DU",
			result: sddl.SDDLString{
				GroupSID: "DU",
			},
		},
		{ // Test multiple sections, no space
			input: "O:AOG:DU",
			result: sddl.SDDLString{
				GroupSID: "DU",
				OwnerSID: "AO",
			},
		},
		{ // Test multiple sections, with space
			input: "O:AO G:DU",
			result: sddl.SDDLString{
				GroupSID: "DU",
				OwnerSID: "AO", // The splitter trims spaces on the ends.
			},
		},
		{ // Test DACL with only flags, SACL following
			input: "D:PAIS:PAI",
			result: sddl.SDDLString{
				DACL: sddl.ACLList{
					Flags: "PAI",
				},
				SACL: sddl.ACLList{
					Flags: "PAI",
				},
			},
		},
		{ // Test DACL with only flags
			input: "D:PAI",
			result: sddl.SDDLString{
				DACL: sddl.ACLList{
					Flags: "PAI",
				},
			},
		},
		{ // Test simple SDDL
			input: "O:AOG:DAD:(A;;RPWPCCDCLCSWRCWDWOGA;;;S-1-0-0)",
			result: sddl.SDDLString{
				OwnerSID: "AO",
				GroupSID: "DA",
				DACL: sddl.ACLList{
					Flags: "",
					ACLEntries: []sddl.ACLEntry{
						{
							Sections: []string{
								"A",
								"",
								"RPWPCCDCLCSWRCWDWOGA",
								"",
								"",
								"S-1-0-0",
							},
						},
					},
				},
			},
		},
		{ // Test multiple ACLs
			input: "O:AOG:DAD:(A;;RPWPCCDCLCSWRCWDWOGA;;;S-1-0-0)(A;;RPWPCCDCLCSWRCWDWOGA;;;S-1-0-0)",
			result: sddl.SDDLString{
				OwnerSID: "AO",
				GroupSID: "DA",
				DACL: sddl.ACLList{
					Flags: "",
					ACLEntries: []sddl.ACLEntry{
						{
							Sections: []string{
								"A",
								"",
								"RPWPCCDCLCSWRCWDWOGA",
								"",
								"",
								"S-1-0-0",
							},
						},
						{
							Sections: []string{
								"A",
								"",
								"RPWPCCDCLCSWRCWDWOGA",
								"",
								"",
								"S-1-0-0",
							},
						},
					},
				},
			},
		},
		{ // Test a particularly weird conditional. We include parentheses inside of a string, and with the SID identifier.
			input: `O:AOG:DAD:(XA; ;FX;;;S-1-1-0; (@User.Title=="PM SID(" && (@User.Division=="Fi || nance" || @User.Division ==" Sales")))`,
			result: sddl.SDDLString{
				OwnerSID: "AO",
				GroupSID: "DA",
				DACL: sddl.ACLList{
					Flags: "",
					ACLEntries: []sddl.ACLEntry{
						{
							Sections: []string{
								"XA",
								" ",
								"FX",
								"",
								"",
								"S-1-1-0",
								` (@User.Title=="PM SID(" && (@User.Division=="Fi || nance" || @User.Division ==" Sales"))`,
							},
						},
					},
				},
			},
		},
	}

	for _, v := range tests {
		res, err := sddl.ParseSDDL(v.input)

		c.Assert(err, chk.IsNil)
		c.Log("Input: ", v.input, " Expected result: ", v.result.String(), " Actual result: ", res.String())
		c.Assert(res, chk.DeepEquals, v.result)
	}
}
