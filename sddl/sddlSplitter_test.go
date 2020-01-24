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
