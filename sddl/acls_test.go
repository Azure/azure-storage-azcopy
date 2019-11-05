package sddl

import (
	chk "gopkg.in/check.v1"
)

func (s *GoSDDLTestSuite) TestParseACEEntries(c *chk.C) {
	parseTests := []struct {
		input     string
		output    ACLs
		expectErr bool
	}{
		{ // Test single ACL, no flags
			input: `(A;;RPWP;;;S-1-1-0)`,
			output: ACLs{
				Entries: []ACE{
					{
						ACEType: EACEType.SDDL_ACCESS_ALLOWED(),
						ACERights: EACERights.SDDL_READ_PROPERTY().
							Add(EACERights.SDDL_WRITE_PROPERTY()),
						AccountSID: SID{
							Revision:            1,
							IdentifierAuthority: 1,
							Subauthorities:      []int64{0},
							Shorthand:           "",
						},
					},
				},
			},
		},
		{ // Test single ACL, w/ flags
			input: `PAI(A;;RPWP;;;S-1-1-0)`,
			output: ACLs{
				ACLFlags: EACLFlags.SDDL_PROTECTED().Add(EACLFlags.SDDL_AUTO_INHERITED()),
				Entries: []ACE{
					{
						ACEType: EACEType.SDDL_ACCESS_ALLOWED(),
						ACERights: EACERights.SDDL_READ_PROPERTY().
							Add(EACERights.SDDL_WRITE_PROPERTY()),
						AccountSID: SID{
							Revision:            1,
							IdentifierAuthority: 1,
							Subauthorities:      []int64{0},
							Shorthand:           "",
						},
					},
				},
			},
		},
		{ // Test space in between ACL Flags
			input: `P AI (A;;RPWP;;;S-1-1-0)`,
			output: ACLs{
				ACLFlags: EACLFlags.SDDL_PROTECTED().Add(EACLFlags.SDDL_AUTO_INHERITED()),
				Entries: []ACE{
					{
						ACEType: EACEType.SDDL_ACCESS_ALLOWED(),
						ACERights: EACERights.SDDL_READ_PROPERTY().
							Add(EACERights.SDDL_WRITE_PROPERTY()),
						AccountSID: SID{
							Revision:            1,
							IdentifierAuthority: 1,
							Subauthorities:      []int64{0},
							Shorthand:           "",
						},
					},
				},
			},
		},
		{ // Test multiple ACLs, w/ flags
			input: `PAI(RA;CI;;;;S-1-1-0; ("Secrecy",TU,0,3))(RA;CI;;;;S-1-1-0; ("Project",TS,0,"Windows","SQL"))`,
			output: ACLs{
				ACLFlags: EACLFlags.SDDL_PROTECTED().Add(EACLFlags.SDDL_AUTO_INHERITED()),
				Entries: []ACE{
					{
						ACEType:  EACEType.SDDL_RESOURCE_ATTRIBUTE(),
						ACEFlags: EACEFlags.SDDL_CONTAINER_INHERIT(),
						AccountSID: SID{
							Revision:            1,
							IdentifierAuthority: 1,
							Subauthorities:      []int64{0},
							Shorthand:           "",
						},
						ResourceAttribute: ACEResourceAttributeValues{
							Name:           "Secrecy",
							Type:           EACEResourceAttributeValuesType.SDDL_UINT(),
							AttributeFlags: 0,
							Items:          []string{"3"},
						},
					},
					{
						ACEType:  EACEType.SDDL_RESOURCE_ATTRIBUTE(),
						ACEFlags: EACEFlags.SDDL_CONTAINER_INHERIT(),
						AccountSID: SID{
							Revision:            1,
							IdentifierAuthority: 1,
							Subauthorities:      []int64{0},
							Shorthand:           "",
						},
						ResourceAttribute: ACEResourceAttributeValues{
							Name:           "Project",
							Type:           EACEResourceAttributeValuesType.SDDL_WSTRING(),
							AttributeFlags: 0,
							Items:          []string{`"Windows"`, `"SQL"`},
						},
					},
				},
			},
		},
		{ // Test multiple ACLs, w/o flags
			input: `(RA;CI;;;;S-1-1-0; ("Secrecy",TU,0,3))(RA;CI;;;;S-1-1-0; ("Project",TS,0,"Windows","SQL"))`,
			output: ACLs{
				Entries: []ACE{
					{
						ACEType:  EACEType.SDDL_RESOURCE_ATTRIBUTE(),
						ACEFlags: EACEFlags.SDDL_CONTAINER_INHERIT(),
						AccountSID: SID{
							Revision:            1,
							IdentifierAuthority: 1,
							Subauthorities:      []int64{0},
							Shorthand:           "",
						},
						ResourceAttribute: ACEResourceAttributeValues{
							Name:           "Secrecy",
							Type:           EACEResourceAttributeValuesType.SDDL_UINT(),
							AttributeFlags: 0,
							Items:          []string{"3"},
						},
					},
					{
						ACEType:  EACEType.SDDL_RESOURCE_ATTRIBUTE(),
						ACEFlags: EACEFlags.SDDL_CONTAINER_INHERIT(),
						AccountSID: SID{
							Revision:            1,
							IdentifierAuthority: 1,
							Subauthorities:      []int64{0},
							Shorthand:           "",
						},
						ResourceAttribute: ACEResourceAttributeValues{
							Name:           "Project",
							Type:           EACEResourceAttributeValuesType.SDDL_WSTRING(),
							AttributeFlags: 0,
							Items:          []string{`"Windows"`, `"SQL"`},
						},
					},
				},
			},
		},
		{ // Test multiple ACLs w/ flags and spaces in between
			input: `PAI (RA;CI;;;;S-1-1-0; ("Secrecy",TU,0,3)) (RA;CI;;;;S-1-1-0; ("Project",TS,0,"Windows","SQL"))`,
			output: ACLs{
				ACLFlags: EACLFlags.SDDL_PROTECTED().Add(EACLFlags.SDDL_AUTO_INHERITED()),
				Entries: []ACE{
					{
						ACEType:  EACEType.SDDL_RESOURCE_ATTRIBUTE(),
						ACEFlags: EACEFlags.SDDL_CONTAINER_INHERIT(),
						AccountSID: SID{
							Revision:            1,
							IdentifierAuthority: 1,
							Subauthorities:      []int64{0},
							Shorthand:           "",
						},
						ResourceAttribute: ACEResourceAttributeValues{
							Name:           "Secrecy",
							Type:           EACEResourceAttributeValuesType.SDDL_UINT(),
							AttributeFlags: 0,
							Items:          []string{"3"},
						},
					},
					{
						ACEType:  EACEType.SDDL_RESOURCE_ATTRIBUTE(),
						ACEFlags: EACEFlags.SDDL_CONTAINER_INHERIT(),
						AccountSID: SID{
							Revision:            1,
							IdentifierAuthority: 1,
							Subauthorities:      []int64{0},
							Shorthand:           "",
						},
						ResourceAttribute: ACEResourceAttributeValues{
							Name:           "Project",
							Type:           EACEResourceAttributeValuesType.SDDL_WSTRING(),
							AttributeFlags: 0,
							Items:          []string{`"Windows"`, `"SQL"`},
						},
					},
				},
			},
		},
		{ // Test closing parentheses inside of string
			input: `PAI(RA;CI;;;;S-1-1-0; ("Secrecy)",TU,0,3))`,
			output: ACLs{
				ACLFlags: EACLFlags.SDDL_PROTECTED().Add(EACLFlags.SDDL_AUTO_INHERITED()),
				Entries: []ACE{
					{
						ACEType:  EACEType.SDDL_RESOURCE_ATTRIBUTE(),
						ACEFlags: EACEFlags.SDDL_CONTAINER_INHERIT(),
						AccountSID: SID{
							Revision:            1,
							IdentifierAuthority: 1,
							Subauthorities:      []int64{0},
							Shorthand:           "",
						},
						ResourceAttribute: ACEResourceAttributeValues{
							Name:           "Secrecy)",
							Type:           EACEResourceAttributeValuesType.SDDL_UINT(),
							AttributeFlags: 0,
							Items:          []string{"3"},
						},
					},
				},
			},
		},
		{ // Since we've tested resource attributes and normal ACLs, let's give conditionals a spin.
			input: `PAI(XA;;RPWP;;;S-1-1-0; (x == y))`,
			output: ACLs{
				ACLFlags: EACLFlags.SDDL_PROTECTED().Add(EACLFlags.SDDL_AUTO_INHERITED()),
				Entries: []ACE{
					{
						ACEType:   EACEType.SDDL_CALLBACK_ACCESS_ALLOWED(),
						ACERights: EACERights.SDDL_READ_PROPERTY().Add(EACERights.SDDL_WRITE_PROPERTY()),
						AccountSID: SID{
							Revision:            1,
							IdentifierAuthority: 1,
							Subauthorities:      []int64{0},
							Shorthand:           "",
						},
						ResourceAttribute: ConditionalACEResourceAttribute{Expression: ConditionalExpression{
							SubExpressions: nil,
							Values:         []string{"x", "y"},
							Operator:       "==",
							inParens:       false,
						}},
					},
				},
			},
		},
		{ // Test multiple sets of flags in between an ACL.
			input:     `P(A;;RPWP;;;S-1-1-0)AI`,
			expectErr: true,
		},
		{ // Test unclosed parentheses
			input:     `P(A;;RPWP;;;S-1-1-0`,
			expectErr: true,
		},
		{ // Test unclosed string
			input:     `P(A;;RPWP;;;"S-1-1-0)`,
			expectErr: true,
		},
		{ // Test unexpected item in bagging area
			input:     `(A))`,
			expectErr: true,
		},
	}

	for _, v := range parseTests {
		c.Log("(ExpectErr: ", v.expectErr, ") Test-parsing ", v.input)
		entries, err := ParseACEEntries(v.input)

		if v.expectErr {
			c.Assert(err, chk.NotNil)
			c.Log("got error: ", err)
		} else { // TODO: Deref conditionals
			c.Assert(err, chk.IsNil)
			c.Assert(entries, chk.DeepEquals, v.output)
		}
	}
}

// This test relies on parsing succeeding. If the above test fails, THIS IS UNRELIABLE.
func (s *GoSDDLTestSuite) TestACEEntriesToString(c *chk.C) {
	toStringTests := []ACLs{
		{ // Test single entry
			Entries: []ACE{
				{
					ACEType: EACEType.SDDL_ACCESS_ALLOWED(),
					ACERights: EACERights.SDDL_READ_PROPERTY().
						Add(EACERights.SDDL_WRITE_PROPERTY()),
					AccountSID: SID{
						Revision:            1,
						IdentifierAuthority: 1,
						Subauthorities:      []int64{0},
						Shorthand:           "",
					},
				},
			},
		},
		{ // Test single entry with flags
			ACLFlags: EACLFlags.SDDL_PROTECTED().Add(EACLFlags.SDDL_AUTO_INHERITED()),
			Entries: []ACE{
				{
					ACEType: EACEType.SDDL_ACCESS_ALLOWED(),
					ACERights: EACERights.SDDL_READ_PROPERTY().
						Add(EACERights.SDDL_WRITE_PROPERTY()),
					AccountSID: SID{
						Revision:            1,
						IdentifierAuthority: 1,
						Subauthorities:      []int64{0},
						Shorthand:           "",
					},
				},
			},
		},
		{ // Test multiple entries without flags
			Entries: []ACE{
				{
					ACEType:  EACEType.SDDL_RESOURCE_ATTRIBUTE(),
					ACEFlags: EACEFlags.SDDL_CONTAINER_INHERIT(),
					AccountSID: SID{
						Revision:            1,
						IdentifierAuthority: 1,
						Subauthorities:      []int64{0},
						Shorthand:           "",
					},
					ResourceAttribute: ACEResourceAttributeValues{
						Name:           "Secrecy",
						Type:           EACEResourceAttributeValuesType.SDDL_UINT(),
						AttributeFlags: 0,
						Items:          []string{"3"},
					},
				},
				{
					ACEType:  EACEType.SDDL_RESOURCE_ATTRIBUTE(),
					ACEFlags: EACEFlags.SDDL_CONTAINER_INHERIT(),
					AccountSID: SID{
						Revision:            1,
						IdentifierAuthority: 1,
						Subauthorities:      []int64{0},
						Shorthand:           "",
					},
					ResourceAttribute: ACEResourceAttributeValues{
						Name:           "Project",
						Type:           EACEResourceAttributeValuesType.SDDL_WSTRING(),
						AttributeFlags: 0,
						Items:          []string{`"Windows"`, `"SQL"`},
					},
				},
			},
		},
		{ // Test multiple entries with flags
			ACLFlags: EACLFlags.SDDL_PROTECTED().Add(EACLFlags.SDDL_AUTO_INHERITED()),
			Entries: []ACE{
				{
					ACEType:  EACEType.SDDL_RESOURCE_ATTRIBUTE(),
					ACEFlags: EACEFlags.SDDL_CONTAINER_INHERIT(),
					AccountSID: SID{
						Revision:            1,
						IdentifierAuthority: 1,
						Subauthorities:      []int64{0},
						Shorthand:           "",
					},
					ResourceAttribute: ACEResourceAttributeValues{
						Name:           "Secrecy",
						Type:           EACEResourceAttributeValuesType.SDDL_UINT(),
						AttributeFlags: 0,
						Items:          []string{"3"},
					},
				},
				{
					ACEType:  EACEType.SDDL_RESOURCE_ATTRIBUTE(),
					ACEFlags: EACEFlags.SDDL_CONTAINER_INHERIT(),
					AccountSID: SID{
						Revision:            1,
						IdentifierAuthority: 1,
						Subauthorities:      []int64{0},
						Shorthand:           "",
					},
					ResourceAttribute: ACEResourceAttributeValues{
						Name:           "Project",
						Type:           EACEResourceAttributeValuesType.SDDL_WSTRING(),
						AttributeFlags: 0,
						Items:          []string{`"Windows"`, `"SQL"`},
					},
				},
			},
		},
		{ // Let's give conditionals a spin.
			ACLFlags: EACLFlags.SDDL_PROTECTED().Add(EACLFlags.SDDL_AUTO_INHERITED()),
			Entries: []ACE{
				{
					ACEType:   EACEType.SDDL_CALLBACK_ACCESS_ALLOWED(),
					ACERights: EACERights.SDDL_READ_PROPERTY().Add(EACERights.SDDL_WRITE_PROPERTY()),
					AccountSID: SID{
						Revision:            1,
						IdentifierAuthority: 1,
						Subauthorities:      []int64{0},
						Shorthand:           "",
					},
					ResourceAttribute: ConditionalACEResourceAttribute{Expression: ConditionalExpression{
						SubExpressions: nil,
						Values:         []string{"x", "y"},
						Operator:       "==",
						inParens:       false,
					}},
				},
			},
		},
	}

	for _, v := range toStringTests {
		out := v.String()

		c.Log("Stringified ", out)

		entries, err := ParseACEEntries(out)

		c.Assert(err, chk.IsNil)
		c.Assert(entries, chk.DeepEquals, v)
	}
}
