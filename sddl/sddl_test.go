package sddl

import (
	chk "gopkg.in/check.v1"
)

func (s *GoSDDLTestSuite) TestParseSDDLString(c *chk.C) {
	parseTests := []struct {
		input     string
		output    SDDL
		expectErr bool // In theory, ParseSDDL will inherit all potential errors of objects under it.
	}{
		{ // Shorthands were used within https://docs.microsoft.com/en-us/windows/win32/secauthz/security-descriptor-string-format, but instead, longhand forms are used so we don't encounter the OS-specific de-portable-izing
			// TODO: Review if we should even DO that automatically. It might have unintended consequences.
			input: `O:S-1-1-0G:S-1-1-0D:(A;;RPWP;;;S-1-1-0)S:(D;;RPWP;;;S-1-1-0)`,
			output: SDDL{
				Owner: SID{
					Revision:            1,
					IdentifierAuthority: 1,
					Subauthorities:      []int64{0},
				},
				Group: SID{
					Revision:            1,
					IdentifierAuthority: 1,
					Subauthorities:      []int64{0},
				},
				DACLs: ACLs{
					Entries: []ACE{
						{
							ACEType:   EACEType.SDDL_ACCESS_ALLOWED(),
							ACERights: EACERights.SDDL_READ_PROPERTY().Add(EACERights.SDDL_WRITE_PROPERTY()),
							AccountSID: SID{
								Revision:            1,
								IdentifierAuthority: 1,
								Subauthorities:      []int64{0},
							},
						},
					},
				},
				SACLs: ACLs{
					Entries: []ACE{
						{
							ACEType:   EACEType.SDDL_ACCESS_DENIED(),
							ACERights: EACERights.SDDL_READ_PROPERTY().Add(EACERights.SDDL_WRITE_PROPERTY()),
							AccountSID: SID{
								Revision:            1,
								IdentifierAuthority: 1,
								Subauthorities:      []int64{0},
							},
						},
					},
				},
			},
		},
		{ // space between segments
			input: `O:S-1-1-0 D:P(RA;CI;;;;S-1-1-0; ("Secrecy",TU,0,3))`,
			output: SDDL{
				Owner: SID{
					Revision:            1,
					IdentifierAuthority: 1,
					Subauthorities:      []int64{0},
				},
				DACLs: ACLs{
					ACLFlags: EACLFlags.SDDL_PROTECTED(),
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
					},
				},
			},
		},
		{ // multiple ACE strings with spaces
			input: `O:S-1-1-0G:S-1-1-0D:(A;;RPWP;;;S-1-1-0) (D;;RPWP;;;S-1-1-0) S:(D;;RPWP;;;S-1-1-0)`,
			output: SDDL{
				Owner: SID{
					Revision:            1,
					IdentifierAuthority: 1,
					Subauthorities:      []int64{0},
				},
				Group: SID{
					Revision:            1,
					IdentifierAuthority: 1,
					Subauthorities:      []int64{0},
				},
				DACLs: ACLs{
					Entries: []ACE{
						{
							ACEType:   EACEType.SDDL_ACCESS_ALLOWED(),
							ACERights: EACERights.SDDL_READ_PROPERTY().Add(EACERights.SDDL_WRITE_PROPERTY()),
							AccountSID: SID{
								Revision:            1,
								IdentifierAuthority: 1,
								Subauthorities:      []int64{0},
							},
						},
						{
							ACEType:   EACEType.SDDL_ACCESS_DENIED(),
							ACERights: EACERights.SDDL_READ_PROPERTY().Add(EACERights.SDDL_WRITE_PROPERTY()),
							AccountSID: SID{
								Revision:            1,
								IdentifierAuthority: 1,
								Subauthorities:      []int64{0},
							},
						},
					},
				},
				SACLs: ACLs{
					Entries: []ACE{
						{
							ACEType:   EACEType.SDDL_ACCESS_DENIED(),
							ACERights: EACERights.SDDL_READ_PROPERTY().Add(EACERights.SDDL_WRITE_PROPERTY()),
							AccountSID: SID{
								Revision:            1,
								IdentifierAuthority: 1,
								Subauthorities:      []int64{0},
							},
						},
					},
				},
			},
		},
		{ // Don't finish parentheses
			input:     `O:S-1-1-0 D:P(RA;CI;;;;S-1-1-0; ("Secrecy",TU,0,3)`,
			expectErr: true,
		},
		{ // Have a invalid SID
			input:     `O:S-1-1 D:P(RA;CI;;;;S-1-1-0; ("Secrecy",TU,0,3))`,
			expectErr: true,
		},
		{ // Don't finish a string
			input:     `O:S-1-1-0 D:P(RA;CI;;;;S-1-1-0; ("Secrecy,TU,0,3))`,
			expectErr: true,
		},
		{ // Don't finish an array
			input:     `O:S-1-1-0 D:P(XA;CI;;;;S-1-1-0; (Member_of{x,y,z))`,
			expectErr: true,
		},
	}

	for _, v := range parseTests {
		c.Log("(ExpectErr: ", v.expectErr, ") Test-parsing ", v.input)
		sddl, err := ParseSDDL(v.input)

		if v.expectErr {
			c.Assert(err, chk.NotNil)
			c.Log("Got error: ", err.Error())
		} else {
			c.Assert(err, chk.IsNil)
			c.Assert(sddl, chk.DeepEquals, v.output)
		}
	}
}

func (s *GoSDDLTestSuite) TestSDDLStringToString(c *chk.C) {
	toStringTests := []SDDL{
		{
			Owner: SID{
				Revision:            1,
				IdentifierAuthority: 1,
				Subauthorities:      []int64{0},
			},
			Group: SID{
				Revision:            1,
				IdentifierAuthority: 1,
				Subauthorities:      []int64{0},
			},
			DACLs: ACLs{
				Entries: []ACE{
					{
						ACEType:   EACEType.SDDL_ACCESS_ALLOWED(),
						ACERights: EACERights.SDDL_READ_PROPERTY().Add(EACERights.SDDL_WRITE_PROPERTY()),
						AccountSID: SID{
							Revision:            1,
							IdentifierAuthority: 1,
							Subauthorities:      []int64{0},
						},
					},
					{
						ACEType:   EACEType.SDDL_CALLBACK_ACCESS_ALLOWED(),
						ACERights: EACERights.SDDL_WRITE_PROPERTY(),
						AccountSID: SID{
							Revision:            1,
							IdentifierAuthority: 1,
							Subauthorities:      []int64{0},
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
			SACLs: ACLs{
				Entries: []ACE{
					{
						ACEType: EACEType.SDDL_RESOURCE_ATTRIBUTE(),
						AccountSID: SID{
							Revision:            1,
							IdentifierAuthority: 1,
							Subauthorities:      []int64{0},
						},
						ResourceAttribute: ACEResourceAttributeValues{
							Name:           "Test",
							Type:           EACEResourceAttributeValuesType.SDDL_WSTRING(),
							AttributeFlags: 0,
							Items:          []string{`"TestValue"`},
						},
					},
				},
			},
		},
		{
			Owner: SID{
				Revision:            1,
				IdentifierAuthority: 1,
				Subauthorities:      []int64{0},
			},
			Group: SID{
				Revision:            1,
				IdentifierAuthority: 1,
				Subauthorities:      []int64{0},
			},
			DACLs: ACLs{
				Entries: []ACE{
					{
						ACEType:   EACEType.SDDL_ACCESS_ALLOWED(),
						ACERights: EACERights.SDDL_READ_PROPERTY().Add(EACERights.SDDL_WRITE_PROPERTY()),
						AccountSID: SID{
							Revision:            1,
							IdentifierAuthority: 1,
							Subauthorities:      []int64{0},
						},
					},
					{
						ACEType:   EACEType.SDDL_ACCESS_DENIED(),
						ACERights: EACERights.SDDL_READ_PROPERTY().Add(EACERights.SDDL_WRITE_PROPERTY()),
						AccountSID: SID{
							Revision:            1,
							IdentifierAuthority: 1,
							Subauthorities:      []int64{0},
						},
					},
				},
			},
			SACLs: ACLs{
				Entries: []ACE{
					{
						ACEType:   EACEType.SDDL_ACCESS_DENIED(),
						ACERights: EACERights.SDDL_READ_PROPERTY().Add(EACERights.SDDL_WRITE_PROPERTY()),
						AccountSID: SID{
							Revision:            1,
							IdentifierAuthority: 1,
							Subauthorities:      []int64{0},
						},
					},
				},
			},
		},
	}

	for _, v := range toStringTests {
		out := v.String()

		c.Log("Stringifying ", out)

		sddl, err := ParseSDDL(out)

		c.Assert(err, chk.IsNil)
		c.Assert(sddl, chk.DeepEquals, v)
	}
}
