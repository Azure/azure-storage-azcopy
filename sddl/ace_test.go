package sddl

import (
	chk "gopkg.in/check.v1"
)

func (s *GoSDDLTestSuite) TestParseACEString(c *chk.C) {
	parseTests := []struct {
		input     string
		output    ACE
		expectErr bool
	}{
		{
			input: `(A;;RPWP;;;S-1-1-0)`,
			output: ACE{
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
		{
			input: `(RA;CI;;;;S-1-1-0; ("Project",TS,0,"Windows","SQL"))`,
			output: ACE{
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
					Items: []string{
						`"Windows"`,
						`"SQL"`,
					},
				},
			},
		},
		{
			input: `(XA;;FX;;;S-1-1-0;(@User.Title=="PM" && (@User.Division == "Finance" || @User.Division == "Sales")))`,
			output: ACE{
				ACEType:   EACEType.SDDL_CALLBACK_ACCESS_ALLOWED(),
				ACERights: EACERights.SDDL_FILE_EXECUTE(),
				AccountSID: SID{
					Revision:            1,
					IdentifierAuthority: 1,
					Subauthorities:      []int64{0},
					Shorthand:           "",
				},
				ResourceAttribute: derefedConditionalACEResourceAttribute{
					Expression: derefedConditionalExpression{
						SubExpressions: []derefedConditionalExpression{
							{
								Values: []string{
									"@User.Title",
									`"PM"`,
								},
								Operator: "==",
							},
							{
								SubExpressions: []derefedConditionalExpression{
									{
										SubExpressions: []derefedConditionalExpression{
											{
												Operator: "==",
												Values: []string{
													"@User.Division",
													`"Finance"`,
												},
											},
											{
												Operator: "==",
												Values: []string{
													"@User.Division",
													`"Sales"`,
												},
											},
										},
										Values: []string{
											subExprIdentifier + "0]",
											subExprIdentifier + "1]",
										},
										Operator: "||",
									},
								},
								Values: []string{
									subExprIdentifier + "0]",
								},
							},
						},
						Values: []string{
							subExprIdentifier + "0]",
							subExprIdentifier + "1]",
						},
						Operator: "&&",
					},
				},
			},
		},
		{ // Test no end
			input:     `(A;;;;`,
			expectErr: true,
		},
		{ // Test unclosed parentheses
			input:     `(A;;(;)`,
			expectErr: true,
		},
		{ // Test unclosed string
			input:     `(A;;";;)`,
			expectErr: true,
		},
		{ // test committing to an undefined region
			input:     `(A;;;;;;;;;;commitsomething)`,
			expectErr: true,
		},
		{ // Test an invalid SID
			input:     `(A;;;;;S-0)`,
			expectErr: true,
		},
		{ // Test a resource attribute type without a resource attribute
			input:     `(RA;;;;;)`,
			expectErr: true,
		},
		{ // Test a non-resource attribute type with a resource attribute
			input:     `(A;CI;;;;S-1-1-0; ("Secrecy",TU,0,3))`,
			expectErr: true,
		},
	}

	for _, v := range parseTests {
		c.Logf("(expectErr: %t) Testing %s", v.expectErr, v.input)
		a, err := ParseACE(v.input)

		if v.expectErr {
			c.Assert(err, chk.NotNil)
			c.Log("Got error: ", err)
		} else {
			c.Assert(err, chk.IsNil)

			// Dereference conditional ACEs
			if ra, ok := a.ResourceAttribute.(ConditionalACEResourceAttribute); ok {
				a.ResourceAttribute = s.dereferenceConditionalACEResourceAttribute(ra)
			}

			c.Assert(a, chk.DeepEquals, v.output)
		}
	}
}

// ToString tests in this case rely upon successful parsing.
// If the above test fails, THIS IS UNRELIABLE.
func (s *GoSDDLTestSuite) TestACEToString(c *chk.C) {
	toStringTests := []ACE{
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
				Items: []string{
					`"Windows"`,
					`"SQL"`,
				},
			},
		},
		{
			ACEType:   EACEType.SDDL_CALLBACK_ACCESS_ALLOWED(),
			ACERights: EACERights.SDDL_FILE_EXECUTE(),
			AccountSID: SID{
				Revision:            1,
				IdentifierAuthority: 1,
				Subauthorities:      []int64{0},
				Shorthand:           "",
			},
			ResourceAttribute: ConditionalACEResourceAttribute{
				ConditionalExpression{
					SubExpressions: []*ConditionalExpression{
						{
							Values: []string{
								"@User.Title",
								`"PM"`,
							},
							Operator: "==",
						},
						{
							SubExpressions: []*ConditionalExpression{
								{
									SubExpressions: []*ConditionalExpression{
										{
											Operator: "==",
											Values: []string{
												"@User.Division",
												`"Finance"`,
											},
										},
										{
											Operator: "==",
											Values: []string{
												"@User.Division",
												`"Sales"`,
											},
										},
									},
									Values: []string{
										subExprIdentifier + "0]",
										subExprIdentifier + "1]",
									},
									Operator: "||",
									inParens: true,
								},
							},
							Values: []string{
								subExprIdentifier + "0]",
							},
						},
					},
					Values: []string{
						subExprIdentifier + "0]",
						subExprIdentifier + "1]",
					},
					Operator: "&&",
				},
			},
		},
	}

	// We ToString it and then parse it because some parameters can be semi-randomized
	for _, v := range toStringTests {
		out := v.String()

		a, err := ParseACE(out)
		c.Assert(err, chk.IsNil)

		c.Log("Got " + a.String())
		if ra, ok := a.ResourceAttribute.(ConditionalACEResourceAttribute); ok {
			a.ResourceAttribute = s.dereferenceConditionalACEResourceAttribute(ra)
		}

		c.Log("Expected " + out)
		if ra, ok := v.ResourceAttribute.(ConditionalACEResourceAttribute); ok {
			v.ResourceAttribute = s.dereferenceConditionalACEResourceAttribute(ra)
		}

		c.Assert(a, chk.DeepEquals, v)
	}
}
