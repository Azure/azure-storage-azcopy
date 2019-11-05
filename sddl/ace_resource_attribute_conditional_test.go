package sddl

import (
	chk "gopkg.in/check.v1"
	"strconv"
)

// This test relies upon the success of the parsing test.
// This is because rather than manually constructing conditional ACEs (a surprising amount of work)
// Because we're aware of parentheses, we can generate an equivalent structure basically all the time.
// We are, however, more antsy about spaces than the original structure.
func (s *GoSDDLTestSuite) TestConditionalACEResourceAttributeToString(c *chk.C) {
	toStringTests := []struct {
		input  string
		output string
	}{
		{ // test bumping contiguous operators to noncontiguous operators
			input:  "(x==y)",
			output: "(x == y)",
		},
		{
			input:  "(x == y || y == x)",
			output: "(x == y || y == x)",
		},
		{ // check explicit parentheses carrying
			input:  "(x == y || (y == x) || y == a)",
			output: "(x == y || (y == x) || y == a)",
		},
		{
			input:  `(@User.Title=="PM" && (@User.Division=="Finance"||@User.Division=="Sales"))`,
			output: `(@User.Title == "PM" && (@User.Division == "Finance" || @User.Division == "Sales"))`,
		},
		{ // Test member_of
			input:  `(Member_of {SID(Smartcard_SID), SID(BO)} && @Device.Bitlocker)`,
			output: `(Member_of {SID(Smartcard_SID), SID(BO)} && @Device.Bitlocker)`,
		},
		{ // test exists
			input:  `(exists x)`,
			output: `(exists x)`,
		},
		{
			input:  `(x)`,
			output: `(x)`,
		},
		{
			input:  `(!(x))`,
			output: `(!(x))`,
		},
		{ // This is invalid format, so we should correct it.
			input:  `(!x)`,
			output: `(!(x))`,
		},
		{ // Test that && rules over ||
			input:  `(x == y || y != z && z != x || y != a)`,
			output: `(x == y || y != z && z != x || y != a)`,
		},
		{ // Test ! with a actual conditional
			input:  `(!(x == y))`,
			output: `(!(x == y))`,
		},
		{ // Test nested ()
			input:  `((((x == y))))`,
			output: `((((x == y))))`,
		},
	}

	for _, v := range toStringTests {
		conditional, err := ParseConditionalACEResourceAttribute(v.input)

		c.Assert(err, chk.IsNil)
		c.Assert(conditional.StringifyResourceAttribute(), chk.Equals, v.output)
	}
}

// This is a set of testing types.
// It's a version of ConditionalACEResourceAttribute, but it dereferences subexpressions.
// This is because pointers can't be properly tested.
// So, we deref at testing time.
// This results in more understandable tests.
type derefedConditionalACEResourceAttribute struct {
	Expression derefedConditionalExpression
}

type derefedConditionalExpression struct {
	SubExpressions []derefedConditionalExpression
	Values         []string
	Operator       string
}

// This is here for ace_test.go
// We just need to plop this into an ACE.
// The actual function is not used.
func (d derefedConditionalACEResourceAttribute) StringifyResourceAttribute() string {
	return ""
}

func (s *GoSDDLTestSuite) dereferenceConditionalACEResourceAttribute(conditional ConditionalACEResourceAttribute) derefedConditionalACEResourceAttribute {
	output := derefedConditionalACEResourceAttribute{}

	s.dereferenceConditionalExpression(conditional.Expression, &output.Expression)

	return output
}

func (s *GoSDDLTestSuite) dereferenceConditionalExpression(conditional ConditionalExpression, target *derefedConditionalExpression) {
	target.Values = conditional.Values
	target.Operator = conditional.Operator

	if len(conditional.SubExpressions) > 0 {
		target.SubExpressions = make([]derefedConditionalExpression, len(conditional.SubExpressions))

		for k, v := range conditional.SubExpressions {
			s.dereferenceConditionalExpression(*v, &target.SubExpressions[k])
		}
	}
}

func (s *GoSDDLTestSuite) TestParseConditionalACEResourceAttribute(c *chk.C) {
	parseTests := []struct {
		input     string
		output    derefedConditionalACEResourceAttribute
		expectErr bool
	}{
		{ // valid example from https://docs.microsoft.com/en-us/windows/win32/secauthz/security-descriptor-definition-language-for-conditional-aces-#examples
			input: `(@User.Title=="PM" && (@User.Division=="Finance"||@User.Division=="Sales"))`,
			output: derefedConditionalACEResourceAttribute{
				derefedConditionalExpression{
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
		{ // valid example from https://docs.microsoft.com/en-us/windows/win32/secauthz/security-descriptor-definition-language-for-conditional-aces-#examples
			input: `(@User.Project Any_of @Resource.Project)`,
			output: derefedConditionalACEResourceAttribute{
				Expression: derefedConditionalExpression{
					SubExpressions: nil,
					Values:         []string{"@User.Project", "@Resource.Project"},
					Operator:       "Any_of",
				},
			},
		},
		{ // valid example from https://docs.microsoft.com/en-us/windows/win32/secauthz/security-descriptor-definition-language-for-conditional-aces-#examples
			input: `(Member_of {SID(Smartcard_SID), SID(BO)} && @Device.Bitlocker)`,
			output: derefedConditionalACEResourceAttribute{Expression: derefedConditionalExpression{
				SubExpressions: []derefedConditionalExpression{
					{
						Values:   []string{"SID(Smartcard_SID)", "SID(BO)"},
						Operator: "Member_of",
					},
					{
						Values: []string{"@Device.Bitlocker"},
					},
				},
				Values: []string{
					subExprIdentifier + "0]",
					subExprIdentifier + "1]",
				},
				Operator: "&&",
			}},
		},
		{ // Test conditional precedence
			input: `(x == y || y != z || z != x)`,
			output: derefedConditionalACEResourceAttribute{Expression: derefedConditionalExpression{
				SubExpressions: []derefedConditionalExpression{
					{ // conditional expression (x == y || y != z)
						SubExpressions: []derefedConditionalExpression{
							{ // x == y
								Values:   []string{"x", "y"},
								Operator: "==",
							},
							{ // y != z
								Values:   []string{"y", "z"},
								Operator: "!=",
							},
						},
						Values: []string{
							subExprIdentifier + "0]",
							subExprIdentifier + "1]",
						},
						Operator: "||",
					},
					{ // z != x
						Values:   []string{"z", "x"},
						Operator: "!=",
					},
				},
				Values: []string{
					subExprIdentifier + "0]",
					subExprIdentifier + "1]",
				},
				Operator: "||",
			}},
		},
		{ // Test the NOT statement
			input: `(!x)`,
			output: derefedConditionalACEResourceAttribute{Expression: derefedConditionalExpression{
				Values:   []string{"x"},
				Operator: "!",
			}},
		},
		{ // Test the NOT statement
			input: `(!(x))`,
			output: derefedConditionalACEResourceAttribute{Expression: derefedConditionalExpression{
				SubExpressions: []derefedConditionalExpression{
					{
						Values: []string{"x"},
					},
				},
				Values:   []string{subExprIdentifier + "0]"},
				Operator: "!",
			}},
		},
		{ // Test the nonzero statement
			input: `(x)`,
			output: derefedConditionalACEResourceAttribute{Expression: derefedConditionalExpression{
				Values:   []string{"x"},
				Operator: "",
			}},
		},
		{ // Test an operator in a totally contiguous statement
			input: `(x==y)`,
			output: derefedConditionalACEResourceAttribute{Expression: derefedConditionalExpression{
				Values:   []string{"x", "y"},
				Operator: "==",
			}},
		},
		{ // Test that && rules over ||
			input: `(x == y || y != z && z != x || y != a)`,
			output: derefedConditionalACEResourceAttribute{Expression: derefedConditionalExpression{
				SubExpressions: []derefedConditionalExpression{
					{
						SubExpressions: []derefedConditionalExpression{
							{
								Values:   []string{"x", "y"},
								Operator: "==",
							},
							{
								Values:   []string{"y", "z"},
								Operator: "!=",
							},
						},
						Values: []string{
							subExprIdentifier + "0]",
							subExprIdentifier + "1]",
						},
						Operator: "||",
					},
					{
						SubExpressions: []derefedConditionalExpression{
							{
								Values:   []string{"z", "x"},
								Operator: "!=",
							},
							{
								Values:   []string{"y", "a"},
								Operator: "!=",
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
					subExprIdentifier + "1]",
				},
				Operator: "&&",
			}},
		},
		{ // Test subexpr == subexpr. This probably will never happen, but I don't want to rule it out.
			input: `((x) == (y))`,
			output: derefedConditionalACEResourceAttribute{
				Expression: derefedConditionalExpression{
					SubExpressions: []derefedConditionalExpression{
						{
							Values:   []string{"x"},
							Operator: "",
						},
						{
							Values:   []string{"y"},
							Operator: "",
						},
					},
					Values:   []string{"subExpr[0]", "subExpr[1]"},
					Operator: "==",
				},
			},
		},

		// The next four tests test the operator separation.
		// >= and > share similar characters.
		{ // Test >=
			input: `(x>=y)`,
			output: derefedConditionalACEResourceAttribute{Expression: derefedConditionalExpression{
				Values:   []string{"x", "y"},
				Operator: ">=",
			}},
		},
		{ // Test >
			input: `(x>y)`,
			output: derefedConditionalACEResourceAttribute{Expression: derefedConditionalExpression{
				Values:   []string{"x", "y"},
				Operator: ">",
			}},
		},
		{ // test <=
			input: `(x<=y)`,
			output: derefedConditionalACEResourceAttribute{Expression: derefedConditionalExpression{
				Values:   []string{"x", "y"},
				Operator: "<=",
			}},
		},
		{ // test <
			input: `(x<y)`,
			output: derefedConditionalACEResourceAttribute{Expression: derefedConditionalExpression{
				Values:   []string{"x", "y"},
				Operator: "<",
			}},
		},
		{ // test unsafe opword in string
			input: `(wordexists)`,
			output: derefedConditionalACEResourceAttribute{Expression: derefedConditionalExpression{
				Values:   []string{"wordexists"},
				Operator: "",
			}},
		},
		{ // test unsafe opword in string
			input: `(existsword)`,
			output: derefedConditionalACEResourceAttribute{Expression: derefedConditionalExpression{
				Values:   []string{"existsword"},
				Operator: "",
			}},
		},

		// The following tests test compliance with explicit start/end data types
		{ // Test unending subexpression
			input:     `((x == y)`,
			expectErr: true,
		},
		{ // Test non-ending member_of array
			input:     `(Member_of{x,y,z)`,
			expectErr: true,
		},
		{ // Test non-ending string
			input:     `("test)`,
			expectErr: true,
		},
		{ // Test non-surrounded expression
			input:     `(test`,
			expectErr: true,
		},

		// The following tests test compliance with value count.
		// ========== less parameters ===========
		// How are these first two any different from the next two?
		// They shouldn't be. It's about how we index the string.
		// The first two used to crash us.
		{ // Test || having less parameters than needed
			input:     `(x ||)`,
			expectErr: true,
		},
		{ // Test && having less parameters than needed
			input:     `(x &&)`,
			expectErr: true,
		},
		{ // Test || having less parameters than needed
			input:     `(x || )`,
			expectErr: true,
		},
		{ // Test && having less parameters than needed
			input:     `(x && )`,
			expectErr: true,
		},
		{ // Test || having less parameters than needed
			input:     `( || x)`,
			expectErr: true,
		},
		{ // Test && having less parameters than needed
			input:     `( && x)`,
			expectErr: true,
		},
		{ // Test || having less parameters than needed
			input:     `(|| x)`,
			expectErr: true,
		},
		{ // Test && having less parameters than needed
			input:     `(&& x)`,
			expectErr: true,
		},
		{ // Test == having less parameters than needed
			input:     `(x ==)`,
			expectErr: true,
		},
		{ // Test != having less parameters than needed
			input:     `(x !=)`,
			expectErr: true,
		},
		{ // Test >= having less parameters than needed
			input:     `(x >=)`,
			expectErr: true,
		},
		{ // Test <= having less parameters than needed
			input:     `(x <=)`,
			expectErr: true,
		},
		{ // Test < having less parameters than needed
			input:     `(x <)`,
			expectErr: true,
		},
		{ // Test > having less parameters than needed
			input:     `(x >)`,
			expectErr: true,
		},
		{ // Test Contains having less parameters than needed
			input:     `(x Contains)`,
			expectErr: true,
		},
		{ // Test Any_of having less parameters than needed
			input:     `(x Any_of)`,
			expectErr: true,
		},
		// ========== no parameters ==========
		{ // Test Member_of having no parameters
			input:     `(Member_of{})`,
			expectErr: true,
		},
		{ // Test exists having no parameters
			input:     `(exists)`,
			expectErr: true,
		},
		{ // Test an empty expression
			input:     `()`,
			expectErr: true,
		},
		{ // Test ! having no parameters
			input:     `(!)`,
			expectErr: true,
		},
		// ========== too many parameters ==========
		{ // Test == having too many parameters
			input:     `(x == y z)`,
			expectErr: true,
		},
		{ // Test != having too many parameters
			input:     `(x != y z)`,
			expectErr: true,
		},
		{ // Test >= having too many parameters
			input:     `(x >= y z)`,
			expectErr: true,
		},
		{ // Test <= having too many parameters
			input:     `(x <= y z)`,
			expectErr: true,
		},
		{ // Test < having too many parameters
			input:     `(x < y z)`,
			expectErr: true,
		},
		{ // Test > having too many parameters
			input:     `(x > y z)`,
			expectErr: true,
		},
		{ // Test Contains having too many parameters
			input:     `(x Contains y z)`,
			expectErr: true,
		},
		{ // Test Any_of having too many parameters
			input:     `(x Any_of y z)`,
			expectErr: true,
		},
		{ // Test ! having too many parameters
			input:     `(!(x) (y))`,
			expectErr: true,
		},
		// ======== weird expression ordering ==========
		{ // Test == having a weird expression ordering before
			input:     `(x y ==)`,
			expectErr: true,
		},
		{ // Test == having a weird expression ordering after
			input:     `(== x y)`,
			expectErr: true,
		},
		{ // Test != having a weird expression ordering before
			input:     `(x y !=)`,
			expectErr: true,
		},
		{ // Test != having a weird expression ordering after
			input:     `(!= x y)`,
			expectErr: true,
		},
		{ // Test >= having a weird expression ordering before
			input:     `(x y >=)`,
			expectErr: true,
		},
		{ // Test >= having a weird expression ordering after
			input:     `(>= x y)`,
			expectErr: true,
		},
		{ // Test <= having a weird expression ordering before
			input:     `(x y <=)`,
			expectErr: true,
		},
		{ // Test <= having a weird expression ordering after
			input:     `(<= x y)`,
			expectErr: true,
		},
		{ // Test < having a weird expression ordering before
			input:     `(x y <)`,
			expectErr: true,
		},
		{ // Test < having a weird expression ordering after
			input:     `(< x y)`,
			expectErr: true,
		},
		{ // Test > having a weird expression ordering before
			input:     `(x y >)`,
			expectErr: true,
		},
		{ // Test > having a weird expression ordering after
			input:     `(> x y)`,
			expectErr: true,
		},
		{ // Test Contains having a weird expression ordering before
			input:     `(x y Contains)`,
			expectErr: true,
		},
		{ // Test Contains having a weird expression ordering after
			input:     `(Contains x y)`,
			expectErr: true,
		},
		{ // Test Any_of having a weird expression ordering before
			input:     `(x y Any_of)`,
			expectErr: true,
		},
		{ // Test Any_of having a weird expression ordering after
			input:     `(Any_of x y)`,
			expectErr: true,
		},
		{ // Ensure that when special conditions collide, they don't mess eachother up
			input: `((")" == x))`,
			output: derefedConditionalACEResourceAttribute{Expression: derefedConditionalExpression{
				SubExpressions: []derefedConditionalExpression{
					{
						Values:   []string{`")"`, "x"},
						Operator: "==",
					},
				},
				Values: []string{subExprIdentifier + "0]"},
			}},
		},
	}

	for _, v := range parseTests {
		conditional, err := ParseConditionalACEResourceAttribute(v.input)

		c.Log("(expectErr: " + strconv.FormatBool(v.expectErr) + ") test-parsing expression " + v.input)

		if !v.expectErr {
			c.Assert(err, chk.IsNil)
			c.Assert(s.dereferenceConditionalACEResourceAttribute(conditional), chk.DeepEquals, v.output)
		} else {
			c.Assert(err, chk.NotNil)
			c.Log("Successfully got error: " + err.Error())
		}
	}
}
