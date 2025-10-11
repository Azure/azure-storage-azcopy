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
	"regexp"
	"testing"

	"github.com/stretchr/testify/assert"
)

// this test uses "contoso" SIDs (don't want real SIDs here). The RID portion of the SIDs should also be fake here (e.g. using 9999x as below)
// Contoso SID is from https://docs.microsoft.com/en-us/windows/security/identity-protection/access-control/security-identifiers
func TestMakingSDDLPortable(t *testing.T) {
	a := assert.New(t)
	translateSID = TranslateContosoSID
	defer func() { translateSID = OSTranslateSID }()

	tests := []struct {
		input          string
		expectedOutput string
	}{
		// simple case
		{"O:BA",
			"O:S-1-5-21-1004336348-1177238915-682003330-BA"}, // our fake Contoso SIDs still end with the textual chars, for ease of test authoring

		// big nasty one (created by generating a real SDDL string from a real Windows file
		// by setting complex permissions on it, then running this powershell (Get-ACL .\testFile.txt).Sddl
		// **** AND THEN replacing our real corporate SIDs with the Contoso ones ***
		{`O:S-1-5-21-1004336348-1177238915-682003330-99991
		G:DUD:AI(A;;0x1201bf;;;S-1-5-21-1004336348-1177238915-682003330-99992)
		(D;ID;CCSWWPLORC;;;S-1-5-21-1004336348-1177238915-682003330-99993)
		(A;ID;0x1200b9;;;S-1-5-21-1004336348-1177238915-682003330-99994)
		(A;ID;FA;;;BA)
		(A;ID;FA;;;SY)
		(A;ID;0x1301bf;;;AU)
		(A;ID;0x1200a9;;;BU)`,

			`O:S-1-5-21-1004336348-1177238915-682003330-99991
		G:S-1-5-21-1004336348-1177238915-682003330-DU
		D:AI(A;;0x1201bf;;;S-1-5-21-1004336348-1177238915-682003330-99992)
		(D;ID;CCSWWPLORC;;;S-1-5-21-1004336348-1177238915-682003330-99993)
		(A;ID;0x1200b9;;;S-1-5-21-1004336348-1177238915-682003330-99994)
		(A;ID;FA;;;S-1-5-21-1004336348-1177238915-682003330-BA)
		(A;ID;FA;;;S-1-5-21-1004336348-1177238915-682003330-SY)
		(A;ID;0x1301bf;;;S-1-5-21-1004336348-1177238915-682003330-AU)
		(A;ID;0x1200a9;;;S-1-5-21-1004336348-1177238915-682003330-BU)`},

		// some conditional ACEs
		{`O:BA
		G:DU
		D:PAI(XA;;0x1200a9;;;IU;(((@USER.SomeProperty == "Not a real SID(just testing)")
		&& (Member_of {SID(S-1-5-21-1004336348-1177238915-682003330-99994)})) ||
	(Member_of {SID(LA), SID(EA)})))`,

			`O:S-1-5-21-1004336348-1177238915-682003330-BA
		G:S-1-5-21-1004336348-1177238915-682003330-DU
		D:PAI(XA;;0x1200a9;;;S-1-5-21-1004336348-1177238915-682003330-IU;(((@USER.SomeProperty == "Not a real SID(just testing)")
		&& (Member_of {SID(S-1-5-21-1004336348-1177238915-682003330-99994)})) ||
	(Member_of {SID(S-1-5-21-1004336348-1177238915-682003330-LA), SID(S-1-5-21-1004336348-1177238915-682003330-EA)})))`},
	}

	// used to remove the end of lines, which are just there to format our tests
	wsRegex := regexp.MustCompile("[\t\r\n]")
	removeEols := func(s string) string {
		return wsRegex.ReplaceAllString(s, "")
	}

	for _, test := range tests {
		test.input = removeEols(test.input)
		test.expectedOutput = removeEols(test.expectedOutput)
		t.Log(test.input)
		t.Log(test.expectedOutput)

		parsed, _ := ParseSDDL(removeEols(test.input))
		portableVersion := parsed.PortableString()

		a.Equal(removeEols(test.expectedOutput), portableVersion)

	}
}

func TranslateContosoSID(sid string) (string, error) {
	const contosoBase = "S-1-5-21-1004336348-1177238915-682003330"
	if len(sid) > 2 {
		// assume its already a full SID
		return sid, nil
	}
	return contosoBase + "-" + sid, nil // unlike real OS function, we leave the BU or whatever on the end instead of making it numeric, but that's OK because we just need to make sure the replacements happen
}
