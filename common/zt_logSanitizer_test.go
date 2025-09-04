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

package common

import (
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestLogSanitizer(t *testing.T) {
	a := assert.New(t)

	cases := []struct {
		raw               string
		expectedSanitized string
	}{
		{"string with no secrets", "string with no secrets"},

		// DON'T redact these
		{"This is the sig that I have and x=y", "This is the sig that I have and x=y"},                           // sig not in a URL
		{"http://foo/path/with/sig/in/it?x=y", "http://foo/path/with/sig/in/it?x=y"},                             // match is not in the QUERY part of a URL
		{"http://www.signature.example.com/blah", "http://www.signature.example.com/blah"},                       // another with match that's not in QUERY part the URL
		{"http://foo?signatureevent=123&x=y", "http://foo?signatureevent=123&x=y"},                               // our keyword (signature) is not the END of the key-value key name
		{"http://foo?something=sig&somethingelse=sig", "http://foo?something=sig&somethingelse=sig"},             // sig is the value
		{"http://foo?something=sigabc&somethingelse=abcsig", "http://foo?something=sigabc&somethingelse=abcsig"}, // sig is inside the value

		// DO redact all of the following
		{"http://foo?sig=somevalue&x=y", "http://foo?sig=-REDACTED-&x=y"},                                                         // remainder of query string is preserved
		{"http://foo?x=y&" + SigAzure + "=somevalue", "http://foo?x=y&sig=-REDACTED-"},                                            // basic case
		{"http://foo?x=y&sig=somevalue\r\nBlah", "http://foo?x=y&sig=-REDACTED-\r\nBlah"},                                         // newline after, case preserved in other text
		{"blah\r\nhttp://foo?x=y&sig=somevalue blah", "blah\r\nhttp://foo?x=y&sig=-REDACTED- blah"},                               // newline before and something else after
		{"http://foo?a=b&" + SigXAmzForAws + "=somevalue&x=y", "http://foo?a=b&x-amz-signature=-REDACTED-&x=y"},                   // AWS (using our official constant for the key
		{"http://foo?a=b&X-Amz-siGnature=somevalue&x=y", "http://foo?a=b&X-Amz-siGnature=-REDACTED-&x=y"},                         // weird caps
		{"http://foo?sIg=somevalue&x=y", "http://foo?sIg=-REDACTED-&x=y"},                                                         // more weird caps
		{"http://foo?a=b&someother-signature=somevalue&x=y", "http://foo?a=b&someother-signature=-REDACTED-&x=y"},                 // unexpected sig type
		{"http://foo?x=y&my-token=somevalue", "http://foo?x=y&my-token=-REDACTED-"},                                               // name ending in "token"
		{"Foo=x;Signature=bar", "Foo=x;Signature=-REDACTED-"},                                                                     // not in a query string
		{"Signature = bar;Foo = x", "Signature = -REDACTED-;Foo = x"},                                                             // not in a query string, with spaces
		{"Foo=x;Signature=bar", "Foo=x;Signature=-REDACTED-"},                                                                     // not in a query string
		{"Foo : x, Signature : bar, Other: z", "Foo : x, Signature : -REDACTED-, Other: z"},                                       // not in a query string, with commas and spaces
		{"http://foo.com/bar?x=y&" + CredXAmzForAws + "=somevalue&x=y", "http://foo.com/bar?x=y&x-amz-credential=-REDACTED-&x=y"}, // AWS with credential

		// two replacements in same string
		{"http://foo?sig=somevalue and http://bar?sig=othervalue BlahBlah", "http://foo?sig=-REDACTED- and http://bar?sig=-REDACTED- BlahBlah"},

		// word "sig" inside the signature
		{"http://foo?sig=sigvalue BlahBlah", "http://foo?sig=-REDACTED- BlahBlah"},
	}

	san := NewAzCopyLogSanitizer()

	for _, x := range cases {
		a.Equal(x.expectedSanitized, san.SanitizeLogMessage(x.raw))
	}

}
