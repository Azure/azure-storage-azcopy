// Copyright © 2017 Microsoft <wastore@microsoft.com>
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

package cmd

import (
	"github.com/Azure/azure-storage-azcopy/common"
	chk "gopkg.in/check.v1"
)

type pathUtilsSuite struct{}

var _ = chk.Suite(&pathUtilsSuite{})

func (s *pathUtilsSuite) TestStripQueryFromSaslessUrl(c *chk.C) {
	tests := []struct {
		full          string
		isRemote      bool
		expectedMain  string
		expectedQuery string
	}{
		// remote urls
		{"http://example.com/abc?foo=bar", true, "http://example.com/abc", "foo=bar"},
		{"http://example.com/abc", true, "http://example.com/abc", ""},
		{"http://example.com/abc?", true, "http://example.com/abc", ""}, // no query string if ? is at very end

		// things that are not URLs, or not to be interpreted as such
		{"http://foo/bar?eee", false, "http://foo/bar?eee", ""}, // note isRemote == false
		{`c:\notUrl`, false, `c:\notUrl`, ""},
		{`\\?\D:\longStyle\Windows\path`, false, `\\?\D:\longStyle\Windows\path`, ""},
	}

	for _, t := range tests {
		loc := common.ELocation.Local()
		if t.isRemote {
			loc = common.ELocation.File()
		}
		m, q := splitQueryFromSaslessResource(t.full, loc)
		c.Assert(m, chk.Equals, t.expectedMain)
		c.Assert(q, chk.Equals, t.expectedQuery)
	}
}

func (s *pathUtilsSuite) TestToReversedString(c *chk.C) {
	t := &benchmarkTraverser{}
	c.Assert("1", chk.Equals, t.toReversedString(1))
	c.Assert("01", chk.Equals, t.toReversedString(10))
	c.Assert("54321", chk.Equals, t.toReversedString(12345))
}
