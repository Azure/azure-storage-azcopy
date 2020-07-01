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
	chk "gopkg.in/check.v1"
)

type parseSizeSuite struct{}

var _ = chk.Suite(&parseSizeSuite{})

func (s *parseSizeSuite) TestParseSize(c *chk.C) {
	b, _ := ParseSizeString("123K", "x")
	c.Assert(b, chk.Equals, int64(123*1024))

	b, _ = ParseSizeString("456m", "x")
	c.Assert(b, chk.Equals, int64(456*1024*1024))

	b, _ = ParseSizeString("789G", "x")
	c.Assert(b, chk.Equals, int64(789*1024*1024*1024))

	expectedError := "foo-bar must be a number immediately followed by K, M or G. E.g. 12k or 200G"

	_, err := ParseSizeString("123", "foo-bar")
	c.Assert(err.Error(), chk.Equals, expectedError)

	_, err = ParseSizeString("123 K", "foo-bar")
	c.Assert(err.Error(), chk.Equals, expectedError)

	_, err = ParseSizeString("123KB", "foo-bar")
	c.Assert(err.Error(), chk.Equals, expectedError)

	_, err = ParseSizeString("123T", "foo-bar") // we don't support terabytes
	c.Assert(err.Error(), chk.Equals, expectedError)

	_, err = ParseSizeString("abcK", "foo-bar")
	c.Assert(err.Error(), chk.Equals, expectedError)

}
