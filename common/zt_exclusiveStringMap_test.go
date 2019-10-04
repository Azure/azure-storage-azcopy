// Copyright © 017 Microsoft <wastore@microsoft.com>
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
	chk "gopkg.in/check.v1"
)

type exclusiveStringMapSuite struct{}

var _ = chk.Suite(&exclusiveStringMapSuite{})

func (s *exclusiveStringMapSuite) TestExclusiveStringMap(c *chk.C) {
	var m *ExclusiveStringMap

	addShouldWork := func(v string) {
		err := m.Add(v)
		c.Assert(err, chk.IsNil)
	}

	addShouldErrorOut := func(v string) {
		err := m.Add(v)
		c.Assert(err, chk.Equals, exclusiveStringMapCollisionError)
	}

	// case sensitive
	m = NewExclusiveStringMap(EFromTo.BlobLocal(), "linux")
	addShouldWork("cat")
	addShouldWork("dog")
	addShouldWork("doG")
	addShouldErrorOut("dog") // collision
	m.Remove("dog")          // remove and try again
	addShouldWork("dog")

	// case insensitive
	m = NewExclusiveStringMap(EFromTo.BlobLocal(), "windows")
	addShouldWork("cat")
	addShouldWork("dog")
	addShouldErrorOut("doG") // collision
	m.Remove("dog")          // remove and try again
	addShouldWork("doG")

}

func (s *exclusiveStringMapSuite) TestChooseRightCaseSensitivity(c *chk.C) {
	test := func(fromTo FromTo, goos string, shouldBeSensitive bool) {
		m := NewExclusiveStringMap(fromTo, goos)
		c.Assert(m.caseSensitive, chk.Equals, shouldBeSensitive)
	}

	test(EFromTo.BlobLocal(), "linux", true)
	test(EFromTo.BlobLocal(), "windows", false)
	test(EFromTo.BlobLocal(), "darwin", false) // default MacOS behaviour is case INsensitive, so assume we are running under that default

	test(EFromTo.LocalFile(), "linux", false) // anything ToFile should be INsensitive
	test(EFromTo.BlobFile(), "linux", false)  // anything ToFile should be INsensitive
	test(EFromTo.BlobBlob(), "windows", true)
}
