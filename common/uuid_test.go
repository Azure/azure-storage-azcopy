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

package common

import (
	chk "gopkg.in/check.v1"
	"strings"
)

type uuidTestSuite struct{}

var _ = chk.Suite(&uuidTestSuite{})

func (s *uuidTestSuite) TestGUIDGenerationAndParsing(c *chk.C) {
	for i := 0; i < 100; i++ {
		uuid := NewUUID()

		// no space is allowed
		containsSpace := strings.Contains(uuid.String(), " ")
		c.Assert(containsSpace, chk.Equals, false)

		parsed, err := ParseUUID(uuid.String())
		c.Assert(err, chk.IsNil)
		c.Assert(parsed, chk.DeepEquals, uuid)
	}
}
