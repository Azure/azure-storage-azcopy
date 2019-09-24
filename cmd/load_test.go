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

package cmd

import (
	"fmt"
	"os"

	chk "gopkg.in/check.v1"
)

type loadCmdTestSuite struct{}

var _ = chk.Suite(&loadCmdTestSuite{})

func (s *loadCmdTestSuite) TestParsingRawArgs(c *chk.C) {
	sampleAccountName := "accountname"
	sampleContainerName := "cont"
	sampleSAS := "st=2019-09-23T07%3A06%3A55Z&se=2019-09-24T07%3A06%3A55Z&sp=racwdl&sv=2018-03-28&sr=c&sig=FGDUz8Jb1F%2Fsh%2FLKdX1IBYQR5n5LKfk46GRAeVDYeW4%3D"
	raw := rawLoadCmdArgs{
		src:        os.TempDir(),
		dst:        fmt.Sprintf("https://%s.blob.core.windows.net/%s?%s", sampleAccountName, sampleContainerName, sampleSAS),
		newSession: true,
		statePath:  "/usr/dir/state",
	}

	cooked, err := raw.cook()
	c.Assert(err, chk.IsNil)

	// validate the straightforward args
	c.Assert(cooked.src, chk.Equals, raw.src)
	c.Assert(cooked.newSession, chk.Equals, raw.newSession)
	c.Assert(cooked.statePath, chk.Equals, raw.statePath)

	// check the dst related args
	c.Assert(cooked.dstAccount, chk.Equals, sampleAccountName)
	c.Assert(cooked.dstContainer, chk.Equals, sampleContainerName)

	// the order of sas query params gets mingled
	c.Assert(len(cooked.dstSAS), chk.Equals, len(sampleSAS))
}
