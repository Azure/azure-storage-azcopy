// +build windows
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

package ste

import (
	chk "gopkg.in/check.v1"
)

type steMiscSuite struct{}

var _ = chk.Suite(&steMiscSuite{})

func (s *concurrencyTunerSuite) Test_IsParentShareRoot(c *chk.C) {
	d := azureFilesDownloader{}

	c.Assert(d.parentIsShareRoot("https://a.file.core.windows.net/share"), chk.Equals, false) // THIS is the share root, not the parent of this
	c.Assert(d.parentIsShareRoot("https://a.file.core.windows.net/share/"), chk.Equals, false)
	c.Assert(d.parentIsShareRoot("https://a.file.core.windows.net/share?aaa/bbb"), chk.Equals, false)
	c.Assert(d.parentIsShareRoot("https://a.file.core.windows.net/share/?aaa/bbb"), chk.Equals, false)

	c.Assert(d.parentIsShareRoot("https://a.file.core.windows.net/share/foo"), chk.Equals, true)
	c.Assert(d.parentIsShareRoot("https://a.file.core.windows.net/share/foo/"), chk.Equals, true)
	c.Assert(d.parentIsShareRoot("https://a.file.core.windows.net/share/foo/?x/y"), chk.Equals, true)
	c.Assert(d.parentIsShareRoot("https://a.file.core.windows.net/share/foo?x/y"), chk.Equals, true)

	c.Assert(d.parentIsShareRoot("https://a.file.core.windows.net/share/foo/bar"), chk.Equals, false)
	c.Assert(d.parentIsShareRoot("https://a.file.core.windows.net/share/foo/bar/"), chk.Equals, false)
	c.Assert(d.parentIsShareRoot("https://a.file.core.windows.net/share/foo/bar?nethe"), chk.Equals, false)
}
