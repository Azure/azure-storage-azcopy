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

type genericFilterSuite struct{}

var _ = chk.Suite(&genericFilterSuite{})

func (s *genericFilterSuite) TestIncludeFilter(c *chk.C) {
	// set up the filters
	raw := rawSyncCmdArgs{}
	includePatternList := raw.parsePatterns("*.pdf;*.jpeg;exactName")
	includeFilter := buildIncludeFilters(includePatternList)[0]

	// test the positive cases
	filesToPass := []string{"bla.pdf", "fancy.jpeg", "socool.jpeg.pdf", "exactName"}
	for _, file := range filesToPass {
		passed := includeFilter.doesPass(storedObject{name: file})
		c.Assert(passed, chk.Equals, true)
	}

	// test the negative cases
	filesNotToPass := []string{"bla.pdff", "fancyjpeg", "socool.jpeg.pdf.wut", "eexactName"}
	for _, file := range filesNotToPass {
		passed := includeFilter.doesPass(storedObject{name: file})
		c.Assert(passed, chk.Equals, false)
	}
}

func (s *genericFilterSuite) TestExcludeFilter(c *chk.C) {
	// set up the filters
	raw := rawSyncCmdArgs{}
	excludePatternList := raw.parsePatterns("*.pdf;*.jpeg;exactName")
	excludeFilterList := buildExcludeFilters(excludePatternList, false)

	// test the positive cases
	filesToPass := []string{"bla.pdfe", "fancy.jjpeg", "socool.png", "eexactName"}
	for _, file := range filesToPass {
		dummyProcessor := &dummyProcessor{}
		err := processIfPassedFilters(excludeFilterList, storedObject{name: file}, dummyProcessor.process)
		c.Assert(err, chk.IsNil)
		c.Assert(len(dummyProcessor.record), chk.Equals, 1)
	}

	// test the negative cases
	filesToNotPass := []string{"bla.pdf", "fancy.jpeg", "socool.jpeg.pdf", "exactName"}
	for _, file := range filesToNotPass {
		dummyProcessor := &dummyProcessor{}
		err := processIfPassedFilters(excludeFilterList, storedObject{name: file}, dummyProcessor.process)
		c.Assert(err, chk.IsNil)
		c.Assert(len(dummyProcessor.record), chk.Equals, 0)
	}
}
