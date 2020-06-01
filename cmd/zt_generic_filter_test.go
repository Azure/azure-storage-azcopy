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
	"strings"
	"time"
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

func (s *genericFilterSuite) TestDateParsingForIncludeAfter(c *chk.C) {
	examples := []struct {
		input                 string // ISO 8601
		expectedValue         string // RFC822Z (with seconds and X for placeholder for local timezone offset)
		expectedErrorContents string // partial error string
	}{
		// success cases
		{"2019-01-31T18:30:15Z", "31 Jan 2019 18:30:15 -0000", ""},         // UTC
		{"2019-01-31T18:30:15.333Z", "31 Jan 2019 18:30:15.333 -0000", ""}, // UTC with fractional seconds
		{"2019-01-31T18:30:15+11:30", "31 Jan 2019 18:30:15 +1130", ""},    // explicit TZ offset (we can't NOT support this, due to the way Go parses the Z portion of the string)
		{"2019-01-31T18:30:15", "31 Jan 2019 18:30:15 X", ""},              // local
		{"2019-01-31T18:30:15.333", "31 Jan 2019 18:30:15.333 X", ""},      // local with fractional seconds
		{"2019-01-31T18:30", "31 Jan 2019 18:30:00 X", ""},                 // local no seconds
		{"2019-01-31T18", "31 Jan 2019 18:00:00 X", ""},                    // local hour only
		{"2019-01-31", "31 Jan 2019 00:00:00 X", ""},                       // local midnight

		// failure cases (these all get Go's cryptic datetime parsing error messages, unfortunately)
		{"2019-03-31 18:30:15", "", "cannot parse \" 18:30:15\" as \"T\""},       // space instead of T
		{"2019-03-31T18:30:15UTC", "", "cannot parse \"UTC\" as \"Z07:00\""},     // "UTC" instead of Z
		{"2019/03/31T18:30:15", "", "cannot parse \"/03/31T18:30:15\" as \"-\""}, //wrong date separator
		{"2019-03-31T18:1:15", "", "cannot parse \"1:15\" as \"04\""},            // single-digit minute
	}

	const expectedFormatWithTz = "02 Jan 2006 15:04:05 -0700"
	const expectedFormatShort = "02 Jan 2006 15:04:05"

	loc, _ := time.LoadLocation("Local")

	for _, x := range examples {
		t, err := includeAfterDateFilter{}.ParseISO8601(x.input)
		if x.expectedErrorContents == "" {
			c.Assert(err, chk.IsNil, chk.Commentf(x.input))
			expString := x.expectedValue
			expectedTime, expErr := time.Parse(expectedFormatWithTz, expString)
			if strings.Contains(expString, " X") {
				// no TZ in expected string
				expString = strings.Replace(x.expectedValue, " X", "", -1)
				expectedTime, expErr = time.ParseInLocation(expectedFormatShort, expString, loc)
			}
			c.Assert(expErr, chk.IsNil)
			foo := expectedTime.String()
			if foo == "" {
			}
			c.Check(t.Equal(expectedTime), chk.Equals, true, chk.Commentf(x.input))
		} else {
			c.Assert(err, chk.Not(chk.IsNil))
			c.Assert(strings.Contains(err.Error(), x.expectedErrorContents), chk.Equals, true, chk.Commentf(x.input))
		}
	}
}
