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
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/Azure/azure-storage-azcopy/v10/traverser"
	"github.com/stretchr/testify/assert"

	chk "gopkg.in/check.v1"
)

type genericFilterSuite struct{}

var _ = chk.Suite(&genericFilterSuite{})

func TestIncludeFilter(t *testing.T) {
	a := assert.New(t)
	// set up the filters
	includePatternList := parsePatterns("*.pdf;*.jpeg;exactName")
	includeFilter := traverser.BuildIncludeFilters(includePatternList)[0]

	// test the positive cases
	filesToPass := []string{"bla.pdf", "fancy.jpeg", "socool.jpeg.pdf", "exactName"}
	for _, file := range filesToPass {
		passed := includeFilter.DoesPass(traverser.StoredObject{Name: file})
		a.True(passed)
	}

	// test the negative cases
	filesNotToPass := []string{"bla.pdff", "fancyjpeg", "socool.jpeg.pdf.wut", "eexactName"}
	for _, file := range filesNotToPass {
		passed := includeFilter.DoesPass(traverser.StoredObject{Name: file})
		a.False(passed)
	}
}

func TestExcludeFilter(t *testing.T) {
	a := assert.New(t)
	// set up the filters
	excludePatternList := parsePatterns("*.pdf;*.jpeg;exactName")
	excludeFilterList := traverser.BuildExcludeFilters(excludePatternList, false)

	// test the positive cases
	filesToPass := []string{"bla.pdfe", "fancy.jjpeg", "socool.png", "eexactName"}
	for _, file := range filesToPass {
		dummyProcessor := &dummyProcessor{}
		err := traverser.ProcessIfPassedFilters(excludeFilterList, traverser.StoredObject{Name: file}, dummyProcessor.process)
		a.Nil(err)
		a.Equal(1, len(dummyProcessor.record))
	}

	// test the negative cases
	filesToNotPass := []string{"bla.pdf", "fancy.jpeg", "socool.jpeg.pdf", "exactName"}
	for _, file := range filesToNotPass {
		dummyProcessor := &dummyProcessor{}
		err := traverser.ProcessIfPassedFilters(excludeFilterList, traverser.StoredObject{Name: file}, dummyProcessor.process)
		a.Equal(traverser.IgnoredError, err)
		a.Zero(len(dummyProcessor.record))
	}
}

func TestDateParsingForIncludeAfter(t *testing.T) {
	a := assert.New(t)
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
		{"2019-03-31T18:30:15UTC", "", "cannot parse \"UTC\" as \".0000000\""},   // "UTC" instead of .0000000
		{"2019/03/31T18:30:15", "", "cannot parse \"/03/31T18:30:15\" as \"-\""}, //wrong date separator
		{"2019-03-31T18:1:15", "", "cannot parse \"1:15\" as \"04\""},            // single-digit minute
	}

	const expectedFormatWithTz = "02 Jan 2006 15:04:05 -0700"
	const expectedFormatShort = "02 Jan 2006 15:04:05"

	loc, _ := time.LoadLocation("Local")

	for _, x := range examples {
		t, err := traverser.IncludeAfterDateFilter{}.ParseISO8601(x.input, true)
		if x.expectedErrorContents == "" {
			a.Nil(err, x.input)
			//fmt.Printf("%v -> %v\n", x.input, t)
			expString := x.expectedValue
			expectedTime, expErr := time.Parse(expectedFormatWithTz, expString)
			if strings.Contains(expString, " X") {
				// no TZ in expected string
				expString = strings.Replace(x.expectedValue, " X", "", -1)
				expectedTime, expErr = time.ParseInLocation(expectedFormatShort, expString, loc)
			}
			a.Nil(expErr)
			foo := expectedTime.String()
			if foo == "" {
			}
			a.True(t.Equal(expectedTime), x.input)
		} else {
			a.NotNil(err)
			a.True(strings.Contains(err.Error(), x.expectedErrorContents), x.input)
		}
	}
}

// When daylight savings ends, in the fall, there's one ambiguous hour (in US timezones, for example, the repeated hour is 1 to 2 am
// on the first Sunday in November).  For the purposes of include-after, we should use the FIRST of the two possible times.
// (If we use the last, we might miss file changes that happened in the hour before it. This could result in regular running
// with include-after failing to pick up changes, if the include-after date fails within the ambiguous hour and files have been
// changed in that hour.  Using the earliest possible interpretation of the date avoids that problem).
// There's no similar ambiguity in spring, because there an hour is just skipped.
func TestDateParsingForIncludeAfter_IsSafeAtDaylightSavingsTransition(t *testing.T) {
	a := assert.New(t)

	dateString, utcEarlyVersion, utcLateVersion, err := findAmbiguousTime()
	if err == noAmbiguousHourError {
		t.Skip(fmt.Sprintf("Cannot run daylight savings test, because local timezone does not appear to have daylight savings time. Local time is %v", time.Now()))
	}
	a.Nil(err)

	fmt.Println("Testing end of daylight saving at " + dateString + " local time")

	// ask for the earliest of the two ambiguous times
	parsed, err := traverser.IncludeAfterDateFilter{}.ParseISO8601(dateString, true) // we use chooseEarliest=true for includeAfter
	a.Nil(err)
	fmt.Printf("For chooseEarliest = true, the times are parsed %v, utcEarly %v, utcLate %v \n", parsed, utcEarlyVersion, utcLateVersion)
	a.True(parsed.Equal(utcEarlyVersion))
	a.False(parsed.Equal(utcLateVersion))

	// ask for the latest of the two ambiguous times
	parsed, err = traverser.IncludeAfterDateFilter{}.ParseISO8601(dateString, false) // we test the false case in this test too, just for completeness
	a.Nil(err)
	fmt.Printf("For chooseEarliest = false, the times are parsed %v, utcEarly %v, utcLate %v \n", parsed, utcEarlyVersion, utcLateVersion)
	a.False(parsed.UTC().Equal(utcEarlyVersion))
	a.True(parsed.UTC().Equal(utcLateVersion))

}

var noAmbiguousHourError = errors.New("could not find hour for end of daylight saving in current local timezone (this might happen if you run the tests in a locale where there is no daylight saving")

// Go's Location object is opaque to us, so we can't directly use it to see when daylight savings ends.
// So we'll just test all the hours in the year, and see!
func findAmbiguousTime() (string, time.Time, time.Time, error) {
	const localTimeFormat = "2006-01-02T15:04:05"
	start := time.Now().UTC()
	end := start.AddDate(1, 0, 0)
	for u := start; u.Before(end); u = u.Add(time.Hour) {
		localString := u.Local().Format(localTimeFormat)
		hourLaterLocalString := u.Add(time.Hour).Local().Format(localTimeFormat)
		if localString == hourLaterLocalString {
			// return the string, and the two UTC times that map to that local time (with their fractional seconds truncated away)
			return localString, u.Truncate(time.Second), u.Add(time.Hour).Truncate(time.Second), nil
		}
	}

	return "", time.Time{}, time.Time{}, noAmbiguousHourError
}
