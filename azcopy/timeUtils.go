// Copyright © 2025 Microsoft <wastore@microsoft.com>
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

package azcopy

import (
	"fmt"
	"time"
)

const ISO8601 = "2006-01-02T15:04:05.0000000Z" // must have 0's for fractional seconds, because Files Service requires fixed width

// ParseISO8601 parses ISO 8601 dates. This routine is needed because GoLang's time.Parse* routines require all expected
// elements to be present.  I.e. you can't specify just a date, and have the time default to 00:00. But ISO 8601 requires
// that and, for usability, that's what we want.  (So that users can omit the whole time, or at least the seconds portion of it, if they wish)
func ParseISO8601(s string, chooseEarliest bool) (time.Time, error) {

	// list of ISO-8601 Go-lang formats in descending order of completeness
	formats := []string{
		ISO8601,                     // Support AzFile's more accurate format
		"2006-01-02T15:04:05Z07:00", // equal to time.RFC3339, which in Go parsing is basically "ISO 8601 with nothing optional"
		"2006-01-02T15:04:05",       // no timezone
		"2006-01-02T15:04",          // no seconds
		"2006-01-02T15",             // no minutes
		"2006-01-02",                // no time
		// we don't want to support the no day, or no month options. They are too vague for our purposes
	}

	loc, err := time.LoadLocation("Local")
	if err != nil {
		return time.Time{}, err
	}

	// Try from most precise to least
	// (If user has some OTHER format, with extra chars we don't expect an any format, all will fail)
	for _, f := range formats {
		t, err := time.ParseInLocation(f, s, loc)
		if err == nil {
			if t.Location() == loc {
				// if we are working in local time, then detect the case where the time falls in the repeated hour
				// at then end of daylight saving, and resolve it according to chooseEarliest
				const localNoTimezone = "2006-01-02T15:04:05"
				var possibleLocalDuplicate time.Time
				if chooseEarliest {
					possibleLocalDuplicate = t.Add(-time.Hour) // test an hour earlier, and favour it, if it's the same local time
				} else {
					possibleLocalDuplicate = t.Add(time.Hour) // test an hour later, and favour it, if it's the same local time
				}
				isSameLocalTime := possibleLocalDuplicate.Format(localNoTimezone) == t.Format(localNoTimezone)
				if isSameLocalTime {
					return possibleLocalDuplicate, nil
				}
			}
			return t, nil
		}
	}

	// Nothing worked. Get fresh error from first format, and supplement it with additional hints.
	_, err = time.ParseInLocation(formats[0], s, loc)
	err = fmt.Errorf("could not parse date/time '%s'. Expecting ISO8601 format, with 4 digit year and 2-digits for all other elements. Error hint: %w",
		s, err)
	return time.Time{}, err
}

// FormatAsUTC is inverse of parseISO8601 (and always uses the most detailed format)
func FormatAsUTC(t time.Time) string {
	return t.UTC().Format(time.RFC3339)
}
