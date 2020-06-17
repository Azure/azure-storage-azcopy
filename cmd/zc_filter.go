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
	"fmt"
	"path"
	"strings"
	"time"

	"github.com/Azure/azure-storage-blob-go/azblob"

	"github.com/Azure/azure-storage-azcopy/common"
)

// Design explanation:
/*
Blob type exclusion is required as a part of the copy enumerators refactor. This would be used in Download and S2S scenarios.
This map is used effectively as a hash set. If an item exists in the set, it does not pass the filter.
*/
type excludeBlobTypeFilter struct {
	blobTypes map[azblob.BlobType]bool
}

func (f *excludeBlobTypeFilter) doesSupportThisOS() (msg string, supported bool) {
	return "", true
}

func (f *excludeBlobTypeFilter) appliesOnlyToFiles() bool {
	return true // there aren't any (real) folders in Blob Storage
}

func (f *excludeBlobTypeFilter) doesPass(object storedObject) bool {
	if _, ok := f.blobTypes[object.blobType]; !ok {
		// For readability purposes, focus on returning false.
		// Basically, the statement says "If the blob type is not present in the list, the object passes the filters."
		return true
	}

	return false
}

type excludeFilter struct {
	pattern     string
	targetsPath bool
}

func (f *excludeFilter) doesSupportThisOS() (msg string, supported bool) {
	msg = ""
	supported = true
	return
}

func (f *excludeFilter) appliesOnlyToFiles() bool {
	return !f.targetsPath
}

func (f *excludeFilter) doesPass(storedObject storedObject) bool {
	matched := false

	if f.targetsPath {
		// Don't actually support patterns here.
		// Isolate the path separator
		pattern := strings.ReplaceAll(f.pattern, common.AZCOPY_PATH_SEPARATOR_STRING, common.DeterminePathSeparator(storedObject.relativePath))
		matched = strings.HasPrefix(storedObject.relativePath, pattern)
	} else {
		var err error
		matched, err = path.Match(f.pattern, storedObject.name)

		// if the pattern failed to match with an error, then we assume the pattern is invalid
		// and let it pass
		if err != nil {
			return true
		}
	}

	if matched {
		return false
	}

	return true
}

func buildExcludeFilters(patterns []string, targetPath bool) []objectFilter {
	filters := make([]objectFilter, 0)
	for _, pattern := range patterns {
		if pattern != "" {
			filters = append(filters, &excludeFilter{pattern: pattern, targetsPath: targetPath})
		}
	}

	return filters
}

// design explanation:
// include filters are different from the exclude ones, which work together in the "AND" manner
// meaning and if an storedObject is rejected by any of the exclude filters, then it is rejected by all of them
// as a result, the exclude filters can be in their own struct, and work correctly
// on the other hand, include filters work in the "OR" manner
// meaning that if an storedObject is accepted by any of the include filters, then it is accepted by all of them
// consequently, all the include patterns must be stored together
type includeFilter struct {
	patterns []string
}

func (f *includeFilter) doesSupportThisOS() (msg string, supported bool) {
	msg = ""
	supported = true
	return
}

func (f *includeFilter) appliesOnlyToFiles() bool {
	return true // includeFilter is a name-pattern-based filter, and we treat those as relating to FILE names only
}

func (f *includeFilter) doesPass(storedObject storedObject) bool {
	if len(f.patterns) == 0 {
		return true
	}

	for _, pattern := range f.patterns {
		checkItem := storedObject.name

		matched := false

		var err error
		matched, err = path.Match(pattern, checkItem) // note: getEnumerationPreFilter below encodes assumptions about the valid wildcards used here

		// if the pattern failed to match with an error, then we assume the pattern is invalid
		// and ignore it
		if err != nil {
			continue
		}

		// if an storedObject is accepted by any of the include filters
		// it is accepted
		if matched {
			return true
		}
	}

	return false
}

// getEnumerationPreFilter returns a prefix, if any, which can be used service-side to pre-select
// things that will pass the filter. E.g. if there's exactly one include pattern, and it is
// "foo*bar", then this routine will return "foo", since only things starting with "foo" can pass the filters.
// Service side enumeration code can be given that prefix, to optimize the enumeration.
func (f *includeFilter) getEnumerationPreFilter() string {
	if len(f.patterns) == 1 {
		pat := f.patterns[0]
		if strings.ContainsAny(pat, "?[\\") {
			// this pattern doesn't just use a *, so it's too complex for us to optimize with a prefix
			return ""
		}
		return strings.Split(pat, "*")[0]
	} else {
		// for simplicity, we won't even try computing a common prefix for all patterns (even though that might help in theory in some cases)
		return ""
	}
}

func buildIncludeFilters(patterns []string) []objectFilter {
	if len(patterns) == 0 {
		return []objectFilter{}
	}

	validPatterns := make([]string, 0)
	for _, pattern := range patterns {
		if pattern != "" {
			validPatterns = append(validPatterns, pattern)
		}
	}

	return []objectFilter{&includeFilter{patterns: validPatterns}}
}

type filterSet []objectFilter

// GetEnumerationPreFilter returns a prefix that is common to all the include filters, or "" if no such prefix can
// be found. (The implementation may return "" even in cases where such a prefix does exist, but in at least the simplest
// cases, it should return a non-empty prefix.)
// The result can be used to optimize enumeration, since anything without this prefix will fail the filterSet
func (fs filterSet) GetEnumerationPreFilter(recursive bool) string {
	if recursive {
		return ""
		// we don't/can't support recursive cases yet, with a strict prefix-based search.
		// Because if the filter is, say "a*", then, then a prefix of "a"
		// will find: enumerationroot/afoo and enumerationroot/abar
		// but it will not find: enumerationroot/virtualdir/afoo
		// even though we want --include-pattern to find that.
		// So, in recursive cases, we just don't use this prefix-based optimization.
		// TODO: consider whether we need to support some way to separately invoke prefix-based optimization
		//   and filtering.  E.g. by a directory-by-directory enumeration (with prefix only within directory),
		//   using the prefix feature in ListBlobs.
	}
	prefix := ""
	for _, f := range fs {
		if participatingFilter, ok := f.(preFilterProvider); ok {
			// this filter knows how to participate in our scheme
			if prefix == "" {
				prefix = participatingFilter.getEnumerationPreFilter()
			} else {
				// prefix already has a value, which means there must be two participating filters, and we can't handle that.
				// Normally this won't happen, because there's only one includeFilter on matter how many include patterns have been supplied.
				return ""
			}
		}
	}
	return prefix
}

////////

// includeAfterDateFilter includes files with Last Modified Times >= the specified threshold
// Used for copy, but doesn't make conceptual sense for sync
type includeAfterDateFilter struct {
	threshold time.Time
}

func (f *includeAfterDateFilter) doesSupportThisOS() (msg string, supported bool) {
	msg = ""
	supported = true
	return
}

func (f *includeAfterDateFilter) appliesOnlyToFiles() bool {
	return true
	// because we don't currently (May 2020) have meaningful LMTs for folders. The meaningful time for a folder is the "change time" not the "last write time", and the change time can only be obtained via NtGetFileInformation, which we don't yet call.
	// TODO: the consequence of this is that folder properties and folder acls can't be moved when using this filter.
	//       Can we live with that, for now?
}

func (f *includeAfterDateFilter) doesPass(storedObject storedObject) bool {
	zeroTime := time.Time{}
	if storedObject.lastModifiedTime == zeroTime {
		panic("cannot use includeAfterDateFilter on an object for which no Last Modified Time has been retrieved")
	}

	return storedObject.lastModifiedTime.After(f.threshold) ||
		storedObject.lastModifiedTime.Equal(f.threshold) // >= is easier for users to understand than >
}

// ParseISO8601 parses ISO 8601 dates. This routine is needed because GoLang's time.Parse* routines require all expected
// elements to be present.  I.e. you can't specify just a date, and have the time default to 00:00. But ISO 8601 requires
// that and, for usability, that's what we want.  (So that users can omit the whole time, or at least the seconds portion of it, if they wish)
func (_ includeAfterDateFilter) ParseISO8601(s string, chooseEarliest bool) (time.Time, error) {

	// list of ISO-8601 Go-lang formats in descending order of completeness
	formats := []string{
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

//FormatAsUTC is inverse of parseISO8601 (and always uses the most detailed format)
func (_ includeAfterDateFilter) FormatAsUTC(t time.Time) string {
	return t.UTC().Format(time.RFC3339)
}
