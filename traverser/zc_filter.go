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

package traverser

import (
	"fmt"
	"path"
	"regexp"
	"strings"
	"time"

	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/blob"

	"github.com/Azure/azure-storage-azcopy/v10/common"
)

const ISO8601 = "2006-01-02T15:04:05.0000000Z" // must have 0's for fractional seconds, because Files Service requires fixed width
// Design explanation:
/*
Blob type exclusion is required as a part of the copy enumerators refactor. This would be used in Download and S2S scenarios.
This map is used effectively as a hash set. If an item exists in the set, it does not pass the filter.
*/
type ExcludeBlobTypeFilter struct {
	BlobTypes map[blob.BlobType]bool
}

func (f *ExcludeBlobTypeFilter) DoesSupportThisOS() (msg string, supported bool) {
	return "", true
}

func (f *ExcludeBlobTypeFilter) AppliesOnlyToFiles() bool {
	return true // there aren't any (real) folders in Blob Storage
}

func (f *ExcludeBlobTypeFilter) DoesPass(object StoredObject) bool {
	if _, ok := f.BlobTypes[object.BlobType]; !ok {
		// For readability purposes, focus on returning false.
		// Basically, the statement says "If the blob type is not present in the list, the object passes the filters."
		return true
	}

	return false
}

// excludeContainerFilter filters out container names that must be excluded
type excludeContainerFilter struct {
	containerNamesList map[string]bool
}

func (s *excludeContainerFilter) DoesSupportThisOS() (msg string, supported bool) {
	return "", true
}

func (s *excludeContainerFilter) AppliesOnlyToFiles() bool {
	return false // excludeContainerFilter is related to container names, not related to files
}

func (s *excludeContainerFilter) DoesPass(storedObject StoredObject) bool {
	if len(s.containerNamesList) == 0 {
		return true
	}

	if _, exists := s.containerNamesList[storedObject.ContainerName]; exists {
		return false
	}
	return true
}

func buildExcludeContainerFilter(containerNames []string) []ObjectFilter {
	excludeContainerSet := make(map[string]bool)
	for _, name := range containerNames {
		excludeContainerSet[name] = true
	}

	return append(make([]ObjectFilter, 0), &excludeContainerFilter{containerNamesList: excludeContainerSet})
}

type excludeFilter struct {
	pattern     string
	targetsPath bool
}

func (f *excludeFilter) DoesSupportThisOS() (msg string, supported bool) {
	msg = ""
	supported = true
	return
}

func (f *excludeFilter) AppliesOnlyToFiles() bool {
	return !f.targetsPath
}

func (f *excludeFilter) DoesPass(storedObject StoredObject) bool {
	matched := false

	if f.targetsPath {
		// Don't actually support Patterns here.
		// Isolate the path separator
		pattern := strings.ReplaceAll(f.pattern, common.AZCOPY_PATH_SEPARATOR_STRING, common.DeterminePathSeparator(storedObject.RelativePath))
		matched = strings.HasPrefix(storedObject.RelativePath, pattern)
	} else {
		var err error
		matched, err = path.Match(f.pattern, storedObject.Name)

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

func BuildExcludeFilters(Patterns []string, targetPath bool) []ObjectFilter {
	filters := make([]ObjectFilter, 0)
	for _, pattern := range Patterns {
		if pattern != "" {
			filters = append(filters, &excludeFilter{pattern: pattern, targetsPath: targetPath})
		}
	}

	return filters
}

// design explanation:
// include filters are different from the exclude ones, which work together in the "AND" manner
// meaning and if an StoredObject is rejected by any of the exclude filters, then it is rejected by all of them
// as a result, the exclude filters can be in their own struct, and work correctly
// on the other hand, include filters work in the "OR" manner
// meaning that if an StoredObject is accepted by any of the include filters, then it is accepted by all of them
// consequently, all the include Patterns must be stored together
type IncludeFilter struct {
	patterns []string
}

func (f *IncludeFilter) DoesSupportThisOS() (msg string, supported bool) {
	msg = ""
	supported = true
	return
}

func (f *IncludeFilter) AppliesOnlyToFiles() bool {
	return true // IncludeFilter is a name-pattern-based filter, and we treat those as relating to FILE names only
}

func (f *IncludeFilter) DoesPass(storedObject StoredObject) bool {
	if len(f.patterns) == 0 {
		return true
	}

	for _, pattern := range f.patterns {
		checkItem := storedObject.Name

		matched := false

		var err error
		matched, err = path.Match(pattern, checkItem) // note: getEnumerationPreFilter below encodes assumptions about the valid wildcards used here

		// if the pattern failed to match with an error, then we assume the pattern is invalid
		// and ignore it
		if err != nil {
			continue
		}

		// if an StoredObject is accepted by any of the include filters
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
func (f *IncludeFilter) getEnumerationPreFilter() string {
	if len(f.patterns) == 1 {
		pat := f.patterns[0]
		if strings.ContainsAny(pat, "?[\\") {
			// this pattern doesn't just use a *, so it's too complex for us to optimize with a prefix
			return ""
		}
		return strings.Split(pat, "*")[0]
	} else {
		// for simplicity, we won't even try computing a common prefix for all Patterns (even though that might help in theory in some cases)
		return ""
	}
}

func BuildIncludeFilters(Patterns []string) []ObjectFilter {
	if len(Patterns) == 0 {
		return []ObjectFilter{}
	}

	validPatterns := make([]string, 0)
	for _, pattern := range Patterns {
		if pattern != "" {
			validPatterns = append(validPatterns, pattern)
		}
	}

	return []ObjectFilter{&IncludeFilter{patterns: validPatterns}}
}

type FilterSet []ObjectFilter

// GetEnumerationPreFilter returns a prefix that is common to all the include filters, or "" if no such prefix can
// be found. (The implementation may return "" even in cases where such a prefix does exist, but in at least the simplest
// cases, it should return a non-empty prefix.)
// The result can be used to optimize enumeration, since anything without this prefix will fail the FilterSet
func (fs FilterSet) GetEnumerationPreFilter(recursive bool) string {
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
				// Normally this won't happen, because there's only one IncludeFilter on matter how many include Patterns have been supplied.
				return ""
			}
		}
	}
	return prefix
}

////////

// includeRegex & excludeRegex
type regexFilter struct {
	patterns   []string
	isIncluded bool
}

func (f *regexFilter) DoesSupportThisOS() (msg string, supported bool) {
	msg = ""
	supported = true
	return
}

func (f *regexFilter) AppliesOnlyToFiles() bool {
	return false
}

func (f *regexFilter) DoesPass(storedObject StoredObject) bool {
	if len(f.patterns) == 0 {
		return true
	}
	for _, pattern := range f.patterns {
		matched := false
		var err error

		matched, err = regexp.MatchString(pattern, storedObject.RelativePath)
		// if pattern fails to match with an error, we assume the pattern is invalid
		if err != nil {
			if f.isIncluded { //if include filter then we ignore it
				continue
			} else { //if exclude filter then we let it pass
				return true
			}
		}
		//check if pattern matched relative path
		//if matched then return isIncluded which is a boolean expression to represent included and excluded
		if matched {
			return f.isIncluded
		}
	}
	return !f.isIncluded
}

func BuildRegexFilters(patterns []string, isIncluded bool) []ObjectFilter {
	if len(patterns) == 0 {
		return []ObjectFilter{}
	}

	filters := make([]string, 0)
	for _, pattern := range patterns {
		if pattern != "" {
			filters = append(filters, pattern)
		}
	}

	return []ObjectFilter{&regexFilter{patterns: filters, isIncluded: isIncluded}}
}

// includeAfterDateFilter includes files with Last Modified Times >= the specified threshold
// Used for copy, but doesn't make conceptual sense for sync
type IncludeAfterDateFilter struct {
	Threshold time.Time
}

func (f *IncludeAfterDateFilter) DoesSupportThisOS() (msg string, supported bool) {
	msg = ""
	supported = true
	return
}

func (f *IncludeAfterDateFilter) AppliesOnlyToFiles() bool {
	return false
}

func (f *IncludeAfterDateFilter) DoesPass(storedObject StoredObject) bool {
	zeroTime := time.Time{}
	if storedObject.LastModifiedTime == zeroTime {
		panic("cannot use IncludeAfterDateFilter on an object for which no Last Modified Time has been retrieved")
	}

	return storedObject.LastModifiedTime.After(f.Threshold) ||
		storedObject.LastModifiedTime.Equal(f.Threshold) // >= is easier for users to understand than >
}

func (IncludeAfterDateFilter) ParseISO8601(s string, chooseEarliest bool) (time.Time, error) {
	return parseISO8601(s, chooseEarliest)
}

func (IncludeAfterDateFilter) FormatAsUTC(t time.Time) string {
	return formatAsUTC(t)
}

// IncludeBeforeDateFilter includes files with Last Modified Times <= the specified Threshold
// Used for copy, but doesn't make conceptual sense for sync
type IncludeBeforeDateFilter struct {
	Threshold time.Time
}

func (f *IncludeBeforeDateFilter) DoesSupportThisOS() (msg string, supported bool) {
	msg = ""
	supported = true
	return
}

func (f *IncludeBeforeDateFilter) AppliesOnlyToFiles() bool {
	return false
}

func (f *IncludeBeforeDateFilter) DoesPass(storedObject StoredObject) bool {
	zeroTime := time.Time{}
	if storedObject.LastModifiedTime == zeroTime {
		panic("cannot use IncludeBeforeDateFilter on an object for which no Last Modified Time has been retrieved")
	}

	return storedObject.LastModifiedTime.Before(f.Threshold) ||
		storedObject.LastModifiedTime.Equal(f.Threshold) // <= is easier for users to understand than <
}

func (_ IncludeBeforeDateFilter) ParseISO8601(s string, chooseEarliest bool) (time.Time, error) {
	return parseISO8601(s, chooseEarliest)
}

func (_ IncludeBeforeDateFilter) FormatAsUTC(t time.Time) string {
	return formatAsUTC(t)
}

type PermDeleteFilter struct {
	DeleteSnapshots bool
	DeleteVersions  bool
}

func (s *PermDeleteFilter) DoesSupportThisOS() (msg string, supported bool) {
	return "", true
}

func (s *PermDeleteFilter) AppliesOnlyToFiles() bool {
	return false
}

func (s *PermDeleteFilter) DoesPass(storedObject StoredObject) bool {
	if (s.DeleteVersions && s.DeleteSnapshots) && storedObject.blobDeleted && (storedObject.BlobVersionID != "" || storedObject.BlobSnapshotID != "") {
		return true
	} else if s.DeleteSnapshots && storedObject.blobDeleted && storedObject.BlobSnapshotID != "" {
		return true
	} else if s.DeleteVersions && storedObject.blobDeleted && storedObject.BlobVersionID != "" {
		return true
	}
	return false
}

func BuildIncludeSoftDeleted(permanentDeleteOption common.PermanentDeleteOption) []ObjectFilter {
	filters := make([]ObjectFilter, 0)
	switch permanentDeleteOption {
	case common.EPermanentDeleteOption.Snapshots():
		filters = append(filters, &PermDeleteFilter{DeleteSnapshots: true})
	case common.EPermanentDeleteOption.Versions():
		filters = append(filters, &PermDeleteFilter{DeleteVersions: true})
	case common.EPermanentDeleteOption.SnapshotsAndVersions():
		filters = append(filters, &PermDeleteFilter{DeleteSnapshots: true, DeleteVersions: true})
	}
	return filters
}

// parseISO8601 parses ISO 8601 dates. This routine is needed because GoLang's time.Parse* routines require all expected
// elements to be present.  I.e. you can't specify just a date, and have the time default to 00:00. But ISO 8601 requires
// that and, for usability, that's what we want.  (So that users can omit the whole time, or at least the seconds portion of it, if they wish)
func parseISO8601(s string, chooseEarliest bool) (time.Time, error) {

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

// formatAsUTC is inverse of parseISO8601 (and always uses the most detailed format)
func formatAsUTC(t time.Time) string {
	return t.UTC().Format(time.RFC3339)
}

type FilterOptions struct {
	IncludePatterns   []string
	ExcludePatterns   []string
	ExcludePaths      []string
	IncludeAttributes []string
	ExcludeAttributes []string
	IncludeRegex      []string
	ExcludeRegex      []string
}

// BuildFilters sets up the filters in a specific order
func BuildFilters(fromTo common.FromTo, source common.ResourceString, recursive bool, opts FilterOptions) []ObjectFilter {
	// Note: includeFilters and includeAttrFilters are ANDed
	// They must both pass to get the file included
	// Same rule applies to excludeFilters and excludeAttrFilters
	filters := BuildIncludeFilters(opts.IncludePatterns)
	if fromTo.From() == common.ELocation.Local() {
		includeAttrFilters := BuildAttrFilters(opts.IncludeAttributes, source.ValueLocal(), true)
		filters = append(filters, includeAttrFilters...)
	}

	filters = append(filters, BuildExcludeFilters(opts.ExcludePatterns, false)...)
	filters = append(filters, BuildExcludeFilters(opts.ExcludePaths, true)...)
	if fromTo.From() == common.ELocation.Local() {
		excludeAttrFilters := BuildAttrFilters(opts.ExcludeAttributes, source.ValueLocal(), false)
		filters = append(filters, excludeAttrFilters...)
	}
	// regex
	filters = append(filters, BuildRegexFilters(opts.IncludeRegex, true)...)
	filters = append(filters, BuildRegexFilters(opts.ExcludeRegex, false)...)

	// after making all filters, log any search prefix computed from them
	if prefixFilter := FilterSet(filters).GetEnumerationPreFilter(recursive); prefixFilter != "" {
		common.LogToJobLogWithPrefix("Search prefix, which may be used to optimize scanning, is: "+prefixFilter, common.LogInfo) // "May be used" because we don't know here which enumerators will use it
	}
	return filters
}
