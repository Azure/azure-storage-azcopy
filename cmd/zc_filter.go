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
	"path"
	"strings"

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

func (f *includeFilter) doesPass(storedObject storedObject) bool {
	if len(f.patterns) == 0 {
		return true
	}

	for _, pattern := range f.patterns {
		checkItem := storedObject.name

		matched := false

		var err error
		matched, err = path.Match(pattern, checkItem)

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

func buildIncludeFilters(patterns []string) []objectFilter {
	validPatterns := make([]string, 0)
	for _, pattern := range patterns {
		if pattern != "" {
			validPatterns = append(validPatterns, pattern)
		}
	}

	return []objectFilter{&includeFilter{patterns: validPatterns}}
}
