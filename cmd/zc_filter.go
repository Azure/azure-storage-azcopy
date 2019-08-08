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

import "path"

type excludeFilter struct {
	pattern string
}

func (f *excludeFilter) doesSupportThisOS() (msg string, supported bool) {
	msg = ""
	supported = true
	return
}

func (f *excludeFilter) doesPass(storedObject storedObject) bool {
	matched, err := path.Match(f.pattern, storedObject.name)

	// if the pattern failed to match with an error, then we assume the pattern is invalid
	// and let it pass
	if err != nil {
		return true
	}

	if matched {
		return false
	}

	return true
}

func buildExcludeFilters(patterns []string) []objectFilter {
	filters := make([]objectFilter, 0)
	for _, pattern := range patterns {
		if pattern != "" {
			filters = append(filters, &excludeFilter{pattern: pattern})
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
		matched, err := path.Match(pattern, storedObject.name)

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
