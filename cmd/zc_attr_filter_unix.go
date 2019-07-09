// +build linux darwin

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

type excludeAttrFilter struct {
	fileAttributes uint32
	filePath       string
}

func (f *excludeAttrFilter) doesSupportThisOS() (msg string, supported bool) {
	msg = "'exclude-attributes' is not supported on this OS. This option will be ignored."
	supported = false
	return
}

func (f *excludeAttrFilter) doesPass(storedObject storedObject) bool {
	// ignore this option on Unix systems
	return true
}

func buildExcludeAttrFilters(attributes []string, fullPath string) []objectFilter {
	// ignore this option on Unix systems
	filters := make([]objectFilter, 0)
	if len(attributes) > 0 {
		filters = append(filters, &excludeAttrFilter{fileAttributes: 0, filePath: fullPath})
	}
	return filters
}

type includeAttrFilter struct {
	fileAttributes uint32
	filePath       string
}

func (f *includeAttrFilter) doesSupportThisOS() (msg string, supported bool) {
	msg = "'include-attributes' is not supported on this OS. This option will be ignored."
	supported = false
	return
}

func (f *includeAttrFilter) doesPass(storedObject storedObject) bool {
	// ignore this option on Unix systems
	return true
}

func buildIncludeAttrFilters(attributes []string, fullPath string) []objectFilter {
	// ignore this option on Unix systems
	filters := make([]objectFilter, 0)
	if len(attributes) > 0 {
		filters = append(filters, &includeAttrFilter{fileAttributes: 0, filePath: fullPath})
	}
	return filters
}