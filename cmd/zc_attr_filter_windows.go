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
	"path/filepath"
	"syscall"
	"strings"
)

type excludeAttrFilter struct {
	fileAttributes uint32
	filePath       string
}

func (f *excludeAttrFilter) doesSupportThisOS() (msg string, supported bool) {
	msg = ""
	supported = true
	return
}

func (f *excludeAttrFilter) doesPass(storedObject storedObject) bool {
	fileName := filepath.Join(f.filePath, storedObject.name)
	lpFileName, err := syscall.UTF16PtrFromString(fileName)

	// If it fails to retrive the pointer from file path, let the filter pass
	if err != nil {
		return true
	}

	attributes, err := syscall.GetFileAttributes(lpFileName)

	if err != nil {
		return true
	}

	// if a file shares one or more attributes with the ones provided by the filter,
	// exclude this file
	if attributes & f.fileAttributes > 0 {
		return false
	}

	return true
}

func buildExcludeAttrFilters(attributes []string, fullPath string) []objectFilter {
	var fileAttributes uint32
	filters := make([]objectFilter, 0)
	// Available attributes (NTFS) include:
	// R = Read-only files
	// A = Files ready for archiving
	// S = System files
	// H = Hidden files
	// C = Compressed files
	// N = Normal files
	// E = Encrypted files
	// T = Temporary files
	// O = Offline files
	// I = Non-indexed files
	// Reference for File Attribute Constants:
	// https://docs.microsoft.com/en-us/windows/win32/fileio/file-attribute-constants
	fileAttributeMap := map[string]uint32 {
		"R" : 1,
		"A" : 32,
		"S" : 4,
		"H" : 2,
		"C" : 2048,
		"N" : 128,
		"E" : 16384,
		"T" : 256,
		"O" : 4096,
		"I" : 8192,
	}

	for _, attribute := range attributes {
		fileAttributes |= fileAttributeMap[strings.ToUpper(attribute)]
	}

	// Don't append the filter if there is no attributes given
	if fileAttributes > 0 {
		filters = append(filters, &excludeAttrFilter{fileAttributes: fileAttributes, filePath: fullPath})
	}
	return filters
}

type includeAttrFilter struct {
	fileAttributes uint32
	filePath       string
}

func (f *includeAttrFilter) doesSupportThisOS() (msg string, supported bool) {
	msg = ""
	supported = true
	return
}

func (f *includeAttrFilter) doesPass(storedObject storedObject) bool {
	fileName := filepath.Join(f.filePath, storedObject.name)
	lpFileName, err := syscall.UTF16PtrFromString(fileName)

	// If it fails to retrive the pointer from file path, let the filter pass
	if err != nil {
		return true
	}

	attributes, err := syscall.GetFileAttributes(lpFileName)

	// If the storedObject has invalid file attributes, let the filter pass
	if err != nil {
		return true
	}

	// if a file shares one or more attributes with the ones provided by the filter,
	// include this file
	if attributes & f.fileAttributes > 0 {
		return true
	}

	return false
}

func buildIncludeAttrFilters(attributes []string, fullPath string) []objectFilter {
	var fileAttributes uint32
	filters := make([]objectFilter, 0)
	fileAttributeMap := map[string]uint32 {
		"R" : 1,
		"A" : 32,
		"S" : 4,
		"H" : 2,
		"C" : 2048,
		"N" : 128,
		"E" : 16384,
		"T" : 256,
		"O" : 4096,
		"I" : 8192,
	}

	for _, attribute := range attributes {
		fileAttributes |= fileAttributeMap[strings.ToUpper(attribute)]
	}

	// Don't append the filter if there is no attributes given
	if fileAttributes > 0 {
		filters = append(filters, &includeAttrFilter{fileAttributes: fileAttributes, filePath: fullPath})
	}
	return filters
}