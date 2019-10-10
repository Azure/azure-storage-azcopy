// Copyright © Microsoft <wastore@microsoft.com>
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
	"strings"
	"syscall"

	"github.com/Azure/azure-storage-azcopy/common"
)

type attrFilter struct {
	fileAttributes  uint32
	filePath        string
	isIncludeFilter bool
}

func (f *attrFilter) doesSupportThisOS() (msg string, supported bool) {
	msg = ""
	supported = true
	return
}

func (f *attrFilter) doesPass(storedObject storedObject) bool {
	fileName := common.GenerateFullPath(f.filePath, storedObject.relativePath)
	lpFileName, _ := syscall.UTF16PtrFromString(fileName)
	attributes, err := syscall.GetFileAttributes(lpFileName)

	// If it fails to get file attributes,
	// let the filter pass
	if err != nil {
		glcm.Info(fmt.Sprintf("Skipping file attribute filter for file %s due to error: %s",
			storedObject.relativePath, err))
		return true
	}

	// if a file shares one or more attributes with the ones provided by the filter,
	// it's a match. Return the appropriate boolean value if there's a match.
	// Otherwise return the opposite value.
	if attributes&f.fileAttributes > 0 {
		return f.isIncludeFilter
	}

	return !f.isIncludeFilter
}

func buildAttrFilters(attributes []string, fullPath string, isIncludeFilter bool) []objectFilter {
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
	fileAttributeMap := map[string]uint32{
		"R": 1,
		"A": 32,
		"S": 4,
		"H": 2,
		"C": 2048,
		"N": 128,
		"E": 16384,
		"T": 256,
		"O": 4096,
		"I": 8192,
	}

	for _, attribute := range attributes {
		fileAttributes |= fileAttributeMap[strings.ToUpper(attribute)]
	}

	// Don't append the filter if there is no attributes given
	if fileAttributes > 0 {
		filters = append(filters, &attrFilter{fileAttributes: fileAttributes, filePath: fullPath, isIncludeFilter: isIncludeFilter})
	}
	return filters
}
