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

	"github.com/Azure/azure-storage-azcopy/v10/common"
)

type attrFilter struct {
	fileAttributes  WindowsAttribute
	filePath        string
	isIncludeFilter bool
}

func (f *attrFilter) DoesSupportThisOS() (msg string, supported bool) {
	msg = ""
	supported = true
	return
}

func (f *attrFilter) AppliesOnlyToFiles() bool {
	return true // keep this filter consistent with include-pattern
}

func (f *attrFilter) DoesPass(storedObject StoredObject) bool {
	fileName := ""
	if !strings.Contains(f.filePath, "*") {
		fileName = common.GenerateFullPath(f.filePath, storedObject.relativePath)
	} else {
		basePath := getPathBeforeFirstWildcard(f.filePath)
		fileName = common.GenerateFullPath(basePath, storedObject.relativePath)
	}

	lpFileName, _ := syscall.UTF16PtrFromString(fileName)
	attributes, err := syscall.GetFileAttributes(lpFileName)

	// If it fails to get file attributes,
	// let the filter pass
	if err != nil {
		glcm.OnInfo(fmt.Sprintf("Skipping file attribute filter for file %s due to error: %s",
			storedObject.relativePath, err))
		return true
	}

	// if a file shares one or more attributes with the ones provided by the filter,
	// it's a match. Return the appropriate boolean value if there's a match.
	// Otherwise return the opposite value.
	if attributes&uint32(f.fileAttributes) > 0 {
		return f.isIncludeFilter
	}

	return !f.isIncludeFilter
}

type WindowsAttribute uint32

const (
	WindowsAttributeReadOnly WindowsAttribute = 1 << iota
	WindowsAttributeHidden
	WindowsAttributeSystemFile
	_ // blanks to increment iota
	_
	WindowsAttributeArchiveReady
	_ // blanks to increment iota
	WindowsAttributeNormalFile
	WindowsAttributeTemporaryFile
	_ // blanks to increment iota
	_
	WindowsAttributeCompressedFile
	WindowsAttributeOfflineFile
	WindowsAttributeNonIndexedFile
	WindowsAttributeEncryptedFile
)

var WindowsAttributeStrings = map[WindowsAttribute]string{
	WindowsAttributeReadOnly:       "R",
	WindowsAttributeHidden:         "H",
	WindowsAttributeSystemFile:     "S",
	WindowsAttributeArchiveReady:   "A",
	WindowsAttributeNormalFile:     "N",
	WindowsAttributeTemporaryFile:  "T",
	WindowsAttributeCompressedFile: "C",
	WindowsAttributeOfflineFile:    "O",
	WindowsAttributeNonIndexedFile: "I",
	WindowsAttributeEncryptedFile:  "E",
}

// Reference for File Attribute Constants:
// https://docs.microsoft.com/en-us/windows/win32/fileio/file-attribute-constants
var WindowsAttributesByName = map[string]WindowsAttribute{
	"R": WindowsAttributeReadOnly,
	"H": WindowsAttributeHidden,
	"S": WindowsAttributeSystemFile,
	"A": WindowsAttributeArchiveReady,
	"N": WindowsAttributeNormalFile,
	"T": WindowsAttributeTemporaryFile,
	"C": WindowsAttributeCompressedFile,
	"O": WindowsAttributeOfflineFile,
	"I": WindowsAttributeNonIndexedFile,
	"E": WindowsAttributeEncryptedFile,
}

func buildAttrFilters(attributes []string, fullPath string, isIncludeFilter bool) []ObjectFilter {
	var fileAttributes WindowsAttribute
	filters := make([]ObjectFilter, 0)

	for _, attribute := range attributes {
		fileAttributes |= WindowsAttributesByName[strings.ToUpper(attribute)]
	}

	// Don't append the filter if there is no attributes given
	if fileAttributes > 0 {
		filters = append(filters, &attrFilter{fileAttributes: fileAttributes, filePath: fullPath, isIncludeFilter: isIncludeFilter})
	}
	return filters
}
