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

// TODO this file was forked from the cmd package, it needs to cleaned to keep only the necessary part

package e2etest

import (
	"path/filepath"
	"strings"
	"syscall"
)

// set file attributes to test file
func (scenarioHelper) setAttributesForLocalFile(filePath string, attrList []string) error {
	lpFilePath, err := syscall.UTF16PtrFromString(filePath)
	if err != nil {
		return err
	}

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
	var attrs uint32
	for _, attribute := range attrList {
		attrs |= fileAttributeMap[strings.ToUpper(attribute)]
	}
	err = syscall.SetFileAttributes(lpFilePath, attrs)
	return err
}

func (s scenarioHelper) setAttributesForLocalFiles(c asserter, dirPath string, fileList []string, attrList []string) {
	for _, fileName := range fileList {
		err := s.setAttributesForLocalFile(filepath.Join(dirPath, fileName), attrList)
		c.AssertNoErr(err)
	}
}
