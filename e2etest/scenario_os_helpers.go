//go:build !windows && !linux

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
	"time"

	"github.com/Azure/azure-storage-azcopy/v10/common"
	"golang.org/x/sys/unix"
)

type osScenarioHelper struct{}

// set file attributes to test file
func (osScenarioHelper) setAttributesForLocalFile() error {
	panic("should never be called")
}

func (osScenarioHelper) setAttributesForLocalFiles(c asserter, dirPath string, fileList []string, attrList []string) {
	panic("should never be called")
}

func (osScenarioHelper) getFileDates(c asserter, filePath string) (createdTime, lastWriteTime time.Time) {
	panic("should never be called")
}

func (osScenarioHelper) getFileAttrs(c asserter, filepath string) *uint32 {
	var ret uint32
	return &ret
}

func (osScenarioHelper) getFileSDDLString(c asserter, filepath string) *string {
	ret := ""
	return &ret
}

func (osScenarioHelper) setFileSDDLString(c asserter, filepath string, sddldata string) {
	panic("should never be called")
}

// nolint
func (osScenarioHelper) Mknod(c asserter, path string, mode uint32, dev int) {
	panic("should never be called")
}

// nolint
func (osScenarioHelper) GetUnixStatAdapterForFile(c asserter, filepath string) common.UnixStatAdapter {
	panic("should never be called")
}

func (osScenarioHelper) IsFileHidden(c asserter, filePath string) bool {
	fileName := filepath.Base(filePath)
	isHidden := strings.HasPrefix(fileName, ".")
	return isHidden
}

func (osScenarioHelper) CreateSpecialFile(filePath string) error {
	err := unix.Mkfifo(filePath, 0666)
	return err
}
