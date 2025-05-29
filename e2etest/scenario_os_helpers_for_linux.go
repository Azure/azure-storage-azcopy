//go:build !windows

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

package e2etest

import (
	"path/filepath"
	"strings"
	"time"

	"github.com/Azure/azure-storage-azcopy/v10/common"
	"github.com/Azure/azure-storage-azcopy/v10/ste"
	"golang.org/x/sys/unix"
)

type osScenarioHelper struct{}

// set file attributes to test file
// nolint
func (osScenarioHelper) setAttributesForLocalFile() error {
	panic("should never be called")
}

// nolint
func (osScenarioHelper) setAttributesForLocalFiles(c asserter, dirPath string, fileList []string, attrList []string) {
	panic("should never be called")
}

// nolint
func (osScenarioHelper) getFileDates(c asserter, filePath string) (createdTime, lastWriteTime time.Time) {
	panic("should never be called")
}

// nolint
func (osScenarioHelper) getFileAttrs(c asserter, filepath string) *uint32 {
	var ret uint32
	return &ret
}

// nolint
func (osScenarioHelper) getFileSDDLString(c asserter, filepath string) *string {
	ret := ""
	return &ret
}

// nolint
func (osScenarioHelper) setFileSDDLString(c asserter, filepath string, sddldata string) {
	panic("should never be called")
}

func (osScenarioHelper) Mknod(c asserter, path string, mode uint32, dev int) {
	c.AssertNoErr(unix.Mknod(path, mode, dev))
}

func (osScenarioHelper) GetUnixStatAdapterForFile(c asserter, filepath string) common.UnixStatAdapter {
	{ // attempt to call statx, if ENOSYS is returned, statx is unavailable
		var stat unix.Statx_t

		statxFlags := unix.AT_STATX_SYNC_AS_STAT | unix.AT_SYMLINK_NOFOLLOW
		// dirfd is a null pointer, because we should only ever be passing relative paths here, and directories will be passed via transferInfo.Source.
		// AT_SYMLINK_NOFOLLOW is not used, because we automagically resolve symlinks. TODO: Add option to not follow symlinks, and use AT_SYMLINK_NOFOLLOW when resolving is disabled.
		err := unix.Statx(0, filepath,
			statxFlags,
			unix.STATX_ALL,
			&stat)

		if err != nil && err != unix.ENOSYS { // catch if statx is unsupported
			c.AssertNoErr(err, "for file "+filepath)
		} else if err == nil {
			return ste.StatxTAdapter(stat)
		}
	}

	var stat unix.Stat_t
	err := unix.Stat(filepath, &stat)
	c.AssertNoErr(err)

	return ste.StatTAdapter(stat)
}

func (osScenarioHelper) IsFileHidden(c asserter, filePath string) bool {
	fileName := filepath.Base(filePath)
	// On Unix-based systems, hidden files start with a dot
	isHidden := strings.HasPrefix(fileName, ".")
	return isHidden
}

func (osScenarioHelper) CreateSpecialFile(filePath string) error {
	err := unix.Mkfifo(filePath, 0666)
	return err
}
