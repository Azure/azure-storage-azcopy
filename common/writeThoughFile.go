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

package common

import (
	"context"
	"fmt"
	"os"
	"regexp"
	"strings"
	"time"
)

const IncludeBeforeFlagName = "include-before"
const IncludeAfterFlagName = "include-after"
const BackupModeFlagName = "backup" // original name, backup mode, matches the name used for the same thing in Robocopy
const PreserveOwnerFlagName = "preserve-owner"
const PreserveSymlinkFlagName = "preserve-symlinks"
const PreserveOwnerDefault = true

// The regex doesn't require a / on the ending, it just requires something similar to the following
// C:
// C:/
// //myShare
// //myShare/
// demonstrated at: https://regexr.com/4mf6l
var RootDriveRegex = regexp.MustCompile(`(?i)(^[A-Z]:\/?$)`)
var RootShareRegex = regexp.MustCompile(`(^\/\/[^\/]*\/?$)`)

func isRootPath(s string) bool {
	shortParentDir := strings.ReplaceAll(ToShortPath(s), OS_PATH_SEPARATOR, AZCOPY_PATH_SEPARATOR_STRING)
	return RootDriveRegex.MatchString(shortParentDir) ||
		RootShareRegex.MatchString(shortParentDir) ||
		strings.EqualFold(shortParentDir, "/")
}

func CreateParentDirectoryIfNotExist(ctx context.Context, destinationPath string, tracker FolderCreationTracker) error {
	// If we're pointing at the root of a drive, don't try because it won't work.
	if isRootPath(destinationPath) {
		return nil
	}

	pathSeparator := DeterminePathSeparator(destinationPath)
	lastIndex := strings.LastIndex(destinationPath, pathSeparator)

	// LastIndex() will return -1 if path separator was not found, we should handle this gracefully
	// instead of allowing AzCopy to crash with an out-of-bounds error.
	if lastIndex == -1 {
		return fmt.Errorf("error: Path separator (%s) not found in destination path. On Linux, this may occur if the destination is the root file, such as '/'. If this is the case, please consider changing your destination path.", pathSeparator)
	}

	directory := destinationPath[:lastIndex]
	return CreateDirectoryIfNotExist(ctx, directory, tracker)
}

func CreateDirectoryIfNotExist(ctx context.Context, directory string, tracker FolderCreationTracker) error {
	// If we're pointing at the root of a drive, don't try because it won't work.
	if isRootPath(directory) {
		return nil
	}

	// try to create the directory if it does not already exist
	if _, err := OSStat(directory); err != nil {
		// if the error is present, try to create the directory
		// stat errors can be present in write-only scenarios, when the directory isn't present, etc.
		// as a result, we care more about the mkdir error than the stat error, because that's the tell.
		// first make sure the parent directory exists but we ignore any error that comes back
		_ = CreateParentDirectoryIfNotExist(ctx, directory, tracker)

		// then create the directory
		mkDirErr := tracker.CreateFolder(ctx, directory, func() error {
			return os.Mkdir(directory, os.ModePerm)
		})

		// another routine might have created the directory at the same time
		// check whether the directory now exists
		if _, err := OSStat(directory); err != nil {
			// no other routine succeeded
			// return the original error we got from Mkdir
			return mkDirErr
		}

		return nil
	} else { // if err is nil, we return err. if err has an error, we return it.
		return nil
	}
}

// EntityTimestamps provides a common interface for file information across platforms
type ExtendedProperties interface {
	// GetLastAccessTime returns the last access time
	GetLastAccessTime() time.Time

	// GetLastWriteTime returns the last write time
	GetLastWriteTime() time.Time

	// GetChangeTime returns the change time (may be same as write time on some platforms)
	GetChangeTime() time.Time
}

type DefaultExtendedProperties struct{}

func (d DefaultExtendedProperties) GetLastAccessTime() time.Time {
	return time.Time{}
}

func (d DefaultExtendedProperties) GetLastWriteTime() time.Time {
	return time.Time{}
}

func (d DefaultExtendedProperties) GetChangeTime() time.Time {
	return time.Time{}
}
