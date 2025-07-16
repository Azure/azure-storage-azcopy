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
	"fmt"
	"os"
	"regexp"
	"strings"
	"time"
)

const (
	// 100-nanosecond intervals from Windows Epoch (January 1, 1601) to Unix Epoch (January 1, 1970).
	TICKS_FROM_WINDOWS_EPOCH_TO_UNIX_EPOCH = 116_444_736_000_000_000

	// time.Time is limited to 9,214,748,364 seconds before and after Unix epoch (January 1, 1970) due to int64 limitations.
	// This is the maximum negative offset from Unix epoch in seconds.
	// It corresponds to the time 1678-01-01T00:00:00Z.
	// This is the minimum time.Time value that can be represented in Go.
	TICKS_FROM_WINDOWS_EPOCH_TO_MIN_UNIX_EPOCH = 24_299_136_000_000_000 // 24299136000000000

	TICKS_FROM_WINDOWS_EPOCH_TO_MIDWAY_POINT = 58_065_120_000_000_000 // Jan 1 1601 to Jan 1 1785
	TICKS_FROM_MIDWAY_EPOCH_TO_UNIX_EPOCH    = 58_379_616_000_000_000 // Jan 1 1785 to Jan 1 1970
)

const IncludeBeforeFlagName = "include-before"
const IncludeAfterFlagName = "include-after"
const BackupModeFlagName = "backup" // original name, backup mode, matches the name used for the same thing in Robocopy
const PreserveOwnerFlagName = "preserve-owner"
const PreserveSymlinkFlagName = "preserve-symlinks"
const PreserveOwnerDefault = true

var (
	// Windows epoch (1601-01-01 UTC)
	WindowsEpochUTC = time.Date(1601, 1, 1, 0, 0, 0, 0, time.UTC)

	// Mid year between Windows and Unix epochs
	MidYearEpochUTC = time.Date(1785, 1, 1, 0, 0, 0, 0, time.UTC)
)

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

func CreateParentDirectoryIfNotExist(destinationPath string, tracker FolderCreationTracker) error {
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
	return CreateDirectoryIfNotExist(directory, tracker)
}

func CreateDirectoryIfNotExist(directory string, tracker FolderCreationTracker) error {
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
		_ = CreateParentDirectoryIfNotExist(directory, tracker)

		// then create the directory
		mkDirErr := tracker.CreateFolder(directory, func() error {
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

// WinEpochNanoSecToTime converts nanoseconds since Windows epoch to time.Time.
// It handles the conversion from Windows epoch (1601-01-01)
func WinEpochNanoSecToTime(nsecWinEpoch uint64) time.Time {
	ticksWinEpoch := nsecWinEpoch / uint64(100) // Convert to 100-nanosecond intervals

	if ticksWinEpoch < uint64(TICKS_FROM_WINDOWS_EPOCH_TO_MIN_UNIX_EPOCH) {
		// If nsec is less than the minimum ticks from Windows epoch to Unix epoch,
		// we are dealing with a time beyond what unix.Time can handle
		// Windows epoch
		windowsEpoch := time.Date(1601, 1, 1, 0, 0, 0, 0, time.UTC)
		return windowsEpoch.Add(time.Duration(nsecWinEpoch)).In(time.UTC)
	}

	ticksUnixEpoch := int64(ticksWinEpoch) - TICKS_FROM_WINDOWS_EPOCH_TO_UNIX_EPOCH
	nsecUnixEpoch := ticksUnixEpoch * 100 // Convert to nanoseconds

	if nsecUnixEpoch >= 0 {
		return time.Unix(0, nsecUnixEpoch).In(time.UTC)
	}

	// It's a Unix epoch time before 1970 and after 1678
	// Use Go's internal unixTime function approach
	seconds := nsecUnixEpoch / 1e9
	nsec := nsecUnixEpoch % 1e9

	// Handle negative nanoseconds (Go's normalization)
	if nsec < 0 {
		seconds--
		nsec += 1e9
	}

	return time.Unix(seconds, nsec).In(time.UTC)
}
