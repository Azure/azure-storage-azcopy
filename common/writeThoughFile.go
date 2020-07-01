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
	"os"
	"regexp"
	"strings"
)

const IncludeAfterFlagName = "include-after"
const BackupModeFlagName = "backup" // original name, backup mode, matches the name used for the same thing in Robocopy
const PreserveOwnerFlagName = "preserve-owner"
const PreserveOwnerDefault = true

// The regex doesn't require a / on the ending, it just requires something similar to the following
// C:
// C:/
// //myShare
// //myShare/
// demonstrated at: https://regexr.com/4mf6l
var RootDriveRegex = regexp.MustCompile(`(?i)(^[A-Z]:\/?$)`)
var RootShareRegex = regexp.MustCompile(`(^\/\/[^\/]*\/?$)`)

func CreateParentDirectoryIfNotExist(destinationPath string, tracker FolderCreationTracker) error {
	// find the parent directory
	directory := destinationPath[:strings.LastIndex(destinationPath, DeterminePathSeparator(destinationPath))]
	return CreateDirectoryIfNotExist(directory, tracker)
}

func CreateDirectoryIfNotExist(directory string, tracker FolderCreationTracker) error {
	// If we're pointing at the root of a drive, don't try because it won't work.
	if shortParentDir := strings.ReplaceAll(ToShortPath(directory), OS_PATH_SEPARATOR, AZCOPY_PATH_SEPARATOR_STRING); RootDriveRegex.MatchString(shortParentDir) || RootShareRegex.MatchString(shortParentDir) || strings.EqualFold(shortParentDir, "/") {
		return nil
	}

	// try to create the directory if it does not already exist
	if _, err := OSStat(directory); err != nil {
		// if the error is present, try to create the directory
		// stat errors can be present in write-only scenarios, when the directory isn't present, etc.
		// as a result, we care more about the mkdir error than the stat error, because that's the tell.
		// first make sure the parent directory exists but we ignore any error that comes back
		CreateParentDirectoryIfNotExist(directory, tracker)

		// then create the directory
		mkDirErr := os.Mkdir(directory, os.ModePerm)

		// if Mkdir succeeds, no error is dropped-- it is nil.
		// therefore, returning here is perfectly acceptable as it either succeeds (or it doesn't)
		if mkDirErr == nil {
			// To run our folder overwrite logic, we have to know if this current job created the folder.
			// As per the comments above, we are technically wrong here in a write-only scenario (maybe it already
			// existed and our Stat failed).  But using overwrite=false on a write-only destination doesn't make
			// a lot of sense anyway. Yes, we'll make the wrong decision here in a write-only scenario, but we'll
			// make the _same_ wrong overwrite decision for all the files too (not just folders). So this is, at least,
			// consistent.
			tracker.RecordCreation(directory)
			return nil
		}

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
