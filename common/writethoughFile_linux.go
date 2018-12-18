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
	"syscall"
	"strings"
)

func CreateFileOfSize(destinationPath string, fileSize int64) (*os.File, error) {
	return CreateFileOfSizeWithWriteThroughOption(destinationPath, fileSize, false)
}

func CreateFileOfSizeWithWriteThroughOption(destinationPath string, fileSize int64, writeThrough bool) (*os.File, error) {
	err := CreateParentDirectoryIfNotExist(destinationPath)
	if err != nil {
		return nil, err
	}

	flags := os.O_RDWR | os.O_CREATE | os.O_TRUNC
	if writeThrough {
		flags = flags | os.O_SYNC  // technically, O_DSYNC may be very slightly faster, but its not exposed in the os package
	}
	f, err := os.OpenFile(destinationPath, flags, 0644)
	if err != nil {
		return nil, err
	}

	err = syscall.Fallocate(int(f.Fd()), 0, 0, fileSize)
	if err != nil {
		// To solve the case that Fallocate cannot work well with cifs/smb3.
		if err == syscall.ENOTSUP {
			if truncateError := f.Truncate(fileSize); truncateError != nil {
				return nil, truncateError
			}
		} else {
			return nil, err
		}
	}
	return f, nil
}

func CreateParentDirectoryIfNotExist(destinationPath string) error {
	// check if parent directory exists
	parentDirectory := destinationPath[:strings.LastIndex(destinationPath, AZCOPY_PATH_SEPARATOR_STRING)]
	_, err := os.Stat(parentDirectory)
	// if the parent directory does not exist, create it and all its parents
	if os.IsNotExist(err) {
		err = os.MkdirAll(parentDirectory, os.ModePerm)
		if err != nil {
			return err
		}
	} else if err != nil {
		return err
	}
	return nil
}
