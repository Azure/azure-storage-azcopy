// +build linux darwin

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

package main

import (
	"os"
	"path"
	"syscall"
)

// ProcessOSSpecificInitialization changes the soft limit for file descriptor for process
// and returns the file descriptor limit for process. If the function fails with some error
// it returns the error.
// Api gets the hard limit for process file descriptor
// and sets the soft limit for process file descriptor to above hard limit
func ProcessOSSpecificInitialization() (uint64, error) {
	var rlimit, zero syscall.Rlimit
	// get the hard limit
	err := syscall.Getrlimit(syscall.RLIMIT_NOFILE, &rlimit)
	if err != nil {
		return 0, err
	}
	// If the hard limit is 0, then raise the exception
	if zero == rlimit {
		return 0, err
	}
	set := rlimit
	// set the current limit to one less than max of the rlimit
	set.Cur = set.Max - 1
	// set the soft limit to above rlimit
	err = syscall.Setrlimit(syscall.RLIMIT_NOFILE, &set)
	if err != nil {
		return 0, err
	}
	return set.Max, nil
}

// GetAzCopyAppPath returns the path of Azcopy folder in local appdata.
// Azcopy folder in local appdata contains all the files created by azcopy locally.
func GetAzCopyAppPath() string {
	localAppData := os.Getenv("HOME")
	azcopyAppDataFolder := path.Join(localAppData, ".azcopy")
	if err := os.Mkdir(azcopyAppDataFolder, os.ModeDir|os.ModePerm); err != nil && !os.IsExist(err) {
		return ""
	}
	return azcopyAppDataFolder
}
