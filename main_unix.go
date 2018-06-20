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
	"os/exec"
	"path"
	"syscall"
	"fmt"
)

func osModifyProcessCommand(cmd *exec.Cmd) *exec.Cmd {
	return cmd
}

// ChangeRLimits changes the filedescriptor limit for Azcopy
// It get the hard limit for process file descriptor
// and set the soft limit for process file descriptor to above hard limit
func ChangeRLimits() {
	var rlimit, zero syscall.Rlimit
	// get the hard limit
	err := syscall.Getrlimit(syscall.RLIMIT_NOFILE, &rlimit)
	if err != nil {
		panic(fmt.Errorf("error getting the rlimit. Failed with error %s", err.Error()))
	}
	// If the hard limit is 0, then raise the exception
	if zero == rlimit {
		panic(fmt.Errorf("hard rlimit is 0 for the process"))
	}
	set := rlimit
	// since the hard limit specifies a value one greater than the maximum file descriptor number
	// set the current to 1 less than max
	set.Cur = set.Max - 1
	// set the soft limit to above rlimit
	err = syscall.Setrlimit(syscall.RLIMIT_NOFILE, &set)
	if err != nil {
		panic(fmt.Errorf("Setrlimit: set failed: %#v %v", set, err.Error()))
	}
}

// GetAzCopyAppPath returns the path of Azcopy folder in local appdata.
// Azcopy folder in local appdata contains all the files created by azcopy locally.
func GetAzCopyAppPath() string {
	// call the changeRLimit api.
	// calling this api internally from GetAzcopyPath api since this api needs to be called
	// only for linux platform
	ChangeRLimits()
	localAppData := os.Getenv("HOME")
	azcopyAppDataFolder := path.Join(localAppData, "/.azcopy")
	if err := os.Mkdir(azcopyAppDataFolder, os.ModeDir|os.ModePerm); err != nil && !os.IsExist(err) {
		return ""
	}
	return azcopyAppDataFolder
}
