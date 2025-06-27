//go:build linux || darwin
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

package cmd

import (
	"math"
	"syscall"
)

// processOSSpecificInitialization changes the soft limit for file descriptor for process
// and returns the new file descriptor limit for process.
// We need to do this because the default limits are low on Linux, and we concurrently open lots of files
// and sockets (both of which count towards this limit).
// Api gets the hard limit for process file descriptor
// and sets the soft limit for process file descriptor to (hard limit - 1)
func processOSSpecificInitialization() (int, error) {
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
	err = syscall.Setrlimit(syscall.RLIMIT_NOFILE, &set)
	if err != nil {
		// on some platforms such as OS X and BSD
		// the max we get is not actually the right number (https://unix.stackexchange.com/questions/350776/setting-os-x-macos-bsd-maxfile)
		// before Go 1.12, the call used to silently set the limit to a certain max number
		// after Go 1.12, the runtime fails the call
		// avoid returning an error and simply proceed with the current number
		return int(rlimit.Cur), nil
	}

	if set.Cur > math.MaxInt32 {
		return math.MaxInt32, nil
	} else {
		return int(set.Cur), nil
	}
}
