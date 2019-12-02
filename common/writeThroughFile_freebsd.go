// uintptr(fileSize int64) below truncates the requested allocation on 32-bit platforms:
// +build !386,!amd64p32,!arm,!armbe,!mips,!mipsle,!mips64p32,!mips64p32le,!ppc,!s390,!sparc

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
		flags = flags | os.O_SYNC
	}
	f, err := os.OpenFile(destinationPath, flags, DEFAULT_FILE_PERM)
	if err != nil {
		return nil, err
	}

	e1, _, _ := syscall.Syscall(syscall.SYS_POSIX_FALLOCATE, uintptr(int(f.Fd())), uintptr(0), uintptr(fileSize))
	if e1 != 0 {
		e1 := syscall.Errno(e1)
		if e1 != syscall.ENOSPC {
			if err = f.Truncate(fileSize); err != nil {
				return nil, err
			}
			return f, nil
		}
		return nil, os.NewSyscallError("posix_fallocate", e1)
	}
	return f, nil
}
