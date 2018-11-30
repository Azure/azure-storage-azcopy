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
	"strings"
	"syscall"
	"unsafe"
)

func CreateFileOfSize(destinationPath string, fileSize int64) (*os.File, error) {
	return CreateFileOfSizeWithWriteThroughOption(destinationPath, fileSize, false)
}


func CreateFileOfSizeWithWriteThroughOption(destinationPath string, fileSize int64, writeThrough bool) (*os.File, error) {
	err := CreateParentDirectoryIfNotExist(destinationPath)
	if err != nil {
		return nil, err
	}

	fd, err := OpenWithWriteThroughSetting(destinationPath, os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0644, writeThrough)
	if err != nil {
		return nil, err
	}
	f := os.NewFile(uintptr(fd), destinationPath)
	if f == nil {
		return nil, os.ErrInvalid
	}

	if truncateError := f.Truncate(fileSize); truncateError != nil {
		return nil, truncateError
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


func makeInheritSa() *syscall.SecurityAttributes {
	var sa syscall.SecurityAttributes
	sa.Length = uint32(unsafe.Sizeof(sa))
	sa.InheritHandle = 1
	return &sa
}

const FILE_ATTRIBUTE_WRITE_THROUGH        = 0x80000000

// Copied from syscall.open, but modified to allow setting of writeThrough option
// Param "perm" is unused both here and in the original Windows version of this routine.
func OpenWithWriteThroughSetting(path string, mode int, perm uint32, writeThrough bool) (fd syscall.Handle, err error) {
	if len(path) == 0 {
		return syscall.InvalidHandle, syscall.ERROR_FILE_NOT_FOUND
	}
	pathp, err := syscall.UTF16PtrFromString(path)
	if err != nil {
		return syscall.InvalidHandle, err
	}
	var access uint32
	switch mode & (syscall.O_RDONLY | syscall.O_WRONLY | syscall.O_RDWR) {
	case syscall.O_RDONLY:
		access = syscall.GENERIC_READ
	case syscall.O_WRONLY:
		access = syscall.GENERIC_WRITE
	case syscall.O_RDWR:
		access = syscall.GENERIC_READ | syscall.GENERIC_WRITE
	}
	if mode&syscall.O_CREAT != 0 {
		access |= syscall.GENERIC_WRITE
	}
	if mode&syscall.O_APPEND != 0 {
		access &^= syscall.GENERIC_WRITE
		access |= syscall.FILE_APPEND_DATA
	}
	sharemode := uint32(syscall.FILE_SHARE_READ | syscall.FILE_SHARE_WRITE)
	var sa *syscall.SecurityAttributes
	if mode&syscall.O_CLOEXEC == 0 {
		sa = makeInheritSa()
	}
	var createmode uint32
	switch {
	case mode&(syscall.O_CREAT|syscall.O_EXCL) == (syscall.O_CREAT | syscall.O_EXCL):
		createmode = syscall.CREATE_NEW
	case mode&(syscall.O_CREAT|syscall.O_TRUNC) == (syscall.O_CREAT | syscall.O_TRUNC):
		createmode = syscall.CREATE_ALWAYS
	case mode&syscall.O_CREAT == syscall.O_CREAT:
		createmode = syscall.OPEN_ALWAYS
	case mode&syscall.O_TRUNC == syscall.O_TRUNC:
		createmode = syscall.TRUNCATE_EXISTING
	default:
		createmode = syscall.OPEN_EXISTING
	}

	var attr uint32
	attr = syscall.FILE_ATTRIBUTE_NORMAL
	if writeThrough {
		attr |= FILE_ATTRIBUTE_WRITE_THROUGH
	}
	h, e := syscall.CreateFile(pathp, access, sharemode, sa, createmode,  attr, 0)
	return h, e
}



