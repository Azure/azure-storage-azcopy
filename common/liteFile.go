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
	"io"
	"syscall"
)

// Light-weight file access, with syscalls direct to file handles
// In testing, this used less CPU that io.File. TBC - why was that?
// TODO: do we need or want this on on-Windows platforms?
type liteFile struct {
	fileHandle syscall.Handle
}

func NewLiteFileForReading(fullPath string) (io.ReadCloser, error) {
	// TODO: this is Windows-specific. See TODO above deciding whether we want/need this on Linux

	pathp, err := syscall.UTF16PtrFromString(fullPath)
	var sa *syscall.SecurityAttributes
	var FILE_FLAG_SEQUENTIAL_SCAN = uint32(0x08000000)
	fileHandle, err := syscall.CreateFile(pathp, syscall.GENERIC_READ, 0, sa, syscall.OPEN_EXISTING, syscall.FILE_ATTRIBUTE_NORMAL|FILE_FLAG_SEQUENTIAL_SCAN, 0)
	if err != nil {
		return nil, err
	}

	return &liteFile{
		fileHandle: fileHandle}, nil
}

func (f *liteFile) Read(b []byte) (int, error) {
	return syscall.Read(f.fileHandle, b)
}

func (f *liteFile) Close() error {
	return syscall.Close(f.fileHandle)
}
