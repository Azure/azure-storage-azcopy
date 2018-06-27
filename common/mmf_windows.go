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
	"reflect"
	"syscall"
	"unsafe"
	"sync"
)

type MMF struct {
	slice []byte
	isMapped bool
	lock sync.Mutex
}

func NewMMF(file *os.File, writable bool, offset int64, length int64) (*MMF, error) {
	prot, access := uint32(syscall.PAGE_READONLY), uint32(syscall.FILE_MAP_READ) // Assume read-only
	if writable {
		prot, access = uint32(syscall.PAGE_READWRITE), uint32(syscall.FILE_MAP_WRITE)
	}
	hMMF, errno := syscall.CreateFileMapping(syscall.Handle(file.Fd()), nil, prot, uint32(int64(length)>>32), uint32(int64(length)&0xffffffff), nil)
	if hMMF == 0 {
		return nil, os.NewSyscallError("CreateFileMapping", errno)
	}
	defer syscall.CloseHandle(hMMF)
	addr, errno := syscall.MapViewOfFile(hMMF, access, uint32(offset>>32), uint32(offset&0xffffffff), uintptr(length))
	m := []byte{}
	h := (*reflect.SliceHeader)(unsafe.Pointer(&m))
	h.Data = addr
	h.Len = int(length)
	h.Cap = h.Len
	return &MMF{slice:m, isMapped:true, lock:sync.Mutex{}}, nil
}

func (m *MMF) Unmap() {
	m.lock.Lock()
	addr := uintptr(unsafe.Pointer(&(([]byte)(m.slice)[0])))
	m.slice = []byte{}
	err := syscall.UnmapViewOfFile(addr)
	if err != nil {
		panic(err)
	}
	m.isMapped = false
	m.lock.Unlock()
}

func (m *MMF) IsUnmapped() bool{
	m.lock.Lock()
	isUnmapped := !m.isMapped
	m.lock.Unlock()
	return isUnmapped
}

func (m* MMF) MMFSlice() []byte {
	return m.slice
}
