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
	// slice represents the actual memory mapped buffer
	slice []byte
	// defines whether source has been mapped or not
	isMapped bool
	// This lock exists to fix a bug in Go's Http Client. Because the http
	// client executes some operations asynchronously (via goroutines), it
	// sometimes attempts to read from the http request stream AFTER the MMF
	// is unmapped. This lock guards against the access violation panic.
	// When the MMF is created, all readers can take the shared (read) access
	// on this lock. When the MMF is no longer needed, exclusive (write) access
	// it requested and once obtained, the MMF is unmapped. If the http client
	// attempts to read again the request body again, our pacer code sees that
	// isMapped is false and gracefully fails the http request (avoiding the
	// access violation panic).
	lock sync.RWMutex

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
	return &MMF{slice:m, isMapped:true, lock:sync.RWMutex{}}, nil
}

// To unmap, we need exclusive (write) access to the MMF and
// then we set isMapped to false so that future readers know
// the MMF is unusable.
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

func (m *MMF) UseMMF() bool {
	m.lock.RLock()
	if !m.isMapped {
		m.lock.RUnlock()
		return false
	}
	return true
}

// RUnlock unlocks the held lock
func (m *MMF) UnuseMMF(){
	m.lock.RUnlock()
}

// Slice() returns the memory mapped byte slice
func (m* MMF) Slice() []byte {
	return m.slice
}
