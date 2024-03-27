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
	"sync"
	"syscall"
	"unsafe"
)

const lineEnding = "\r\n"

type MMF struct {
	// slice represents the actual memory mapped buffer
	slice []byte

	length int64
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
	var fileSize = offset + length
	hMMF, errno := syscall.CreateFileMapping(syscall.Handle(file.Fd()), nil, prot, uint32(fileSize>>32), uint32(fileSize&0xffffffff), nil)
	if hMMF == 0 {
		return nil, os.NewSyscallError("CreateFileMapping", errno)
	}
	defer syscall.CloseHandle(hMMF) //nolint:errcheck
	addr, _ := syscall.MapViewOfFile(hMMF, access, uint32(offset>>32), uint32(offset&0xffffffff), uintptr(length))

	if !writable {
		// pre-fetch the memory mapped file so that performance is better when it is read
		err := prefetchVirtualMemory(&memoryRangeEntry{VirtualAddress: addr, NumberOfBytes: int(length)})
		if err != nil {
			panic(err)
		}
	}

	m := unsafe.Slice((*byte)(unsafe.Pointer(addr)), int(length))
	return &MMF{slice: m, length: length, isMapped: true, lock: sync.RWMutex{}}, nil
}

// To unmap, we need exclusive (write) access to the MMF and
// then we set isMapped to false so that future readers know
// the MMF is unusable.
func (m *MMF) Unmap() {
	m.lock.Lock()
	addr := uintptr(unsafe.Pointer(&(([]byte)(m.slice)[0])))
	m.slice = []byte{}
	// Modified pages in the unmapped view are not written to disk until their share count
	// reaches zero, or in other words, until they are unmapped or trimmed from the working
	// sets of all processes that share the pages. Even then, the modified pages are written
	// "lazily" to disk; that is, modifications may be cached in memory and written to disk
	// at a later time. To avoid modifications to be cached in memory,explicitly flushing
	// modified pages using the FlushViewOfFile function.
	_ = syscall.FlushViewOfFile(addr, uintptr(m.length))
	err := syscall.UnmapViewOfFile(addr)
	PanicIfErr(err)
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
func (m *MMF) UnuseMMF() {
	m.lock.RUnlock()
}

// Slice() returns the memory mapped byte slice
func (m *MMF) Slice() []byte {
	return m.slice
}

type memoryRangeEntry struct {
	VirtualAddress uintptr
	NumberOfBytes  int
}

var procPrefetchVirtualMemory *syscall.Proc

func init() {
	// only load the DLL once
	var modkernel32, _ = syscall.LoadDLL("kernel32.dll")
	procPrefetchVirtualMemory, _ = modkernel32.FindProc("PrefetchVirtualMemory")
}

func prefetchVirtualMemory(virtualAddresses *memoryRangeEntry) (err error) {
	// if the version of Windows does not support this functionality, just skip
	if procPrefetchVirtualMemory == nil {
		return nil
	}

	// make system call to prefetch the memory range
	hProcess, _ := syscall.GetCurrentProcess()
	r1, _, e1 := syscall.SyscallN(procPrefetchVirtualMemory.Addr(), 4, uintptr(hProcess), 1, uintptr(unsafe.Pointer(virtualAddresses)), 0, 0, 0)

	if r1 == 0 {
		if e1 != 0 {
			return e1
		} else {
			return nil
		}
	}
	return nil
}
