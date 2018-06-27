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

package common

import (
	"os"
	"syscall"
	"sync"
)

type MMF struct {
	slice []byte
	isMapped bool
	lock sync.Mutex
}

func NewMMF(file *os.File, writable bool, offset int64, length int64) (*MMF, error) {
	prot, flags := syscall.PROT_READ, syscall.MAP_SHARED // Assume read-only
	if writable {
		prot, flags = syscall.PROT_READ|syscall.PROT_WRITE, syscall.MAP_SHARED
	}
	addr, err := syscall.Mmap(int(file.Fd()), offset, int(length), prot, flags)
	return &MMF{slice : (addr), isMapped:true, lock : sync.Mutex{}}, err
}

func (m *MMF) Unmap() {
	m.lock.Lock()
	err := syscall.Munmap(m.slice)
	m.slice = nil
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
