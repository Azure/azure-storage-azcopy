//go:build linux || darwin
// +build linux darwin

package cmd

import (
	"os"
	"syscall"
)

type MMF []byte

func NewMMF(file *os.File, writable bool, offset int64, length int64) (MMF, error) {
	prot, flags := syscall.PROT_READ, syscall.MAP_SHARED // Assume read-only
	if writable {
		prot, flags = syscall.PROT_READ|syscall.PROT_WRITE, syscall.MAP_SHARED
	}
	addr, err := syscall.Mmap(int(file.Fd()), offset, int(length), prot, flags)
	return MMF(addr), err
}

func (m *MMF) Unmap() {
	err := syscall.Munmap(*m)
	*m = nil
	if err != nil {
		panic(err)
	}
}
