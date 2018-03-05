package handlers

import (
"os"
"syscall"
)

type MMap []byte

func Map(file *os.File, writable bool, offset int64, length int) (MMap, error) {
	prot, flags := syscall.PROT_READ, syscall.MAP_SHARED // Assume read-only
	if writable {
		prot, flags = syscall.PROT_READ|syscall.PROT_WRITE, syscall.MAP_SHARED
	}
	addr, err := syscall.Mmap(int(file.Fd()), offset, length, prot, flags)
	return MMap(addr), err
}

func (m *MMap) Unmap() {
	err := syscall.Munmap(*m)
	*m = nil
	if err != nil {
		panic(err)
	}
}
