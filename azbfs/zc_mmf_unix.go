//go:build linux || darwin
// +build linux darwin

package azbfs

import (
	"os"
	"syscall"
)

//nolint:unused
type mmf []byte

//nolint:unused,deadcode
func newMMF(file *os.File, writable bool, offset int64, length int) (mmf, error) {
	prot, flags := syscall.PROT_READ, syscall.MAP_SHARED // Assume read-only
	if writable {
		prot, flags = syscall.PROT_READ|syscall.PROT_WRITE, syscall.MAP_SHARED
	}
	addr, err := syscall.Mmap(int(file.Fd()), offset, length, prot, flags)
	return mmf(addr), err
}

//nolint:unused
func (m *mmf) unmap() {
	err := syscall.Munmap(*m)
	*m = nil
	if err != nil {
		panic(err)
	}
}
