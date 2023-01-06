package azbfs

import (
	"os"
	"reflect"
	"syscall"
	"unsafe"
)

//nolint:unused
type mmf []byte

//nolint:deadcode
func newMMF(file *os.File, writable bool, offset int64, length int) (mmf, error) {
	prot, access := uint32(syscall.PAGE_READONLY), uint32(syscall.FILE_MAP_READ) // Assume read-only
	if writable {
		prot, access = uint32(syscall.PAGE_READWRITE), uint32(syscall.FILE_MAP_WRITE)
	}
	maxSize := int64(offset + int64(length))
	hMMF, errno := syscall.CreateFileMapping(syscall.Handle(file.Fd()), nil, prot, uint32(maxSize>>32), uint32(maxSize&0xffffffff), nil)
	if hMMF == 0 {
		return nil, os.NewSyscallError("CreateFileMapping", errno)
	}
	defer syscall.CloseHandle(hMMF) //nolint:errcheck
	addr, errno := syscall.MapViewOfFile(hMMF, access, uint32(offset>>32), uint32(offset&0xffffffff), uintptr(length))
	m := mmf{}
	h := (*reflect.SliceHeader)(unsafe.Pointer(&m))
	h.Data = addr
	h.Len = length
	h.Cap = h.Len
	return m, nil
}

//nolint:unused
func (m *mmf) unmap() {
	addr := uintptr(unsafe.Pointer(&(([]byte)(*m)[0])))
	*m = mmf{}
	err := syscall.UnmapViewOfFile(addr)
	if err != nil {
		panic(err)
	}
}
