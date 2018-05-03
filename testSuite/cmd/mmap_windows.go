package cmd

import (
	"os"
	"reflect"
	"syscall"
	"unsafe"
)

type MMF []byte

func NewMMF(file *os.File, writable bool, offset int64, length int64) (MMF, error) {
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
	m := MMF{}
	h := (*reflect.SliceHeader)(unsafe.Pointer(&m))
	h.Data = addr
	h.Len = int(length)
	h.Cap = h.Len
	return m, nil
}

func (m *MMF) Unmap() {
	addr := uintptr(unsafe.Pointer(&(([]byte)(*m)[0])))
	*m = MMF{}
	err := syscall.UnmapViewOfFile(addr)
	if err != nil {
		panic(err)
	}
}
