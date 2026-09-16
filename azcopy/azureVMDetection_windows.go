package azcopy

import (
	"runtime"
	"unsafe"

	"golang.org/x/sys/windows"
)

var procGetSystemFirmwareTable = windows.NewLazySystemDLL("kernel32.dll").NewProc("GetSystemFirmwareTable")

func probeAzureVM() bool {
	if err := procGetSystemFirmwareTable.Find(); err != nil {
		return false
	}
	return azurePublicCloudFromFirmware(func(buffer []byte) uint32 {
		var address uintptr
		if len(buffer) > 0 {
			address = uintptr(unsafe.Pointer(&buffer[0]))
		}
		written, _, _ := procGetSystemFirmwareTable.Call(0x52534d42, 0, address, uintptr(len(buffer)))
		runtime.KeepAlive(buffer)
		return uint32(written)
	})
}

func azurePublicCloudFromFirmware(read func([]byte) uint32) bool {
	size := read(nil)
	if size < 8 || size > maxSMBIOSTableBytes {
		return false
	}
	buffer := make([]byte, size)
	written := read(buffer)
	if written < 8 || written > size {
		return false
	}
	return azurePublicCloudFromSMBIOS(buffer[:written])
}
