//go:build windows

package azcopy

import (
	"runtime"
	"unsafe"

	hostinfointernal "github.com/Azure/azure-storage-azcopy/v10/azcopy/internal/hostinfo"
	"golang.org/x/sys/windows"
)

const (
	firmwareTableProviderRSMB = 0x52534d42
	rawSMBIOSFirmwareTableID  = 0
)

var procGetSystemFirmwareTable = windows.NewLazySystemDLL("kernel32.dll").NewProc("GetSystemFirmwareTable")

func probeAzureVM() bool {
	if err := procGetSystemFirmwareTable.Find(); err != nil {
		return false
	}
	return hostinfointernal.AzurePublicCloudFromFirmware(func(buffer []byte) uint32 {
		var address uintptr
		if len(buffer) > 0 {
			address = uintptr(unsafe.Pointer(&buffer[0]))
		}
		written, _, _ := procGetSystemFirmwareTable.Call(firmwareTableProviderRSMB, rawSMBIOSFirmwareTableID, address, uintptr(len(buffer)))
		runtime.KeepAlive(buffer)
		return uint32(written)
	})
}
