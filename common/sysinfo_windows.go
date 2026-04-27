//go:build windows
// +build windows

package common

import (
	"fmt"
	"syscall"
	"unsafe"
)

// MEMORYSTATUSEX structure for Windows API
type memoryStatusEx struct {
	Length               uint32
	MemoryLoad           uint32
	TotalPhys            uint64
	AvailPhys            uint64
	TotalPageFile        uint64
	AvailPageFile        uint64
	TotalVirtual         uint64
	AvailVirtual         uint64
	AvailExtendedVirtual uint64
}

// MemorySourceDetails contains details about where the memory limit was sourced from.
type MemorySourceDetails struct {
	HostMemoryBytes  int64  // Memory from OS APIs
	CgroupLimitBytes int64  // Always 0 on Windows
	EffectiveBytes   int64  // The value actually used
	Source           string // "host" or "error"
}

// Retrieves the available memory on a Windows system using GlobalMemoryStatusEx API.
// Returns available physical memory in bytes.
func GetMemAvailable() (int64, error) {
	kernel32 := syscall.MustLoadDLL("kernel32.dll")
	globalMemoryStatusEx := kernel32.MustFindProc("GlobalMemoryStatusEx")

	var memStatus memoryStatusEx
	memStatus.Length = uint32(unsafe.Sizeof(memStatus))

	ret, _, err := globalMemoryStatusEx.Call(uintptr(unsafe.Pointer(&memStatus)))
	if ret == 0 {
		return 0, fmt.Errorf("GetMemAvailable failed with error: %v", err)
	}

	// Convert to int64 and return available physical memory in bytes
	return int64(memStatus.AvailPhys), nil
}

// Retrieves the total physical memory on a Windows system using GlobalMemoryStatusEx API.
// Returns total physical memory in bytes.
func GetTotalPhysicalMemory() (int64, error) {
	kernel32 := syscall.MustLoadDLL("kernel32.dll")
	globalMemoryStatusEx := kernel32.MustFindProc("GlobalMemoryStatusEx")

	var memStatus memoryStatusEx
	memStatus.Length = uint32(unsafe.Sizeof(memStatus))

	ret, _, err := globalMemoryStatusEx.Call(uintptr(unsafe.Pointer(&memStatus)))
	if ret == 0 {
		return 0, fmt.Errorf("GetTotalPhysicalMemory failed with error: %v", err)
	}

	// Convert to int64 and return total physical memory in bytes
	return int64(memStatus.TotalPhys), nil
}

// GetMemorySourceDetails returns details about the memory detection.
// On Windows, cgroups are not applicable, so host memory is always used.
func GetMemorySourceDetails() MemorySourceDetails {
	details := MemorySourceDetails{}

	hostMem, err := GetTotalPhysicalMemory()
	if err != nil {
		details.Source = "error"
		return details
	}

	details.HostMemoryBytes = hostMem
	details.EffectiveBytes = hostMem
	details.Source = "host"
	return details
}
