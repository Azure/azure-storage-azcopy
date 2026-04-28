//go:build darwin
// +build darwin

package common

import (
	"fmt"
)

// MemorySourceDetails contains details about where the memory limit was sourced from.
type MemorySourceDetails struct {
	HostMemoryBytes  int64  // Memory from OS APIs
	CgroupLimitBytes int64  // Always 0 on Darwin
	EffectiveBytes   int64  // The value actually used
	Source           string // "host" or "error"
}

// GetMemAvailable retrieves the available memory on a Darwin system.
func GetMemAvailable() (int64, error) {
	return -1, fmt.Errorf("GetMemAvailable is not implemented for this platform")
}

// GetTotalPhysicalMemory retrieves the total physical memory on a Darwin system.
func GetTotalPhysicalMemory() (int64, error) {
	return -1, fmt.Errorf("GetTotalPhysicalMemory is not implemented for this platform")
}

// GetMemorySourceDetails returns details about the memory detection.
// On Darwin, cgroups are not applicable, so host memory is used when available.
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
