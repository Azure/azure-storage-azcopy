//go:build linux || darwin
// +build linux darwin

package common

import (
	"fmt"
)

// GetMemAvailable retrieves the available memory on a Darwin system.
func GetMemAvailable() (int64, error) {
	return -1, fmt.Errorf("GetMemAvailable is not implemented for this platform")
}

// GetTotalPhysicalMemory retrieves the total physical memory on a Darwin system.
func GetTotalPhysicalMemory() (int64, error) {
	return -1, fmt.Errorf("GetTotalPhysicalMemory is not implemented for this platform")
}
