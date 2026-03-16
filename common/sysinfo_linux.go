//go:build linux || darwin
// +build linux darwin

package common

import (
	"bufio"
	"fmt"
	"os"
	"strconv"
	"strings"
	"sync"

	"golang.org/x/sys/unix"
)

var (
	// Cache for total physical memory since it never changes during program execution
	cachedTotalMemory int64
	totalMemoryOnce   sync.Once
	totalMemoryError  error
)

// Retrieves the available memory on a Linux system by reading and parsing the /proc/meminfo file.
// Utilizes the MemAvailable field introduced in Linux kernel version 3.14 and above.
// Includes a TODO to implement a rough estimate for systems running on kernel versions prior to 3.14 where MemAvailable is not available.
func GetMemAvailable() (int64, error) {
	// Read /proc/meminfo directly without shell commands
	file, err := os.Open("/proc/meminfo")
	if err != nil {
		return 0, fmt.Errorf("GetMemAvailable failed to open /proc/meminfo: %v", err)
	}
	defer file.Close()

	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		line := scanner.Text()
		if strings.HasPrefix(line, "MemAvailable") {
			var multiplier int64
			var result int
			tokens := strings.Fields(line)
			if len(tokens) != 3 {
				return 0, fmt.Errorf("GetMemAvailable invalid output[%s] of /proc/meminfo", line)
			}

			value := tokens[1]
			multiplierStr := tokens[2]

			if multiplierStr == "kB" {
				multiplier = 1024
			} else {
				// "/proc/meminfo" output always in kB only. If we are getting different string, something wrong.
				return 0, fmt.Errorf("MemAvailable value is not in kB, output[%s]", line)
			}

			if result, err = strconv.Atoi(value); err != nil {
				return 0, err
			}
			return int64(result) * multiplier, nil
		}
	}

	if err := scanner.Err(); err != nil {
		return 0, fmt.Errorf("GetMemAvailable failed to scan /proc/meminfo: %v", err)
	}

	// If we reached here, means MemAvailable entry not found in /proc/meminfo.
	var kernelVersion unix.Utsname
	_ = unix.Uname(&kernelVersion)

	return 0, fmt.Errorf("MemAvailable entry not found, kernel version: %+v", kernelVersion)
}

// getTotalPhysicalMemoryInternal performs the actual memory reading operation
func getTotalPhysicalMemoryInternal() (int64, error) {
	// Read /proc/meminfo directly without shell commands
	file, err := os.Open("/proc/meminfo")
	if err != nil {
		return 0, fmt.Errorf("GetTotalPhysicalMemory failed to open /proc/meminfo: %v", err)
	}
	defer file.Close()

	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		line := scanner.Text()
		if strings.HasPrefix(line, "MemTotal") {
			var multiplier int64
			var result int
			tokens := strings.Fields(line)
			if len(tokens) != 3 {
				return 0, fmt.Errorf("GetTotalPhysicalMemory invalid output[%s] of /proc/meminfo", line)
			}

			value := tokens[1]
			multiplierStr := tokens[2]

			if multiplierStr == "kB" {
				multiplier = 1024
			} else {
				// "/proc/meminfo" output always in kB only. If we are getting different string, something wrong.
				return 0, fmt.Errorf("MemTotal value is not in kB, output[%s]", line)
			}

			if result, err = strconv.Atoi(value); err != nil {
				return 0, err
			}
			return int64(result) * multiplier, nil
		}
	}

	if err := scanner.Err(); err != nil {
		return 0, fmt.Errorf("GetTotalPhysicalMemory failed to scan /proc/meminfo: %v", err)
	}

	// If we reached here, means MemTotal entry not found in /proc/meminfo.
	var kernelVersion unix.Utsname
	_ = unix.Uname(&kernelVersion)

	return 0, fmt.Errorf("MemTotal entry not found, kernel version: %+v", kernelVersion)
}

// Retrieves the total physical memory on a Linux system by reading and parsing the /proc/meminfo file.
// Returns total physical memory in bytes.
// This function caches the result since total physical memory never changes during program execution.
func GetTotalPhysicalMemory() (int64, error) {
	totalMemoryOnce.Do(func() {
		cachedTotalMemory, totalMemoryError = getTotalPhysicalMemoryInternal()
	})
	return cachedTotalMemory, totalMemoryError
}
