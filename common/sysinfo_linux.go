//go:build linux || darwin
// +build linux darwin

package common

import (
	"bufio"
	"fmt"
	"log"
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

// getCgroupMemoryLimit reads the container's cgroup memory limit.
// Returns the limit in bytes, or 0 if not running in a cgroup or the limit is "max" (unlimited).
func getCgroupMemoryLimit() int64 {
	// Try cgroup v2 first: /sys/fs/cgroup/memory.max
	if data, err := os.ReadFile("/sys/fs/cgroup/memory.max"); err == nil {
		trimmed := strings.TrimSpace(string(data))
		log.Printf("[getCgroupMemoryLimit] cgroup v2 /sys/fs/cgroup/memory.max = %q", trimmed)
		if trimmed != "max" { // "max" means no limit
			if limit, err := strconv.ParseInt(trimmed, 10, 64); err == nil && limit > 0 {
				log.Printf("[getCgroupMemoryLimit] Using cgroup v2 limit: %d bytes (%.2f GB)", limit, float64(limit)/(1024*1024*1024))
				return limit
			}
		}
	} else {
		log.Printf("[getCgroupMemoryLimit] cgroup v2 /sys/fs/cgroup/memory.max not available: %v", err)
	}

	// Fall back to cgroup v1: /sys/fs/cgroup/memory/memory.limit_in_bytes
	if data, err := os.ReadFile("/sys/fs/cgroup/memory/memory.limit_in_bytes"); err == nil {
		trimmed := strings.TrimSpace(string(data))
		log.Printf("[getCgroupMemoryLimit] cgroup v1 /sys/fs/cgroup/memory/memory.limit_in_bytes = %q", trimmed)
		if limit, err := strconv.ParseInt(trimmed, 10, 64); err == nil && limit > 0 {
			// cgroup v1 reports a very large number (close to max int64) when unlimited
			// Treat values > 1 exabyte as "no limit"
			const oneExabyte = int64(1) << 60
			if limit < oneExabyte {
				log.Printf("[getCgroupMemoryLimit] Using cgroup v1 limit: %d bytes (%.2f GB)", limit, float64(limit)/(1024*1024*1024))
				return limit
			}
			log.Printf("[getCgroupMemoryLimit] cgroup v1 value %d exceeds 1 exabyte, treating as unlimited", limit)
		}
	} else {
		log.Printf("[getCgroupMemoryLimit] cgroup v1 /sys/fs/cgroup/memory/memory.limit_in_bytes not available: %v", err)
	}

	log.Printf("[getCgroupMemoryLimit] No cgroup memory limit found, returning 0")
	return 0
}

// getTotalPhysicalMemoryInternal performs the actual memory reading operation.
// It checks cgroup memory limits first (for container environments), then falls back
// to /proc/meminfo. Returns the minimum of the two if both are available.
func getTotalPhysicalMemoryInternal() (int64, error) {
	// Read host memory from /proc/meminfo
	hostMemory, err := getHostMemoryFromProcMeminfo()
	if err != nil {
		return 0, err
	}
	log.Printf("[getTotalPhysicalMemoryInternal] Host memory from /proc/meminfo: %d bytes (%.2f GB)", hostMemory, float64(hostMemory)/(1024*1024*1024))

	// Check cgroup limit (container memory)
	cgroupLimit := getCgroupMemoryLimit()
	log.Printf("[getTotalPhysicalMemoryInternal] Cgroup memory limit: %d bytes (%.2f GB)", cgroupLimit, float64(cgroupLimit)/(1024*1024*1024))

	if cgroupLimit > 0 && cgroupLimit < hostMemory {
		log.Printf("[getTotalPhysicalMemoryInternal] Using CGROUP limit (container memory): %d bytes (%.2f GB)", cgroupLimit, float64(cgroupLimit)/(1024*1024*1024))
		return cgroupLimit, nil
	}

	log.Printf("[getTotalPhysicalMemoryInternal] Using HOST memory (no cgroup limit or cgroup >= host): %d bytes (%.2f GB)", hostMemory, float64(hostMemory)/(1024*1024*1024))
	return hostMemory, nil
}

// getHostMemoryFromProcMeminfo reads MemTotal from /proc/meminfo
func getHostMemoryFromProcMeminfo() (int64, error) {
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
