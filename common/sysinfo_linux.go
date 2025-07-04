//go:build linux || darwin
// +build linux darwin

package common

import (
	"bufio"
	"fmt"
	"os/exec"
	"strconv"
	"strings"

	"golang.org/x/sys/unix"
)

// Retrieves the available memory on a Linux system by reading and parsing the /proc/meminfo file.
// Utilizes the MemAvailable field introduced in Linux kernel version 3.14 and above.
// Includes a TODO to implement a rough estimate for systems running on kernel versions prior to 3.14 where MemAvailable is not available.
func GetMemAvailable() (int64, error) {

	// command to get the Available Memory
	cmdStr := `cat /proc/meminfo`
	cmd := exec.Command("sh", "-c", cmdStr)

	stdOut, err := cmd.StdoutPipe()
	if err != nil {
		err := fmt.Errorf("GetMemAvailable failed with error: %v", err)
		return 0, err
	}

	// Flow will be like this, set the scanner to stdOut and start the cmd.
	scanner := bufio.NewScanner(stdOut)
	err = cmd.Start()
	if err != nil {
		err := fmt.Errorf("GetMemAvailable failed with error: %v", err)
		return 0, err
	}

	// Set the split function for the scanning operation.
	scanner.Split(bufio.ScanLines)

	// Scan the stdOutput.
	for scanner.Scan() {
		if strings.HasPrefix(scanner.Text(), "MemAvailable") {
			var multiplier int64
			var result int
			tokens := strings.Fields(scanner.Text())
			if len(tokens) != 3 {
				err := fmt.Errorf("GetMemAvailable invalid ouput[%s] of /proc/meminfo", scanner.Text())
				return 0, err
			}

			value := tokens[1]
			multiplerStr := tokens[2]

			if multiplerStr == "kB" {
				multiplier = 1024
			} else {
				// "/proc/meminfo" output always in kB only. If we are getting different string, something wrong.
				err := fmt.Errorf("MemAvailable value is not in kB, output[%s]", scanner.Text())
				return 0, err
			}

			if result, err = strconv.Atoi(value); err != nil {
				return 0, err
			}
			return int64(result) * int64(multiplier), nil
		}
	}

	if err = cmd.Wait(); err != nil {
		err := fmt.Errorf("GetMemAvailable failed with error: %v", err)
		return 0, err
	}

	// If we reached here, means MemAvailable entry not found in cat /proc/meminfo.
	var kernelVersion unix.Utsname
	_ = unix.Uname(&kernelVersion)

	err = fmt.Errorf(fmt.Sprintf("MemAvailable entry not found, kernel version: %+v", kernelVersion))
	return 0, err
}

// Retrieves the total physical memory on a Linux system by reading and parsing the /proc/meminfo file.
// Returns total physical memory in bytes.
func GetTotalPhysicalMemory() (int64, error) {
	// command to get the Total Memory
	cmdStr := `cat /proc/meminfo`
	cmd := exec.Command("sh", "-c", cmdStr)

	stdOut, err := cmd.StdoutPipe()
	if err != nil {
		err := fmt.Errorf("GetTotalPhysicalMemory failed with error: %v", err)
		return 0, err
	}

	// Flow will be like this, set the scanner to stdOut and start the cmd.
	scanner := bufio.NewScanner(stdOut)
	err = cmd.Start()
	if err != nil {
		err := fmt.Errorf("GetTotalPhysicalMemory failed with error: %v", err)
		return 0, err
	}

	// Set the split function for the scanning operation.
	scanner.Split(bufio.ScanLines)

	// Scan the stdOutput.
	for scanner.Scan() {
		if strings.HasPrefix(scanner.Text(), "MemTotal") {
			var multiplier int64
			var result int
			tokens := strings.Fields(scanner.Text())
			if len(tokens) != 3 {
				err := fmt.Errorf("GetTotalPhysicalMemory invalid ouput[%s] of /proc/meminfo", scanner.Text())
				return 0, err
			}

			value := tokens[1]
			multiplerStr := tokens[2]

			if multiplerStr == "kB" {
				multiplier = 1024
			} else {
				// "/proc/meminfo" output always in kB only. If we are getting different string, something wrong.
				err := fmt.Errorf("MemTotal value is not in kB, output[%s]", scanner.Text())
				return 0, err
			}

			if result, err = strconv.Atoi(value); err != nil {
				return 0, err
			}
			return int64(result) * int64(multiplier), nil
		}
	}

	if err = cmd.Wait(); err != nil {
		err := fmt.Errorf("GetTotalPhysicalMemory failed with error: %v", err)
		return 0, err
	}

	// If we reached here, means MemTotal entry not found in cat /proc/meminfo.
	var kernelVersion unix.Utsname
	_ = unix.Uname(&kernelVersion)

	err = fmt.Errorf(fmt.Sprintf("MemTotal entry not found, kernel version: %+v", kernelVersion))
	return 0, err
}
