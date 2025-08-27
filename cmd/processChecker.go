package cmd

import (
	"fmt"
	"github.com/Azure/azure-storage-azcopy/v10/common"
	"os"
	"path"
	"runtime"
	"strconv"
	"strings"
	"syscall"
)

// isProcessRunning checks if a process with the given PID is running
func isProcessRunning(pid int) bool {
	// In Unix, this can falsely return a Process for pid even when it does not exist
	// We perform a signal check to test
	process, err := os.FindProcess(pid)
	if err != nil {
		return false
	}
	if runtime.GOOS == "windows" {
		return true
	}
	// In Unix, we need to check whether the process actually exists
	err = process.Signal(syscall.Signal(0))
	return err == nil
}

// cleanupStalePidFiles removes PID files for processes that are no longer running
func cleanupStalePidFiles(pidsSubDir string, currentPid int) error {
	f, err := os.Open(pidsSubDir)
	if err != nil {
		return err
	}
	defer f.Close()

	fileNames, err := f.Readdirnames(-1) // Read all filenames
	if err != nil {
		return err
	}

	for _, fileName := range fileNames {
		// Extract PID from filename
		pidStr := strings.TrimSuffix(fileName, ".pid")
		pid, err := strconv.Atoi(pidStr)
		if err != nil {
			// Not a valid PID, remove the file
			os.Remove(path.Join(pidsSubDir, fileName))
			continue
		}
		if pid == currentPid { // Skip current process
			continue
		}
		if !isProcessRunning(pid) {
			// Process is not running, remove the stale PID file
			os.Remove(path.Join(pidsSubDir, fileName))
		}
	}
	return nil
}

// WarnMultipleProcesses warns if there are multiple AzCopy processes running
func WarnMultipleProcesses(directory string, currentPid int) {
	currPidFileName := fmt.Sprintf("%d.pid", currentPid)
	pidsSubDir := path.Join(directory, "pids") // Made subdir to not clog main dir
	err := os.MkdirAll(pidsSubDir, 0755)
	if err != nil {
		return
	}
	cleanupStalePidFiles(pidsSubDir, currentPid) // Clean up inactive PID files
	f, err := os.Open(pidsSubDir)
	if err != nil {
		return
	}
	defer f.Close()
	// Check if there is more than one pid file
	_, err = f.Readdirnames(1)
	if err == nil { // EOF err if there is only one file
		glcm.Warn(common.ERR_MULTIPLE_PROCESSES)
	}
	pidFilePath := path.Join(pidsSubDir, currPidFileName) // E.g "\.azcopy\pids\\XXX.pid"
	// Creates .pid file with specific pid
	pidFile, err := os.OpenFile(pidFilePath, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0644)
	if err != nil {
		return
	}
	defer pidFile.Close()
	glcm.RegisterCloseFunc(func() { // Should also handle scenarios not exit cleanly
		err = os.Remove(pidFilePath)
		if err != nil {
			return
		}
	})
}

// AsyncWarnMultipleProcesses warns if there are multiple AzCopy processes running
func AsyncWarnMultipleProcesses(directory string, currentPid int) {
	go func() {
		WarnMultipleProcesses(directory, currentPid)
	}()
}
