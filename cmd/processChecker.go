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

// isProcessRunning checks if a process with the given PID is running.
// It is part of the cleanup of pids dir
// This will ensure we only warn about multiple *active* processes and not just the presence of pid file.
func isProcessRunning(pid int) bool {
	process, err := os.FindProcess(pid)
	if err != nil {
		return false
	}
	if runtime.GOOS == "windows" {
		return true // If it passes err handling above, the process is active.
	}
	// In Unix, we need to check whether the process actually exists
	// os.FindProcess falsely returns a Process for pid even when it does not exist
	// We perform a signal check to test
	// https://go.dev/pkg/os/?m=all,old#FindProcess
	err = process.Signal(syscall.Signal(0))
	return err == nil
}

// cleanupStalePidFiles removes PID files for processes that are no longer running
func cleanupStalePidFiles(pidsSubDir string, currentPid int) error {
	f, err := os.Open(pidsSubDir)
	if err != nil {
		glcm.Info("failed to open pids dir" + err.Error())
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
			err := os.Remove(path.Join(pidsSubDir, fileName))
			if err != nil {
				glcm.Info("failed to remove stale pid file" + err.Error())
				return err
			}
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
		glcm.Info("failed to make pids dir" + err.Error())
		return
	}
	err = cleanupStalePidFiles(pidsSubDir, currentPid) // First, clean up inactive PID files
	if err != nil {
		glcm.Info("failed to clean up stale pids" + err.Error())
		return
	}

	f, err := os.Open(pidsSubDir)
	if err != nil {
		glcm.Info("failed to open pids subdir" + err.Error())
		return
	}
	defer f.Close()

	// Check if there is more than one pid file
	_, err = f.Readdirnames(1)
	if err == nil { // nil check works here, there will be EOF err if only one file
		glcm.Info(common.WARN_MULTIPLE_PROCESSES)
	}
	pidFilePath := path.Join(pidsSubDir, currPidFileName) // E.g "\.azcopy\pids\\XXX.pid"
	// Creates .pid file with specific pid
	pidFile, err := os.OpenFile(pidFilePath, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0644)
	if err != nil {
		glcm.Info("failed to create pid file" + err.Error())
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
// We log errors with info because this warning check should not be show-stopping
func AsyncWarnMultipleProcesses(directory string, currentPid int) {
	go func() {
		WarnMultipleProcesses(directory, currentPid)
	}()
}
