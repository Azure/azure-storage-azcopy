package cmd

import (
	"fmt"
	"os"
	"path"
)

// WarnMultipleProcesses warns if there are multiple AzCopy processes running
func WarnMultipleProcesses(directory string, currentPid int) error {
	currPidFileName := fmt.Sprintf("%d.pid", currentPid)
	// Made subdir to not clog main dir
	pidsSubDir := path.Join(directory, "pids")
	if err := os.MkdirAll(pidsSubDir, 0755); err != nil {
		glcm.Error(fmt.Sprintf("error creating pids dir: %v", err))
	}
	filePath := path.Join(pidsSubDir, currPidFileName) // E.g "\.azcopy\pids\\XXX.pid"

	dir, err := os.ReadDir(pidsSubDir)
	if err != nil {
		glcm.Error(fmt.Sprintf("error reading dir: %v", err))
		return err
	}
	for _, fileName := range dir {
		if fileName.Name() != currPidFileName {
			return fmt.Errorf("%w", ErrMultipleProcesses)
		}
	}
	// Creates .pid file with specific pid
	file, err := os.OpenFile(filePath, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0644)
	if err != nil {
		glcm.Error(fmt.Sprintf("error creating the .pid file: %v", err))
		return err
	}
	defer file.Close()
	return nil
}

// CleanUpPidFile removes the PID files after the AzCopy run has exited. Ensures we only check against
// active Azcopy instances
func CleanUpPidFile(directory string, processID int) {
	currPidFileName := fmt.Sprintf("%d.pid", processID)
	pidSubDir := path.Join(directory, "pids")
	pidFilePath := path.Join(pidSubDir, currPidFileName)

	if err := os.Remove(pidFilePath); err != nil {
		glcm.Error(fmt.Sprintf("%v error removing the %v file after exiting", err, pidFilePath))
	}
}
