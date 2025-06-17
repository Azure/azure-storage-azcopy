package cmd

import (
	"fmt"
	"os"
	"path"
	"regexp"
	"strconv"
)

// WarnMultipleProcesses warns if there are multiple AzCopy processes running
func WarnMultipleProcesses(directory string, currentPid int) error {
	currPidFileName := strconv.Itoa(currentPid) + ".pid"

	pidsSubDir := path.Join(directory, "pids") // Made subdir to not clog main dir
	if err := os.MkdirAll(pidsSubDir, 0755); err != nil {
		glcm.Error(fmt.Sprintf("error creating pids dir: %v", err))
	}
	filePath := path.Join(pidsSubDir, currPidFileName) // E.g "\.azcopy\pids\\XXX.pid"
	// Creates .pid file with specific pid
	file, err := os.OpenFile(filePath, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0644)
	if err != nil {
		glcm.Error(fmt.Sprintf("error creating the .pid file: %v", err))
		return err
	}
	defer file.Close()

	// Check if there is a matching .pid file in directory
	pidFileRegex, _ := regexp.Compile(`\.pid$`)
	dir, err := os.ReadDir(pidsSubDir)
	if err != nil {
		glcm.Error(fmt.Sprintf("error reading dir: %v", err))
		return err
	}
	for _, fileName := range dir {
		if matched := pidFileRegex.MatchString(fileName.Name()); matched {
			if fileName.Name() != currPidFileName {
				glcm.Warn("More than one AzCopy process is running. It is best practice to run a single process per VM.")
				return nil
			}
		}
	}
	return nil
}

// CleanUpPidFile removes the PID files after the AzCopy run has exited. Ensures we only check against
// active Azcopy instances
func CleanUpPidFile(directory string, processID int) {
	currPidFileName := strconv.Itoa(processID) + ".pid"
	pidSubDir := path.Join(directory, "pids")
	pidFilePath := path.Join(pidSubDir, currPidFileName)

	if err := os.Remove(pidFilePath); err != nil {
		glcm.Error(fmt.Sprintf("error removing the %v file after exiting", pidFilePath))
	}
}
