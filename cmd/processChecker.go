package cmd

import (
	"fmt"
	"github.com/Azure/azure-storage-azcopy/v10/common"
	"io"
	"os"
	"path"
)

// WarnMultipleProcesses warns if there are multiple AzCopy processes running
func WarnMultipleProcesses(directory string, currentPid int) {
	go func() {
		currPidFileName := fmt.Sprintf("%d.pid", currentPid)
		// Made subdir to not clog main dir
		pidsSubDir := path.Join(directory, "pids")
		err := os.MkdirAll(pidsSubDir, 0755)
		if err != nil {
			return
		}
		filePath := path.Join(pidsSubDir, currPidFileName) // E.g "\.azcopy\pids\\XXX.pid"

		f, err := os.Open(pidsSubDir)
		if err != nil {
			return
		}
		defer f.Close()
		var names []string
		names, err = f.Readdirnames(2) // read up to two names
		if err != nil && err != io.EOF {
			return
		}
		if len(names) > 1 {
			glcm.Warn(common.ERR_MULTIPLE_PROCESSES)
		}
		// Creates .pid file with specific pid
		file, _ := os.OpenFile(filePath, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0644)
		defer func(file *os.File) {
			err := file.Close()
			if err != nil {

			}
		}(file)
	}()
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
