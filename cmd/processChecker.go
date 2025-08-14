package cmd

import (
	"fmt"
	"github.com/Azure/azure-storage-azcopy/v10/common"
	"os"
	"path"
)

// WarnMultipleProcesses warns if there are multiple AzCopy processes running
func WarnMultipleProcesses(directory string, currentPid int) {
	currPidFileName := fmt.Sprintf("%d.pid", currentPid)
	// Made subdir to not clog main dir
	pidsSubDir := path.Join(directory, "pids")
	err := os.MkdirAll(pidsSubDir, 0755)
	if err != nil {
		return
	}
	f, err := os.Open(pidsSubDir)
	if err != nil {
		return
	}
	defer f.Close()
	_, err = f.Readdirnames(1)
	if err == nil {
		glcm.OnWarning(common.ERR_MULTIPLE_PROCESSES)
	}
	pidFilePath := path.Join(pidsSubDir, currPidFileName) // E.g "\.azcopy\pids\\XXX.pid"
	// Creates .pid file with specific pid
	pidFile, err := os.OpenFile(pidFilePath, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0644)
	if err != nil {
		return
	}
	defer pidFile.Close()
	glcm.RegisterCloseFunc(func() {
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
