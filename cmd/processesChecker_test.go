package cmd

import (
	"os"
	"path"
	"testing"

	"github.com/Azure/azure-storage-azcopy/v10/common"
	"github.com/stretchr/testify/assert"
)

func Test_WarnMultipleProcesses(t *testing.T) {
	a := assert.New(t)
	err := os.Mkdir("temp", 0777) // Temp dir to simulate .azcopy dir
	pidsDir := path.Join("temp", "pids")
	defer func() {
		err := os.RemoveAll("temp")
		if err != nil {
			return
		}
	}() // Cleanup
	a.NoError(err)

	WarnMultipleProcesses("temp", 1)

	pid1 := path.Join(pidsDir, "1.pid")
	_, err = os.Stat(pid1)
	a.NoError(err, "first .pid file should exist")

	// Act
	WarnMultipleProcesses("temp", 2) // Additional AzCopy process

	dirEntry, _ := os.ReadDir(pidsDir)
	// Check only one file
	a.Equal(1, len(dirEntry), "Should contain 1 .pid file")
}

func Test_CleanUpPidFiles(t *testing.T) {
	// Arrange
	a := assert.New(t)
	t.Cleanup(func() {
		_ = os.RemoveAll("temp1")
	})
	err := os.Mkdir("temp1", 0777) // Temp dir to represent .azcopy dir
	a.NoError(err)
	pidsDir := path.Join("temp1", "pids")
	err1 := os.MkdirAll(pidsDir, 0777)
	a.NoError(err1)

	// Create the pid files
	pid1path := path.Join(pidsDir, "1.pid")
	pid1, err := os.Create(pid1path)
	a.NoError(err, "first pid file should be created")

	pid2path := path.Join(pidsDir, "2.pid")
	pid2, err := os.Create(pid2path)
	a.NoError(err, "second pid file should be created")

	// Close files before performing cleanup
	err = pid1.Close()
	if err != nil {
		return
	}
	err = pid2.Close()
	if err != nil {
		return
	}

	// Act & Assert
	CleanUpPidFile("temp1", 1)
	_, err = os.Stat(pid1path)
	a.True(os.IsNotExist(err))
	dir, _ := os.ReadDir(pidsDir)
	a.Equal(1, len(dir), "expected one PID file remaining")

	CleanUpPidFile("temp1", 2)
	_, err = os.Stat(pid2path)
	a.True(os.IsNotExist(err))
	dir, _ = os.ReadDir(pidsDir)
	a.Equal(0, len(dir), "expected no PID files remaining")
}

// Test_MultipleProcessWithMockedLCM validates warn messages are logged when there's multiple AzCopy instances
func Test_MultipleProcessWithMockedLCM(t *testing.T) {
	a := assert.New(t)

	// Arrange
	tempDir, err := os.MkdirTemp("", "temp")
	a.NoError(err)
	defer func(path string) { // Cleanup temp dir
		err := os.RemoveAll(path)
		if err != nil {
			return
		}
	}(tempDir)

	pidsDir := path.Join(tempDir, "pids")
	err = os.MkdirAll(pidsDir, 0777)
	a.NoError(err)
	fakePidPath := path.Join(pidsDir, "123.pid") // Simulate multiple process
	fakePidFile, err := os.Create(fakePidPath)
	a.NoError(err)
	err = fakePidFile.Close()
	if err != nil {
		return
	}

	// set up interceptor
	mockedRPC := interceptor{}
	Rpc = mockedRPC.intercept
	mockLCM := mockedLifecycleManager{warnLog: make(chan string, 50)}
	mockLCM.SetOutputFormat(common.EOutputFormat.Text()) // text format
	mockedRPC.init()
	glcm = &mockLCM

	// Act
	WarnMultipleProcesses(tempDir, 456)

	// Assert
	errorMessages := mockLCM.GatherAllLogs(mockLCM.warnLog) // check mocked LCM warnLogs
	if errorMessages != nil {
		a.Equal(common.ERR_MULTIPLE_PROCESSES, errorMessages[0])
	}
}
