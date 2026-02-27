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
	a.Equal(2, len(dirEntry), "Should contain 2 .pid files")
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
