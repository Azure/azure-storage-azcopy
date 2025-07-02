package e2etest

import (
	"github.com/Azure/azure-storage-azcopy/v10/cmd"
	"github.com/Azure/azure-storage-azcopy/v10/common"
	"github.com/stretchr/testify/assert"
	"os"
	"path"
	"testing"
)

func Test_WarnMultipleProcesses(t *testing.T) {
	a := assert.New(t)
	err := os.Mkdir("temp", 0777) // Temp dir to simulate .azcopy dir
	pidsDir := path.Join("temp", "pids")
	defer os.RemoveAll("temp") // Cleanup
	a.NoError(err)

	err1 := cmd.WarnMultipleProcesses("temp", 1)
	a.NoError(err1)

	pid1 := path.Join(pidsDir, "1.pid")
	_, err = os.Stat(pid1)
	a.NoError(err, "first .pid file should exist")

	// Act
	err2 := cmd.WarnMultipleProcesses("temp", 2) // Additional AzCopy process, err message
	a.EqualError(err2, common.ERR_MULTIPLE_PROCESSES)
	a.NotNil(err2)

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
	pid1.Close()
	pid2.Close()

	// Act & Assert
	cmd.CleanUpPidFile("temp1", 1)
	_, err = os.Stat(pid1path)
	a.True(os.IsNotExist(err))
	dir, _ := os.ReadDir(pidsDir)
	a.Equal(1, len(dir), "expected one PID file remaining")

	cmd.CleanUpPidFile("temp1", 2)
	_, err = os.Stat(pid2path)
	a.True(os.IsNotExist(err))
	dir, _ = os.ReadDir(pidsDir)
	a.Equal(0, len(dir), "expected no PID files remaining")
}
