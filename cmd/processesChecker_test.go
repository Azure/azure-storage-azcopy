package cmd

import (
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

	err1 := WarnMultipleProcesses("temp", 1)
	a.NoError(err1)

	pid1 := path.Join(pidsDir, "1.pid")
	_, err = os.Stat(pid1)
	a.NoError(err, "first .pid file should exist")

	// Act
	err2 := WarnMultipleProcesses("temp", 2) // Additional AzCopy process, warning should output
	a.NoError(err2)
	a.Nil(err2)

	pid2 := path.Join(pidsDir, "2.pid")
	_, err = os.Stat(pid2)
	a.NoError(err, "second .pid file should exist")

	// Check only two files exist
	files, err := os.ReadDir(pidsDir)
	a.NoError(err)
	count := 0
	for _, _ = range files {
		count++
	}
	a.Equal(2, count, "Should contain 2 .pid files")
}
