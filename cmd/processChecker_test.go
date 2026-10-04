package cmd

import (
	"fmt"
	"os"
	"path"
	"testing"

	"github.com/Azure/azure-storage-azcopy/v10/common"
	"github.com/stretchr/testify/assert"
)

func processCheckerTestDirectory(t *testing.T) string {
	t.Helper()
	directory, err := os.MkdirTemp(".", "process-checker-")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		assert.NoError(t, os.RemoveAll(directory))
	})
	return directory
}

func Test_WarnMultipleProcesses(t *testing.T) {
	a := assert.New(t)
	directory := processCheckerTestDirectory(t)
	pidsDir := path.Join(directory, "pids")
	currentPid := os.Getpid()
	mockLCM := mockedLifecycleManager{infoLog: make(chan string, 50)}
	originalLCM := glcm
	glcm = &mockLCM
	t.Cleanup(func() { glcm = originalLCM })

	WarnMultipleProcesses(directory, currentPid)

	currentPidPath := path.Join(pidsDir, fmt.Sprintf("%d.pid", currentPid))
	_, err := os.Stat(currentPidPath)
	a.NoError(err, "first .pid file should exist")
	a.NoError(os.WriteFile(path.Join(pidsDir, "0.pid"), nil, 0644))

	WarnMultipleProcesses(directory, currentPid)

	dirEntry, err := os.ReadDir(pidsDir)
	a.NoError(err)
	a.Len(dirEntry, 1, "Should contain only the current process's .pid file")
	a.Empty(mockLCM.GatherAllLogs(mockLCM.infoLog), "The current or stale process must not trigger a warning")
}

// Test_MultipleProcessWithMockedLCM validates warn messages are logged when there's multiple AzCopy instances
func Test_MultipleProcessWithMockedLCM(t *testing.T) {
	a := assert.New(t)

	directory := processCheckerTestDirectory(t)
	pidsDir := path.Join(directory, "pids")
	err := os.MkdirAll(pidsDir, 0777)
	a.NoError(err)

	otherPid := os.Getppid()
	if !isProcessRunning(otherPid) {
		t.Skip("parent process is not available")
	}
	otherPidPath := path.Join(pidsDir, fmt.Sprintf("%d.pid", otherPid))
	a.NoError(os.WriteFile(otherPidPath, nil, 0644))

	mockLCM := mockedLifecycleManager{infoLog: make(chan string, 50)}
	mockLCM.SetOutputFormat(common.EOutputFormat.Text()) // text format
	originalLCM := glcm
	glcm = &mockLCM
	t.Cleanup(func() { glcm = originalLCM })

	// Act
	WarnMultipleProcesses(directory, os.Getpid())

	// Assert
	a.Equal([]string{common.WARN_MULTIPLE_PROCESSES}, mockLCM.GatherAllLogs(mockLCM.infoLog))
}

func Test_CleanUpStalePids(t *testing.T) {
	a := assert.New(t)

	directory := processCheckerTestDirectory(t)
	pidsDir := path.Join(directory, "pids")
	err := os.MkdirAll(pidsDir, 0777)
	a.NoError(err)

	currentPidFile := fmt.Sprintf("%d.pid", os.Getpid())
	for _, fileName := range []string{"0.pid", "-1.pid", "invalid.pid", currentPidFile} {
		a.NoError(os.WriteFile(path.Join(pidsDir, fileName), nil, 0644))
	}

	// Act
	a.NoError(cleanupStalePidFiles(pidsDir, os.Getpid()))

	dirEntry, err := os.ReadDir(pidsDir)
	a.NoError(err)
	if a.Len(dirEntry, 1, "Should remove stale and invalid PID files but retain the current process") {
		a.Equal(currentPidFile, dirEntry[0].Name())
	}
}

func Test_IsProcessRunning(t *testing.T) {
	a := assert.New(t)
	a.True(isProcessRunning(os.Getpid()))
	a.False(isProcessRunning(0))
	a.False(isProcessRunning(-1))
}
