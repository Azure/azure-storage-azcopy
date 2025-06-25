package cmd

import (
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

	err1 := WarnMultipleProcesses("temp", 1)
	a.NoError(err1)

	pid1 := path.Join(pidsDir, "1.pid")
	_, err = os.Stat(pid1)
	a.NoError(err, "first .pid file should exist")

	// Act
	err2 := WarnMultipleProcesses("temp", 2) // Additional AzCopy process, err message
	a.EqualError(err2, common.ERR_MULTIPLE_PROCESSES)
	a.NotNil(err2)

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

//func TestMockedWarnMultipleProcess(t *testing.T) {
//	a := assert.New(t)
//	bsc := getBlobServiceClient()
//	cc, containerName := createNewContainer(a, bsc)
//	defer deleteContainer(a, cc)
//
//	// set up the container with blobs
//	blobName := []string{"sub1/test/testing.txt", "sub1/test/testing2.txt"}
//	scenarioHelper{}.generateBlobsFromList(a, cc, blobName, blockBlobDefaultData)
//	a.NotNil(cc)
//
//	// set up interceptor
//	mockedRPC := interceptor{}
//	Rpc = mockedRPC.intercept
//	mockedLcm := mockedLifecycleManager{ // Set up mocked lifecycle manager to capture warning
//		infoLog: make(chan string, 50),
//		warnLog: make(chan string, 50),
//	}
//	mockedLcm.SetOutputFormat(common.EOutputFormat.Text())
//	glcm = &mockedLcm
//
//	// construct the raw input to simulate user input
//	rawBlobURLWithSAS := scenarioHelper{}.getRawBlobURLWithSAS(a, containerName, blobName[0])
//	raw := getDefaultRemoveRawInput(rawBlobURLWithSAS.String())
//	raw.recursive = true
//
//	runCopyAndVerify(a, raw, func(err error) {
//		runCopyAndVerify(a, raw, func(err error) {
//			warnMessages := mockedLcm.GatherAllLogs(mockedLcm.warnLog)
//			a.Equal("more than one AzCopy process is running", warnMessages)
//			a.True(len(warnMessages) > 0)
//
//		})
//	})
//
//}
