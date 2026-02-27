// Copyright © Microsoft <wastore@microsoft.com>
//
// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files (the "Software"), to deal
// in the Software without restriction, including without limitation the rights
// to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
// copies of the Software, and to permit persons to whom the Software is
// furnished to do so, subject to the following conditions:
//
// The above copyright notice and this permission notice shall be included in
// all copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
// THE SOFTWARE.

package cmd

import (
	"github.com/Azure/azure-storage-azcopy/v10/common"
	"github.com/Azure/azure-storage-azcopy/v10/jobsAdmin"
	"github.com/stretchr/testify/assert"
	"io/fs"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestSyncUploadWithExcludeAttrFlag(t *testing.T) {
	a := assert.New(t)
	bsc := getBlobServiceClient()

	srcDirName := scenarioHelper{}.generateLocalDirectory(a)
	defer os.RemoveAll(srcDirName)
	fileList := scenarioHelper{}.generateCommonRemoteScenarioForLocal(a, srcDirName, "")

	// add special files with attributes that we wish to exclude
	filesToExclude := []string{"file1.pdf", "file2.txt", "file3"}
	scenarioHelper{}.generateLocalFilesFromList(a, srcDirName, filesToExclude)
	attrList := []string{"H", "I", "C"}
	excludeAttrsStr := "H;I;S"
	scenarioHelper{}.setAttributesForLocalFiles(a, srcDirName, filesToExclude, attrList)

	// set up the destination as an empty container
	cc, containerName := createNewContainer(a, bsc)
	defer deleteContainer(a, cc)

	// set up interceptor
	mockedRPC := interceptor{}
	jobsAdmin.ExecuteNewCopyJobPartOrder = func(order common.CopyJobPartOrderRequest) common.CopyJobPartOrderResponse {
		return mockedRPC.intercept(order)
	}
	mockedRPC.init()

	rawContainerURLWithSAS := scenarioHelper{}.getRawContainerURLWithSAS(a, containerName)
	raw := getDefaultSyncRawInput(srcDirName, rawContainerURLWithSAS.String())
	raw.excludeFileAttributes = excludeAttrsStr

	runSyncAndVerify(a, raw, func(err error) {
		a.Nil(err)
		validateUploadTransfersAreScheduled(a, "", "", fileList, mockedRPC)
	})
}

func TestSyncUploadWithIncludeAttrFlag(t *testing.T) {
	a := assert.New(t)
	bsc := getBlobServiceClient()

	srcDirName := scenarioHelper{}.generateLocalDirectory(a)
	defer os.RemoveAll(srcDirName)
	scenarioHelper{}.generateCommonRemoteScenarioForLocal(a, srcDirName, "")

	// add special files with attributes that we wish to include
	filesToInclude := []string{"file1.txt", "file2.pdf", "file3.pdf"}
	scenarioHelper{}.generateLocalFilesFromList(a, srcDirName, filesToInclude)
	attrList := []string{"H", "I", "C"}
	includeAttrsStr := "H;I;S"
	scenarioHelper{}.setAttributesForLocalFiles(a, srcDirName, filesToInclude, attrList)

	// set up the destination as an empty container
	cc, containerName := createNewContainer(a, bsc)
	defer deleteContainer(a, cc)

	// set up interceptor
	mockedRPC := interceptor{}
	jobsAdmin.ExecuteNewCopyJobPartOrder = func(order common.CopyJobPartOrderRequest) common.CopyJobPartOrderResponse {
		return mockedRPC.intercept(order)
	}
	mockedRPC.init()

	rawContainerURLWithSAS := scenarioHelper{}.getRawContainerURLWithSAS(a, containerName)
	raw := getDefaultSyncRawInput(srcDirName, rawContainerURLWithSAS.String())
	raw.includeFileAttributes = includeAttrsStr

	runSyncAndVerify(a, raw, func(err error) {
		a.Nil(err)
		validateUploadTransfersAreScheduled(a, "", "", filesToInclude, mockedRPC)
	})
}

// Asserting that name filter and attribute filter are ANDed
// Create one file that matches only the name filter
// Create one file that matches only the attribute filter
// Create one file that matches both
// Only the last file should be transferred
func TestSyncUploadWithIncludeAndIncludeAttrFlags(t *testing.T) {
	a := assert.New(t)
	bsc := getBlobServiceClient()

	srcDirName := scenarioHelper{}.generateLocalDirectory(a)
	defer os.RemoveAll(srcDirName)
	scenarioHelper{}.generateCommonRemoteScenarioForLocal(a, srcDirName, "")

	fileList := []string{"file1.txt", "file2.png", "file3.txt"}
	scenarioHelper{}.generateLocalFilesFromList(a, srcDirName, fileList)
	includeString := "*.txt"
	includeAttrsStr := "H;I;S"
	attrList := []string{"H", "I", "C"}
	scenarioHelper{}.setAttributesForLocalFiles(a, srcDirName, fileList[1:], attrList)

	cc, containerName := createNewContainer(a, bsc)
	defer deleteContainer(a, cc)

	mockedRPC := interceptor{}
	jobsAdmin.ExecuteNewCopyJobPartOrder = func(order common.CopyJobPartOrderRequest) common.CopyJobPartOrderResponse {
		return mockedRPC.intercept(order)
	}
	mockedRPC.init()

	rawContainerURLWithSAS := scenarioHelper{}.getRawContainerURLWithSAS(a, containerName)
	raw := getDefaultSyncRawInput(srcDirName, rawContainerURLWithSAS.String())
	raw.includeFileAttributes = includeAttrsStr
	raw.include = includeString

	runSyncAndVerify(a, raw, func(err error) {
		a.Nil(err)
		validateUploadTransfersAreScheduled(a, "", "", fileList[2:], mockedRPC)
	})
}

// Asserting that name filter and attribute filter are ANDed
// Create one file that matches only the name filter
// Create one file that matches only the attribute filter
// Create one file that matches both
// None of them should be transferred
func TestSyncUploadWithExcludeAndExcludeAttrFlags(t *testing.T) {
	a := assert.New(t)
	bsc := getBlobServiceClient()

	srcDirName := scenarioHelper{}.generateLocalDirectory(a)
	defer os.RemoveAll(srcDirName)
	commonFileList := scenarioHelper{}.generateCommonRemoteScenarioForLocal(a, srcDirName, "")

	fileList := []string{"file1.bin", "file2.png", "file3.bin"}
	scenarioHelper{}.generateLocalFilesFromList(a, srcDirName, fileList)
	excludeString := "*.bin"
	excludeAttrsStr := "H;I;S"
	attrList := []string{"H", "I", "C"}
	scenarioHelper{}.setAttributesForLocalFiles(a, srcDirName, fileList[1:], attrList)

	cc, containerName := createNewContainer(a, bsc)
	defer deleteContainer(a, cc)

	mockedRPC := interceptor{}
	jobsAdmin.ExecuteNewCopyJobPartOrder = func(order common.CopyJobPartOrderRequest) common.CopyJobPartOrderResponse {
		return mockedRPC.intercept(order)
	}
	mockedRPC.init()

	rawContainerURLWithSAS := scenarioHelper{}.getRawContainerURLWithSAS(a, containerName)
	raw := getDefaultSyncRawInput(srcDirName, rawContainerURLWithSAS.String())
	raw.excludeFileAttributes = excludeAttrsStr
	raw.exclude = excludeString

	runSyncAndVerify(a, raw, func(err error) {
		a.Nil(err)
		validateUploadTransfersAreScheduled(a, "", "", commonFileList, mockedRPC)
	})
}

// mouthfull of a test name, but this ensures that case insensitivity doesn't cause the unintended deletion of files
func TestSyncDownloadWithDeleteDestinationOnCaseInsensitiveFS(t *testing.T) {
	a := assert.New(t)
	bsc := getBlobServiceClient()

	dstDirName := scenarioHelper{}.generateLocalDirectory(a)
	defer os.RemoveAll(dstDirName)
	fileList := []string{"FileWithCaps", "FiLeTwO", "FoOBaRBaZ"}

	cc, containerName := createNewContainer(a, bsc)
	defer deleteContainer(a, cc)

	scenarioHelper{}.generateBlobsFromList(a, cc, fileList, "Hello, World!")

	// let the local files be in the future; we don't want to do _anything_ to them; not delete nor download.
	time.Sleep(time.Second * 5)

	scenarioHelper{}.generateLocalFilesFromList(a, dstDirName, fileList)

	mockedRPC := interceptor{}
	jobsAdmin.ExecuteNewCopyJobPartOrder = func(order common.CopyJobPartOrderRequest) common.CopyJobPartOrderResponse {
		return mockedRPC.intercept(order)
	}
	mockedRPC.init()

	rawContainerURLWithSAS := scenarioHelper{}.getRawContainerURLWithSAS(a, containerName)
	raw := getDefaultSyncRawInput(rawContainerURLWithSAS.String(), dstDirName)
	raw.recursive = true
	raw.deleteDestination = "true"

	runSyncAndVerify(a, raw, func(err error) {
		// It should not have deleted them
		seenFiles := make(map[string]bool)
		filepath.Walk(dstDirName, func(path string, info fs.FileInfo, err error) error {
			if path == dstDirName {
				return nil
			}

			seenFiles[filepath.Base(path)] = true
			return nil
		})

		a.Equal(len(fileList), len(seenFiles))
		for _, v := range fileList {
			a.True(seenFiles[v])
		}

		// It should not have downloaded them
		a.Zero(len(mockedRPC.transfers))
	})
}
