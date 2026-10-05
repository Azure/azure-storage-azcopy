// Copyright © 2017 Microsoft <wastore@microsoft.com>
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
	"context"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/Azure/azure-storage-azcopy/v10/jobsAdmin"
	"github.com/stretchr/testify/assert"

	"github.com/Azure/azure-storage-azcopy/v10/common"
)

// regular file->blob sync
func TestSyncUploadWithSingleFile(t *testing.T) {
	a := assert.New(t)
	bsc := getBlobServiceClient()
	cc, containerName := createNewContainer(a, bsc)
	defer deleteContainer(a, cc)

	for _, srcFileName := range []string{"singlefileisbest", "打麻将.txt", "%4509%4254$85140&"} {
		// set up the source as a single file
		srcDirName := scenarioHelper{}.generateLocalDirectory(a)
		defer os.RemoveAll(srcDirName)
		fileList := []string{srcFileName}
		scenarioHelper{}.generateLocalFilesFromList(a, srcDirName, fileList)

		// set up the destination container with a single blob
		time.Sleep(time.Second) // later LMT
		dstBlobName := srcFileName
		scenarioHelper{}.generateBlobsFromList(a, cc, []string{dstBlobName}, blockBlobDefaultData)
		a.NotNil(cc)

		// set up interceptor
		mockedRPC := interceptor{}
		mockedRPC.init()

		// construct the raw input to simulate user input
		rawBlobURLWithSAS := scenarioHelper{}.getRawBlobURLWithSAS(a, containerName, dstBlobName)
		raw := getDefaultSyncRawInput(filepath.Join(srcDirName, srcFileName), rawBlobURLWithSAS.String())

		// the blob was created after the file, so no sync should happen
		runSyncAndVerify(a, raw, mockedRPC.intercept, mockedRPC.delete, func(err error) {
			a.Nil(err)

			// validate that the right number of transfers were scheduled
			a.Zero(len(mockedRPC.transfers))
		})

		// recreate the file to have a later last modified time
		time.Sleep(time.Second)
		scenarioHelper{}.generateLocalFilesFromList(a, srcDirName, []string{srcFileName})
		mockedRPC.reset()

		// the file was created after the blob, so the sync should happen
		runSyncAndVerify(a, raw, mockedRPC.intercept, mockedRPC.delete, func(err error) {
			a.Nil(err)

			// if source and destination already point to files, the relative path is an empty string ""
			validateUploadTransfersAreScheduled(a, common.AZCOPY_PATH_SEPARATOR_STRING, common.AZCOPY_PATH_SEPARATOR_STRING, []string{""}, mockedRPC)
		})
	}
}

// regular directory->container sync but destination is empty, so everything has to be transferred
// this test seems to flake out.
func TestSyncUploadWithEmptyDestination(t *testing.T) {
	a := assert.New(t)
	bsc := getBlobServiceClient()

	// set up the source with numerous files
	srcDirName := scenarioHelper{}.generateLocalDirectory(a)
	defer os.RemoveAll(srcDirName)
	fileList := scenarioHelper{}.generateCommonRemoteScenarioForLocal(a, srcDirName, "")
	time.Sleep(time.Second)

	// set up an empty container
	cc, containerName := createNewContainer(a, bsc)
	defer deleteContainer(a, cc)

	// set up interceptor
	mockedRPC := interceptor{}
	mockedRPC.init()

	// construct the raw input to simulate user input
	rawContainerURLWithSAS := scenarioHelper{}.getRawContainerURLWithSAS(a, containerName)
	raw := getDefaultSyncRawInput(srcDirName, rawContainerURLWithSAS.String())

	runSyncAndVerify(a, raw, mockedRPC.intercept, mockedRPC.delete, func(err error) {
		a.Nil(err)

		// validate that the right number of transfers were scheduled
		a.Equal(len(fileList), len(mockedRPC.transfers))

		// validate that the right transfers were sent
		validateUploadTransfersAreScheduled(a, common.AZCOPY_PATH_SEPARATOR_STRING, common.AZCOPY_PATH_SEPARATOR_STRING, fileList, mockedRPC)
	})

	// turn off recursive, this time only top blobs should be transferred
	raw.recursive = false
	mockedRPC.reset()

	runSyncAndVerify(a, raw, mockedRPC.intercept, mockedRPC.delete, func(err error) {
		a.Nil(err)
		a.NotEqual(len(fileList), len(mockedRPC.transfers))

		for _, transfer := range mockedRPC.transfers {
			source := strings.TrimPrefix(transfer.Source, "/")
			a.False(strings.Contains(source, common.AZCOPY_PATH_SEPARATOR_STRING))
		}
	})
}

// regular directory->container sync but destination is identical to the source, transfers are scheduled based on lmt
func TestSyncUploadWithIdenticalDestination(t *testing.T) {
	a := assert.New(t)
	bsc := getBlobServiceClient()

	// set up the source with numerous files
	srcDirName := scenarioHelper{}.generateLocalDirectory(a)
	defer os.RemoveAll(srcDirName)
	fileList := scenarioHelper{}.generateCommonRemoteScenarioForLocal(a, srcDirName, "")

	// set up an the container with the exact same files, but later lmts
	cc, containerName := createNewContainer(a, bsc)
	defer deleteContainer(a, cc)

	// wait for 1 second so that the last modified times of the blobs are guaranteed to be newer
	time.Sleep(time.Second)
	scenarioHelper{}.generateBlobsFromList(a, cc, fileList, blockBlobDefaultData)

	// set up interceptor
	mockedRPC := interceptor{}
	mockedRPC.init()

	// construct the raw input to simulate user input
	rawContainerURLWithSAS := scenarioHelper{}.getRawContainerURLWithSAS(a, containerName)
	raw := getDefaultSyncRawInput(srcDirName, rawContainerURLWithSAS.String())

	runSyncAndVerify(a, raw, mockedRPC.intercept, mockedRPC.delete, func(err error) {
		a.Nil(err)

		// validate that the right number of transfers were scheduled
		a.Zero(len(mockedRPC.transfers))
	})

	// refresh the files' last modified time so that they are newer
	scenarioHelper{}.generateLocalFilesFromList(a, srcDirName, fileList)
	mockedRPC.reset()

	runSyncAndVerify(a, raw, mockedRPC.intercept, mockedRPC.delete, func(err error) {
		a.Nil(err)
		validateUploadTransfersAreScheduled(a, common.AZCOPY_PATH_SEPARATOR_STRING, common.AZCOPY_PATH_SEPARATOR_STRING, fileList, mockedRPC)
	})
}

// regular container->directory sync where destination is missing some files from source, and also has some extra files
func TestSyncUploadWithMismatchedDestination(t *testing.T) {
	a := assert.New(t)
	bsc := getBlobServiceClient()

	// set up the source with numerous files
	srcDirName := scenarioHelper{}.generateLocalDirectory(a)
	defer os.RemoveAll(srcDirName)
	fileList := scenarioHelper{}.generateCommonRemoteScenarioForLocal(a, srcDirName, "")

	// set up an the container with half of the files, but later lmts
	// also add some extra blobs that are not present at the source
	extraBlobs := []string{"extraFile1.pdf", "extraFile2.txt"}
	cc, containerName := createNewContainer(a, bsc)
	defer deleteContainer(a, cc)
	scenarioHelper{}.generateBlobsFromList(a, cc, fileList[0:len(fileList)/2], blockBlobDefaultData)
	scenarioHelper{}.generateBlobsFromList(a, cc, extraBlobs, blockBlobDefaultData)
	expectedOutput := fileList[len(fileList)/2:]

	// set up interceptor
	mockedRPC := interceptor{}
	mockedRPC.init()

	// construct the raw input to simulate user input
	rawContainerURLWithSAS := scenarioHelper{}.getRawContainerURLWithSAS(a, containerName)
	raw := getDefaultSyncRawInput(srcDirName, rawContainerURLWithSAS.String())

	runSyncAndVerify(a, raw, mockedRPC.intercept, mockedRPC.delete, func(err error) {
		a.Nil(err)
		validateUploadTransfersAreScheduled(a, common.AZCOPY_PATH_SEPARATOR_STRING, common.AZCOPY_PATH_SEPARATOR_STRING, expectedOutput, mockedRPC)

		// make sure the extra blobs were deleted
		validateDeleteTransfersAreScheduled(a, extraBlobs, mockedRPC)
	})
}

// include flag limits the scope of source/destination comparison
func TestSyncUploadWithIncludePatternFlag(t *testing.T) {
	a := assert.New(t)
	bsc := getBlobServiceClient()

	// set up the source with numerous files
	srcDirName := scenarioHelper{}.generateLocalDirectory(a)
	defer os.RemoveAll(srcDirName)
	scenarioHelper{}.generateCommonRemoteScenarioForLocal(a, srcDirName, "")

	// add special files that we wish to include
	filesToInclude := []string{"important.pdf", "includeSub/amazing.jpeg", "exactName"}
	scenarioHelper{}.generateLocalFilesFromList(a, srcDirName, filesToInclude)
	includeString := "*.pdf;*.jpeg;exactName"

	// set up the destination as an empty container
	cc, containerName := createNewContainer(a, bsc)
	defer deleteContainer(a, cc)

	// set up interceptor
	mockedRPC := interceptor{}
	mockedRPC.init()

	// construct the raw input to simulate user input
	rawContainerURLWithSAS := scenarioHelper{}.getRawContainerURLWithSAS(a, containerName)
	raw := getDefaultSyncRawInput(srcDirName, rawContainerURLWithSAS.String())
	raw.include = includeString

	runSyncAndVerify(a, raw, mockedRPC.intercept, mockedRPC.delete, func(err error) {
		a.Nil(err)
		validateUploadTransfersAreScheduled(a, common.AZCOPY_PATH_SEPARATOR_STRING, common.AZCOPY_PATH_SEPARATOR_STRING, filesToInclude, mockedRPC)
	})
}

// exclude flag limits the scope of source/destination comparison
func TestSyncUploadWithExcludePatternFlag(t *testing.T) {
	a := assert.New(t)
	bsc := getBlobServiceClient()

	// set up the source with numerous files
	srcDirName := scenarioHelper{}.generateLocalDirectory(a)
	defer os.RemoveAll(srcDirName)
	fileList := scenarioHelper{}.generateCommonRemoteScenarioForLocal(a, srcDirName, "")

	// add special files that we wish to exclude
	filesToExclude := []string{"notGood.pdf", "excludeSub/lame.jpeg", "exactName"}
	scenarioHelper{}.generateLocalFilesFromList(a, srcDirName, filesToExclude)
	excludeString := "*.pdf;*.jpeg;exactName"

	// set up the destination as an empty container
	cc, containerName := createNewContainer(a, bsc)
	defer deleteContainer(a, cc)

	// set up interceptor
	mockedRPC := interceptor{}
	jobsAdmin.ExecuteNewCopyJobPartOrder = func(order common.CopyJobPartOrderRequest) common.CopyJobPartOrderResponse {
		return mockedRPC.intercept(order)
	}
	mockedRPC.init()

	// construct the raw input to simulate user input
	rawContainerURLWithSAS := scenarioHelper{}.getRawContainerURLWithSAS(a, containerName)
	raw := getDefaultSyncRawInput(srcDirName, rawContainerURLWithSAS.String())
	raw.exclude = excludeString

	runSyncAndVerify(a, raw, mockedRPC.intercept, mockedRPC.delete, func(err error) {
		a.Nil(err)
		validateUploadTransfersAreScheduled(a, common.AZCOPY_PATH_SEPARATOR_STRING, common.AZCOPY_PATH_SEPARATOR_STRING, fileList, mockedRPC)
	})
}

// include and exclude flag can work together to limit the scope of source/destination comparison
func TestSyncUploadWithIncludeAndExcludePatternFlag(t *testing.T) {
	a := assert.New(t)
	bsc := getBlobServiceClient()

	// set up the source with numerous files
	srcDirName := scenarioHelper{}.generateLocalDirectory(a)
	defer os.RemoveAll(srcDirName)
	scenarioHelper{}.generateCommonRemoteScenarioForLocal(a, srcDirName, "")

	// add special files that we wish to include
	filesToInclude := []string{"important.pdf", "includeSub/amazing.jpeg"}
	scenarioHelper{}.generateLocalFilesFromList(a, srcDirName, filesToInclude)
	includeString := "*.pdf;*.jpeg;exactName"

	// add special files that we wish to exclude
	// note that the excluded files also match the include string
	filesToExclude := []string{"sorry.pdf", "exclude/notGood.jpeg", "exactName", "sub/exactName"}
	scenarioHelper{}.generateLocalFilesFromList(a, srcDirName, filesToExclude)
	excludeString := "so*;not*;exactName"

	// set up the destination as an empty container
	cc, containerName := createNewContainer(a, bsc)
	defer deleteContainer(a, cc)

	// set up interceptor
	mockedRPC := interceptor{}
	mockedRPC.init()

	// construct the raw input to simulate user input
	rawContainerURLWithSAS := scenarioHelper{}.getRawContainerURLWithSAS(a, containerName)
	raw := getDefaultSyncRawInput(srcDirName, rawContainerURLWithSAS.String())
	raw.include = includeString
	raw.exclude = excludeString

	runSyncAndVerify(a, raw, mockedRPC.intercept, mockedRPC.delete, func(err error) {
		a.Nil(err)
		validateUploadTransfersAreScheduled(a, common.AZCOPY_PATH_SEPARATOR_STRING, common.AZCOPY_PATH_SEPARATOR_STRING, filesToInclude, mockedRPC)
	})
}

// a specific path is avoided in the comparison
func TestSyncUploadWithExcludePathFlag(t *testing.T) {
	a := assert.New(t)
	bsc := getBlobServiceClient()

	// set up the source with numerous files
	srcDirName := scenarioHelper{}.generateLocalDirectory(a)
	defer os.RemoveAll(srcDirName)
	fileList := scenarioHelper{}.generateCommonRemoteScenarioForLocal(a, srcDirName, "")

	// add special files that we wish to exclude
	filesToExclude := []string{"excludeSub/notGood.pdf", "excludeSub/lame.jpeg", "exactName"}
	scenarioHelper{}.generateLocalFilesFromList(a, srcDirName, filesToExclude)
	excludeString := "excludeSub;exactName"

	// set up the destination as an empty container
	cc, containerName := createNewContainer(a, bsc)
	defer deleteContainer(a, cc)

	// set up interceptor
	mockedRPC := interceptor{}
	mockedRPC.init()

	// construct the raw input to simulate user input
	rawContainerURLWithSAS := scenarioHelper{}.getRawContainerURLWithSAS(a, containerName)
	raw := getDefaultSyncRawInput(srcDirName, rawContainerURLWithSAS.String())
	raw.excludePath = excludeString

	runSyncAndVerify(a, raw, mockedRPC.intercept, mockedRPC.delete, func(err error) {
		a.Nil(err)
		validateUploadTransfersAreScheduled(a, common.AZCOPY_PATH_SEPARATOR_STRING, common.AZCOPY_PATH_SEPARATOR_STRING, fileList, mockedRPC)
	})

	// now set up the destination with the blobs to be excluded, and make sure they are not touched
	scenarioHelper{}.generateBlobsFromList(a, cc, filesToExclude, blockBlobDefaultData)

	// re-create the ones at the source so that their lmts are newer
	scenarioHelper{}.generateLocalFilesFromList(a, srcDirName, filesToExclude)

	mockedRPC.reset()
	runSyncAndVerify(a, raw, mockedRPC.intercept, mockedRPC.delete, func(err error) {
		a.Nil(err)
		validateUploadTransfersAreScheduled(a, common.AZCOPY_PATH_SEPARATOR_STRING, common.AZCOPY_PATH_SEPARATOR_STRING, fileList, mockedRPC)

		// make sure the extra blobs were not touched
		for _, blobName := range filesToExclude {
			exists := scenarioHelper{}.blobExists(cc.NewBlobClient(blobName))
			a.True(exists)
		}
	})
}

// validate the bug fix for this scenario
func TestSyncUploadWithMissingDestination(t *testing.T) {
	a := assert.New(t)
	bsc := getBlobServiceClient()

	// set up the source with numerous files
	srcDirName := scenarioHelper{}.generateLocalDirectory(a)
	defer os.RemoveAll(srcDirName)
	scenarioHelper{}.generateCommonRemoteScenarioForLocal(a, srcDirName, "")

	// set up the destination as an non-existent container
	cc, containerName := getContainerClient(a, bsc)

	// validate that the container does not exist
	_, err := cc.GetProperties(context.Background(), nil)
	a.NotNil(err)

	// set up interceptor
	mockedRPC := interceptor{}
	mockedRPC.init()

	// construct the raw input to simulate user input
	rawContainerURLWithSAS := scenarioHelper{}.getRawContainerURLWithSAS(a, containerName)
	raw := getDefaultSyncRawInput(srcDirName, rawContainerURLWithSAS.String())

	runSyncAndVerify(a, raw, mockedRPC.intercept, mockedRPC.delete, func(err error) {
		// error should not be nil, but the app should not crash either
		a.NotNil(err)

		// validate that the right number of transfers were scheduled
		a.Zero(len(mockedRPC.transfers))
	})
}

func TestDryrunSyncLocaltoBlob(t *testing.T) {
	a := assert.New(t)
	bsc := getBlobServiceClient()

	//set up local src
	blobsToInclude := []string{"AzURE2.jpeg", "sub1/aTestOne.txt", "sub1/sub2/testTwo.pdf"}
	srcDirName := scenarioHelper{}.generateLocalDirectory(a)
	defer os.RemoveAll(srcDirName)
	scenarioHelper{}.generateLocalFilesFromList(a, srcDirName, blobsToInclude)

	//set up dst container
	dstContainerClient, dstContainerName := createNewContainer(a, bsc)
	defer deleteContainer(a, dstContainerClient)
	blobsToDelete := []string{"testThree.jpeg"}
	scenarioHelper{}.generateBlobsFromList(a, dstContainerClient, blobsToDelete, blockBlobDefaultData)

	// set up interceptor
	mockedLcm := mockedLifecycleManager{dryrunLog: make(chan string, 50)}
	mockedLcm.SetOutputFormat(EOutputFormat.Text())
	glcm = &mockedLcm

	// construct the raw input to simulate user input
	dstContainerURLWithSAS := scenarioHelper{}.getRawContainerURLWithSAS(a, dstContainerName)
	raw := getDefaultSyncRawInput(srcDirName, dstContainerURLWithSAS.String())
	raw.dryrun = true
	raw.deleteDestination = "true"

	runSyncAndVerify(a, raw, dryrunNewCopyJobPartOrder, dryrunDelete, func(err error) {
		a.Nil(err)

		msg := mockedLcm.GatherAllLogs(mockedLcm.dryrunLog)
		sort.Strings(msg)
		for i := 0; i < len(msg); i++ {
			if strings.Contains(msg[i], "DRYRUN: remove") {
				a.True(strings.Contains(msg[i], dstContainerClient.URL()))
			} else {
				a.True(strings.Contains(msg[i], "DRYRUN: copy"))
				a.True(strings.Contains(msg[i], srcDirName))
				a.True(strings.Contains(msg[i], dstContainerClient.URL()))
			}
		}

		a.True(testDryrunStatements(blobsToInclude, msg))
		a.True(testDryrunStatements(blobsToDelete, msg))
	})
}
