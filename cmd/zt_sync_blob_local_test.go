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
	"bytes"
	"context"
	"github.com/Azure/azure-sdk-for-go/sdk/azcore/streaming"
	"github.com/Azure/azure-sdk-for-go/sdk/azcore/to"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/blockblob"
	"github.com/Azure/azure-storage-azcopy/v10/jobsAdmin"
	"github.com/stretchr/testify/assert"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/Azure/azure-storage-azcopy/v10/common"
)

const (
	defaultLogVerbosityForSync = "WARNING"
)

// regular blob->file sync
func TestSyncDownloadWithSingleFile(t *testing.T) {
	a := assert.New(t)
	bsc := getBlobServiceClient()
	cc, containerName := createNewContainer(a, bsc)
	defer deleteContainer(a, cc)

	for _, blobName := range []string{"singleblobisbest", "打麻将.txt", "%4509%4254$85140&"} {
		// set up the container with a single blob
		blobList := []string{blobName}
		scenarioHelper{}.generateBlobsFromList(a, cc, blobList, blockBlobDefaultData)
		a.NotNil(cc)

		// set up the destination as a single file
		time.Sleep(time.Second)
		dstDirName := scenarioHelper{}.generateLocalDirectory(a)
		defer os.RemoveAll(dstDirName)
		dstFileName := blobName
		scenarioHelper{}.generateLocalFilesFromList(a, dstDirName, blobList)

		// set up interceptor
		mockedRPC := interceptor{}
		jobsAdmin.ExecuteNewCopyJobPartOrder = func(order common.CopyJobPartOrderRequest) common.CopyJobPartOrderResponse {
			return mockedRPC.intercept(order)
		}
		mockedRPC.init()

		// construct the raw input to simulate user input
		rawBlobURLWithSAS := scenarioHelper{}.getRawBlobURLWithSAS(a, containerName, blobList[0])
		raw := getDefaultSyncRawInput(rawBlobURLWithSAS.String(), filepath.Join(dstDirName, dstFileName))

		// the file was created after the blob, so no sync should happen
		runSyncAndVerify(a, raw, func(err error) {
			a.Nil(err)

			// validate that the right number of transfers were scheduled
			a.Zero(len(mockedRPC.transfers))
		})

		// Sleep a bit to offset LMTs
		time.Sleep(5 * time.Second)
		// recreate the blob to have a later last modified time
		time.Sleep(time.Second)
		scenarioHelper{}.generateBlobsFromList(a, cc, blobList, blockBlobDefaultData)
		mockedRPC.reset()

		runSyncAndVerify(a, raw, func(err error) {
			a.Nil(err)

			validateDownloadTransfersAreScheduled(a, common.AZCOPY_PATH_SEPARATOR_STRING, common.AZCOPY_PATH_SEPARATOR_STRING, []string{""}, mockedRPC)
		})
	}
}

// regular container->directory sync but destination is empty, so everything has to be transferred
func TestSyncDownloadWithEmptyDestination(t *testing.T) {
	a := assert.New(t)
	bsc := getBlobServiceClient()

	// set up the container with numerous blobs
	cc, containerName := createNewContainer(a, bsc)
	blobList := scenarioHelper{}.generateCommonRemoteScenarioForBlob(a, cc, "")
	defer deleteContainer(a, cc)
	a.NotNil(cc)
	a.NotZero(len(blobList))

	// set up the destination with an empty folder
	dstDirName := scenarioHelper{}.generateLocalDirectory(a)
	defer os.RemoveAll(dstDirName)

	// set up interceptor
	mockedRPC := interceptor{}
	jobsAdmin.ExecuteNewCopyJobPartOrder = func(order common.CopyJobPartOrderRequest) common.CopyJobPartOrderResponse {
		return mockedRPC.intercept(order)
	}
	mockedRPC.init()

	// construct the raw input to simulate user input
	rawContainerURLWithSAS := scenarioHelper{}.getRawContainerURLWithSAS(a, containerName)
	raw := getDefaultSyncRawInput(rawContainerURLWithSAS.String(), dstDirName)

	runSyncAndVerify(a, raw, func(err error) {
		a.Nil(err)

		// validate that the right number of transfers were scheduled
		a.Equal(len(blobList), len(mockedRPC.transfers))

		// validate that the right transfers were sent
		validateDownloadTransfersAreScheduled(a, common.AZCOPY_PATH_SEPARATOR_STRING, common.AZCOPY_PATH_SEPARATOR_STRING, blobList, mockedRPC)
	})

	// turn off recursive, this time only top blobs should be transferred
	raw.recursive = false
	mockedRPC.reset()

	runSyncAndVerify(a, raw, func(err error) {
		a.Nil(err)
		a.NotEqual(len(blobList), len(mockedRPC.transfers))

		for _, transfer := range mockedRPC.transfers {
			source := strings.TrimPrefix(transfer.Source, "/")
			a.False(strings.Contains(source, common.AZCOPY_PATH_SEPARATOR_STRING))
		}
	})
}

// regular container->directory sync but destination is identical to the source, transfers are scheduled based on lmt
func TestSyncDownloadWithIdenticalDestination(t *testing.T) {
	a := assert.New(t)
	bsc := getBlobServiceClient()

	// set up the container with numerous blobs
	cc, containerName := createNewContainer(a, bsc)
	blobList := scenarioHelper{}.generateCommonRemoteScenarioForBlob(a, cc, "")
	defer deleteContainer(a, cc)
	a.NotNil(cc)
	a.NotZero(len(blobList))

	// set up the destination with a folder that have the exact same files
	dstDirName := scenarioHelper{}.generateLocalDirectory(a)
	defer os.RemoveAll(dstDirName)
	scenarioHelper{}.generateLocalFilesFromList(a, dstDirName, blobList)

	// set up interceptor
	mockedRPC := interceptor{}
	jobsAdmin.ExecuteNewCopyJobPartOrder = func(order common.CopyJobPartOrderRequest) common.CopyJobPartOrderResponse {
		return mockedRPC.intercept(order)
	}
	mockedRPC.init()

	// construct the raw input to simulate user input
	rawContainerURLWithSAS := scenarioHelper{}.getRawContainerURLWithSAS(a, containerName)
	raw := getDefaultSyncRawInput(rawContainerURLWithSAS.String(), dstDirName)

	runSyncAndVerify(a, raw, func(err error) {
		a.Nil(err)

		// validate that the right number of transfers were scheduled
		a.Zero(len(mockedRPC.transfers))
	})

	// refresh the blobs' last modified time so that they are newer
	scenarioHelper{}.generateBlobsFromList(a, cc, blobList, blockBlobDefaultData)
	mockedRPC.reset()

	runSyncAndVerify(a, raw, func(err error) {
		a.Nil(err)
		validateDownloadTransfersAreScheduled(a, common.AZCOPY_PATH_SEPARATOR_STRING, common.AZCOPY_PATH_SEPARATOR_STRING, blobList, mockedRPC)
	})
}

// regular container->directory sync where destination is missing some files from source, and also has some extra files
func TestSyncDownloadWithMismatchedDestination(t *testing.T) {
	a := assert.New(t)
	bsc := getBlobServiceClient()

	// set up the container with numerous blobs
	cc, containerName := createNewContainer(a, bsc)
	blobList := scenarioHelper{}.generateCommonRemoteScenarioForBlob(a, cc, "")
	defer deleteContainer(a, cc)
	a.NotNil(cc)
	a.NotZero(len(blobList))

	// set up the destination with a folder that have half of the files from source
	dstDirName := scenarioHelper{}.generateLocalDirectory(a)
	defer os.RemoveAll(dstDirName)
	scenarioHelper{}.generateLocalFilesFromList(a, dstDirName, blobList[0:len(blobList)/2])
	scenarioHelper{}.generateLocalFilesFromList(a, dstDirName, []string{"extraFile1.pdf, extraFile2.txt"})
	expectedOutput := blobList[len(blobList)/2:] // the missing half of source files should be transferred

	// set up interceptor
	mockedRPC := interceptor{}
	jobsAdmin.ExecuteNewCopyJobPartOrder = func(order common.CopyJobPartOrderRequest) common.CopyJobPartOrderResponse {
		return mockedRPC.intercept(order)
	}
	mockedRPC.init()

	// construct the raw input to simulate user input
	rawContainerURLWithSAS := scenarioHelper{}.getRawContainerURLWithSAS(a, containerName)
	raw := getDefaultSyncRawInput(rawContainerURLWithSAS.String(), dstDirName)

	runSyncAndVerify(a, raw, func(err error) {
		a.Nil(err)
		validateDownloadTransfersAreScheduled(a, common.AZCOPY_PATH_SEPARATOR_STRING, common.AZCOPY_PATH_SEPARATOR_STRING, expectedOutput, mockedRPC)

		// make sure the extra files were deleted
		currentDstFileList, err := os.ReadDir(dstDirName)
		extraFilesFound := false
		for _, file := range currentDstFileList {
			if strings.Contains(file.Name(), "extra") {
				extraFilesFound = true
			}
		}

		a.False(extraFilesFound)
	})
}

// include flag limits the scope of source/destination comparison
func TestSyncDownloadWithIncludePatternFlag(t *testing.T) {
	a := assert.New(t)
	bsc := getBlobServiceClient()

	// set up the container with numerous blobs
	cc, containerName := createNewContainer(a, bsc)
	blobList := scenarioHelper{}.generateCommonRemoteScenarioForBlob(a, cc, "")
	defer deleteContainer(a, cc)
	a.NotNil(cc)
	a.NotZero(len(blobList))

	// add special blobs that we wish to include
	blobsToInclude := []string{"important.pdf", "includeSub/amazing.jpeg", "exactName"}
	scenarioHelper{}.generateBlobsFromList(a, cc, blobsToInclude, blockBlobDefaultData)

	includeString := "*.pdf;*.jpeg;exactName"

	// set up the destination with an empty folder
	dstDirName := scenarioHelper{}.generateLocalDirectory(a)
	defer os.RemoveAll(dstDirName)

	// set up interceptor
	mockedRPC := interceptor{}
	jobsAdmin.ExecuteNewCopyJobPartOrder = func(order common.CopyJobPartOrderRequest) common.CopyJobPartOrderResponse {
		return mockedRPC.intercept(order)
	}
	mockedRPC.init()

	// construct the raw input to simulate user input
	rawContainerURLWithSAS := scenarioHelper{}.getRawContainerURLWithSAS(a, containerName)
	raw := getDefaultSyncRawInput(rawContainerURLWithSAS.String(), dstDirName)
	raw.include = includeString

	runSyncAndVerify(a, raw, func(err error) {
		a.Nil(err)
		validateDownloadTransfersAreScheduled(a, common.AZCOPY_PATH_SEPARATOR_STRING, common.AZCOPY_PATH_SEPARATOR_STRING, blobsToInclude, mockedRPC)
	})
}

// exclude flag limits the scope of source/destination comparison
func TestSyncDownloadWithExcludePatternFlag(t *testing.T) {
	a := assert.New(t)
	bsc := getBlobServiceClient()

	// set up the container with numerous blobs
	cc, containerName := createNewContainer(a, bsc)
	blobList := scenarioHelper{}.generateCommonRemoteScenarioForBlob(a, cc, "")
	defer deleteContainer(a, cc)
	a.NotNil(cc)
	a.NotZero(len(blobList))

	// add special blobs that we wish to exclude
	blobsToExclude := []string{"notGood.pdf", "excludeSub/lame.jpeg", "exactName"}
	scenarioHelper{}.generateBlobsFromList(a, cc, blobsToExclude, blockBlobDefaultData)
	excludeString := "*.pdf;*.jpeg;exactName"

	// set up the destination with an empty folder
	dstDirName := scenarioHelper{}.generateLocalDirectory(a)
	defer os.RemoveAll(dstDirName)

	// set up interceptor
	mockedRPC := interceptor{}
	jobsAdmin.ExecuteNewCopyJobPartOrder = func(order common.CopyJobPartOrderRequest) common.CopyJobPartOrderResponse {
		return mockedRPC.intercept(order)
	}
	mockedRPC.init()

	// construct the raw input to simulate user input
	rawContainerURLWithSAS := scenarioHelper{}.getRawContainerURLWithSAS(a, containerName)
	raw := getDefaultSyncRawInput(rawContainerURLWithSAS.String(), dstDirName)
	raw.exclude = excludeString

	runSyncAndVerify(a, raw, func(err error) {
		a.Nil(err)
		validateDownloadTransfersAreScheduled(a, common.AZCOPY_PATH_SEPARATOR_STRING, common.AZCOPY_PATH_SEPARATOR_STRING, blobList, mockedRPC)
	})
}

// include and exclude flag can work together to limit the scope of source/destination comparison
func TestSyncDownloadWithIncludeAndExcludePatternFlag(t *testing.T) {
	a := assert.New(t)
	bsc := getBlobServiceClient()

	// set up the container with numerous blobs
	cc, containerName := createNewContainer(a, bsc)
	blobList := scenarioHelper{}.generateCommonRemoteScenarioForBlob(a, cc, "")
	defer deleteContainer(a, cc)
	a.NotNil(cc)
	a.NotZero(len(blobList))

	// add special blobs that we wish to include
	blobsToInclude := []string{"important.pdf", "includeSub/amazing.jpeg"}
	scenarioHelper{}.generateBlobsFromList(a, cc, blobsToInclude, blockBlobDefaultData)
	includeString := "*.pdf;*.jpeg;exactName"

	// add special blobs that we wish to exclude
	// note that the excluded files also match the include string
	blobsToExclude := []string{"sorry.pdf", "exclude/notGood.jpeg", "exactName", "sub/exactName"}
	scenarioHelper{}.generateBlobsFromList(a, cc, blobsToExclude, blockBlobDefaultData)
	excludeString := "so*;not*;exactName"

	// set up the destination with an empty folder
	dstDirName := scenarioHelper{}.generateLocalDirectory(a)
	defer os.RemoveAll(dstDirName)

	// set up interceptor
	mockedRPC := interceptor{}
	jobsAdmin.ExecuteNewCopyJobPartOrder = func(order common.CopyJobPartOrderRequest) common.CopyJobPartOrderResponse {
		return mockedRPC.intercept(order)
	}
	mockedRPC.init()

	// construct the raw input to simulate user input
	rawContainerURLWithSAS := scenarioHelper{}.getRawContainerURLWithSAS(a, containerName)
	raw := getDefaultSyncRawInput(rawContainerURLWithSAS.String(), dstDirName)
	raw.include = includeString
	raw.exclude = excludeString

	runSyncAndVerify(a, raw, func(err error) {
		a.Nil(err)
		validateDownloadTransfersAreScheduled(a, common.AZCOPY_PATH_SEPARATOR_STRING, common.AZCOPY_PATH_SEPARATOR_STRING, blobsToInclude, mockedRPC)
	})
}

// a specific path is avoided in the comparison
func TestSyncDownloadWithExcludePathFlag(t *testing.T) {
	a := assert.New(t)
	bsc := getBlobServiceClient()

	// set up the container with numerous blobs
	cc, containerName := createNewContainer(a, bsc)
	blobList := scenarioHelper{}.generateCommonRemoteScenarioForBlob(a, cc, "")
	defer deleteContainer(a, cc)
	a.NotNil(cc)
	a.NotZero(len(blobList))

	// add special blobs that we wish to exclude
	blobsToExclude := []string{"excludeSub/notGood.pdf", "excludeSub/lame.jpeg", "exactName"}
	scenarioHelper{}.generateBlobsFromList(a, cc, blobsToExclude, blockBlobDefaultData)

	excludeString := "excludeSub;exactName"

	// set up the destination with an empty folder
	dstDirName := scenarioHelper{}.generateLocalDirectory(a)
	defer os.RemoveAll(dstDirName)

	// set up interceptor
	mockedRPC := interceptor{}
	jobsAdmin.ExecuteNewCopyJobPartOrder = func(order common.CopyJobPartOrderRequest) common.CopyJobPartOrderResponse {
		return mockedRPC.intercept(order)
	}
	mockedRPC.init()

	// construct the raw input to simulate user input
	rawContainerURLWithSAS := scenarioHelper{}.getRawContainerURLWithSAS(a, containerName)
	raw := getDefaultSyncRawInput(rawContainerURLWithSAS.String(), dstDirName)
	raw.excludePath = excludeString

	runSyncAndVerify(a, raw, func(err error) {
		a.Nil(err)
		validateDownloadTransfersAreScheduled(a, common.AZCOPY_PATH_SEPARATOR_STRING, common.AZCOPY_PATH_SEPARATOR_STRING, blobList, mockedRPC)
	})

	// now set up the destination with the files to be excluded, and make sure they are not touched
	scenarioHelper{}.generateLocalFilesFromList(a, dstDirName, blobsToExclude)

	// re-create the ones at the source so that their lmts are newer
	scenarioHelper{}.generateBlobsFromList(a, cc, blobsToExclude, blockBlobDefaultData)

	mockedRPC.reset()
	runSyncAndVerify(a, raw, func(err error) {
		a.Nil(err)
		validateDownloadTransfersAreScheduled(a, common.AZCOPY_PATH_SEPARATOR_STRING, common.AZCOPY_PATH_SEPARATOR_STRING, blobList, mockedRPC)

		// make sure the extra files were not touched
		for _, blobName := range blobsToExclude {
			_, err := os.Stat(filepath.Join(dstDirName, blobName))
			a.Nil(err)
		}
	})
}

// validate the bug fix for this scenario
func TestSyncDownloadWithMissingDestination(t *testing.T) {
	a := assert.New(t)
	bsc := getBlobServiceClient()

	// set up the container with numerous blobs
	cc, containerName := createNewContainer(a, bsc)
	blobList := scenarioHelper{}.generateCommonRemoteScenarioForBlob(a, cc, "")
	defer deleteContainer(a, cc)
	a.NotNil(cc)
	a.NotZero(len(blobList))

	// set up the destination as a missing folder
	baseDirName := scenarioHelper{}.generateLocalDirectory(a)
	dstDirName := filepath.Join(baseDirName, "imbatman")
	defer os.RemoveAll(baseDirName)

	// set up interceptor
	mockedRPC := interceptor{}
	jobsAdmin.ExecuteNewCopyJobPartOrder = func(order common.CopyJobPartOrderRequest) common.CopyJobPartOrderResponse {
		return mockedRPC.intercept(order)
	}
	mockedRPC.init()

	// construct the raw input to simulate user input
	rawContainerURLWithSAS := scenarioHelper{}.getRawContainerURLWithSAS(a, containerName)
	raw := getDefaultSyncRawInput(rawContainerURLWithSAS.String(), dstDirName)

	runSyncAndVerify(a, raw, func(err error) {
		// error should not be nil, but the app should not crash either
		a.Nil(err)

		// validate that the right number of transfers were scheduled
		a.Equal(len(mockedRPC.transfers), len(blobList), "Expected to transfer the container's worth of blobs")
	})
}

// download a blob representing an ADLS directory to a local file
// we should recognize that there is a type mismatch
func TestSyncDownloadADLSDirectoryTypeMismatch(t *testing.T) {
	a := assert.New(t)
	bsc := getBlobServiceClient()
	blobName := "adlsdir"

	// set up the destination as a single file
	dstDirName := scenarioHelper{}.generateLocalDirectory(a)
	defer os.RemoveAll(dstDirName)
	dstFileName := blobName
	scenarioHelper{}.generateLocalFilesFromList(a, dstDirName, []string{blobName})

	// set up the container
	containerClient, containerName := createNewContainer(a, bsc)
	defer deleteContainer(a, containerClient)
	a.NotNil(containerClient)

	// create a single blob that represents an ADLS directory
	_, err := containerClient.NewBlockBlobClient(blobName).Upload(context.Background(), streaming.NopCloser(bytes.NewReader(nil)),
		&blockblob.UploadOptions{
			Metadata: map[string]*string{"hdi_isfolder": to.Ptr("true")},
		})
	a.Nil(err)

	// set up interceptor
	mockedRPC := interceptor{}
	jobsAdmin.ExecuteNewCopyJobPartOrder = func(order common.CopyJobPartOrderRequest) common.CopyJobPartOrderResponse {
		return mockedRPC.intercept(order)
	}
	mockedRPC.init()

	// construct the raw input to simulate user input
	rawBlobURLWithSAS := scenarioHelper{}.getRawBlobURLWithSAS(a, containerName, blobName)
	raw := getDefaultSyncRawInput(rawBlobURLWithSAS.String(), filepath.Join(dstDirName, dstFileName))

	// the file was created after the blob, so no sync should happen
	runSyncAndVerify(a, raw, func(err error) {
		a.NotNil(err)

		// validate that the right number of transfers were scheduled
		a.Zero(len(mockedRPC.transfers))
	})
}

// adls directory -> local directory sync
// we should download every blob except the blob representing the directory
func TestSyncDownloadWithADLSDirectory(t *testing.T) {
	a := assert.New(t)
	bsc := getBlobServiceClient()
	adlsDirName := "adlsdir"

	// set up the container with numerous blobs
	containerClient, containerName := createNewContainer(a, bsc)
	blobList := scenarioHelper{}.generateCommonRemoteScenarioForBlob(a, containerClient, adlsDirName+"/")
	defer deleteContainer(a, containerClient)
	a.NotNil(containerClient)
	a.NotZero(len(blobList))

	// create a single blob that represents the ADLS directory
	_, err := containerClient.NewBlockBlobClient(adlsDirName).Upload(context.Background(), streaming.NopCloser(bytes.NewReader(nil)),
		&blockblob.UploadOptions{
			Metadata: map[string]*string{"hdi_isfolder": to.Ptr("true")},
		})
	a.Nil(err)

	// create an extra blob that represents an empty ADLS directory, which should never be picked up
	_, err = containerClient.NewBlockBlobClient(adlsDirName+"/neverpickup").Upload(context.Background(), streaming.NopCloser(bytes.NewReader(nil)),
		&blockblob.UploadOptions{
			Metadata: map[string]*string{"hdi_isfolder": to.Ptr("true")},
		})
	a.Nil(err)

	// set up the destination with an empty folder
	dstDirName := scenarioHelper{}.generateLocalDirectory(a)
	defer os.RemoveAll(dstDirName)

	// set up interceptor
	mockedRPC := interceptor{}
	jobsAdmin.ExecuteNewCopyJobPartOrder = func(order common.CopyJobPartOrderRequest) common.CopyJobPartOrderResponse {
		return mockedRPC.intercept(order)
	}
	mockedRPC.init()

	// construct the raw input to simulate user input
	rawContainerURLWithSAS := scenarioHelper{}.getRawBlobURLWithSAS(a, containerName, adlsDirName)
	raw := getDefaultSyncRawInput(rawContainerURLWithSAS.String(), dstDirName)

	runSyncAndVerify(a, raw, func(err error) {
		a.Nil(err)

		// validate that the right number of transfers were scheduled
		a.Equal(len(blobList), len(mockedRPC.transfers))
	})

	// turn off recursive, this time only top blobs should be transferred
	raw.recursive = false
	mockedRPC.reset()

	runSyncAndVerify(a, raw, func(err error) {
		a.Nil(err)
		a.NotEqual(len(blobList), len(mockedRPC.transfers))

		for _, transfer := range mockedRPC.transfers {
			source := strings.TrimPrefix(transfer.Source, "/")
			a.False(strings.Contains(source, common.AZCOPY_PATH_SEPARATOR_STRING))
		}
	})
}
