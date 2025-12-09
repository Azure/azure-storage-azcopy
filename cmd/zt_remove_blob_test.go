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
	"encoding/json"
	"fmt"
	"log"
	"net/url"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/Azure/azure-sdk-for-go/sdk/azcore/to"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/blob"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/container"
	blobsas "github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/sas"
	blobservice "github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/service"
	"github.com/Azure/azure-storage-azcopy/v10/common"
	"github.com/Azure/azure-storage-azcopy/v10/jobsAdmin"
	"github.com/stretchr/testify/assert"
)

func TestRemoveSingleBlob(t *testing.T) {
	a := assert.New(t)
	bsc := getBlobServiceClient()
	cc, containerName := createNewContainer(a, bsc)
	defer deleteContainer(a, cc)

	for _, blobName := range []string{"top/mid/low/singleblobisbest", "打麻将.txt", "%4509%4254$85140&"} {
		// set up the container with a single blob
		blobList := []string{blobName}
		scenarioHelper{}.generateBlobsFromList(a, cc, blobList, blockBlobDefaultData)
		a.NotNil(cc)

		// set up interceptor
		mockedRPC := interceptor{}
		jobsAdmin.ExecuteNewCopyJobPartOrder = func(order common.CopyJobPartOrderRequest) common.CopyJobPartOrderResponse {
			return mockedRPC.intercept(order)
		}
		mockedRPC.init()

		// construct the raw input to simulate user input
		rawBlobURLWithSAS := scenarioHelper{}.getRawBlobURLWithSAS(a, containerName, blobList[0])
		raw := getDefaultRemoveRawInput(rawBlobURLWithSAS.String())

		runOldCopyAndVerify(a, raw, func(err error) {
			a.Nil(err)

			// note that when we are targeting single blobs, the relative path is empty ("") since the root path already points to the blob
			validateRemoveTransfersAreScheduled(a, true, []string{""}, mockedRPC)
		})
	}
}

func TestRemoveBlobsUnderContainer(t *testing.T) {
	a := assert.New(t)
	bsc := getBlobServiceClient()

	// set up the container with numerous blobs
	cc, containerName := createNewContainer(a, bsc)
	defer deleteContainer(a, cc)
	blobList := scenarioHelper{}.generateCommonRemoteScenarioForBlob(a, cc, "")
	a.NotNil(cc)
	a.NotZero(len(blobList))

	// set up interceptor
	mockedRPC := interceptor{}
	jobsAdmin.ExecuteNewCopyJobPartOrder = func(order common.CopyJobPartOrderRequest) common.CopyJobPartOrderResponse {
		return mockedRPC.intercept(order)
	}
	mockedRPC.init()

	// construct the raw input to simulate user input
	rawContainerURLWithSAS := scenarioHelper{}.getRawContainerURLWithSAS(a, containerName)
	raw := getDefaultRemoveRawInput(rawContainerURLWithSAS.String())
	raw.recursive = true
	raw.includeDirectoryStubs = false // The test target is a DFS account, which coincidentally created our directory stubs. Thus, we mustn't include them, since this is a test of blob.

	runOldCopyAndVerify(a, raw, func(err error) {
		a.Nil(err)

		// validate that the right number of transfers were scheduled
		a.Equal(len(blobList), len(mockedRPC.transfers))

		// validate that the right transfers were sent
		validateRemoveTransfersAreScheduled(a, true, blobList, mockedRPC)
	})

	// turn off recursive, this time only top blobs should be deleted
	raw.recursive = false
	mockedRPC.reset()

	runOldCopyAndVerify(a, raw, func(err error) {
		a.Nil(err)
		a.NotEqual(len(blobList), len(mockedRPC.transfers))

		for _, transfer := range mockedRPC.transfers {
			source := strings.TrimPrefix(transfer.Source, "/")
			a.False(strings.Contains(source, common.AZCOPY_PATH_SEPARATOR_STRING))
		}
	})
}

func TestRemoveBlobsUnderVirtualDir(t *testing.T) {
	a := assert.New(t)
	t.Skip("Enable after setting Account to non-HNS")
	bsc := getBlobServiceClient()
	vdirName := "vdir1/vdir2/vdir3/"

	// set up the container with numerous blobs
	cc, containerName := createNewContainer(a, bsc)
	defer deleteContainer(a, cc)
	blobList := scenarioHelper{}.generateCommonRemoteScenarioForBlob(a, cc, vdirName)
	a.NotNil(cc)
	a.NotZero(len(blobList))

	// set up interceptor
	mockedRPC := interceptor{}
	jobsAdmin.ExecuteNewCopyJobPartOrder = func(order common.CopyJobPartOrderRequest) common.CopyJobPartOrderResponse {
		return mockedRPC.intercept(order)
	}
	mockedRPC.init()

	// construct the raw input to simulate user input
	rawVirtualDirectoryURLWithSAS := scenarioHelper{}.getRawBlobURLWithSAS(a, containerName, vdirName)
	raw := getDefaultRemoveRawInput(rawVirtualDirectoryURLWithSAS.String())
	raw.recursive = true

	runOldCopyAndVerify(a, raw, func(err error) {
		a.Nil(err)

		// validate that the right number of transfers were scheduled
		a.Equal(len(blobList), len(mockedRPC.transfers))

		// validate that the right transfers were sent
		expectedTransfers := scenarioHelper{}.shaveOffPrefix(blobList, vdirName)
		validateRemoveTransfersAreScheduled(a, true, expectedTransfers, mockedRPC)
	})

	// turn off recursive, this time only top blobs should be deleted
	raw.recursive = false
	mockedRPC.reset()

	runOldCopyAndVerify(a, raw, func(err error) {
		a.Nil(err)
		a.NotEqual(len(blobList), len(mockedRPC.transfers))

		for _, transfer := range mockedRPC.transfers {
			source := strings.TrimPrefix(transfer.Source, "/")
			a.False(strings.Contains(source, common.AZCOPY_PATH_SEPARATOR_STRING))
		}
	})
}

// include flag limits the scope of the delete
func TestRemoveWithIncludeFlag(t *testing.T) {
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

	// set up interceptor
	mockedRPC := interceptor{}
	jobsAdmin.ExecuteNewCopyJobPartOrder = func(order common.CopyJobPartOrderRequest) common.CopyJobPartOrderResponse {
		return mockedRPC.intercept(order)
	}
	mockedRPC.init()

	// construct the raw input to simulate user input
	rawContainerURLWithSAS := scenarioHelper{}.getRawContainerURLWithSAS(a, containerName)
	raw := getDefaultRemoveRawInput(rawContainerURLWithSAS.String())
	raw.include = includeString
	raw.recursive = true

	runOldCopyAndVerify(a, raw, func(err error) {
		a.Nil(err)
		validateRemoveTransfersAreScheduled(a, true, blobsToInclude, mockedRPC)
	})
}

// exclude flag limits the scope of the delete
func TestRemoveWithExcludeFlag(t *testing.T) {
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

	// set up interceptor
	mockedRPC := interceptor{}
	jobsAdmin.ExecuteNewCopyJobPartOrder = func(order common.CopyJobPartOrderRequest) common.CopyJobPartOrderResponse {
		return mockedRPC.intercept(order)
	}
	mockedRPC.init()

	// construct the raw input to simulate user input
	rawContainerURLWithSAS := scenarioHelper{}.getRawContainerURLWithSAS(a, containerName)
	raw := getDefaultRemoveRawInput(rawContainerURLWithSAS.String())
	raw.exclude = excludeString
	raw.recursive = true
	raw.includeDirectoryStubs = false // The test target is a DFS account, which coincidentally created our directory stubs. Thus, we mustn't include them, since this is a test of blob.

	runOldCopyAndVerify(a, raw, func(err error) {
		a.Nil(err)
		validateRemoveTransfersAreScheduled(a, true, blobList, mockedRPC)
	})
}

// include and exclude flag can work together to limit the scope of the delete
func TestRemoveWithIncludeAndExcludeFlag(t *testing.T) {
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

	// set up interceptor
	mockedRPC := interceptor{}
	jobsAdmin.ExecuteNewCopyJobPartOrder = func(order common.CopyJobPartOrderRequest) common.CopyJobPartOrderResponse {
		return mockedRPC.intercept(order)
	}
	mockedRPC.init()

	// construct the raw input to simulate user input
	rawContainerURLWithSAS := scenarioHelper{}.getRawContainerURLWithSAS(a, containerName)
	raw := getDefaultRemoveRawInput(rawContainerURLWithSAS.String())
	raw.include = includeString
	raw.exclude = excludeString
	raw.recursive = true

	runOldCopyAndVerify(a, raw, func(err error) {
		a.Nil(err)
		validateRemoveTransfersAreScheduled(a, true, blobsToInclude, mockedRPC)
	})
}

// note: list-of-files flag is used
func TestRemoveListOfBlobsAndVirtualDirs(t *testing.T) {
	a := assert.New(t)
	t.Skip("Enable after setting Account to non-HNS")
	bsc := getBlobServiceClient()
	vdirName := "megadir"

	// set up the container with numerous blobs and a vdir
	cc, containerName := createNewContainer(a, bsc)
	defer deleteContainer(a, cc)
	a.NotNil(cc)
	blobListPart1 := scenarioHelper{}.generateCommonRemoteScenarioForBlob(a, cc, "")
	blobListPart2 := scenarioHelper{}.generateCommonRemoteScenarioForBlob(a, cc, vdirName+"/")
	blobList := append(blobListPart1, blobListPart2...)
	a.NotZero(len(blobList))

	// set up interceptor
	mockedRPC := interceptor{}
	jobsAdmin.ExecuteNewCopyJobPartOrder = func(order common.CopyJobPartOrderRequest) common.CopyJobPartOrderResponse {
		return mockedRPC.intercept(order)
	}
	mockedRPC.init()

	// construct the raw input to simulate user input
	rawContainerURLWithSAS := scenarioHelper{}.getRawContainerURLWithSAS(a, containerName)
	raw := getDefaultRemoveRawInput(rawContainerURLWithSAS.String())
	raw.recursive = true

	// make the input for list-of-files
	listOfFiles := append(blobListPart1, vdirName)

	// add some random files that don't actually exist
	listOfFiles = append(listOfFiles, "WUTAMIDOING")
	listOfFiles = append(listOfFiles, "DONTKNOW")
	raw.listOfFilesToCopy = scenarioHelper{}.generateListOfFiles(a, listOfFiles)

	runOldCopyAndVerify(a, raw, func(err error) {
		a.Nil(err)

		// validate that the right number of transfers were scheduled
		a.Equal(len(blobList), len(mockedRPC.transfers))

		// validate that the right transfers were sent
		validateRemoveTransfersAreScheduled(a, true, blobList, mockedRPC)
	})

	// turn off recursive, this time only top blobs should be deleted
	raw.recursive = false
	mockedRPC.reset()

	runOldCopyAndVerify(a, raw, func(err error) {
		a.Nil(err)
		a.NotEqual(len(blobList), len(mockedRPC.transfers))

		for _, transfer := range mockedRPC.transfers {
			source, err := url.PathUnescape(transfer.Source)
			a.Nil(err)

			// if the transfer is under the given dir, make sure only the top level files were scheduled
			if strings.HasPrefix(source, vdirName) {
				trimmedSource := strings.TrimPrefix(source, vdirName+"/")
				a.False(strings.Contains(trimmedSource, common.AZCOPY_PATH_SEPARATOR_STRING))
			}
		}
	})
}

// note: list-of-files flag is used
func TestRemoveListOfBlobsWithIncludeAndExclude(t *testing.T) {
	a := assert.New(t)
	bsc := getBlobServiceClient()
	vdirName := "megadir"

	// set up the container with numerous blobs and a vdir
	cc, containerName := createNewContainer(a, bsc)
	defer deleteContainer(a, cc)
	a.NotNil(cc)
	blobListPart1 := scenarioHelper{}.generateCommonRemoteScenarioForBlob(a, cc, "")
	scenarioHelper{}.generateCommonRemoteScenarioForBlob(a, cc, vdirName+"/")

	// add special blobs that we wish to include
	blobsToInclude := []string{"important.pdf", "includeSub/amazing.jpeg"}
	scenarioHelper{}.generateBlobsFromList(a, cc, blobsToInclude, blockBlobDefaultData)
	includeString := "*.pdf;*.jpeg;exactName"

	// add special blobs that we wish to exclude
	// note that the excluded files also match the include string
	blobsToExclude := []string{"sorry.pdf", "exclude/notGood.jpeg", "exactName", "sub/exactName"}
	scenarioHelper{}.generateBlobsFromList(a, cc, blobsToExclude, blockBlobDefaultData)
	excludeString := "so*;not*;exactName"

	// set up interceptor
	mockedRPC := interceptor{}
	jobsAdmin.ExecuteNewCopyJobPartOrder = func(order common.CopyJobPartOrderRequest) common.CopyJobPartOrderResponse {
		return mockedRPC.intercept(order)
	}
	mockedRPC.init()

	// construct the raw input to simulate user input
	rawContainerURLWithSAS := scenarioHelper{}.getRawContainerURLWithSAS(a, containerName)
	raw := getDefaultRemoveRawInput(rawContainerURLWithSAS.String())
	raw.recursive = true
	raw.include = includeString
	raw.exclude = excludeString

	// make the input for list-of-files
	listOfFiles := append(blobListPart1, vdirName)

	// add some random files that don't actually exist
	listOfFiles = append(listOfFiles, "WUTAMIDOING")
	listOfFiles = append(listOfFiles, "DONTKNOW")

	// add files to both include and exclude
	listOfFiles = append(listOfFiles, blobsToInclude...)
	listOfFiles = append(listOfFiles, blobsToExclude...)
	raw.listOfFilesToCopy = scenarioHelper{}.generateListOfFiles(a, listOfFiles)

	runOldCopyAndVerify(a, raw, func(err error) {
		a.Nil(err)

		// validate that the right number of transfers were scheduled
		a.Equal(len(blobsToInclude), len(mockedRPC.transfers))

		// validate that the right transfers were sent
		validateRemoveTransfersAreScheduled(a, true, blobsToInclude, mockedRPC)
	})
}

func TestRemoveBlobsWithDirectoryStubs(t *testing.T) {
	a := assert.New(t)
	t.Skip("Enable after setting Account to non-HNS")
	bsc := getBlobServiceClient()
	vdirName := "vdir1/"

	// set up the container with numerous blobs
	cc, containerName := createNewContainer(a, bsc)
	defer deleteContainer(a, cc)
	blobAndDirStubsList := scenarioHelper{}.generateCommonRemoteScenarioForWASB(a, cc, vdirName)
	a.NotNil(cc)
	a.NotZero(len(blobAndDirStubsList))

	// set up interceptor
	mockedRPC := interceptor{}
	jobsAdmin.ExecuteNewCopyJobPartOrder = func(order common.CopyJobPartOrderRequest) common.CopyJobPartOrderResponse {
		return mockedRPC.intercept(order)
	}
	mockedRPC.init()

	// construct the raw input to simulate user input
	rawVirtualDirectoryURLWithSAS := scenarioHelper{}.getRawBlobURLWithSAS(a, containerName, vdirName)
	raw := getDefaultRemoveRawInput(rawVirtualDirectoryURLWithSAS.String())
	raw.recursive = true

	runOldCopyAndVerify(a, raw, func(err error) {
		a.Nil(err)

		// validate that the right number of transfers were scheduled
		a.Equal(len(blobAndDirStubsList), len(mockedRPC.transfers))

		// validate that the right transfers were sent
		expectedTransfers := scenarioHelper{}.shaveOffPrefix(blobAndDirStubsList, strings.TrimSuffix(vdirName, "/"))
		expectedTransfers = scenarioHelper{}.shaveOffPrefix(expectedTransfers, "/")
		validateRemoveTransfersAreScheduled(a, true, expectedTransfers, mockedRPC)
	})

	// turn off recursive, this time only top blobs should be deleted
	raw.recursive = false
	mockedRPC.reset()

	runOldCopyAndVerify(a, raw, func(err error) {
		a.Nil(err)

		// there should be exactly 20 top files, no directory stubs should be included
		a.Equal(20, len(mockedRPC.transfers))

		for _, transfer := range mockedRPC.transfers {
			source := strings.TrimPrefix(transfer.Source, "/")
			a.False(strings.Contains(source, common.AZCOPY_PATH_SEPARATOR_STRING))
		}
	})
}

func TestRemoveBlobsWithDirectoryStubsWithListOfFiles(t *testing.T) {
	a := assert.New(t)
	t.Skip("Enable after setting Account to non-HNS")
	bsc := getBlobServiceClient()
	vdirName := "vdir1/"

	// set up the container with numerous blobs
	cc, containerName := createNewContainer(a, bsc)
	defer deleteContainer(a, cc)
	blobAndDirStubsList := scenarioHelper{}.generateCommonRemoteScenarioForWASB(a, cc, vdirName)
	a.NotNil(cc)
	a.NotZero(len(blobAndDirStubsList))

	// set up another empty dir
	vdirName2 := "emptydir"
	createNewDirectoryStub(a, cc, vdirName2)
	blobAndDirStubsList = append(blobAndDirStubsList, vdirName2)

	// set up interceptor
	mockedRPC := interceptor{}
	jobsAdmin.ExecuteNewCopyJobPartOrder = func(order common.CopyJobPartOrderRequest) common.CopyJobPartOrderResponse {
		return mockedRPC.intercept(order)
	}
	mockedRPC.init()

	// construct the raw input to simulate user input
	rawContainerURLWithSAS := scenarioHelper{}.getRawContainerURLWithSAS(a, containerName)
	raw := getDefaultRemoveRawInput(rawContainerURLWithSAS.String())
	raw.recursive = true

	// make the input for list-of-files
	listOfFiles := []string{vdirName, vdirName2}
	raw.listOfFilesToCopy = scenarioHelper{}.generateListOfFiles(a, listOfFiles)

	runOldCopyAndVerify(a, raw, func(err error) {
		a.Nil(err)

		// validate that the right number of transfers were scheduled
		a.Equal(len(blobAndDirStubsList), len(mockedRPC.transfers))

		// validate that the right transfers were sent
		validateRemoveTransfersAreScheduled(a, true, blobAndDirStubsList, mockedRPC)
	})

	// turn off recursive, this time an error should be thrown
	raw.recursive = false
	mockedRPC.reset()

	runOldCopyAndVerify(a, raw, func(err error) {
		a.NotNil(err)
		a.Zero(len(mockedRPC.transfers))
	})
}

func TestDryrunRemoveSingleBlob(t *testing.T) {
	a := assert.New(t)
	bsc := getBlobServiceClient()
	cc, containerName := createNewContainer(a, bsc)
	defer deleteContainer(a, cc)

	// set up the container with a single blob
	blobName := []string{"sub1/test/testing.txt"}
	scenarioHelper{}.generateBlobsFromList(a, cc, blobName, blockBlobDefaultData)
	a.NotNil(cc)

	// set up interceptor
	mockedRPC := interceptor{}
	jobsAdmin.ExecuteNewCopyJobPartOrder = func(order common.CopyJobPartOrderRequest) common.CopyJobPartOrderResponse {
		return mockedRPC.intercept(order)
	}
	mockedLcm := mockedLifecycleManager{dryrunLog: make(chan string, 50)}
	mockedLcm.SetOutputFormat(EOutputFormat.Text()) // text format
	glcm = &mockedLcm

	// construct the raw input to simulate user input
	rawBlobURLWithSAS := scenarioHelper{}.getRawBlobURLWithSAS(a, containerName, blobName[0])
	raw := getDefaultRemoveRawInput(rawBlobURLWithSAS.String())
	raw.dryrun = true

	runOldCopyAndVerify(a, raw, func(err error) {
		a.Nil(err)
		// validate that none where transferred
		a.Zero(len(mockedRPC.transfers))

		msg := <-mockedLcm.dryrunLog
		// comparing message printed for dry run
		a.True(strings.Contains(msg, "DRYRUN: remove"))
		a.True(strings.Contains(msg, cc.URL()))
		a.True(strings.Contains(msg, blobName[0]))
	})
}

func TestDryrunRemoveBlobsUnderContainer(t *testing.T) {
	a := assert.New(t)
	bsc := getBlobServiceClient()
	cc, containerName := createNewContainer(a, bsc)
	defer deleteContainer(a, cc)

	// set up the container with a single blob
	blobList := []string{"AzURE2021.jpeg", "sub1/dir2/HELLO-4.txt", "sub1/test/testing.txt"}
	scenarioHelper{}.generateBlobsFromList(a, cc, blobList, blockBlobDefaultData)
	a.NotNil(cc)

	// set up interceptor
	mockedRPC := interceptor{}
	jobsAdmin.ExecuteNewCopyJobPartOrder = func(order common.CopyJobPartOrderRequest) common.CopyJobPartOrderResponse {
		return mockedRPC.intercept(order)
	}
	mockedLcm := mockedLifecycleManager{dryrunLog: make(chan string, 50)}
	mockedLcm.SetOutputFormat(EOutputFormat.Text()) // text format
	glcm = &mockedLcm

	// construct the raw input to simulate user input
	rawBlobURLWithSAS := scenarioHelper{}.getRawContainerURLWithSAS(a, containerName)
	raw := getDefaultRemoveRawInput(rawBlobURLWithSAS.String())
	raw.dryrun = true
	raw.recursive = true

	runOldCopyAndVerify(a, raw, func(err error) {
		a.Nil(err)
		// validate that none where transferred
		a.Zero(len(mockedRPC.transfers))

		msg := mockedLcm.GatherAllLogs(mockedLcm.dryrunLog)
		for i := 0; i < len(blobList); i++ {
			a.True(strings.Contains(msg[i], "DRYRUN: remove"))
			a.True(strings.Contains(msg[i], cc.URL()))
		}

		a.True(testDryrunStatements(blobList, msg))
	})
}

func TestDryrunRemoveBlobsUnderContainerJson(t *testing.T) {
	a := assert.New(t)
	bsc := getBlobServiceClient()
	cc, containerName := createNewContainer(a, bsc)
	defer deleteContainer(a, cc)

	// set up the container with a single blob
	blobName := []string{"tech.txt"}
	scenarioHelper{}.generateBlobsFromList(a, cc, blobName, blockBlobDefaultData)
	a.NotNil(cc)

	// set up interceptor
	mockedRPC := interceptor{}
	jobsAdmin.ExecuteNewCopyJobPartOrder = func(order common.CopyJobPartOrderRequest) common.CopyJobPartOrderResponse {
		return mockedRPC.intercept(order)
	}
	mockedLcm := mockedLifecycleManager{dryrunLog: make(chan string, 50)}
	mockedLcm.SetOutputFormat(EOutputFormat.Json()) // json format
	glcm = &mockedLcm

	// construct the raw input to simulate user input
	rawBlobURLWithSAS := scenarioHelper{}.getRawContainerURLWithSAS(a, containerName)
	raw := getDefaultRemoveRawInput(rawBlobURLWithSAS.String())
	raw.dryrun = true

	runOldCopyAndVerify(a, raw, func(err error) {
		a.Nil(err)
		// validate that none where transferred
		a.Zero(len(mockedRPC.transfers))

		msg := <-mockedLcm.dryrunLog
		deleteTransfer := DryrunTransfer{}
		errMarshal := json.Unmarshal([]byte(msg), &deleteTransfer)
		a.Nil(errMarshal)
		// comparing some values of deleteTransfer
		targetUri := cc.NewBlobClient(blobName[0]).URL()
		a.Equal(targetUri, deleteTransfer.Source)
		a.Equal("", deleteTransfer.Destination)
		a.Equal("File", deleteTransfer.EntityType.String())
		a.Equal("BlockBlob", deleteTransfer.BlobType.String())
	})
}

func TestRemoveSingleBlobWithFromTo(t *testing.T) {
	a := assert.New(t)
	bsc := getBlobServiceClient()
	cc, containerName := createNewContainer(a, bsc)
	defer deleteContainer(a, cc)

	for _, blobName := range []string{"top/mid/low/singleblobisbest", "打麻将.txt", "%4509%4254$85140&"} {
		// set up the container with a single blob
		blobList := []string{blobName}
		scenarioHelper{}.generateBlobsFromList(a, cc, blobList, blockBlobDefaultData)
		a.NotNil(cc)

		// set up interceptor
		mockedRPC := interceptor{}
		jobsAdmin.ExecuteNewCopyJobPartOrder = func(order common.CopyJobPartOrderRequest) common.CopyJobPartOrderResponse {
			return mockedRPC.intercept(order)
		}
		mockedRPC.init()

		// construct the raw input to simulate user input
		rawBlobURLWithSAS := scenarioHelper{}.getRawBlobURLWithSAS(a, containerName, blobList[0])
		raw := getDefaultRemoveRawInput(rawBlobURLWithSAS.String())
		raw.fromTo = "BlobTrash"

		runOldCopyAndVerify(a, raw, func(err error) {
			a.Nil(err)

			// note that when we are targeting single blobs, the relative path is empty ("") since the root path already points to the blob
			validateRemoveTransfersAreScheduled(a, true, []string{""}, mockedRPC)
		})
	}
}

func TestRemoveBlobsUnderContainerWithFromTo(t *testing.T) {
	a := assert.New(t)
	bsc := getBlobServiceClient()

	// set up the container with numerous blobs
	cc, containerName := createNewContainer(a, bsc)
	defer deleteContainer(a, cc)
	blobList := scenarioHelper{}.generateCommonRemoteScenarioForBlob(a, cc, "")
	a.NotNil(cc)
	a.NotZero(len(blobList))

	// set up interceptor
	mockedRPC := interceptor{}
	jobsAdmin.ExecuteNewCopyJobPartOrder = func(order common.CopyJobPartOrderRequest) common.CopyJobPartOrderResponse {
		return mockedRPC.intercept(order)
	}
	mockedRPC.init()

	// construct the raw input to simulate user input
	rawContainerURLWithSAS := scenarioHelper{}.getRawContainerURLWithSAS(a, containerName)
	raw := getDefaultRemoveRawInput(rawContainerURLWithSAS.String())
	raw.fromTo = "BlobTrash"
	raw.recursive = true
	raw.includeDirectoryStubs = false // The test target is a DFS account, which coincidentally created our directory stubs. Thus, we mustn't include them, since this is a test of blob.

	runOldCopyAndVerify(a, raw, func(err error) {
		a.Nil(err)

		// validate that the right number of transfers were scheduled
		a.Equal(len(blobList), len(mockedRPC.transfers))

		// validate that the right transfers were sent
		validateRemoveTransfersAreScheduled(a, true, blobList, mockedRPC)
	})

	// turn off recursive, this time only top blobs should be deleted
	raw.recursive = false
	mockedRPC.reset()

	runOldCopyAndVerify(a, raw, func(err error) {
		a.Nil(err)
		a.NotEqual(len(blobList), len(mockedRPC.transfers))

		for _, transfer := range mockedRPC.transfers {
			source := strings.TrimPrefix(transfer.Source, "/")
			a.False(strings.Contains(source, common.AZCOPY_PATH_SEPARATOR_STRING))
		}
	})
}

func TestRemoveBlobsUnderVirtualDirWithFromTo(t *testing.T) {
	a := assert.New(t)
	t.Skip("Enable after setting Account to non-HNS")
	bsc := getBlobServiceClient()
	vdirName := "vdir1/vdir2/vdir3/"

	// set up the container with numerous blobs
	cc, containerName := createNewContainer(a, bsc)
	defer deleteContainer(a, cc)
	blobList := scenarioHelper{}.generateCommonRemoteScenarioForBlob(a, cc, vdirName)
	a.NotNil(cc)
	a.NotZero(len(blobList))

	// set up interceptor
	mockedRPC := interceptor{}
	jobsAdmin.ExecuteNewCopyJobPartOrder = func(order common.CopyJobPartOrderRequest) common.CopyJobPartOrderResponse {
		return mockedRPC.intercept(order)
	}
	mockedRPC.init()

	// construct the raw input to simulate user input
	rawVirtualDirectoryURLWithSAS := scenarioHelper{}.getRawBlobURLWithSAS(a, containerName, vdirName)
	raw := getDefaultRemoveRawInput(rawVirtualDirectoryURLWithSAS.String())
	raw.fromTo = "BlobTrash"
	raw.recursive = true

	runOldCopyAndVerify(a, raw, func(err error) {
		a.Nil(err)

		// validate that the right number of transfers were scheduled
		a.Equal(len(blobList), len(mockedRPC.transfers))

		// validate that the right transfers were sent
		expectedTransfers := scenarioHelper{}.shaveOffPrefix(blobList, vdirName)
		validateRemoveTransfersAreScheduled(a, true, expectedTransfers, mockedRPC)
	})

	// turn off recursive, this time only top blobs should be deleted
	raw.recursive = false
	mockedRPC.reset()

	runOldCopyAndVerify(a, raw, func(err error) {
		a.Nil(err)
		a.NotEqual(len(blobList), len(mockedRPC.transfers))

		for _, transfer := range mockedRPC.transfers {
			source := strings.TrimPrefix(transfer.Source, "/")
			a.False(strings.Contains(source, common.AZCOPY_PATH_SEPARATOR_STRING))
		}
	})
}

func TestPermDeleteSnapshotsVersionsUnderSingleBlob(t *testing.T) {
	a := assert.New(t)
	bsc := setUpAccountPermDelete(a)
	os.Setenv("AZCOPY_DISABLE_HIERARCHICAL_SCAN", "true")

	time.Sleep(time.Second * 10)

	// set up the container with numerous blobs
	cc, containerName := createNewContainer(a, bsc)
	defer deleteContainer(a, cc)
	blobName, blobList, _ := scenarioHelper{}.generateCommonRemoteScenarioForSoftDelete(a, cc, "")
	a.NotNil(cc)
	a.Equal(3, len(blobList))

	pager := cc.NewListBlobsFlatPager(&container.ListBlobsFlatOptions{
		Prefix:  to.Ptr(blobName),
		Include: container.ListBlobsInclude{Deleted: true, Snapshots: true},
	})
	list, err := pager.NextPage(ctx)
	a.Nil(err)
	a.NotNil(list.Segment.BlobItems)
	a.Equal(4, len(list.Segment.BlobItems))

	// set up interceptor
	mockedRPC := interceptor{}
	jobsAdmin.ExecuteNewCopyJobPartOrder = func(order common.CopyJobPartOrderRequest) common.CopyJobPartOrderResponse {
		return mockedRPC.intercept(order)
	}
	mockedRPC.init()

	// construct the raw input to simulate user input
	rawBlobURLWithSAS := scenarioHelper{}.getRawBlobURLWithSAS(a, containerName, blobName)
	raw := getDefaultRemoveRawInput(rawBlobURLWithSAS.String())
	raw.recursive = true
	raw.permanentDeleteOption = "snapshotsandversions"
	runOldCopyAndVerify(a, raw, func(err error) {
		a.Nil(err)

		// validate that the right number of transfers were scheduled
		a.Equal(3, len(mockedRPC.transfers))
	})
}

func TestPermDeleteSnapshotsVersionsUnderContainer(t *testing.T) {
	a := assert.New(t)
	bsc := setUpAccountPermDelete(a)
	os.Setenv("AZCOPY_DISABLE_HIERARCHICAL_SCAN", "true")

	time.Sleep(time.Second * 10)

	// set up the container with numerous blobs
	cc, containerName := createNewContainer(a, bsc)
	defer deleteContainer(a, cc)
	_, blobList, listOfTransfers := scenarioHelper{}.generateCommonRemoteScenarioForSoftDelete(a, cc, "")
	a.NotNil(cc)
	a.Equal(3, len(blobList))

	// set up interceptor
	mockedRPC := interceptor{}
	jobsAdmin.ExecuteNewCopyJobPartOrder = func(order common.CopyJobPartOrderRequest) common.CopyJobPartOrderResponse {
		return mockedRPC.intercept(order)
	}
	mockedRPC.init()

	// construct the raw input to simulate user input
	rawContainerURLWithSAS := scenarioHelper{}.getRawContainerURLWithSAS(a, containerName)
	raw := getDefaultRemoveRawInput(rawContainerURLWithSAS.String())
	raw.recursive = true
	raw.permanentDeleteOption = "snapshotsandversions"
	runOldCopyAndVerify(a, raw, func(err error) {
		a.Nil(err)

		// validate that the right number of transfers were scheduled
		a.Equal(len(listOfTransfers), len(mockedRPC.transfers))
	})
}

func setUpAccountPermDelete(a *assert.Assertions) *blobservice.Client {
	accountName, accountKey := getAccountAndKey()
	rawURL := fmt.Sprintf("https://%s.blob.core.windows.net/", accountName)

	credential, err := blob.NewSharedKeyCredential(accountName, accountKey)
	if err != nil {
		panic(err)
	}

	client, err := blobservice.NewClientWithSharedKeyCredential(rawURL, credential, nil)
	if err != nil {
		log.Fatal(err)
	}

	sasURL, err := client.GetSASURL(
		blobsas.AccountResourceTypes{Service: true, Container: true, Object: true},
		blobsas.AccountPermissions{Read: true, List: true, Write: true, Delete: true, PermanentDelete: true, DeletePreviousVersion: true, Add: true, Create: true, Update: true, Process: true, Tag: true},
		time.Now().Add(12*time.Hour),
		nil)

	if err != nil {
		log.Fatal(err)
	}
	client, err = blobservice.NewClientWithNoCredential(sasURL, nil)
	if err != nil {
		log.Fatal(err)
	}
	_, err = client.SetProperties(ctx, &blobservice.SetPropertiesOptions{
		DeleteRetentionPolicy: &blobservice.RetentionPolicy{Enabled: to.Ptr(true), Days: to.Ptr(int32(5)), AllowPermanentDelete: to.Ptr(true)},
	})
	a.Nil(err)

	return client
}
