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
	"bytes"
	"context"
	"encoding/json"
	"sort"
	"strings"
	"testing"

	"github.com/Azure/azure-sdk-for-go/sdk/azcore/streaming"
	"github.com/Azure/azure-sdk-for-go/sdk/azcore/to"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/blockblob"
	"github.com/Azure/azure-storage-azcopy/v10/jobsAdmin"
	"github.com/stretchr/testify/assert"

	"github.com/Azure/azure-storage-azcopy/v10/common"
)

// regular blob->file sync
func TestSyncS2SWithSingleBlob(t *testing.T) {
	a := assert.New(t)
	bsc := getBlobServiceClient()
	srcContainerClient, srcContainerName := createNewContainer(a, bsc)
	dstContainerClient, dstContainerName := createNewContainer(a, bsc)
	defer deleteContainer(a, srcContainerClient)
	defer deleteContainer(a, dstContainerClient)

	for _, blobName := range []string{"singleblobisbest", "打麻将.txt", "%4509%4254$85140&"} {
		// set up the source container with a single blob
		blobList := []string{blobName}
		scenarioHelper{}.generateBlobsFromList(a, srcContainerClient, blobList, blockBlobDefaultData)

		// set up the destination container with the same single blob
		scenarioHelper{}.generateBlobsFromList(a, dstContainerClient, blobList, blockBlobDefaultData)

		// set up interceptor
		mockedRPC := interceptor{}
		jobsAdmin.ExecuteNewCopyJobPartOrder = func(order common.CopyJobPartOrderRequest) common.CopyJobPartOrderResponse {
			return mockedRPC.intercept(order)
		}
		mockedRPC.init()

		// construct the raw input to simulate user input
		srcBlobURLWithSAS := scenarioHelper{}.getRawBlobURLWithSAS(a, srcContainerName, blobList[0])
		dstBlobURLWithSAS := scenarioHelper{}.getRawBlobURLWithSAS(a, dstContainerName, blobList[0])
		raw := getDefaultSyncRawInput(srcBlobURLWithSAS.String(), dstBlobURLWithSAS.String())

		// the destination was created after the source, so no sync should happen
		runSyncAndVerify(a, raw, func(err error) {
			a.NoError(err)

			// validate that the right number of transfers were scheduled
			a.Zero(len(mockedRPC.transfers))
		})

		// recreate the source blob to have a later last modified time
		scenarioHelper{}.generateBlobsFromList(a, srcContainerClient, blobList, blockBlobDefaultData)
		mockedRPC.reset()

		runSyncAndVerify(a, raw, func(err error) {
			a.NoError(err)
			validateS2SSyncTransfersAreScheduled(a, []string{""}, mockedRPC)
		})
	}
}

// regular container->container sync but destination is empty, so everything has to be transferred
func TestSyncS2SWithEmptyDestination(t *testing.T) {
	a := assert.New(t)
	bsc := getBlobServiceClient()
	srcContainerClient, srcContainerName := createNewContainer(a, bsc)
	dstContainerClient, dstContainerName := createNewContainer(a, bsc)
	defer deleteContainer(a, srcContainerClient)
	defer deleteContainer(a, dstContainerClient)

	// set up the source container with numerous blobs
	blobList := scenarioHelper{}.generateCommonRemoteScenarioForBlob(a, srcContainerClient, "")
	a.NotZero(len(blobList))

	// set up interceptor
	mockedRPC := interceptor{}
	jobsAdmin.ExecuteNewCopyJobPartOrder = func(order common.CopyJobPartOrderRequest) common.CopyJobPartOrderResponse {
		return mockedRPC.intercept(order)
	}
	mockedRPC.init()

	// construct the raw input to simulate user input
	srcContainerURLWithSAS := scenarioHelper{}.getRawContainerURLWithSAS(a, srcContainerName)
	dstContainerURLWithSAS := scenarioHelper{}.getRawContainerURLWithSAS(a, dstContainerName)
	raw := getDefaultSyncRawInput(srcContainerURLWithSAS.String(), dstContainerURLWithSAS.String())

	// all blobs at source should be synced to destination
	runSyncAndVerify(a, raw, func(err error) {
		a.NoError(err)

		// validate that the right number of transfers were scheduled
		a.Equal(len(blobList), len(mockedRPC.transfers))

		// validate that the right transfers were sent
		validateS2SSyncTransfersAreScheduled(a, blobList, mockedRPC)
	})

	// turn off recursive, this time only top blobs should be transferred
	raw.recursive = false
	mockedRPC.reset()
	runSyncAndVerify(a, raw, func(err error) {
		a.NoError(err)
		a.NotEqual(len(blobList), len(mockedRPC.transfers))

		for _, transfer := range mockedRPC.transfers {
			source := strings.TrimPrefix(transfer.Source, "/")
			a.False(strings.Contains(source, common.AZCOPY_PATH_SEPARATOR_STRING))
		}
	})
}

// regular container->container sync but destination is identical to the source, transfers are scheduled based on lmt
func TestSyncS2SWithIdenticalDestination(t *testing.T) {
	a := assert.New(t)
	bsc := getBlobServiceClient()
	srcContainerClient, srcContainerName := createNewContainer(a, bsc)
	dstContainerClient, dstContainerName := createNewContainer(a, bsc)
	defer deleteContainer(a, srcContainerClient)
	defer deleteContainer(a, dstContainerClient)

	// set up the source container with numerous blobs
	blobList := scenarioHelper{}.generateCommonRemoteScenarioForBlob(a, srcContainerClient, "")
	a.NotZero(len(blobList))

	// set up the destination with the exact same files
	scenarioHelper{}.generateBlobsFromList(a, dstContainerClient, blobList, blockBlobDefaultData)

	// set up interceptor
	mockedRPC := interceptor{}
	jobsAdmin.ExecuteNewCopyJobPartOrder = func(order common.CopyJobPartOrderRequest) common.CopyJobPartOrderResponse {
		return mockedRPC.intercept(order)
	}
	mockedRPC.init()

	// construct the raw input to simulate user input
	srcContainerURLWithSAS := scenarioHelper{}.getRawContainerURLWithSAS(a, srcContainerName)
	dstContainerURLWithSAS := scenarioHelper{}.getRawContainerURLWithSAS(a, dstContainerName)
	raw := getDefaultSyncRawInput(srcContainerURLWithSAS.String(), dstContainerURLWithSAS.String())

	// nothing should be sync since the source is older
	runSyncAndVerify(a, raw, func(err error) {
		a.NoError(err)

		// validate that the right number of transfers were scheduled
		a.Zero(len(mockedRPC.transfers))
	})

	// refresh the source blobs' last modified time so that they get synced
	scenarioHelper{}.generateBlobsFromList(a, srcContainerClient, blobList, blockBlobDefaultData)
	mockedRPC.reset()
	runSyncAndVerify(a, raw, func(err error) {
		a.NoError(err)
		validateS2SSyncTransfersAreScheduled(a, blobList, mockedRPC)
	})
}

// regular container->container sync where destination is missing some files from source, and also has some extra files
func TestSyncS2SWithMismatchedDestination(t *testing.T) {
	a := assert.New(t)
	t.Skip("Enable after setting Account to non-HNS")
	bsc := getBlobServiceClient()
	srcContainerClient, srcContainerName := createNewContainer(a, bsc)
	dstContainerClient, dstContainerName := createNewContainer(a, bsc)
	defer deleteContainer(a, srcContainerClient)
	defer deleteContainer(a, dstContainerClient)

	// set up the container with numerous blobs
	blobList := scenarioHelper{}.generateCommonRemoteScenarioForBlob(a, srcContainerClient, "")
	a.NotZero(len(blobList))

	// set up the destination with half of the blobs from source
	scenarioHelper{}.generateBlobsFromList(a, dstContainerClient, blobList[0:len(blobList)/2], blockBlobDefaultData)
	expectedOutput := blobList[len(blobList)/2:] // the missing half of source blobs should be transferred

	// add some extra blobs that shouldn't be included
	scenarioHelper{}.generateCommonRemoteScenarioForBlob(a, dstContainerClient, "extra")

	// set up interceptor
	mockedRPC := interceptor{}
	jobsAdmin.ExecuteNewCopyJobPartOrder = func(order common.CopyJobPartOrderRequest) common.CopyJobPartOrderResponse {
		return mockedRPC.intercept(order)
	}
	mockedRPC.init()

	// construct the raw input to simulate user input
	srcContainerURLWithSAS := scenarioHelper{}.getRawContainerURLWithSAS(a, srcContainerName)
	dstContainerURLWithSAS := scenarioHelper{}.getRawContainerURLWithSAS(a, dstContainerName)
	raw := getDefaultSyncRawInput(srcContainerURLWithSAS.String(), dstContainerURLWithSAS.String())

	runSyncAndVerify(a, raw, func(err error) {
		a.NoError(err)
		validateS2SSyncTransfersAreScheduled(a, expectedOutput, mockedRPC)

		// make sure the extra blobs were deleted
		extraFilesFound := false
		pager := dstContainerClient.NewListBlobsFlatPager(nil)
		for pager.More() {
			listResponse, err := pager.NextPage(ctx)
			a.NoError(err)

			// if ever the extra blobs are found, note it down
			for _, blob := range listResponse.Segment.BlobItems {
				if strings.Contains(*blob.Name, "extra") {
					extraFilesFound = true
				}
			}
		}

		a.False(extraFilesFound)
	})
}

// include flag limits the scope of source/destination comparison
func TestSyncS2SWithIncludePatternFlag(t *testing.T) {
	a := assert.New(t)
	bsc := getBlobServiceClient()
	srcContainerClient, srcContainerName := createNewContainer(a, bsc)
	dstContainerClient, dstContainerName := createNewContainer(a, bsc)
	defer deleteContainer(a, srcContainerClient)
	defer deleteContainer(a, dstContainerClient)

	// set up the source container with numerous blobs
	blobList := scenarioHelper{}.generateCommonRemoteScenarioForBlob(a, srcContainerClient, "")
	a.NotZero(len(blobList))

	// add special blobs that we wish to include
	blobsToInclude := []string{"important.pdf", "includeSub/amazing.jpeg", "exactName"}
	scenarioHelper{}.generateBlobsFromList(a, srcContainerClient, blobsToInclude, blockBlobDefaultData)
	includeString := "*.pdf;*.jpeg;exactName"

	// set up interceptor
	mockedRPC := interceptor{}
	jobsAdmin.ExecuteNewCopyJobPartOrder = func(order common.CopyJobPartOrderRequest) common.CopyJobPartOrderResponse {
		return mockedRPC.intercept(order)
	}
	mockedRPC.init()

	// construct the raw input to simulate user input
	srcContainerURLWithSAS := scenarioHelper{}.getRawContainerURLWithSAS(a, srcContainerName)
	dstContainerURLWithSAS := scenarioHelper{}.getRawContainerURLWithSAS(a, dstContainerName)
	raw := getDefaultSyncRawInput(srcContainerURLWithSAS.String(), dstContainerURLWithSAS.String())
	raw.include = includeString

	// verify that only the blobs specified by the include flag are synced
	runSyncAndVerify(a, raw, func(err error) {
		a.NoError(err)
		validateS2SSyncTransfersAreScheduled(a, blobsToInclude, mockedRPC)
	})
}

// exclude flag limits the scope of source/destination comparison
func TestSyncS2SWithExcludePatternFlag(t *testing.T) {
	a := assert.New(t)
	bsc := getBlobServiceClient()
	srcContainerClient, srcContainerName := createNewContainer(a, bsc)
	dstContainerClient, dstContainerName := createNewContainer(a, bsc)
	defer deleteContainer(a, srcContainerClient)
	defer deleteContainer(a, dstContainerClient)

	// set up the source container with numerous blobs
	blobList := scenarioHelper{}.generateCommonRemoteScenarioForBlob(a, srcContainerClient, "")
	a.NotZero(len(blobList))

	// add special blobs that we wish to exclude
	blobsToExclude := []string{"notGood.pdf", "excludeSub/lame.jpeg", "exactName"}
	scenarioHelper{}.generateBlobsFromList(a, srcContainerClient, blobsToExclude, blockBlobDefaultData)
	excludeString := "*.pdf;*.jpeg;exactName"

	// set up interceptor
	mockedRPC := interceptor{}
	jobsAdmin.ExecuteNewCopyJobPartOrder = func(order common.CopyJobPartOrderRequest) common.CopyJobPartOrderResponse {
		return mockedRPC.intercept(order)
	}
	mockedRPC.init()

	// construct the raw input to simulate user input
	srcContainerURLWithSAS := scenarioHelper{}.getRawContainerURLWithSAS(a, srcContainerName)
	dstContainerURLWithSAS := scenarioHelper{}.getRawContainerURLWithSAS(a, dstContainerName)
	raw := getDefaultSyncRawInput(srcContainerURLWithSAS.String(), dstContainerURLWithSAS.String())
	raw.exclude = excludeString

	// make sure the list doesn't include the blobs specified by the exclude flag
	runSyncAndVerify(a, raw, func(err error) {
		a.NoError(err)
		validateS2SSyncTransfersAreScheduled(a, blobList, mockedRPC)
	})
}

// include and exclude flag can work together to limit the scope of source/destination comparison
func TestSyncS2SWithIncludeAndExcludePatternFlag(t *testing.T) {
	a := assert.New(t)
	bsc := getBlobServiceClient()
	srcContainerClient, srcContainerName := createNewContainer(a, bsc)
	dstContainerClient, dstContainerName := createNewContainer(a, bsc)
	defer deleteContainer(a, srcContainerClient)
	defer deleteContainer(a, dstContainerClient)

	// set up the source container with numerous blobs
	blobList := scenarioHelper{}.generateCommonRemoteScenarioForBlob(a, srcContainerClient, "")
	a.NotZero(len(blobList))

	// add special blobs that we wish to include
	blobsToInclude := []string{"important.pdf", "includeSub/amazing.jpeg"}
	scenarioHelper{}.generateBlobsFromList(a, srcContainerClient, blobsToInclude, blockBlobDefaultData)
	includeString := "*.pdf;*.jpeg;exactName"

	// add special blobs that we wish to exclude
	// note that the excluded files also match the include string
	blobsToExclude := []string{"sorry.pdf", "exclude/notGood.jpeg", "exactName", "sub/exactName"}
	scenarioHelper{}.generateBlobsFromList(a, srcContainerClient, blobsToExclude, blockBlobDefaultData)
	excludeString := "so*;not*;exactName"

	// set up interceptor
	mockedRPC := interceptor{}
	jobsAdmin.ExecuteNewCopyJobPartOrder = func(order common.CopyJobPartOrderRequest) common.CopyJobPartOrderResponse {
		return mockedRPC.intercept(order)
	}
	mockedRPC.init()

	// construct the raw input to simulate user input
	srcContainerURLWithSAS := scenarioHelper{}.getRawContainerURLWithSAS(a, srcContainerName)
	dstContainerURLWithSAS := scenarioHelper{}.getRawContainerURLWithSAS(a, dstContainerName)
	raw := getDefaultSyncRawInput(srcContainerURLWithSAS.String(), dstContainerURLWithSAS.String())
	raw.include = includeString
	raw.exclude = excludeString

	// verify that only the blobs specified by the include flag are synced
	runSyncAndVerify(a, raw, func(err error) {
		a.NoError(err)
		validateS2SSyncTransfersAreScheduled(a, blobsToInclude, mockedRPC)
	})
}

// a specific path is avoided in the comparison
func TestSyncS2SWithExcludePathFlag(t *testing.T) {
	a := assert.New(t)
	bsc := getBlobServiceClient()
	srcContainerClient, srcContainerName := createNewContainer(a, bsc)
	dstContainerClient, dstContainerName := createNewContainer(a, bsc)
	defer deleteContainer(a, srcContainerClient)
	defer deleteContainer(a, dstContainerClient)

	// set up the source container with numerous blobs
	blobList := scenarioHelper{}.generateCommonRemoteScenarioForBlob(a, srcContainerClient, "")
	a.NotZero(len(blobList))

	// add special blobs that we wish to exclude
	blobsToExclude := []string{"excludeSub/notGood.pdf", "excludeSub/lame.jpeg", "exactName"}
	scenarioHelper{}.generateBlobsFromList(a, srcContainerClient, blobsToExclude, blockBlobDefaultData)
	excludeString := "excludeSub;exactName"

	// set up interceptor
	mockedRPC := interceptor{}
	jobsAdmin.ExecuteNewCopyJobPartOrder = func(order common.CopyJobPartOrderRequest) common.CopyJobPartOrderResponse {
		return mockedRPC.intercept(order)
	}
	mockedRPC.init()

	// construct the raw input to simulate user input
	srcContainerURLWithSAS := scenarioHelper{}.getRawContainerURLWithSAS(a, srcContainerName)
	dstContainerURLWithSAS := scenarioHelper{}.getRawContainerURLWithSAS(a, dstContainerName)
	raw := getDefaultSyncRawInput(srcContainerURLWithSAS.String(), dstContainerURLWithSAS.String())
	raw.excludePath = excludeString

	// make sure the list doesn't include the blobs specified by the exclude flag
	runSyncAndVerify(a, raw, func(err error) {
		a.NoError(err)
		validateS2SSyncTransfersAreScheduled(a, blobList, mockedRPC)
	})

	// now set up the destination with the blobs to be excluded, and make sure they are not touched
	scenarioHelper{}.generateBlobsFromList(a, dstContainerClient, blobsToExclude, blockBlobDefaultData)

	// re-create the ones at the source so that their lmts are newer
	scenarioHelper{}.generateBlobsFromList(a, srcContainerClient, blobsToExclude, blockBlobDefaultData)

	mockedRPC.reset()
	runSyncAndVerify(a, raw, func(err error) {
		a.NoError(err)
		validateS2SSyncTransfersAreScheduled(a, blobList, mockedRPC)

		// make sure the extra blobs were not touched
		for _, blobName := range blobsToExclude {
			exists := scenarioHelper{}.blobExists(dstContainerClient.NewBlobClient(blobName))
			a.True(exists)
		}
	})
}

// validate the bug fix for this scenario
func TestSyncS2SWithMissingDestination(t *testing.T) {
	a := assert.New(t)
	bsc := getBlobServiceClient()
	srcContainerClient, srcContainerName := createNewContainer(a, bsc)
	dstContainerClient, dstContainerName := createNewContainer(a, bsc)
	defer deleteContainer(a, srcContainerClient)

	// delete the destination container to simulate non-existing destination, or recently removed destination
	deleteContainer(a, dstContainerClient)

	// set up the container with numerous blobs
	blobList := scenarioHelper{}.generateCommonRemoteScenarioForBlob(a, srcContainerClient, "")
	a.NotZero(len(blobList))

	// set up interceptor
	mockedRPC := interceptor{}
	jobsAdmin.ExecuteNewCopyJobPartOrder = func(order common.CopyJobPartOrderRequest) common.CopyJobPartOrderResponse {
		return mockedRPC.intercept(order)
	}
	mockedRPC.init()

	// construct the raw input to simulate user input
	srcContainerURLWithSAS := scenarioHelper{}.getRawContainerURLWithSAS(a, srcContainerName)
	dstContainerURLWithSAS := scenarioHelper{}.getRawContainerURLWithSAS(a, dstContainerName)
	raw := getDefaultSyncRawInput(srcContainerURLWithSAS.String(), dstContainerURLWithSAS.String())

	// verify error is thrown
	runSyncAndVerify(a, raw, func(err error) {
		// error should not be nil, but the app should not crash either
		a.NotNil(err)

		// validate that the right number of transfers were scheduled
		a.Zero(len(mockedRPC.transfers))
	})
}

// there is a type mismatch between the source and destination
func TestSyncS2SMismatchContainerAndBlob(t *testing.T) {
	a := assert.New(t)
	bsc := getBlobServiceClient()
	srcContainerClient, srcContainerName := createNewContainer(a, bsc)
	dstContainerClient, dstContainerName := createNewContainer(a, bsc)
	defer deleteContainer(a, srcContainerClient)
	defer deleteContainer(a, dstContainerClient)

	// set up the source container with numerous blobs
	blobList := scenarioHelper{}.generateCommonRemoteScenarioForBlob(a, srcContainerClient, "")
	a.NotZero(len(blobList))

	// set up the destination container with a single blob
	singleBlobName := "single"
	scenarioHelper{}.generateBlobsFromList(a, dstContainerClient, []string{singleBlobName}, blockBlobDefaultData)

	// set up interceptor
	mockedRPC := interceptor{}
	jobsAdmin.ExecuteNewCopyJobPartOrder = func(order common.CopyJobPartOrderRequest) common.CopyJobPartOrderResponse {
		return mockedRPC.intercept(order)
	}
	mockedRPC.init()

	// construct the raw input to simulate user input
	srcContainerURLWithSAS := scenarioHelper{}.getRawContainerURLWithSAS(a, srcContainerName)
	dstBlobURLWithSAS := scenarioHelper{}.getRawBlobURLWithSAS(a, dstContainerName, singleBlobName)
	raw := getDefaultSyncRawInput(srcContainerURLWithSAS.String(), dstBlobURLWithSAS.String())

	// type mismatch, we should not get an error
	runSyncAndVerify(a, raw, func(err error) {
		a.NoError(err)

		// validate that the right number of transfers were scheduled
		a.Equal(len(mockedRPC.transfers), len(blobList))
	})

	// reverse the source and destination
	raw = getDefaultSyncRawInput(dstBlobURLWithSAS.String(), srcContainerURLWithSAS.String())

	// type mismatch again, we should also not get an error
	runSyncAndVerify(a, raw, func(err error) {
		a.NoError(err)

		// validate that the right number of transfers were scheduled
		a.Equal(len(mockedRPC.transfers), len(blobList))
	})
}

// container <-> virtual dir sync
func TestSyncS2SContainerAndEmptyVirtualDir(t *testing.T) {
	a := assert.New(t)
	bsc := getBlobServiceClient()
	srcContainerClient, srcContainerName := createNewContainer(a, bsc)
	dstContainerClient, dstContainerName := createNewContainer(a, bsc)
	defer deleteContainer(a, srcContainerClient)
	defer deleteContainer(a, dstContainerClient)

	// set up the source container with numerous blobs
	blobList := scenarioHelper{}.generateCommonRemoteScenarioForBlob(a, srcContainerClient, "")
	a.NotZero(len(blobList))

	// set up interceptor
	mockedRPC := interceptor{}
	jobsAdmin.ExecuteNewCopyJobPartOrder = func(order common.CopyJobPartOrderRequest) common.CopyJobPartOrderResponse {
		return mockedRPC.intercept(order)
	}
	mockedRPC.init()

	// construct the raw input to simulate user input
	srcContainerURLWithSAS := scenarioHelper{}.getRawContainerURLWithSAS(a, srcContainerName)
	dstVirtualDirURLWithSAS := scenarioHelper{}.getRawBlobURLWithSAS(a, dstContainerName, "emptydir/")
	raw := getDefaultSyncRawInput(srcContainerURLWithSAS.String(), dstVirtualDirURLWithSAS.String())

	// verify that targeting a virtual directory works fine
	runSyncAndVerify(a, raw, func(err error) {
		a.NoError(err)

		// validate that the right number of transfers were scheduled
		a.Equal(len(blobList), len(mockedRPC.transfers))

		// validate that the right transfers were sent
		validateS2SSyncTransfersAreScheduled(a, blobList, mockedRPC)
	})

	// turn off recursive, this time only top blobs should be transferred
	raw.recursive = false
	mockedRPC.reset()

	runSyncAndVerify(a, raw, func(err error) {
		a.NoError(err)
		a.NotEqual(len(blobList), len(mockedRPC.transfers))

		for _, transfer := range mockedRPC.transfers {
			source := strings.TrimPrefix(transfer.Source, "/")
			a.False(strings.Contains(source, common.AZCOPY_PATH_SEPARATOR_STRING))
		}
	})
}

// regular vdir -> vdir sync
func TestSyncS2SBetweenVirtualDirs(t *testing.T) {
	a := assert.New(t)
	bsc := getBlobServiceClient()
	srcContainerClient, srcContainerName := createNewContainer(a, bsc)
	dstContainerClient, dstContainerName := createNewContainer(a, bsc)
	defer deleteContainer(a, srcContainerClient)
	defer deleteContainer(a, dstContainerClient)

	// set up the source container with numerous blobs
	vdirName := "vdir"
	blobList := scenarioHelper{}.generateCommonRemoteScenarioForBlob(a, srcContainerClient, vdirName+common.AZCOPY_PATH_SEPARATOR_STRING)
	a.NotZero(len(blobList))

	// set up the destination with the exact same files
	scenarioHelper{}.generateBlobsFromList(a, dstContainerClient, blobList, blockBlobDefaultData)

	// set up interceptor
	mockedRPC := interceptor{}
	jobsAdmin.ExecuteNewCopyJobPartOrder = func(order common.CopyJobPartOrderRequest) common.CopyJobPartOrderResponse {
		return mockedRPC.intercept(order)
	}
	mockedRPC.init()

	// construct the raw input to simulate user input
	srcContainerURLWithSAS := scenarioHelper{}.getRawContainerURLWithSAS(a, srcContainerName)
	dstContainerURLWithSAS := scenarioHelper{}.getRawContainerURLWithSAS(a, dstContainerName)
	srcContainerURLWithSAS.Path += common.AZCOPY_PATH_SEPARATOR_STRING + vdirName
	dstContainerURLWithSAS.Path += common.AZCOPY_PATH_SEPARATOR_STRING + vdirName
	raw := getDefaultSyncRawInput(srcContainerURLWithSAS.String(), dstContainerURLWithSAS.String())

	// nothing should be synced since the source is older
	runSyncAndVerify(a, raw, func(err error) {
		a.NoError(err)

		// validate that the right number of transfers were scheduled
		a.Zero(len(mockedRPC.transfers))
	})

	// refresh the blobs' last modified time so that they are newer
	scenarioHelper{}.generateBlobsFromList(a, srcContainerClient, blobList, blockBlobDefaultData)
	mockedRPC.reset()
	expectedList := scenarioHelper{}.shaveOffPrefix(blobList, vdirName+common.AZCOPY_PATH_SEPARATOR_STRING)
	runSyncAndVerify(a, raw, func(err error) {
		a.NoError(err)
		validateS2SSyncTransfersAreScheduled(a, expectedList, mockedRPC)
	})
}

// examine situation where a blob has the same name as virtual dir
// trailing slash is used to disambiguate the path as a vdir
func TestSyncS2SBetweenVirtualDirsWithConflictingBlob(t *testing.T) {
	a := assert.New(t)
	t.Skip("Enable after setting Account to non-HNS")
	bsc := getBlobServiceClient()
	srcContainerClient, srcContainerName := createNewContainer(a, bsc)
	dstContainerClient, dstContainerName := createNewContainer(a, bsc)
	defer deleteContainer(a, srcContainerClient)
	defer deleteContainer(a, dstContainerClient)

	// set up the source container with numerous blobs
	vdirName := "vdir"
	blobList := scenarioHelper{}.generateCommonRemoteScenarioForBlob(a, srcContainerClient, vdirName+common.AZCOPY_PATH_SEPARATOR_STRING)
	a.NotZero(len(blobList))

	// set up the destination with the exact same files
	scenarioHelper{}.generateBlobsFromList(a, dstContainerClient, blobList, blockBlobDefaultData)

	// create a blob at the destination with the exact same name as the vdir
	scenarioHelper{}.generateBlobsFromList(a, dstContainerClient, []string{vdirName}, blockBlobDefaultData)

	// set up interceptor
	mockedRPC := interceptor{}
	jobsAdmin.ExecuteNewCopyJobPartOrder = func(order common.CopyJobPartOrderRequest) common.CopyJobPartOrderResponse {
		return mockedRPC.intercept(order)
	}
	mockedRPC.init()

	// case 1: vdir -> blob sync: should fail
	srcContainerURLWithSAS := scenarioHelper{}.getRawContainerURLWithSAS(a, srcContainerName)
	dstContainerURLWithSAS := scenarioHelper{}.getRawContainerURLWithSAS(a, dstContainerName)
	srcContainerURLWithSAS.Path += common.AZCOPY_PATH_SEPARATOR_STRING + vdirName
	dstContainerURLWithSAS.Path += common.AZCOPY_PATH_SEPARATOR_STRING + vdirName
	// construct the raw input to simulate user input
	raw := getDefaultSyncRawInput(srcContainerURLWithSAS.String(), dstContainerURLWithSAS.String())
	runSyncAndVerify(a, raw, func(err error) {
		a.NotNil(err)

		// validate that the right number of transfers were scheduled
		a.Zero(len(mockedRPC.transfers))
	})

	// case 2: blob -> vdir sync: simply swap src and dst, should fail too
	raw = getDefaultSyncRawInput(dstContainerURLWithSAS.String(), srcContainerURLWithSAS.String())
	runSyncAndVerify(a, raw, func(err error) {
		a.NotNil(err)

		// validate that the right number of transfers were scheduled
		a.Zero(len(mockedRPC.transfers))
	})

	// case 3: blob -> blob: if source is also a blob, then single blob to blob sync happens
	// create a blob at the source with the exact same name as the vdir
	scenarioHelper{}.generateBlobsFromList(a, srcContainerClient, []string{vdirName}, blockBlobDefaultData)
	raw = getDefaultSyncRawInput(srcContainerURLWithSAS.String(), dstContainerURLWithSAS.String())
	runSyncAndVerify(a, raw, func(err error) {
		a.NoError(err)
		validateS2SSyncTransfersAreScheduled(a, []string{""}, mockedRPC)
	})

	// refresh the dst blobs' last modified time so that they are newer
	scenarioHelper{}.generateBlobsFromList(a, srcContainerClient, blobList, blockBlobDefaultData)
	mockedRPC.reset()

	// case 4: vdir -> vdir: adding a trailing slash helps to clarify it should be treated as virtual dir
	srcContainerURLWithSAS.Path += common.AZCOPY_PATH_SEPARATOR_STRING
	dstContainerURLWithSAS.Path += common.AZCOPY_PATH_SEPARATOR_STRING
	raw = getDefaultSyncRawInput(srcContainerURLWithSAS.String(), dstContainerURLWithSAS.String())
	expectedList := scenarioHelper{}.shaveOffPrefix(blobList, vdirName+common.AZCOPY_PATH_SEPARATOR_STRING)
	runSyncAndVerify(a, raw, func(err error) {
		a.NoError(err)
		validateS2SSyncTransfersAreScheduled(a, expectedList, mockedRPC)
	})
}

// sync a vdir with a blob representing an ADLS directory
// we should recognize this and sync with the virtual directory instead
func TestSyncS2SADLSDirectory(t *testing.T) {
	a := assert.New(t)
	bsc := getBlobServiceClient()
	srcContainerClient, srcContainerName := createNewContainer(a, bsc)
	dstContainerClient, dstContainerName := createNewContainer(a, bsc)
	defer deleteContainer(a, srcContainerClient)
	defer deleteContainer(a, dstContainerClient)

	// set up the source container with numerous blobs
	vdirName := "vdir"
	blobList := scenarioHelper{}.generateCommonRemoteScenarioForBlob(a, srcContainerClient, vdirName+common.AZCOPY_PATH_SEPARATOR_STRING)
	a.NotZero(len(blobList))

	// set up the destination with the exact same files
	scenarioHelper{}.generateBlobsFromList(a, dstContainerClient, blobList, blockBlobDefaultData)

	// create an ADLS Gen2 directory at the source with the exact same name as the vdir
	_, err := srcContainerClient.NewBlockBlobClient(vdirName).Upload(context.Background(), streaming.NopCloser(bytes.NewReader(nil)),
		&blockblob.UploadOptions{
			Metadata: map[string]*string{"hdi_isfolder": to.Ptr("true")},
		})
	a.NoError(err)

	// set up interceptor
	mockedRPC := interceptor{}
	jobsAdmin.ExecuteNewCopyJobPartOrder = func(order common.CopyJobPartOrderRequest) common.CopyJobPartOrderResponse {
		return mockedRPC.intercept(order)
	}
	mockedRPC.init()

	// ADLS Gen2 directory -> vdir sync: should work
	// but since the src files are older, nothing should be synced
	srcContainerURLWithSAS := scenarioHelper{}.getRawContainerURLWithSAS(a, srcContainerName)
	dstContainerURLWithSAS := scenarioHelper{}.getRawContainerURLWithSAS(a, dstContainerName)
	srcContainerURLWithSAS.Path += common.AZCOPY_PATH_SEPARATOR_STRING + vdirName
	dstContainerURLWithSAS.Path += common.AZCOPY_PATH_SEPARATOR_STRING + vdirName
	// construct the raw input to simulate user input
	raw := getDefaultSyncRawInput(srcContainerURLWithSAS.String(), dstContainerURLWithSAS.String())
	runSyncAndVerify(a, raw, func(err error) {
		a.NoError(err)

		// validate that the right number of transfers were scheduled
		a.Zero(len(mockedRPC.transfers))
	})

	// refresh the sources blobs' last modified time so that they are newer
	scenarioHelper{}.generateBlobsFromList(a, srcContainerClient, blobList, blockBlobDefaultData)
	mockedRPC.reset()

	expectedTransfers := scenarioHelper{}.shaveOffPrefix(blobList, vdirName+common.AZCOPY_PATH_SEPARATOR_STRING)
	runSyncAndVerify(a, raw, func(err error) {
		a.NoError(err)
		validateS2SSyncTransfersAreScheduled(a, expectedTransfers, mockedRPC)
	})
}

// testing multiple include regular expression
func TestSyncS2SWithIncludeRegexFlag(t *testing.T) {
	a := assert.New(t)
	bsc := getBlobServiceClient()
	srcContainerClient, srcContainerName := createNewContainer(a, bsc)
	dstContainerClient, dstContainerName := createNewContainer(a, bsc)
	defer deleteContainer(a, srcContainerClient)
	defer deleteContainer(a, dstContainerClient)

	// set up the source container with numerous blobs
	blobList := scenarioHelper{}.generateCommonRemoteScenarioForBlob(a, srcContainerClient, "")
	a.NotZero(len(blobList))

	// add special blobs that we wish to include
	blobsToInclude := []string{"tessssssssssssst.txt", "zxcfile.txt", "subOne/tetingessssss.jpeg", "subOne/subTwo/tessssst.pdf"}
	scenarioHelper{}.generateBlobsFromList(a, srcContainerClient, blobsToInclude, blockBlobDefaultData)
	includeString := "es{4,};^zxc"

	// set up interceptor
	mockedRPC := interceptor{}
	jobsAdmin.ExecuteNewCopyJobPartOrder = func(order common.CopyJobPartOrderRequest) common.CopyJobPartOrderResponse {
		return mockedRPC.intercept(order)
	}
	mockedRPC.init()

	// construct the raw input to simulate user input
	srcContainerURLWithSAS := scenarioHelper{}.getRawContainerURLWithSAS(a, srcContainerName)
	dstContainerURLWithSAS := scenarioHelper{}.getRawContainerURLWithSAS(a, dstContainerName)
	raw := getDefaultSyncRawInput(srcContainerURLWithSAS.String(), dstContainerURLWithSAS.String())
	raw.includeRegex = includeString

	// verify that only the blobs specified by the include flag are synced
	runSyncAndVerify(a, raw, func(err error) {
		a.NoError(err)
		// validate that the right number of transfers were scheduled
		a.Equal(len(blobsToInclude), len(mockedRPC.transfers))
		// comparing is names of files, since not in order need to sort each string and the compare them
		actualTransfer := []string{}
		for i := 0; i < len(mockedRPC.transfers); i++ {
			actualTransfer = append(actualTransfer, strings.Trim(mockedRPC.transfers[i].Source, "/"))
		}
		sort.Strings(actualTransfer)
		sort.Strings(blobsToInclude)
		a.Equal(blobsToInclude, actualTransfer)

		validateS2SSyncTransfersAreScheduled(a, blobsToInclude, mockedRPC)
	})
}

// testing multiple exclude regular expressions
func TestSyncS2SWithExcludeRegexFlag(t *testing.T) {
	a := assert.New(t)
	bsc := getBlobServiceClient()
	srcContainerClient, srcContainerName := createNewContainer(a, bsc)
	dstContainerClient, dstContainerName := createNewContainer(a, bsc)
	defer deleteContainer(a, srcContainerClient)
	defer deleteContainer(a, dstContainerClient)

	// set up the source container with blobs
	blobList := scenarioHelper{}.generateCommonRemoteScenarioForBlob(a, srcContainerClient, "")
	a.NotZero(len(blobList))

	// add special blobs that we wish to exclude
	blobsToExclude := []string{"tessssssssssssst.txt", "subOne/dogs.jpeg", "subOne/subTwo/tessssst.pdf"}
	scenarioHelper{}.generateBlobsFromList(a, srcContainerClient, blobsToExclude, blockBlobDefaultData)
	excludeString := "es{4,};o(g)"

	// set up interceptor
	mockedRPC := interceptor{}
	jobsAdmin.ExecuteNewCopyJobPartOrder = func(order common.CopyJobPartOrderRequest) common.CopyJobPartOrderResponse {
		return mockedRPC.intercept(order)
	}
	mockedRPC.init()

	// construct the raw input to simulate user input
	srcContainerURLWithSAS := scenarioHelper{}.getRawContainerURLWithSAS(a, srcContainerName)
	dstContainerURLWithSAS := scenarioHelper{}.getRawContainerURLWithSAS(a, dstContainerName)
	raw := getDefaultSyncRawInput(srcContainerURLWithSAS.String(), dstContainerURLWithSAS.String())
	raw.excludeRegex = excludeString

	// make sure the list doesn't include the blobs specified by the exclude flag
	runSyncAndVerify(a, raw, func(err error) {
		a.NoError(err)
		// validate that the right number of transfers were scheduled
		a.Equal(len(blobList), len(mockedRPC.transfers))
		// all blobs from the blobList are transferred
		validateS2SSyncTransfersAreScheduled(a, blobList, mockedRPC)
	})
}

// testing with both include and exclude regular expression flags
func TestSyncS2SWithIncludeAndExcludeRegexFlag(t *testing.T) {
	a := assert.New(t)
	bsc := getBlobServiceClient()
	srcContainerClient, srcContainerName := createNewContainer(a, bsc)
	dstContainerClient, dstContainerName := createNewContainer(a, bsc)
	defer deleteContainer(a, srcContainerClient)
	defer deleteContainer(a, dstContainerClient)

	// set up the source container with numerous blobs
	blobList := scenarioHelper{}.generateCommonRemoteScenarioForBlob(a, srcContainerClient, "")
	a.NotZero(len(blobList))

	// add special blobs that we wish to include
	blobsToInclude := []string{"tessssssssssssst.txt", "zxcfile.txt", "subOne/tetingessssss.jpeg"}
	scenarioHelper{}.generateBlobsFromList(a, srcContainerClient, blobsToInclude, blockBlobDefaultData)
	includeString := "es{4,};^zxc"

	// add special blobs that we wish to exclude
	blobsToExclude := []string{"zxca.txt", "subOne/dogs.jpeg", "subOne/subTwo/zxcat.pdf"}
	scenarioHelper{}.generateBlobsFromList(a, srcContainerClient, blobsToExclude, blockBlobDefaultData)
	excludeString := "^zxca;o(g)"

	// set up interceptor
	mockedRPC := interceptor{}
	jobsAdmin.ExecuteNewCopyJobPartOrder = func(order common.CopyJobPartOrderRequest) common.CopyJobPartOrderResponse {
		return mockedRPC.intercept(order)
	}
	mockedRPC.init()

	// construct the raw input to simulate user input
	srcContainerURLWithSAS := scenarioHelper{}.getRawContainerURLWithSAS(a, srcContainerName)
	dstContainerURLWithSAS := scenarioHelper{}.getRawContainerURLWithSAS(a, dstContainerName)
	raw := getDefaultSyncRawInput(srcContainerURLWithSAS.String(), dstContainerURLWithSAS.String())
	raw.includeRegex = includeString
	raw.excludeRegex = excludeString

	// verify that only the blobs specified by the include flag are synced
	runSyncAndVerify(a, raw, func(err error) {
		a.NoError(err)
		// validate that the right number of transfers were scheduled
		a.Equal(len(blobsToInclude), len(mockedRPC.transfers))
		// comparing is names of files, since not in order need to sort each string and the compare them
		actualTransfer := []string{}
		for i := 0; i < len(mockedRPC.transfers); i++ {
			actualTransfer = append(actualTransfer, strings.Trim(mockedRPC.transfers[i].Source, "/"))
		}
		sort.Strings(actualTransfer)
		sort.Strings(blobsToInclude)
		a.Equal(blobsToInclude, actualTransfer)

		validateS2SSyncTransfersAreScheduled(a, blobsToInclude, mockedRPC)
	})
}

func TestDryrunSyncBlobtoBlob(t *testing.T) {
	a := assert.New(t)
	bsc := getBlobServiceClient()

	// set up src container
	srcContainerClient, srcContainerName := createNewContainer(a, bsc)
	defer deleteContainer(a, srcContainerClient)
	blobsToInclude := []string{"AzURE2.jpeg", "sub1/aTestOne.txt", "sub1/sub2/testTwo.pdf"}
	scenarioHelper{}.generateBlobsFromList(a, srcContainerClient, blobsToInclude, blockBlobDefaultData)

	// set up dst container
	dstContainerClient, dstContainerName := createNewContainer(a, bsc)
	defer deleteContainer(a, dstContainerClient)
	blobsToDelete := []string{"testThree.jpeg"}
	scenarioHelper{}.generateBlobsFromList(a, dstContainerClient, blobsToDelete, blockBlobDefaultData)

	// set up interceptor
	mockedRPC := interceptor{}
	jobsAdmin.ExecuteNewCopyJobPartOrder = func(order common.CopyJobPartOrderRequest) common.CopyJobPartOrderResponse {
		return mockedRPC.intercept(order)
	}
	mockedLcm := mockedLifecycleManager{dryrunLog: make(chan string, 50)}
	mockedLcm.SetOutputFormat(common.EOutputFormat.Text())
	glcm = &mockedLcm

	// construct the raw input to simulate user input
	srcContainerURLWithSAS := scenarioHelper{}.getRawContainerURLWithSAS(a, srcContainerName)
	dstContainerURLWithSAS := scenarioHelper{}.getRawContainerURLWithSAS(a, dstContainerName)
	raw := getDefaultSyncRawInput(srcContainerURLWithSAS.String(), dstContainerURLWithSAS.String())
	raw.dryrun = true
	raw.deleteDestination = "true"

	runSyncAndVerify(a, raw, func(err error) {
		a.NoError(err)
		validateS2SSyncTransfersAreScheduled(a, []string{}, mockedRPC)

		msg := mockedLcm.GatherAllLogs(mockedLcm.dryrunLog)
		sort.Strings(msg)
		for i := 0; i < len(msg); i++ {
			if strings.Contains(msg[i], "DRYRUN: remove") {
				a.True(strings.Contains(msg[i], dstContainerClient.URL()))
			} else {
				a.True(strings.Contains(msg[i], "DRYRUN: copy"))
				a.True(strings.Contains(msg[i], srcContainerClient.URL()))
				a.True(strings.Contains(msg[i], dstContainerClient.URL()))
			}
		}

		a.True(testDryrunStatements(blobsToInclude, msg))
		a.True(testDryrunStatements(blobsToDelete, msg))
	})
}

func TestDryrunSyncBlobtoBlobJson(t *testing.T) {
	a := assert.New(t)
	bsc := getBlobServiceClient()

	// set up src container
	srcContainerClient, srcContainerName := createNewContainer(a, bsc)
	defer deleteContainer(a, srcContainerClient)

	// set up dst container
	dstContainerClient, dstContainerName := createNewContainer(a, bsc)
	defer deleteContainer(a, dstContainerClient)
	blobsToDelete := []string{"testThree.jpeg"}
	scenarioHelper{}.generateBlobsFromList(a, dstContainerClient, blobsToDelete, blockBlobDefaultData)

	// set up interceptor
	mockedRPC := interceptor{}
	jobsAdmin.ExecuteNewCopyJobPartOrder = func(order common.CopyJobPartOrderRequest) common.CopyJobPartOrderResponse {
		return mockedRPC.intercept(order)
	}
	mockedLcm := mockedLifecycleManager{dryrunLog: make(chan string, 50)}
	mockedLcm.SetOutputFormat(common.EOutputFormat.Json())
	glcm = &mockedLcm

	// construct the raw input to simulate user input
	srcContainerURLWithSAS := scenarioHelper{}.getRawContainerURLWithSAS(a, srcContainerName)
	dstContainerURLWithSAS := scenarioHelper{}.getRawContainerURLWithSAS(a, dstContainerName)
	raw := getDefaultSyncRawInput(srcContainerURLWithSAS.String(), dstContainerURLWithSAS.String())
	raw.dryrun = true
	raw.deleteDestination = "true"

	runSyncAndVerify(a, raw, func(err error) {
		a.NoError(err)
		validateS2SSyncTransfersAreScheduled(a, []string{}, mockedRPC)

		msg := <-mockedLcm.dryrunLog
		syncMessage := DryrunTransfer{}
		errMarshal := json.Unmarshal([]byte(msg), &syncMessage)
		a.Nil(errMarshal)
		a.True(strings.Contains(syncMessage.Source, blobsToDelete[0]))
		a.Equal(common.EEntityType.File(), syncMessage.EntityType)
		a.Equal(common.EBlobType.BlockBlob(), syncMessage.BlobType)

	})
}
