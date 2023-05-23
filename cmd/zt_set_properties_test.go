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
	"github.com/Azure/azure-storage-azcopy/v10/common"
	"github.com/Azure/azure-storage-blob-go/azblob"
	"github.com/stretchr/testify/assert"
	"net/url"
	"strings"
	"testing"
	"time"
)

type transferParams struct {
	blockBlobTier common.BlockBlobTier
	pageBlobTier  common.PageBlobTier
	metadata      string
	blobTags      common.BlobTags
}

func (tp transferParams) getMetadata() common.Metadata {
	metadataString := tp.metadata

	metadataMap, err := common.StringToMetadata(metadataString)
	if err != nil {
		panic("unable to form Metadata from string: " + err.Error())
	}
	return metadataMap
}

func (scenarioHelper) generateBlobsFromListWithAccessTier(a *assert.Assertions, containerURL azblob.ContainerURL, blobList []string, data string, accessTier azblob.AccessTierType) {
	for _, blobName := range blobList {
		blob := containerURL.NewBlockBlobURL(blobName)
		cResp, err := blob.Upload(ctx, strings.NewReader(data), azblob.BlobHTTPHeaders{},
			nil, azblob.BlobAccessConditions{}, accessTier, nil, azblob.ClientProvidedKeyOptions{}, azblob.ImmutabilityPolicyOptions{})
		a.Nil(err)
		a.Equal(201, cResp.StatusCode())
	}

	// sleep a bit so that the blobs' lmts are guaranteed to be in the past
	time.Sleep(time.Millisecond * 1050)
}

func createNewBlockBlobWithAccessTier(a *assert.Assertions, container azblob.ContainerURL, prefix string, accessTier azblob.AccessTierType) (blob azblob.BlockBlobURL, name string) {
	blob, name = getBlockBlobURL(a, container, prefix)

	cResp, err := blob.Upload(ctx, strings.NewReader(blockBlobDefaultData), azblob.BlobHTTPHeaders{},
		nil, azblob.BlobAccessConditions{}, accessTier, nil, azblob.ClientProvidedKeyOptions{}, azblob.ImmutabilityPolicyOptions{})

	a.Nil(err)
	a.Equal(201, cResp.StatusCode())

	return
}

func (scenarioHelper) generateCommonRemoteScenarioForBlobWithAccessTier(a *assert.Assertions, containerURL azblob.ContainerURL, prefix string, accessTier azblob.AccessTierType) (blobList []string) {
	blobList = make([]string, 50)

	for i := 0; i < 10; i++ {
		_, blobName1 := createNewBlockBlobWithAccessTier(a, containerURL, prefix+"top", accessTier)
		_, blobName2 := createNewBlockBlobWithAccessTier(a, containerURL, prefix+"sub1/", accessTier)
		_, blobName3 := createNewBlockBlobWithAccessTier(a, containerURL, prefix+"sub2/", accessTier)
		_, blobName4 := createNewBlockBlobWithAccessTier(a, containerURL, prefix+"sub1/sub3/sub5/", accessTier)
		_, blobName5 := createNewBlockBlobWithAccessTier(a, containerURL, prefix+specialNames[i], accessTier)

		blobList[5*i] = blobName1
		blobList[5*i+1] = blobName2
		blobList[5*i+2] = blobName3
		blobList[5*i+3] = blobName4
		blobList[5*i+4] = blobName5
	}

	// sleep a bit so that the blobs' lmts are guaranteed to be in the past
	time.Sleep(time.Millisecond * 1050)
	return
}

func checkMapsEqual(a *assert.Assertions, mapA map[string]string, mapB map[string]string) {
	a.Equal(len(mapB), len(mapA))
	for k, v := range mapA {
		a.Equal(v, mapB[k])
	}
}

func validateSetPropertiesTransfersAreScheduled(a *assert.Assertions, isSrcEncoded bool, expectedTransfers []string, transferParams transferParams, mockedRPC interceptor) {

	// validate that the right number of transfers were scheduled
	a.Equal(len(expectedTransfers), len(mockedRPC.transfers))

	// validate that the right transfers were sent
	lookupMap := scenarioHelper{}.convertListToMap(expectedTransfers)
	for _, transfer := range mockedRPC.transfers {
		srcRelativeFilePath := transfer.Source
		a.Equal(transferParams.blockBlobTier.ToAccessTierType(), transfer.BlobTier)
		checkMapsEqual(a, transfer.Metadata, transferParams.getMetadata())
		checkMapsEqual(a, transfer.BlobTags, transferParams.blobTags)

		if isSrcEncoded {
			srcRelativeFilePath, _ = url.PathUnescape(srcRelativeFilePath)
		}

		// look up the source from the expected transfers, make sure it exists
		_, srcExist := lookupMap[srcRelativeFilePath]
		a.True(srcExist)

		delete(lookupMap, srcRelativeFilePath)
	}
}

func TestSetPropertiesSingleBlobForBlobTier(t *testing.T) {
	a := assert.New(t)
	bsu := getBSU()
	containerURL, containerName := createNewContainer(a, bsu)
	defer deleteContainer(a, containerURL)

	for _, blobName := range []string{"top/mid/low/singleblobisbest", "打麻将.txt", "%4509%4254$85140&"} {
		// set up the container with a single blob
		blobList := []string{blobName}

		// upload the data with given accessTier
		scenarioHelper{}.generateBlobsFromListWithAccessTier(a, containerURL, blobList, blockBlobDefaultData, azblob.AccessTierHot)
		a.NotNil(containerURL)

		// set up interceptor
		mockedRPC := interceptor{}
		Rpc = mockedRPC.intercept
		mockedRPC.init()

		// construct the raw input to simulate user input
		rawBlobURLWithSAS := scenarioHelper{}.getRawBlobURLWithSAS(a, containerName, blobList[0])
		transferParams := transferParams{
			blockBlobTier: common.EBlockBlobTier.Cool(),
			pageBlobTier:  common.EPageBlobTier.None(),
			metadata:      "abc=def;metadata=value",
			blobTags:      common.BlobTags{"abc": "fgd"},
		}
		raw := getDefaultSetPropertiesRawInput(rawBlobURLWithSAS.String(), transferParams)

		runCopyAndVerify(a, raw, func(err error) {
			a.Nil(err)

			// note that when we are targeting single blobs, the relative path is empty ("") since the root path already points to the blob
			validateSetPropertiesTransfersAreScheduled(a, true, []string{""}, transferParams, mockedRPC)
		})
	}
}

func TestSetPropertiesBlobsUnderContainerForBlobTier(t *testing.T) {
	a := assert.New(t)
	bsu := getBSU()

	// set up the container with numerous blobs
	containerURL, containerName := createNewContainer(a, bsu)
	defer deleteContainer(a, containerURL)
	blobList := scenarioHelper{}.generateCommonRemoteScenarioForBlobWithAccessTier(a, containerURL, "", azblob.AccessTierHot)
	a.NotNil(containerURL)
	a.NotZero(len(blobList))

	// set up interceptor
	mockedRPC := interceptor{}
	Rpc = mockedRPC.intercept
	mockedRPC.init()

	// construct the raw input to simulate user input
	rawContainerURLWithSAS := scenarioHelper{}.getRawContainerURLWithSAS(a, containerName)
	transferParams := transferParams{
		blockBlobTier: common.EBlockBlobTier.Cool(),
		pageBlobTier:  common.EPageBlobTier.None(),
		metadata:      "",
		blobTags:      common.BlobTags{},
	}
	raw := getDefaultSetPropertiesRawInput(rawContainerURLWithSAS.String(), transferParams)
	raw.recursive = true
	raw.includeDirectoryStubs = false // The test target is a DFS account, which coincidentally created our directory stubs. Thus, we mustn't include them, since this is a test of blob.

	runCopyAndVerify(a, raw, func(err error) {
		a.Nil(err)

		// validate that the right number of transfers were scheduled
		a.Equal(len(blobList), len(mockedRPC.transfers))

		// validate that the right transfers were sent
		validateSetPropertiesTransfersAreScheduled(a, true, blobList, transferParams, mockedRPC)
		//TODO: I don't think we need to change ^ this function from remove, do we?
	})

	// turn off recursive, this time only top blobs should be changed
	raw.recursive = false
	mockedRPC.reset()

	runCopyAndVerify(a, raw, func(err error) {
		a.Nil(err)
		a.NotEqual(len(blobList), len(mockedRPC.transfers))

		for _, transfer := range mockedRPC.transfers {
			a.False(strings.Contains(transfer.Source, common.AZCOPY_PATH_SEPARATOR_STRING))
		}
	})
}

// TODO: func TestRemoveBlobsUnderVirtualDir(a *assert.Assertions)

func TestSetPropertiesWithIncludeFlagForBlobTier(t *testing.T) {
	a := assert.New(t)
	bsu := getBSU()

	// set up the container with numerous blobs
	containerURL, containerName := createNewContainer(a, bsu)
	blobList := scenarioHelper{}.generateCommonRemoteScenarioForBlobWithAccessTier(a, containerURL, "", azblob.AccessTierHot)
	defer deleteContainer(a, containerURL)
	a.NotNil(containerURL)
	a.NotZero(len(blobList))

	// add special blobs that we wish to include
	blobsToInclude := []string{"important.pdf", "includeSub/amazing.jpeg", "exactName"}
	scenarioHelper{}.generateBlobsFromListWithAccessTier(a, containerURL, blobsToInclude, blockBlobDefaultData, azblob.AccessTierHot)
	includeString := "*.pdf;*.jpeg;exactName"

	// set up interceptor
	mockedRPC := interceptor{}
	Rpc = mockedRPC.intercept
	mockedRPC.init()

	// construct the raw input to simulate user input
	rawContainerURLWithSAS := scenarioHelper{}.getRawContainerURLWithSAS(a, containerName)
	transferParams := transferParams{
		blockBlobTier: common.EBlockBlobTier.Cool(),
		pageBlobTier:  common.EPageBlobTier.None(),
		metadata:      "",
		blobTags:      common.BlobTags{},
	}
	raw := getDefaultSetPropertiesRawInput(rawContainerURLWithSAS.String(), transferParams)
	raw.include = includeString
	raw.recursive = true

	runCopyAndVerify(a, raw, func(err error) {
		a.Nil(err)
		validateDownloadTransfersAreScheduled(a, "", "", blobsToInclude, mockedRPC)
		validateSetPropertiesTransfersAreScheduled(a, true, blobsToInclude, transferParams, mockedRPC)
	})
}

func TestSetPropertiesWithExcludeFlagForBlobTier(t *testing.T) {
	a := assert.New(t)
	bsu := getBSU()

	// set up the container with numerous blobs
	containerURL, containerName := createNewContainer(a, bsu)
	blobList := scenarioHelper{}.generateCommonRemoteScenarioForBlobWithAccessTier(a, containerURL, "", azblob.AccessTierHot)
	defer deleteContainer(a, containerURL)
	a.NotNil(containerURL)
	a.NotZero(len(blobList))

	// add special blobs that we wish to exclude
	blobsToExclude := []string{"notGood.pdf", "excludeSub/lame.jpeg", "exactName"}
	scenarioHelper{}.generateBlobsFromListWithAccessTier(a, containerURL, blobsToExclude, blockBlobDefaultData, azblob.AccessTierHot)
	excludeString := "*.pdf;*.jpeg;exactName"

	// set up interceptor
	mockedRPC := interceptor{}
	Rpc = mockedRPC.intercept
	mockedRPC.init()

	// construct the raw input to simulate user input
	rawContainerURLWithSAS := scenarioHelper{}.getRawContainerURLWithSAS(a, containerName)
	transferParams := transferParams{
		blockBlobTier: common.EBlockBlobTier.Cool(),
		pageBlobTier:  common.EPageBlobTier.None(),
		metadata:      "",
		blobTags:      common.BlobTags{},
	}

	raw := getDefaultSetPropertiesRawInput(rawContainerURLWithSAS.String(), transferParams)
	raw.exclude = excludeString
	raw.recursive = true
	raw.includeDirectoryStubs = false // The test target is a DFS account, which coincidentally created our directory stubs. Thus, we mustn't include them, since this is a test of blob.

	runCopyAndVerify(a, raw, func(err error) {
		a.Nil(err)
		validateDownloadTransfersAreScheduled(a, "", "", blobList, mockedRPC)
		validateSetPropertiesTransfersAreScheduled(a, true, blobList, transferParams, mockedRPC)
	})
}

func TestSetPropertiesWithIncludeAndExcludeFlagForBlobTier(t *testing.T) {
	a := assert.New(t)
	bsu := getBSU()

	// set up the container with numerous blobs
	containerURL, containerName := createNewContainer(a, bsu)
	blobList := scenarioHelper{}.generateCommonRemoteScenarioForBlobWithAccessTier(a, containerURL, "", azblob.AccessTierHot)
	defer deleteContainer(a, containerURL)
	a.NotNil(containerURL)
	a.NotZero(len(blobList))

	// add special blobs that we wish to include
	blobsToInclude := []string{"important.pdf", "includeSub/amazing.jpeg"}
	scenarioHelper{}.generateBlobsFromListWithAccessTier(a, containerURL, blobsToInclude, blockBlobDefaultData, azblob.AccessTierHot)
	includeString := "*.pdf;*.jpeg;exactName"

	// add special blobs that we wish to exclude
	// note that the excluded files also match the include string
	blobsToExclude := []string{"sorry.pdf", "exclude/notGood.jpeg", "exactName", "sub/exactName"}
	scenarioHelper{}.generateBlobsFromListWithAccessTier(a, containerURL, blobsToExclude, blockBlobDefaultData, azblob.AccessTierHot)
	excludeString := "so*;not*;exactName"

	// set up interceptor
	mockedRPC := interceptor{}
	Rpc = mockedRPC.intercept
	mockedRPC.init()

	// construct the raw input to simulate user input
	rawContainerURLWithSAS := scenarioHelper{}.getRawContainerURLWithSAS(a, containerName)
	transferParams := transferParams{
		blockBlobTier: common.EBlockBlobTier.Cool(),
		pageBlobTier:  common.EPageBlobTier.None(),
		metadata:      "",
		blobTags:      common.BlobTags{},
	}

	raw := getDefaultSetPropertiesRawInput(rawContainerURLWithSAS.String(), transferParams)
	raw.include = includeString
	raw.exclude = excludeString
	raw.recursive = true

	runCopyAndVerify(a, raw, func(err error) {
		a.Nil(err)
		validateDownloadTransfersAreScheduled(a, "", "", blobsToInclude, mockedRPC)
		validateSetPropertiesTransfersAreScheduled(a, true, blobsToInclude, transferParams, mockedRPC)
	})
}

// note: list-of-files flag is used
func TestSetPropertiesListOfBlobsAndVirtualDirsForBlobTier(t *testing.T) {
	a := assert.New(t)
	t.Skip("Enable after setting Account to non-HNS")
	bsu := getBSU()
	vdirName := "megadir"

	// set up the container with numerous blobs and a vdir
	containerURL, containerName := createNewContainer(a, bsu)
	defer deleteContainer(a, containerURL)
	a.NotNil(containerURL)
	blobListPart1 := scenarioHelper{}.generateCommonRemoteScenarioForBlobWithAccessTier(a, containerURL, "", azblob.AccessTierHot)
	blobListPart2 := scenarioHelper{}.generateCommonRemoteScenarioForBlobWithAccessTier(a, containerURL, vdirName+"/", azblob.AccessTierHot)

	blobList := append(blobListPart1, blobListPart2...)
	a.NotZero(len(blobList))

	// set up interceptor
	mockedRPC := interceptor{}
	Rpc = mockedRPC.intercept
	mockedRPC.init()

	// construct the raw input to simulate user input
	rawContainerURLWithSAS := scenarioHelper{}.getRawContainerURLWithSAS(a, containerName)
	transferParams := transferParams{
		blockBlobTier: common.EBlockBlobTier.Cool(),
		pageBlobTier:  common.EPageBlobTier.None(),
		metadata:      "",
		blobTags:      common.BlobTags{},
	}

	raw := getDefaultSetPropertiesRawInput(rawContainerURLWithSAS.String(), transferParams)
	raw.recursive = true

	// make the input for list-of-files
	listOfFiles := append(blobListPart1, vdirName)

	// add some random files that don't actually exist
	listOfFiles = append(listOfFiles, "WUTAMIDOING")
	listOfFiles = append(listOfFiles, "DONTKNOW")
	raw.listOfFilesToCopy = scenarioHelper{}.generateListOfFiles(a, listOfFiles)

	runCopyAndVerify(a, raw, func(err error) {
		a.Nil(err)

		// validate that the right number of transfers were scheduled
		a.Equal(len(blobList), len(mockedRPC.transfers))

		// validate that the right transfers were sent
		validateSetPropertiesTransfersAreScheduled(a, true, blobList, transferParams, mockedRPC)
	})

	// turn off recursive, this time only top blobs should be deleted
	raw.recursive = false
	mockedRPC.reset()

	runCopyAndVerify(a, raw, func(err error) {
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

func TestSetPropertiesListOfBlobsWithIncludeAndExcludeForBlobTier(t *testing.T) {
	a := assert.New(t)
	bsu := getBSU()
	vdirName := "megadir"

	// set up the container with numerous blobs and a vdir
	containerURL, containerName := createNewContainer(a, bsu)
	defer deleteContainer(a, containerURL)
	a.NotNil(containerURL)
	blobListPart1 := scenarioHelper{}.generateCommonRemoteScenarioForBlobWithAccessTier(a, containerURL, "", azblob.AccessTierHot)
	scenarioHelper{}.generateCommonRemoteScenarioForBlobWithAccessTier(a, containerURL, vdirName+"/", azblob.AccessTierHot)

	// add special blobs that we wish to include
	blobsToInclude := []string{"important.pdf", "includeSub/amazing.jpeg"}
	scenarioHelper{}.generateBlobsFromListWithAccessTier(a, containerURL, blobsToInclude, blockBlobDefaultData, azblob.AccessTierHot)

	includeString := "*.pdf;*.jpeg;exactName"

	// add special blobs that we wish to exclude
	// note that the excluded files also match the include string
	blobsToExclude := []string{"sorry.pdf", "exclude/notGood.jpeg", "exactName", "sub/exactName"}
	scenarioHelper{}.generateBlobsFromListWithAccessTier(a, containerURL, blobsToExclude, blockBlobDefaultData, azblob.AccessTierHot)
	excludeString := "so*;not*;exactName"

	// set up interceptor
	mockedRPC := interceptor{}
	Rpc = mockedRPC.intercept
	mockedRPC.init()

	// construct the raw input to simulate user input
	rawContainerURLWithSAS := scenarioHelper{}.getRawContainerURLWithSAS(a, containerName)
	transferParams := transferParams{
		blockBlobTier: common.EBlockBlobTier.Cool(),
		pageBlobTier:  common.EPageBlobTier.None(),
		metadata:      "",
		blobTags:      common.BlobTags{},
	}

	raw := getDefaultSetPropertiesRawInput(rawContainerURLWithSAS.String(), transferParams)

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

	runCopyAndVerify(a, raw, func(err error) {
		a.Nil(err)

		// validate that the right number of transfers were scheduled
		a.Equal(len(blobsToInclude), len(mockedRPC.transfers))

		// validate that the right transfers were sent
		validateSetPropertiesTransfersAreScheduled(a, true, blobsToInclude, transferParams, mockedRPC)
	})
}

func TestSetPropertiesSingleBlobWithFromToForBlobTier(t *testing.T) {
	a := assert.New(t)
	bsu := getBSU()
	containerURL, containerName := createNewContainer(a, bsu)
	defer deleteContainer(a, containerURL)

	for _, blobName := range []string{"top/mid/low/singleblobisbest", "打麻将.txt", "%4509%4254$85140&"} {
		// set up the container with a single blob
		blobList := []string{blobName}
		scenarioHelper{}.generateBlobsFromListWithAccessTier(a, containerURL, blobList, blockBlobDefaultData, azblob.AccessTierHot)
		a.NotNil(containerURL)

		// set up interceptor
		mockedRPC := interceptor{}
		Rpc = mockedRPC.intercept
		mockedRPC.init()

		// construct the raw input to simulate user input
		rawBlobURLWithSAS := scenarioHelper{}.getRawBlobURLWithSAS(a, containerName, blobList[0])
		transferParams := transferParams{
			blockBlobTier: common.EBlockBlobTier.Cool(),
			pageBlobTier:  common.EPageBlobTier.None(),
			metadata:      "",
			blobTags:      common.BlobTags{},
		}

		raw := getDefaultSetPropertiesRawInput(rawBlobURLWithSAS.String(), transferParams)
		raw.fromTo = "BlobNone"

		runCopyAndVerify(a, raw, func(err error) {
			a.Nil(err)

			// note that when we are targeting single blobs, the relative path is empty ("") since the root path already points to the blob
			validateSetPropertiesTransfersAreScheduled(a, true, []string{""}, transferParams, mockedRPC)
		})
	}
}

func TestSetPropertiesBlobsUnderContainerWithFromToForBlobTier(t *testing.T) {
	a := assert.New(t)
	bsu := getBSU()

	// set up the container with numerous blobs
	containerURL, containerName := createNewContainer(a, bsu)
	defer deleteContainer(a, containerURL)
	blobList := scenarioHelper{}.generateCommonRemoteScenarioForBlobWithAccessTier(a, containerURL, "", azblob.AccessTierHot)

	a.NotNil(containerURL)
	a.NotZero(len(blobList))

	// set up interceptor
	mockedRPC := interceptor{}
	Rpc = mockedRPC.intercept
	mockedRPC.init()

	// construct the raw input to simulate user input
	rawContainerURLWithSAS := scenarioHelper{}.getRawContainerURLWithSAS(a, containerName)
	transferParams := transferParams{
		blockBlobTier: common.EBlockBlobTier.Cool(),
		pageBlobTier:  common.EPageBlobTier.None(),
		metadata:      "",
		blobTags:      common.BlobTags{},
	}

	raw := getDefaultSetPropertiesRawInput(rawContainerURLWithSAS.String(), transferParams)
	raw.fromTo = "BlobNone"
	raw.recursive = true
	raw.includeDirectoryStubs = false // The test target is a DFS account, which coincidentally created our directory stubs. Thus, we mustn't include them, since this is a test of blob.

	runCopyAndVerify(a, raw, func(err error) {
		a.Nil(err)

		// validate that the right number of transfers were scheduled
		a.Equal(len(blobList), len(mockedRPC.transfers))

		// validate that the right transfers were sent
		validateSetPropertiesTransfersAreScheduled(a, true, blobList, transferParams, mockedRPC)
	})

	// turn off recursive, this time only top blobs should be deleted
	raw.recursive = false
	mockedRPC.reset()

	runCopyAndVerify(a, raw, func(err error) {
		a.Nil(err)
		a.NotEqual(len(blobList), len(mockedRPC.transfers))

		for _, transfer := range mockedRPC.transfers {
			a.False(strings.Contains(transfer.Source, common.AZCOPY_PATH_SEPARATOR_STRING))
		}
	})
}

func TestSetPropertiesBlobsUnderVirtualDirWithFromToForBlobTier(t *testing.T) {
	a := assert.New(t)
	t.Skip("Enable after setting Account to non-HNS")
	bsu := getBSU()
	vdirName := "vdir1/vdir2/vdir3/"

	// set up the container with numerous blobs
	containerURL, containerName := createNewContainer(a, bsu)
	defer deleteContainer(a, containerURL)
	blobList := scenarioHelper{}.generateCommonRemoteScenarioForBlobWithAccessTier(a, containerURL, vdirName, azblob.AccessTierHot)

	a.NotNil(containerURL)
	a.NotZero(len(blobList))

	// set up interceptor
	mockedRPC := interceptor{}
	Rpc = mockedRPC.intercept
	mockedRPC.init()

	// construct the raw input to simulate user input
	rawVirtualDirectoryURLWithSAS := scenarioHelper{}.getRawBlobURLWithSAS(a, containerName, vdirName)
	transferParams := transferParams{
		blockBlobTier: common.EBlockBlobTier.Cool(),
		pageBlobTier:  common.EPageBlobTier.None(),
		metadata:      "",
		blobTags:      common.BlobTags{},
	}

	raw := getDefaultSetPropertiesRawInput(rawVirtualDirectoryURLWithSAS.String(), transferParams)
	raw.fromTo = "BlobNone"
	raw.recursive = true

	runCopyAndVerify(a, raw, func(err error) {
		a.Nil(err)

		// validate that the right number of transfers were scheduled
		a.Equal(len(blobList), len(mockedRPC.transfers))

		// validate that the right transfers were sent
		expectedTransfers := scenarioHelper{}.shaveOffPrefix(blobList, vdirName)
		validateSetPropertiesTransfersAreScheduled(a, true, expectedTransfers, transferParams, mockedRPC)
	})

	// turn off recursive, this time only top blobs should be deleted
	raw.recursive = false
	mockedRPC.reset()

	runCopyAndVerify(a, raw, func(err error) {
		a.Nil(err)
		a.NotEqual(len(blobList), len(mockedRPC.transfers))

		for _, transfer := range mockedRPC.transfers {
			a.False(strings.Contains(transfer.Source, common.AZCOPY_PATH_SEPARATOR_STRING))
		}
	})
}

///////////////////////////////// METADATA /////////////////////////////////

func TestSetPropertiesSingleBlobForMetadata(t *testing.T) {
	a := assert.New(t)
	bsu := getBSU()
	containerURL, containerName := createNewContainer(a, bsu)
	defer deleteContainer(a, containerURL)

	for _, blobName := range []string{"top/mid/low/singleblobisbest", "打麻将.txt", "%4509%4254$85140&"} {
		// set up the container with a single blob
		blobList := []string{blobName}

		// upload the data with given accessTier
		scenarioHelper{}.generateBlobsFromListWithAccessTier(a, containerURL, blobList, blockBlobDefaultData, azblob.AccessTierHot)
		a.NotNil(containerURL)

		// set up interceptor
		mockedRPC := interceptor{}
		Rpc = mockedRPC.intercept
		mockedRPC.init()

		// construct the raw input to simulate user input
		rawBlobURLWithSAS := scenarioHelper{}.getRawBlobURLWithSAS(a, containerName, blobList[0])
		transferParams := transferParams{
			blockBlobTier: common.EBlockBlobTier.None(),
			pageBlobTier:  common.EPageBlobTier.None(),
			metadata:      "abc=def;metadata=value",
			blobTags:      common.BlobTags{},
		}
		raw := getDefaultSetPropertiesRawInput(rawBlobURLWithSAS.String(), transferParams)

		runCopyAndVerify(a, raw, func(err error) {
			a.Nil(err)

			// note that when we are targeting single blobs, the relative path is empty ("") since the root path already points to the blob
			validateSetPropertiesTransfersAreScheduled(a, true, []string{""}, transferParams, mockedRPC)
		})
	}
}

func TestSetPropertiesSingleBlobForEmptyMetadata(t *testing.T) {
	a := assert.New(t)
	bsu := getBSU()
	containerURL, containerName := createNewContainer(a, bsu)
	defer deleteContainer(a, containerURL)

	for _, blobName := range []string{"top/mid/low/singleblobisbest", "打麻将.txt", "%4509%4254$85140&"} {
		// set up the container with a single blob
		blobList := []string{blobName}

		// upload the data with given accessTier
		scenarioHelper{}.generateBlobsFromListWithAccessTier(a, containerURL, blobList, blockBlobDefaultData, azblob.AccessTierHot)
		a.NotNil(containerURL)

		// set up interceptor
		mockedRPC := interceptor{}
		Rpc = mockedRPC.intercept
		mockedRPC.init()

		// construct the raw input to simulate user input
		rawBlobURLWithSAS := scenarioHelper{}.getRawBlobURLWithSAS(a, containerName, blobList[0])
		transferParams := transferParams{
			blockBlobTier: common.EBlockBlobTier.None(),
			pageBlobTier:  common.EPageBlobTier.None(),
			metadata:      "",
			blobTags:      common.BlobTags{},
		}
		raw := getDefaultSetPropertiesRawInput(rawBlobURLWithSAS.String(), transferParams)

		runCopyAndVerify(a, raw, func(err error) {
			a.Nil(err)

			// note that when we are targeting single blobs, the relative path is empty ("") since the root path already points to the blob
			validateSetPropertiesTransfersAreScheduled(a, true, []string{""}, transferParams, mockedRPC)
		})
	}
}

func TestSetPropertiesBlobsUnderContainerForMetadata(t *testing.T) {
	a := assert.New(t)
	bsu := getBSU()

	// set up the container with numerous blobs
	containerURL, containerName := createNewContainer(a, bsu)
	defer deleteContainer(a, containerURL)
	blobList := scenarioHelper{}.generateCommonRemoteScenarioForBlobWithAccessTier(a, containerURL, "", azblob.AccessTierHot)
	a.NotNil(containerURL)
	a.NotZero(len(blobList))

	// set up interceptor
	mockedRPC := interceptor{}
	Rpc = mockedRPC.intercept
	mockedRPC.init()

	// construct the raw input to simulate user input
	rawContainerURLWithSAS := scenarioHelper{}.getRawContainerURLWithSAS(a, containerName)
	transferParams := transferParams{
		blockBlobTier: common.EBlockBlobTier.None(),
		pageBlobTier:  common.EPageBlobTier.None(),
		metadata:      "abc=def;metadata=value",
		blobTags:      common.BlobTags{},
	}
	raw := getDefaultSetPropertiesRawInput(rawContainerURLWithSAS.String(), transferParams)
	raw.recursive = true
	raw.includeDirectoryStubs = false // The test target is a DFS account, which coincidentally created our directory stubs. Thus, we mustn't include them, since this is a test of blob.

	runCopyAndVerify(a, raw, func(err error) {
		a.Nil(err)

		// validate that the right number of transfers were scheduled
		a.Equal(len(blobList), len(mockedRPC.transfers))

		// validate that the right transfers were sent
		validateSetPropertiesTransfersAreScheduled(a, true, blobList, transferParams, mockedRPC)
	})

	// turn off recursive, this time only top blobs should be changed
	raw.recursive = false
	mockedRPC.reset()

	runCopyAndVerify(a, raw, func(err error) {
		a.Nil(err)
		a.NotEqual(len(blobList), len(mockedRPC.transfers))

		for _, transfer := range mockedRPC.transfers {
			a.False(strings.Contains(transfer.Source, common.AZCOPY_PATH_SEPARATOR_STRING))
		}
	})
}

func TestSetPropertiesWithIncludeFlagForMetadata(t *testing.T) {
	a := assert.New(t)
	bsu := getBSU()

	// set up the container with numerous blobs
	containerURL, containerName := createNewContainer(a, bsu)
	blobList := scenarioHelper{}.generateCommonRemoteScenarioForBlobWithAccessTier(a, containerURL, "", azblob.AccessTierHot)
	defer deleteContainer(a, containerURL)
	a.NotNil(containerURL)
	a.NotZero(len(blobList))

	// add special blobs that we wish to include
	blobsToInclude := []string{"important.pdf", "includeSub/amazing.jpeg", "exactName"}
	scenarioHelper{}.generateBlobsFromListWithAccessTier(a, containerURL, blobsToInclude, blockBlobDefaultData, azblob.AccessTierHot)
	includeString := "*.pdf;*.jpeg;exactName"

	// set up interceptor
	mockedRPC := interceptor{}
	Rpc = mockedRPC.intercept
	mockedRPC.init()

	// construct the raw input to simulate user input
	rawContainerURLWithSAS := scenarioHelper{}.getRawContainerURLWithSAS(a, containerName)
	transferParams := transferParams{
		blockBlobTier: common.EBlockBlobTier.None(),
		pageBlobTier:  common.EPageBlobTier.None(),
		metadata:      "abc=def;metadata=value",
		blobTags:      common.BlobTags{},
	}
	raw := getDefaultSetPropertiesRawInput(rawContainerURLWithSAS.String(), transferParams)
	raw.include = includeString
	raw.recursive = true

	runCopyAndVerify(a, raw, func(err error) {
		a.Nil(err)
		validateDownloadTransfersAreScheduled(a, "", "", blobsToInclude, mockedRPC)
		validateSetPropertiesTransfersAreScheduled(a, true, blobsToInclude, transferParams, mockedRPC)
	})
}

func TestSetPropertiesWithExcludeFlagForMetadata(t *testing.T) {
	a := assert.New(t)
	bsu := getBSU()

	// set up the container with numerous blobs
	containerURL, containerName := createNewContainer(a, bsu)
	blobList := scenarioHelper{}.generateCommonRemoteScenarioForBlobWithAccessTier(a, containerURL, "", azblob.AccessTierHot)
	defer deleteContainer(a, containerURL)
	a.NotNil(containerURL)
	a.NotZero(len(blobList))

	// add special blobs that we wish to exclude
	blobsToExclude := []string{"notGood.pdf", "excludeSub/lame.jpeg", "exactName"}
	scenarioHelper{}.generateBlobsFromListWithAccessTier(a, containerURL, blobsToExclude, blockBlobDefaultData, azblob.AccessTierHot)
	excludeString := "*.pdf;*.jpeg;exactName"

	// set up interceptor
	mockedRPC := interceptor{}
	Rpc = mockedRPC.intercept
	mockedRPC.init()

	// construct the raw input to simulate user input
	rawContainerURLWithSAS := scenarioHelper{}.getRawContainerURLWithSAS(a, containerName)
	transferParams := transferParams{
		blockBlobTier: common.EBlockBlobTier.None(),
		pageBlobTier:  common.EPageBlobTier.None(),
		metadata:      "abc=def;metadata=value",
		blobTags:      common.BlobTags{},
	}

	raw := getDefaultSetPropertiesRawInput(rawContainerURLWithSAS.String(), transferParams)
	raw.exclude = excludeString
	raw.recursive = true
	raw.includeDirectoryStubs = false // The test target is a DFS account, which coincidentally created our directory stubs. Thus, we mustn't include them, since this is a test of blob.

	runCopyAndVerify(a, raw, func(err error) {
		a.Nil(err)
		validateDownloadTransfersAreScheduled(a, "", "", blobList, mockedRPC)
		validateSetPropertiesTransfersAreScheduled(a, true, blobList, transferParams, mockedRPC)
	})
}

func TestSetPropertiesWithIncludeAndExcludeFlagForMetadata(t *testing.T) {
	a := assert.New(t)
	bsu := getBSU()

	// set up the container with numerous blobs
	containerURL, containerName := createNewContainer(a, bsu)
	blobList := scenarioHelper{}.generateCommonRemoteScenarioForBlobWithAccessTier(a, containerURL, "", azblob.AccessTierHot)
	defer deleteContainer(a, containerURL)
	a.NotNil(containerURL)
	a.NotZero(len(blobList))

	// add special blobs that we wish to include
	blobsToInclude := []string{"important.pdf", "includeSub/amazing.jpeg"}
	scenarioHelper{}.generateBlobsFromListWithAccessTier(a, containerURL, blobsToInclude, blockBlobDefaultData, azblob.AccessTierHot)
	includeString := "*.pdf;*.jpeg;exactName"

	// add special blobs that we wish to exclude
	// note that the excluded files also match the include string
	blobsToExclude := []string{"sorry.pdf", "exclude/notGood.jpeg", "exactName", "sub/exactName"}
	scenarioHelper{}.generateBlobsFromListWithAccessTier(a, containerURL, blobsToExclude, blockBlobDefaultData, azblob.AccessTierHot)
	excludeString := "so*;not*;exactName"

	// set up interceptor
	mockedRPC := interceptor{}
	Rpc = mockedRPC.intercept
	mockedRPC.init()

	// construct the raw input to simulate user input
	rawContainerURLWithSAS := scenarioHelper{}.getRawContainerURLWithSAS(a, containerName)
	transferParams := transferParams{
		blockBlobTier: common.EBlockBlobTier.None(),
		pageBlobTier:  common.EPageBlobTier.None(),
		metadata:      "abc=def;metadata=value",
		blobTags:      common.BlobTags{},
	}

	raw := getDefaultSetPropertiesRawInput(rawContainerURLWithSAS.String(), transferParams)
	raw.include = includeString
	raw.exclude = excludeString
	raw.recursive = true

	runCopyAndVerify(a, raw, func(err error) {
		a.Nil(err)
		validateDownloadTransfersAreScheduled(a, "", "", blobsToInclude, mockedRPC)
		validateSetPropertiesTransfersAreScheduled(a, true, blobsToInclude, transferParams, mockedRPC)
	})
}

// note: list-of-files flag is used
func TestSetPropertiesListOfBlobsAndVirtualDirsForMetadata(t *testing.T) {
	a := assert.New(t)
	t.Skip("Enable after setting Account to non-HNS")
	bsu := getBSU()
	vdirName := "megadir"

	// set up the container with numerous blobs and a vdir
	containerURL, containerName := createNewContainer(a, bsu)
	defer deleteContainer(a, containerURL)
	a.NotNil(containerURL)
	blobListPart1 := scenarioHelper{}.generateCommonRemoteScenarioForBlobWithAccessTier(a, containerURL, "", azblob.AccessTierHot)
	blobListPart2 := scenarioHelper{}.generateCommonRemoteScenarioForBlobWithAccessTier(a, containerURL, vdirName+"/", azblob.AccessTierHot)

	blobList := append(blobListPart1, blobListPart2...)
	a.NotZero(len(blobList))

	// set up interceptor
	mockedRPC := interceptor{}
	Rpc = mockedRPC.intercept
	mockedRPC.init()

	// construct the raw input to simulate user input
	rawContainerURLWithSAS := scenarioHelper{}.getRawContainerURLWithSAS(a, containerName)
	transferParams := transferParams{
		blockBlobTier: common.EBlockBlobTier.None(),
		pageBlobTier:  common.EPageBlobTier.None(),
		metadata:      "abc=def;metadata=value",
		blobTags:      common.BlobTags{},
	}

	raw := getDefaultSetPropertiesRawInput(rawContainerURLWithSAS.String(), transferParams)
	raw.recursive = true

	// make the input for list-of-files
	listOfFiles := append(blobListPart1, vdirName)

	// add some random files that don't actually exist
	listOfFiles = append(listOfFiles, "WUTAMIDOING")
	listOfFiles = append(listOfFiles, "DONTKNOW")
	raw.listOfFilesToCopy = scenarioHelper{}.generateListOfFiles(a, listOfFiles)

	runCopyAndVerify(a, raw, func(err error) {
		a.Nil(err)

		// validate that the right number of transfers were scheduled
		a.Equal(len(blobList), len(mockedRPC.transfers))

		// validate that the right transfers were sent
		validateSetPropertiesTransfersAreScheduled(a, true, blobList, transferParams, mockedRPC)
	})

	// turn off recursive, this time only top blobs should be deleted
	raw.recursive = false
	mockedRPC.reset()

	runCopyAndVerify(a, raw, func(err error) {
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

func TestSetPropertiesListOfBlobsWithIncludeAndExcludeForMetadata(t *testing.T) {
	a := assert.New(t)
	bsu := getBSU()
	vdirName := "megadir"

	// set up the container with numerous blobs and a vdir
	containerURL, containerName := createNewContainer(a, bsu)
	defer deleteContainer(a, containerURL)
	a.NotNil(containerURL)
	blobListPart1 := scenarioHelper{}.generateCommonRemoteScenarioForBlobWithAccessTier(a, containerURL, "", azblob.AccessTierHot)
	scenarioHelper{}.generateCommonRemoteScenarioForBlobWithAccessTier(a, containerURL, vdirName+"/", azblob.AccessTierHot)

	// add special blobs that we wish to include
	blobsToInclude := []string{"important.pdf", "includeSub/amazing.jpeg"}
	scenarioHelper{}.generateBlobsFromListWithAccessTier(a, containerURL, blobsToInclude, blockBlobDefaultData, azblob.AccessTierHot)

	includeString := "*.pdf;*.jpeg;exactName"

	// add special blobs that we wish to exclude
	// note that the excluded files also match the include string
	blobsToExclude := []string{"sorry.pdf", "exclude/notGood.jpeg", "exactName", "sub/exactName"}
	scenarioHelper{}.generateBlobsFromListWithAccessTier(a, containerURL, blobsToExclude, blockBlobDefaultData, azblob.AccessTierHot)
	excludeString := "so*;not*;exactName"

	// set up interceptor
	mockedRPC := interceptor{}
	Rpc = mockedRPC.intercept
	mockedRPC.init()

	// construct the raw input to simulate user input
	rawContainerURLWithSAS := scenarioHelper{}.getRawContainerURLWithSAS(a, containerName)
	transferParams := transferParams{
		blockBlobTier: common.EBlockBlobTier.None(),
		pageBlobTier:  common.EPageBlobTier.None(),
		metadata:      "abc=def;metadata=value",
		blobTags:      common.BlobTags{},
	}

	raw := getDefaultSetPropertiesRawInput(rawContainerURLWithSAS.String(), transferParams)

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

	runCopyAndVerify(a, raw, func(err error) {
		a.Nil(err)

		// validate that the right number of transfers were scheduled
		a.Equal(len(blobsToInclude), len(mockedRPC.transfers))

		// validate that the right transfers were sent
		validateSetPropertiesTransfersAreScheduled(a, true, blobsToInclude, transferParams, mockedRPC)
	})
}

func TestSetPropertiesSingleBlobWithFromToForMetadata(t *testing.T) {
	a := assert.New(t)
	bsu := getBSU()
	containerURL, containerName := createNewContainer(a, bsu)
	defer deleteContainer(a, containerURL)

	for _, blobName := range []string{"top/mid/low/singleblobisbest", "打麻将.txt", "%4509%4254$85140&"} {
		// set up the container with a single blob
		blobList := []string{blobName}
		scenarioHelper{}.generateBlobsFromListWithAccessTier(a, containerURL, blobList, blockBlobDefaultData, azblob.AccessTierHot)
		a.NotNil(containerURL)

		// set up interceptor
		mockedRPC := interceptor{}
		Rpc = mockedRPC.intercept
		mockedRPC.init()

		// construct the raw input to simulate user input
		rawBlobURLWithSAS := scenarioHelper{}.getRawBlobURLWithSAS(a, containerName, blobList[0])
		transferParams := transferParams{
			blockBlobTier: common.EBlockBlobTier.None(),
			pageBlobTier:  common.EPageBlobTier.None(),
			metadata:      "abc=def;metadata=value",
			blobTags:      common.BlobTags{},
		}

		raw := getDefaultSetPropertiesRawInput(rawBlobURLWithSAS.String(), transferParams)
		raw.fromTo = "BlobNone"

		runCopyAndVerify(a, raw, func(err error) {
			a.Nil(err)

			// note that when we are targeting single blobs, the relative path is empty ("") since the root path already points to the blob
			validateSetPropertiesTransfersAreScheduled(a, true, []string{""}, transferParams, mockedRPC)
		})
	}
}

func TestSetPropertiesBlobsUnderContainerWithFromToForMetadata(t *testing.T) {
	a := assert.New(t)
	bsu := getBSU()

	// set up the container with numerous blobs
	containerURL, containerName := createNewContainer(a, bsu)
	defer deleteContainer(a, containerURL)
	blobList := scenarioHelper{}.generateCommonRemoteScenarioForBlobWithAccessTier(a, containerURL, "", azblob.AccessTierHot)

	a.NotNil(containerURL)
	a.NotZero(len(blobList))

	// set up interceptor
	mockedRPC := interceptor{}
	Rpc = mockedRPC.intercept
	mockedRPC.init()

	// construct the raw input to simulate user input
	rawContainerURLWithSAS := scenarioHelper{}.getRawContainerURLWithSAS(a, containerName)
	transferParams := transferParams{
		blockBlobTier: common.EBlockBlobTier.None(),
		pageBlobTier:  common.EPageBlobTier.None(),
		metadata:      "abc=def;metadata=value",
		blobTags:      common.BlobTags{},
	}

	raw := getDefaultSetPropertiesRawInput(rawContainerURLWithSAS.String(), transferParams)
	raw.fromTo = "BlobNone"
	raw.recursive = true
	raw.includeDirectoryStubs = false // The test target is a DFS account, which coincidentally created our directory stubs. Thus, we mustn't include them, since this is a test of blob.

	runCopyAndVerify(a, raw, func(err error) {
		a.Nil(err)

		// validate that the right number of transfers were scheduled
		a.Equal(len(blobList), len(mockedRPC.transfers))

		// validate that the right transfers were sent
		validateSetPropertiesTransfersAreScheduled(a, true, blobList, transferParams, mockedRPC)
	})

	// turn off recursive, this time only top blobs should be deleted
	raw.recursive = false
	mockedRPC.reset()

	runCopyAndVerify(a, raw, func(err error) {
		a.Nil(err)
		a.NotEqual(len(blobList), len(mockedRPC.transfers))

		for _, transfer := range mockedRPC.transfers {
			a.False(strings.Contains(transfer.Source, common.AZCOPY_PATH_SEPARATOR_STRING))
		}
	})
}

func TestSetPropertiesBlobsUnderVirtualDirWithFromToForMetadata(t *testing.T) {
	a := assert.New(t)
	t.Skip("Enable after setting Account to non-HNS")
	bsu := getBSU()
	vdirName := "vdir1/vdir2/vdir3/"

	// set up the container with numerous blobs
	containerURL, containerName := createNewContainer(a, bsu)
	defer deleteContainer(a, containerURL)
	blobList := scenarioHelper{}.generateCommonRemoteScenarioForBlobWithAccessTier(a, containerURL, vdirName, azblob.AccessTierHot)

	a.NotNil(containerURL)
	a.NotZero(len(blobList))

	// set up interceptor
	mockedRPC := interceptor{}
	Rpc = mockedRPC.intercept
	mockedRPC.init()

	// construct the raw input to simulate user input
	rawVirtualDirectoryURLWithSAS := scenarioHelper{}.getRawBlobURLWithSAS(a, containerName, vdirName)
	transferParams := transferParams{
		blockBlobTier: common.EBlockBlobTier.None(),
		pageBlobTier:  common.EPageBlobTier.None(),
		metadata:      "abc=def;metadata=value",
		blobTags:      common.BlobTags{},
	}

	raw := getDefaultSetPropertiesRawInput(rawVirtualDirectoryURLWithSAS.String(), transferParams)
	raw.fromTo = "BlobNone"
	raw.recursive = true

	runCopyAndVerify(a, raw, func(err error) {
		a.Nil(err)

		// validate that the right number of transfers were scheduled
		a.Equal(len(blobList), len(mockedRPC.transfers))

		// validate that the right transfers were sent
		expectedTransfers := scenarioHelper{}.shaveOffPrefix(blobList, vdirName)
		validateSetPropertiesTransfersAreScheduled(a, true, expectedTransfers, transferParams, mockedRPC)
	})

	// turn off recursive, this time only top blobs should be deleted
	raw.recursive = false
	mockedRPC.reset()

	runCopyAndVerify(a, raw, func(err error) {
		a.Nil(err)
		a.NotEqual(len(blobList), len(mockedRPC.transfers))

		for _, transfer := range mockedRPC.transfers {
			a.False(strings.Contains(transfer.Source, common.AZCOPY_PATH_SEPARATOR_STRING))
		}
	})
}

///////////////////////////////// TAGS /////////////////////////////////

func TestSetPropertiesSingleBlobForBlobTags(t *testing.T) {
	a := assert.New(t)
	bsu := getBSU()
	containerURL, containerName := createNewContainer(a, bsu)
	defer deleteContainer(a, containerURL)

	for _, blobName := range []string{"top/mid/low/singleblobisbest", "打麻将.txt", "%4509%4254$85140&"} {
		// set up the container with a single blob
		blobList := []string{blobName}

		// upload the data with given accessTier
		scenarioHelper{}.generateBlobsFromListWithAccessTier(a, containerURL, blobList, blockBlobDefaultData, azblob.AccessTierHot)
		a.NotNil(containerURL)

		// set up interceptor
		mockedRPC := interceptor{}
		Rpc = mockedRPC.intercept
		mockedRPC.init()

		// construct the raw input to simulate user input
		rawBlobURLWithSAS := scenarioHelper{}.getRawBlobURLWithSAS(a, containerName, blobList[0])
		transferParams := transferParams{
			blockBlobTier: common.EBlockBlobTier.None(),
			pageBlobTier:  common.EPageBlobTier.None(),
			metadata:      "",
			blobTags:      common.BlobTags{"abc": "fgd"},
		}
		raw := getDefaultSetPropertiesRawInput(rawBlobURLWithSAS.String(), transferParams)

		runCopyAndVerify(a, raw, func(err error) {
			a.Nil(err)

			// note that when we are targeting single blobs, the relative path is empty ("") since the root path already points to the blob
			validateSetPropertiesTransfersAreScheduled(a, true, []string{""}, transferParams, mockedRPC)
		})
	}
}

func TestSetPropertiesSingleBlobForEmptyBlobTags(t *testing.T) {
	a := assert.New(t)
	bsu := getBSU()
	containerURL, containerName := createNewContainer(a, bsu)
	defer deleteContainer(a, containerURL)

	for _, blobName := range []string{"top/mid/low/singleblobisbest", "打麻将.txt", "%4509%4254$85140&"} {
		// set up the container with a single blob
		blobList := []string{blobName}

		// upload the data with given accessTier
		scenarioHelper{}.generateBlobsFromListWithAccessTier(a, containerURL, blobList, blockBlobDefaultData, azblob.AccessTierHot)
		a.NotNil(containerURL)

		// set up interceptor
		mockedRPC := interceptor{}
		Rpc = mockedRPC.intercept
		mockedRPC.init()

		// construct the raw input to simulate user input
		rawBlobURLWithSAS := scenarioHelper{}.getRawBlobURLWithSAS(a, containerName, blobList[0])
		transferParams := transferParams{
			blockBlobTier: common.EBlockBlobTier.None(),
			pageBlobTier:  common.EPageBlobTier.None(),
			metadata:      "",
			blobTags:      common.BlobTags{},
		}
		raw := getDefaultSetPropertiesRawInput(rawBlobURLWithSAS.String(), transferParams)

		runCopyAndVerify(a, raw, func(err error) {
			a.Nil(err)

			// note that when we are targeting single blobs, the relative path is empty ("") since the root path already points to the blob
			validateSetPropertiesTransfersAreScheduled(a, true, []string{""}, transferParams, mockedRPC)
		})
	}
}

func TestSetPropertiesBlobsUnderContainerForBlobTags(t *testing.T) {
	a := assert.New(t)
	bsu := getBSU()

	// set up the container with numerous blobs
	containerURL, containerName := createNewContainer(a, bsu)
	defer deleteContainer(a, containerURL)
	blobList := scenarioHelper{}.generateCommonRemoteScenarioForBlobWithAccessTier(a, containerURL, "", azblob.AccessTierHot)
	a.NotNil(containerURL)
	a.NotZero(len(blobList))

	// set up interceptor
	mockedRPC := interceptor{}
	Rpc = mockedRPC.intercept
	mockedRPC.init()

	// construct the raw input to simulate user input
	rawContainerURLWithSAS := scenarioHelper{}.getRawContainerURLWithSAS(a, containerName)
	transferParams := transferParams{
		blockBlobTier: common.EBlockBlobTier.None(),
		pageBlobTier:  common.EPageBlobTier.None(),
		metadata:      "",
		blobTags:      common.BlobTags{"abc": "fgd"},
	}
	raw := getDefaultSetPropertiesRawInput(rawContainerURLWithSAS.String(), transferParams)
	raw.recursive = true
	raw.includeDirectoryStubs = false // The test target is a DFS account, which coincidentally created our directory stubs. Thus, we mustn't include them, since this is a test of blob.

	runCopyAndVerify(a, raw, func(err error) {
		a.Nil(err)

		// validate that the right number of transfers were scheduled
		a.Equal(len(blobList), len(mockedRPC.transfers))

		// validate that the right transfers were sent
		validateSetPropertiesTransfersAreScheduled(a, true, blobList, transferParams, mockedRPC)
	})

	// turn off recursive, this time only top blobs should be changed
	raw.recursive = false
	mockedRPC.reset()

	runCopyAndVerify(a, raw, func(err error) {
		a.Nil(err)
		a.NotEqual(len(blobList), len(mockedRPC.transfers))

		for _, transfer := range mockedRPC.transfers {
			a.False(strings.Contains(transfer.Source, common.AZCOPY_PATH_SEPARATOR_STRING))
		}
	})
}

func TestSetPropertiesWithIncludeFlagForBlobTags(t *testing.T) {
	a := assert.New(t)
	bsu := getBSU()

	// set up the container with numerous blobs
	containerURL, containerName := createNewContainer(a, bsu)
	blobList := scenarioHelper{}.generateCommonRemoteScenarioForBlobWithAccessTier(a, containerURL, "", azblob.AccessTierHot)
	defer deleteContainer(a, containerURL)
	a.NotNil(containerURL)
	a.NotZero(len(blobList))

	// add special blobs that we wish to include
	blobsToInclude := []string{"important.pdf", "includeSub/amazing.jpeg", "exactName"}
	scenarioHelper{}.generateBlobsFromListWithAccessTier(a, containerURL, blobsToInclude, blockBlobDefaultData, azblob.AccessTierHot)
	includeString := "*.pdf;*.jpeg;exactName"

	// set up interceptor
	mockedRPC := interceptor{}
	Rpc = mockedRPC.intercept
	mockedRPC.init()

	// construct the raw input to simulate user input
	rawContainerURLWithSAS := scenarioHelper{}.getRawContainerURLWithSAS(a, containerName)
	transferParams := transferParams{
		blockBlobTier: common.EBlockBlobTier.None(),
		pageBlobTier:  common.EPageBlobTier.None(),
		metadata:      "",
		blobTags:      common.BlobTags{"abc": "fgd"},
	}
	raw := getDefaultSetPropertiesRawInput(rawContainerURLWithSAS.String(), transferParams)
	raw.include = includeString
	raw.recursive = true

	runCopyAndVerify(a, raw, func(err error) {
		a.Nil(err)
		validateDownloadTransfersAreScheduled(a, "", "", blobsToInclude, mockedRPC)
		validateSetPropertiesTransfersAreScheduled(a, true, blobsToInclude, transferParams, mockedRPC)
	})
}

func TestSetPropertiesWithExcludeFlagForBlobTags(t *testing.T) {
	a := assert.New(t)
	bsu := getBSU()

	// set up the container with numerous blobs
	containerURL, containerName := createNewContainer(a, bsu)
	blobList := scenarioHelper{}.generateCommonRemoteScenarioForBlobWithAccessTier(a, containerURL, "", azblob.AccessTierHot)
	defer deleteContainer(a, containerURL)
	a.NotNil(containerURL)
	a.NotZero(len(blobList))

	// add special blobs that we wish to exclude
	blobsToExclude := []string{"notGood.pdf", "excludeSub/lame.jpeg", "exactName"}
	scenarioHelper{}.generateBlobsFromListWithAccessTier(a, containerURL, blobsToExclude, blockBlobDefaultData, azblob.AccessTierHot)
	excludeString := "*.pdf;*.jpeg;exactName"

	// set up interceptor
	mockedRPC := interceptor{}
	Rpc = mockedRPC.intercept
	mockedRPC.init()

	// construct the raw input to simulate user input
	rawContainerURLWithSAS := scenarioHelper{}.getRawContainerURLWithSAS(a, containerName)
	transferParams := transferParams{
		blockBlobTier: common.EBlockBlobTier.None(),
		pageBlobTier:  common.EPageBlobTier.None(),
		metadata:      "",
		blobTags:      common.BlobTags{"abc": "fgd"},
	}

	raw := getDefaultSetPropertiesRawInput(rawContainerURLWithSAS.String(), transferParams)
	raw.exclude = excludeString
	raw.recursive = true
	raw.includeDirectoryStubs = false // The test target is a DFS account, which coincidentally created our directory stubs. Thus, we mustn't include them, since this is a test of blob.

	runCopyAndVerify(a, raw, func(err error) {
		a.Nil(err)
		validateDownloadTransfersAreScheduled(a, "", "", blobList, mockedRPC)
		validateSetPropertiesTransfersAreScheduled(a, true, blobList, transferParams, mockedRPC)
	})
}

func TestSetPropertiesWithIncludeAndExcludeFlagForBlobTags(t *testing.T) {
	a := assert.New(t)
	bsu := getBSU()

	// set up the container with numerous blobs
	containerURL, containerName := createNewContainer(a, bsu)
	blobList := scenarioHelper{}.generateCommonRemoteScenarioForBlobWithAccessTier(a, containerURL, "", azblob.AccessTierHot)
	defer deleteContainer(a, containerURL)
	a.NotNil(containerURL)
	a.NotZero(len(blobList))

	// add special blobs that we wish to include
	blobsToInclude := []string{"important.pdf", "includeSub/amazing.jpeg"}
	scenarioHelper{}.generateBlobsFromListWithAccessTier(a, containerURL, blobsToInclude, blockBlobDefaultData, azblob.AccessTierHot)
	includeString := "*.pdf;*.jpeg;exactName"

	// add special blobs that we wish to exclude
	// note that the excluded files also match the include string
	blobsToExclude := []string{"sorry.pdf", "exclude/notGood.jpeg", "exactName", "sub/exactName"}
	scenarioHelper{}.generateBlobsFromListWithAccessTier(a, containerURL, blobsToExclude, blockBlobDefaultData, azblob.AccessTierHot)
	excludeString := "so*;not*;exactName"

	// set up interceptor
	mockedRPC := interceptor{}
	Rpc = mockedRPC.intercept
	mockedRPC.init()

	// construct the raw input to simulate user input
	rawContainerURLWithSAS := scenarioHelper{}.getRawContainerURLWithSAS(a, containerName)
	transferParams := transferParams{
		blockBlobTier: common.EBlockBlobTier.None(),
		pageBlobTier:  common.EPageBlobTier.None(),
		metadata:      "",
		blobTags:      common.BlobTags{"abc": "fgd"},
	}

	raw := getDefaultSetPropertiesRawInput(rawContainerURLWithSAS.String(), transferParams)
	raw.include = includeString
	raw.exclude = excludeString
	raw.recursive = true

	runCopyAndVerify(a, raw, func(err error) {
		a.Nil(err)
		validateDownloadTransfersAreScheduled(a, "", "", blobsToInclude, mockedRPC)
		validateSetPropertiesTransfersAreScheduled(a, true, blobsToInclude, transferParams, mockedRPC)
	})
}

// note: list-of-files flag is used
func TestSetPropertiesListOfBlobsAndVirtualDirsForBlobTags(t *testing.T) {
	a := assert.New(t)
	t.Skip("Enable after setting Account to non-HNS")
	bsu := getBSU()
	vdirName := "megadir"

	// set up the container with numerous blobs and a vdir
	containerURL, containerName := createNewContainer(a, bsu)
	defer deleteContainer(a, containerURL)
	a.NotNil(containerURL)
	blobListPart1 := scenarioHelper{}.generateCommonRemoteScenarioForBlobWithAccessTier(a, containerURL, "", azblob.AccessTierHot)
	blobListPart2 := scenarioHelper{}.generateCommonRemoteScenarioForBlobWithAccessTier(a, containerURL, vdirName+"/", azblob.AccessTierHot)

	blobList := append(blobListPart1, blobListPart2...)
	a.NotZero(len(blobList))

	// set up interceptor
	mockedRPC := interceptor{}
	Rpc = mockedRPC.intercept
	mockedRPC.init()

	// construct the raw input to simulate user input
	rawContainerURLWithSAS := scenarioHelper{}.getRawContainerURLWithSAS(a, containerName)
	transferParams := transferParams{
		blockBlobTier: common.EBlockBlobTier.None(),
		pageBlobTier:  common.EPageBlobTier.None(),
		metadata:      "",
		blobTags:      common.BlobTags{"abc": "fgd"},
	}

	raw := getDefaultSetPropertiesRawInput(rawContainerURLWithSAS.String(), transferParams)
	raw.recursive = true

	// make the input for list-of-files
	listOfFiles := append(blobListPart1, vdirName)

	// add some random files that don't actually exist
	listOfFiles = append(listOfFiles, "WUTAMIDOING")
	listOfFiles = append(listOfFiles, "DONTKNOW")
	raw.listOfFilesToCopy = scenarioHelper{}.generateListOfFiles(a, listOfFiles)

	runCopyAndVerify(a, raw, func(err error) {
		a.Nil(err)

		// validate that the right number of transfers were scheduled
		a.Equal(len(blobList), len(mockedRPC.transfers))

		// validate that the right transfers were sent
		validateSetPropertiesTransfersAreScheduled(a, true, blobList, transferParams, mockedRPC)
	})

	// turn off recursive, this time only top blobs should be deleted
	raw.recursive = false
	mockedRPC.reset()

	runCopyAndVerify(a, raw, func(err error) {
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

func TestSetPropertiesListOfBlobsWithIncludeAndExcludeForBlobTags(t *testing.T) {
	a := assert.New(t)
	bsu := getBSU()
	vdirName := "megadir"

	// set up the container with numerous blobs and a vdir
	containerURL, containerName := createNewContainer(a, bsu)
	defer deleteContainer(a, containerURL)
	a.NotNil(containerURL)
	blobListPart1 := scenarioHelper{}.generateCommonRemoteScenarioForBlobWithAccessTier(a, containerURL, "", azblob.AccessTierHot)
	scenarioHelper{}.generateCommonRemoteScenarioForBlobWithAccessTier(a, containerURL, vdirName+"/", azblob.AccessTierHot)

	// add special blobs that we wish to include
	blobsToInclude := []string{"important.pdf", "includeSub/amazing.jpeg"}
	scenarioHelper{}.generateBlobsFromListWithAccessTier(a, containerURL, blobsToInclude, blockBlobDefaultData, azblob.AccessTierHot)

	includeString := "*.pdf;*.jpeg;exactName"

	// add special blobs that we wish to exclude
	// note that the excluded files also match the include string
	blobsToExclude := []string{"sorry.pdf", "exclude/notGood.jpeg", "exactName", "sub/exactName"}
	scenarioHelper{}.generateBlobsFromListWithAccessTier(a, containerURL, blobsToExclude, blockBlobDefaultData, azblob.AccessTierHot)
	excludeString := "so*;not*;exactName"

	// set up interceptor
	mockedRPC := interceptor{}
	Rpc = mockedRPC.intercept
	mockedRPC.init()

	// construct the raw input to simulate user input
	rawContainerURLWithSAS := scenarioHelper{}.getRawContainerURLWithSAS(a, containerName)
	transferParams := transferParams{
		blockBlobTier: common.EBlockBlobTier.None(),
		pageBlobTier:  common.EPageBlobTier.None(),
		metadata:      "",
		blobTags:      common.BlobTags{"abc": "fgd"},
	}

	raw := getDefaultSetPropertiesRawInput(rawContainerURLWithSAS.String(), transferParams)

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

	runCopyAndVerify(a, raw, func(err error) {
		a.Nil(err)

		// validate that the right number of transfers were scheduled
		a.Equal(len(blobsToInclude), len(mockedRPC.transfers))

		// validate that the right transfers were sent
		validateSetPropertiesTransfersAreScheduled(a, true, blobsToInclude, transferParams, mockedRPC)
	})
}

func TestSetPropertiesSingleBlobWithFromToForBlobTags(t *testing.T) {
	a := assert.New(t)
	bsu := getBSU()
	containerURL, containerName := createNewContainer(a, bsu)
	defer deleteContainer(a, containerURL)

	for _, blobName := range []string{"top/mid/low/singleblobisbest", "打麻将.txt", "%4509%4254$85140&"} {
		// set up the container with a single blob
		blobList := []string{blobName}
		scenarioHelper{}.generateBlobsFromListWithAccessTier(a, containerURL, blobList, blockBlobDefaultData, azblob.AccessTierHot)
		a.NotNil(containerURL)

		// set up interceptor
		mockedRPC := interceptor{}
		Rpc = mockedRPC.intercept
		mockedRPC.init()

		// construct the raw input to simulate user input
		rawBlobURLWithSAS := scenarioHelper{}.getRawBlobURLWithSAS(a, containerName, blobList[0])
		transferParams := transferParams{
			blockBlobTier: common.EBlockBlobTier.None(),
			pageBlobTier:  common.EPageBlobTier.None(),
			metadata:      "",
			blobTags:      common.BlobTags{"abc": "fgd"},
		}

		raw := getDefaultSetPropertiesRawInput(rawBlobURLWithSAS.String(), transferParams)
		raw.fromTo = "BlobNone"

		runCopyAndVerify(a, raw, func(err error) {
			a.Nil(err)

			// note that when we are targeting single blobs, the relative path is empty ("") since the root path already points to the blob
			validateSetPropertiesTransfersAreScheduled(a, true, []string{""}, transferParams, mockedRPC)
		})
	}
}

func TestSetPropertiesBlobsUnderContainerWithFromToForBlobTags(t *testing.T) {
	a := assert.New(t)
	bsu := getBSU()

	// set up the container with numerous blobs
	containerURL, containerName := createNewContainer(a, bsu)
	defer deleteContainer(a, containerURL)
	blobList := scenarioHelper{}.generateCommonRemoteScenarioForBlobWithAccessTier(a, containerURL, "", azblob.AccessTierHot)

	a.NotNil(containerURL)
	a.NotZero(len(blobList))

	// set up interceptor
	mockedRPC := interceptor{}
	Rpc = mockedRPC.intercept
	mockedRPC.init()

	// construct the raw input to simulate user input
	rawContainerURLWithSAS := scenarioHelper{}.getRawContainerURLWithSAS(a, containerName)
	transferParams := transferParams{
		blockBlobTier: common.EBlockBlobTier.None(),
		pageBlobTier:  common.EPageBlobTier.None(),
		metadata:      "",
		blobTags:      common.BlobTags{"abc": "fgd"},
	}

	raw := getDefaultSetPropertiesRawInput(rawContainerURLWithSAS.String(), transferParams)
	raw.fromTo = "BlobNone"
	raw.recursive = true
	raw.includeDirectoryStubs = false // The test target is a DFS account, which coincidentally created our directory stubs. Thus, we mustn't include them, since this is a test of blob.

	runCopyAndVerify(a, raw, func(err error) {
		a.Nil(err)

		// validate that the right number of transfers were scheduled
		a.Equal(len(blobList), len(mockedRPC.transfers))

		// validate that the right transfers were sent
		validateSetPropertiesTransfersAreScheduled(a, true, blobList, transferParams, mockedRPC)
	})

	// turn off recursive, this time only top blobs should be deleted
	raw.recursive = false
	mockedRPC.reset()

	runCopyAndVerify(a, raw, func(err error) {
		a.Nil(err)
		a.NotEqual(len(blobList), len(mockedRPC.transfers))

		for _, transfer := range mockedRPC.transfers {
			a.False(strings.Contains(transfer.Source, common.AZCOPY_PATH_SEPARATOR_STRING))
		}
	})
}

func TestSetPropertiesBlobsUnderVirtualDirWithFromToForBlobTags(t *testing.T) {
	a := assert.New(t)
	t.Skip("Enable after setting Account to non-HNS")
	bsu := getBSU()
	vdirName := "vdir1/vdir2/vdir3/"

	// set up the container with numerous blobs
	containerURL, containerName := createNewContainer(a, bsu)
	defer deleteContainer(a, containerURL)
	blobList := scenarioHelper{}.generateCommonRemoteScenarioForBlobWithAccessTier(a, containerURL, vdirName, azblob.AccessTierHot)

	a.NotNil(containerURL)
	a.NotZero(len(blobList))

	// set up interceptor
	mockedRPC := interceptor{}
	Rpc = mockedRPC.intercept
	mockedRPC.init()

	// construct the raw input to simulate user input
	rawVirtualDirectoryURLWithSAS := scenarioHelper{}.getRawBlobURLWithSAS(a, containerName, vdirName)
	transferParams := transferParams{
		blockBlobTier: common.EBlockBlobTier.None(),
		pageBlobTier:  common.EPageBlobTier.None(),
		metadata:      "",
		blobTags:      common.BlobTags{"abc": "fgd"},
	}

	raw := getDefaultSetPropertiesRawInput(rawVirtualDirectoryURLWithSAS.String(), transferParams)
	raw.fromTo = "BlobNone"
	raw.recursive = true

	runCopyAndVerify(a, raw, func(err error) {
		a.Nil(err)

		// validate that the right number of transfers were scheduled
		a.Equal(len(blobList), len(mockedRPC.transfers))

		// validate that the right transfers were sent
		expectedTransfers := scenarioHelper{}.shaveOffPrefix(blobList, vdirName)
		validateSetPropertiesTransfersAreScheduled(a, true, expectedTransfers, transferParams, mockedRPC)
	})

	// turn off recursive, this time only top blobs should be deleted
	raw.recursive = false
	mockedRPC.reset()

	runCopyAndVerify(a, raw, func(err error) {
		a.Nil(err)
		a.NotEqual(len(blobList), len(mockedRPC.transfers))

		for _, transfer := range mockedRPC.transfers {
			a.False(strings.Contains(transfer.Source, common.AZCOPY_PATH_SEPARATOR_STRING))
		}
	})
}
