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
	chk "gopkg.in/check.v1"
	"net/url"
	"strings"
	"time"
)

func (scenarioHelper) generateBlobsFromListWithAccessTier(c *chk.C, containerURL azblob.ContainerURL, blobList []string, data string, accessTier azblob.AccessTierType) {
	for _, blobName := range blobList {
		blob := containerURL.NewBlockBlobURL(blobName)
		cResp, err := blob.Upload(ctx, strings.NewReader(data), azblob.BlobHTTPHeaders{},
			nil, azblob.BlobAccessConditions{}, accessTier, nil, azblob.ClientProvidedKeyOptions{})
		c.Assert(err, chk.IsNil)
		c.Assert(cResp.StatusCode(), chk.Equals, 201)
	}

	// sleep a bit so that the blobs' lmts are guaranteed to be in the past
	time.Sleep(time.Millisecond * 1050)
}

func createNewBlockBlobWithAccessTier(c *chk.C, container azblob.ContainerURL, prefix string, accessTier azblob.AccessTierType) (blob azblob.BlockBlobURL, name string) {
	blob, name = getBlockBlobURL(c, container, prefix)

	cResp, err := blob.Upload(ctx, strings.NewReader(blockBlobDefaultData), azblob.BlobHTTPHeaders{},
		nil, azblob.BlobAccessConditions{}, accessTier, nil, azblob.ClientProvidedKeyOptions{})

	c.Assert(err, chk.IsNil)
	c.Assert(cResp.StatusCode(), chk.Equals, 201)

	return
}

func (scenarioHelper) generateCommonRemoteScenarioForBlobWithAccessTier(c *chk.C, containerURL azblob.ContainerURL, prefix string, accessTier azblob.AccessTierType) (blobList []string) {
	blobList = make([]string, 50)

	for i := 0; i < 10; i++ {
		_, blobName1 := createNewBlockBlobWithAccessTier(c, containerURL, prefix+"top", accessTier)
		_, blobName2 := createNewBlockBlobWithAccessTier(c, containerURL, prefix+"sub1/", accessTier)
		_, blobName3 := createNewBlockBlobWithAccessTier(c, containerURL, prefix+"sub2/", accessTier)
		_, blobName4 := createNewBlockBlobWithAccessTier(c, containerURL, prefix+"sub1/sub3/sub5/", accessTier)
		_, blobName5 := createNewBlockBlobWithAccessTier(c, containerURL, prefix+specialNames[i], accessTier)

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

func (s *cmdIntegrationSuite) TestSetPropertiesSingleBlob(c *chk.C) {
	bsu := getBSU()
	containerURL, containerName := createNewContainer(c, bsu)
	defer deleteContainer(c, containerURL)

	for _, blobName := range []string{"top/mid/low/singleblobisbest", "打麻将.txt", "%4509%4254$85140&"} {
		// set up the container with a single blob
		blobList := []string{blobName}

		// upload the data with given accessTier
		scenarioHelper{}.generateBlobsFromListWithAccessTier(c, containerURL, blobList, blockBlobDefaultData, azblob.AccessTierHot)
		c.Assert(containerURL, chk.NotNil)

		// set up interceptor
		mockedRPC := interceptor{}
		Rpc = mockedRPC.intercept
		mockedRPC.init()

		// construct the raw input to simulate user input
		rawBlobURLWithSAS := scenarioHelper{}.getRawBlobURLWithSAS(c, containerName, blobList[0])
		raw := getDefaultSetPropertiesRawInput(rawBlobURLWithSAS.String(), common.EBlockBlobTier.Cool())

		runCopyAndVerify(c, raw, func(err error) {
			c.Assert(err, chk.IsNil)

			// note that when we are targeting single blobs, the relative path is empty ("") since the root path already points to the blob
			validateRemoveTransfersAreScheduled(c, true, []string{""}, mockedRPC)
			//TODO: I don't think we need to change ^ this function from remove, do we?
		})
	}
}

func (s *cmdIntegrationSuite) TestSetPropertiesBlobsUnderContainer(c *chk.C) {
	bsu := getBSU()

	// set up the container with numerous blobs
	containerURL, containerName := createNewContainer(c, bsu)
	defer deleteContainer(c, containerURL)
	blobList := scenarioHelper{}.generateCommonRemoteScenarioForBlobWithAccessTier(c, containerURL, "", azblob.AccessTierHot)
	c.Assert(containerURL, chk.NotNil)
	c.Assert(len(blobList), chk.Not(chk.Equals), 0)

	// set up interceptor
	mockedRPC := interceptor{}
	Rpc = mockedRPC.intercept
	mockedRPC.init()

	// construct the raw input to simulate user input
	rawContainerURLWithSAS := scenarioHelper{}.getRawContainerURLWithSAS(c, containerName)
	raw := getDefaultSetPropertiesRawInput(rawContainerURLWithSAS.String(), common.EBlockBlobTier.Cool())
	raw.recursive = true
	raw.includeDirectoryStubs = false // The test target is a DFS account, which coincidentally created our directory stubs. Thus, we mustn't include them, since this is a test of blob.

	runCopyAndVerify(c, raw, func(err error) {
		c.Assert(err, chk.IsNil)

		// validate that the right number of transfers were scheduled
		c.Assert(len(mockedRPC.transfers), chk.Equals, len(blobList))

		// validate that the right transfers were sent
		validateRemoveTransfersAreScheduled(c, true, blobList, mockedRPC)
		//TODO: I don't think we need to change ^ this function from remove, do we?
	})

	// turn off recursive, this time only top blobs should be changed
	raw.recursive = false
	mockedRPC.reset()

	runCopyAndVerify(c, raw, func(err error) {
		c.Assert(err, chk.IsNil)
		c.Assert(len(mockedRPC.transfers), chk.Not(chk.Equals), len(blobList))

		for _, transfer := range mockedRPC.transfers {
			c.Assert(strings.Contains(transfer.Source, common.AZCOPY_PATH_SEPARATOR_STRING), chk.Equals, false)
		}
	})
}

// TODO: func (s *cmdIntegrationSuite) TestRemoveBlobsUnderVirtualDir(c *chk.C)

func (s *cmdIntegrationSuite) TestSetPropertiesWithIncludeFlag(c *chk.C) {
	bsu := getBSU()

	// set up the container with numerous blobs
	containerURL, containerName := createNewContainer(c, bsu)
	blobList := scenarioHelper{}.generateCommonRemoteScenarioForBlobWithAccessTier(c, containerURL, "", azblob.AccessTierHot)
	defer deleteContainer(c, containerURL)
	c.Assert(containerURL, chk.NotNil)
	c.Assert(len(blobList), chk.Not(chk.Equals), 0)

	// add special blobs that we wish to include
	blobsToInclude := []string{"important.pdf", "includeSub/amazing.jpeg", "exactName"}
	scenarioHelper{}.generateBlobsFromListWithAccessTier(c, containerURL, blobsToInclude, blockBlobDefaultData, azblob.AccessTierHot)
	includeString := "*.pdf;*.jpeg;exactName"

	// set up interceptor
	mockedRPC := interceptor{}
	Rpc = mockedRPC.intercept
	mockedRPC.init()

	// construct the raw input to simulate user input
	rawContainerURLWithSAS := scenarioHelper{}.getRawContainerURLWithSAS(c, containerName)
	raw := getDefaultSetPropertiesRawInput(rawContainerURLWithSAS.String(), common.EBlockBlobTier.Cool())
	raw.include = includeString
	raw.recursive = true

	runCopyAndVerify(c, raw, func(err error) {
		c.Assert(err, chk.IsNil)
		validateDownloadTransfersAreScheduled(c, "", "", blobsToInclude, mockedRPC)
	})
}

func (s *cmdIntegrationSuite) TestSetPropertiesWithExcludeFlag(c *chk.C) {
	bsu := getBSU()

	// set up the container with numerous blobs
	containerURL, containerName := createNewContainer(c, bsu)
	blobList := scenarioHelper{}.generateCommonRemoteScenarioForBlobWithAccessTier(c, containerURL, "", azblob.AccessTierHot)
	defer deleteContainer(c, containerURL)
	c.Assert(containerURL, chk.NotNil)
	c.Assert(len(blobList), chk.Not(chk.Equals), 0)

	// add special blobs that we wish to exclude
	blobsToExclude := []string{"notGood.pdf", "excludeSub/lame.jpeg", "exactName"}
	scenarioHelper{}.generateBlobsFromListWithAccessTier(c, containerURL, blobsToExclude, blockBlobDefaultData, azblob.AccessTierHot)
	excludeString := "*.pdf;*.jpeg;exactName"

	// set up interceptor
	mockedRPC := interceptor{}
	Rpc = mockedRPC.intercept
	mockedRPC.init()

	// construct the raw input to simulate user input
	rawContainerURLWithSAS := scenarioHelper{}.getRawContainerURLWithSAS(c, containerName)
	raw := getDefaultSetPropertiesRawInput(rawContainerURLWithSAS.String(), common.EBlockBlobTier.Cool())
	raw.exclude = excludeString
	raw.recursive = true
	raw.includeDirectoryStubs = false // The test target is a DFS account, which coincidentally created our directory stubs. Thus, we mustn't include them, since this is a test of blob.

	runCopyAndVerify(c, raw, func(err error) {
		c.Assert(err, chk.IsNil)
		validateDownloadTransfersAreScheduled(c, "", "", blobList, mockedRPC)
	})
}

func (s *cmdIntegrationSuite) TestSetPropertiesWithIncludeAndExcludeFlag(c *chk.C) {
	bsu := getBSU()

	// set up the container with numerous blobs
	containerURL, containerName := createNewContainer(c, bsu)
	blobList := scenarioHelper{}.generateCommonRemoteScenarioForBlobWithAccessTier(c, containerURL, "", azblob.AccessTierHot)
	defer deleteContainer(c, containerURL)
	c.Assert(containerURL, chk.NotNil)
	c.Assert(len(blobList), chk.Not(chk.Equals), 0)

	// add special blobs that we wish to include
	blobsToInclude := []string{"important.pdf", "includeSub/amazing.jpeg"}
	scenarioHelper{}.generateBlobsFromListWithAccessTier(c, containerURL, blobsToInclude, blockBlobDefaultData, azblob.AccessTierHot)
	includeString := "*.pdf;*.jpeg;exactName"

	// add special blobs that we wish to exclude
	// note that the excluded files also match the include string
	blobsToExclude := []string{"sorry.pdf", "exclude/notGood.jpeg", "exactName", "sub/exactName"}
	scenarioHelper{}.generateBlobsFromListWithAccessTier(c, containerURL, blobsToExclude, blockBlobDefaultData, azblob.AccessTierHot)
	excludeString := "so*;not*;exactName"

	// set up interceptor
	mockedRPC := interceptor{}
	Rpc = mockedRPC.intercept
	mockedRPC.init()

	// construct the raw input to simulate user input
	rawContainerURLWithSAS := scenarioHelper{}.getRawContainerURLWithSAS(c, containerName)
	raw := getDefaultSetPropertiesRawInput(rawContainerURLWithSAS.String(), common.EBlockBlobTier.Cool())
	raw.include = includeString
	raw.exclude = excludeString
	raw.recursive = true

	runCopyAndVerify(c, raw, func(err error) {
		c.Assert(err, chk.IsNil)
		validateDownloadTransfersAreScheduled(c, "", "", blobsToInclude, mockedRPC)
	})
}

// note: list-of-files flag is used
func (s *cmdIntegrationSuite) TestSetPropertiesListOfBlobsAndVirtualDirs(c *chk.C) {
	c.Skip("Enable after setting Account to non-HNS")
	bsu := getBSU()
	vdirName := "megadir"

	// set up the container with numerous blobs and a vdir
	containerURL, containerName := createNewContainer(c, bsu)
	defer deleteContainer(c, containerURL)
	c.Assert(containerURL, chk.NotNil)
	blobListPart1 := scenarioHelper{}.generateCommonRemoteScenarioForBlobWithAccessTier(c, containerURL, "", azblob.AccessTierHot)
	blobListPart2 := scenarioHelper{}.generateCommonRemoteScenarioForBlobWithAccessTier(c, containerURL, vdirName+"/", azblob.AccessTierHot)

	blobList := append(blobListPart1, blobListPart2...)
	c.Assert(len(blobList), chk.Not(chk.Equals), 0)

	// set up interceptor
	mockedRPC := interceptor{}
	Rpc = mockedRPC.intercept
	mockedRPC.init()

	// construct the raw input to simulate user input
	rawContainerURLWithSAS := scenarioHelper{}.getRawContainerURLWithSAS(c, containerName)
	raw := getDefaultSetPropertiesRawInput(rawContainerURLWithSAS.String(), common.EBlockBlobTier.Cool())
	raw.recursive = true

	// make the input for list-of-files
	listOfFiles := append(blobListPart1, vdirName)

	// add some random files that don't actually exist
	listOfFiles = append(listOfFiles, "WUTAMIDOING")
	listOfFiles = append(listOfFiles, "DONTKNOW")
	raw.listOfFilesToCopy = scenarioHelper{}.generateListOfFiles(c, listOfFiles)

	runCopyAndVerify(c, raw, func(err error) {
		c.Assert(err, chk.IsNil)

		// validate that the right number of transfers were scheduled
		c.Assert(len(mockedRPC.transfers), chk.Equals, len(blobList))

		// validate that the right transfers were sent
		validateRemoveTransfersAreScheduled(c, true, blobList, mockedRPC)
	})

	// turn off recursive, this time only top blobs should be deleted
	raw.recursive = false
	mockedRPC.reset()

	runCopyAndVerify(c, raw, func(err error) {
		c.Assert(err, chk.IsNil)
		c.Assert(len(mockedRPC.transfers), chk.Not(chk.Equals), len(blobList))

		for _, transfer := range mockedRPC.transfers {
			source, err := url.PathUnescape(transfer.Source)
			c.Assert(err, chk.IsNil)

			// if the transfer is under the given dir, make sure only the top level files were scheduled
			if strings.HasPrefix(source, vdirName) {
				trimmedSource := strings.TrimPrefix(source, vdirName+"/")
				c.Assert(strings.Contains(trimmedSource, common.AZCOPY_PATH_SEPARATOR_STRING), chk.Equals, false)
			}
		}
	})
}

func (s *cmdIntegrationSuite) TestSetPropertiesListOfBlobsWithIncludeAndExclude(c *chk.C) {
	bsu := getBSU()
	vdirName := "megadir"

	// set up the container with numerous blobs and a vdir
	containerURL, containerName := createNewContainer(c, bsu)
	defer deleteContainer(c, containerURL)
	c.Assert(containerURL, chk.NotNil)
	blobListPart1 := scenarioHelper{}.generateCommonRemoteScenarioForBlobWithAccessTier(c, containerURL, "", azblob.AccessTierHot)
	scenarioHelper{}.generateCommonRemoteScenarioForBlobWithAccessTier(c, containerURL, vdirName+"/", azblob.AccessTierHot)

	// add special blobs that we wish to include
	blobsToInclude := []string{"important.pdf", "includeSub/amazing.jpeg"}
	scenarioHelper{}.generateBlobsFromListWithAccessTier(c, containerURL, blobsToInclude, blockBlobDefaultData, azblob.AccessTierHot)

	includeString := "*.pdf;*.jpeg;exactName"

	// add special blobs that we wish to exclude
	// note that the excluded files also match the include string
	blobsToExclude := []string{"sorry.pdf", "exclude/notGood.jpeg", "exactName", "sub/exactName"}
	scenarioHelper{}.generateBlobsFromListWithAccessTier(c, containerURL, blobsToExclude, blockBlobDefaultData, azblob.AccessTierHot)
	excludeString := "so*;not*;exactName"

	// set up interceptor
	mockedRPC := interceptor{}
	Rpc = mockedRPC.intercept
	mockedRPC.init()

	// construct the raw input to simulate user input
	rawContainerURLWithSAS := scenarioHelper{}.getRawContainerURLWithSAS(c, containerName)
	raw := getDefaultSetPropertiesRawInput(rawContainerURLWithSAS.String(), common.EBlockBlobTier.Cool())

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
	raw.listOfFilesToCopy = scenarioHelper{}.generateListOfFiles(c, listOfFiles)

	runCopyAndVerify(c, raw, func(err error) {
		c.Assert(err, chk.IsNil)

		// validate that the right number of transfers were scheduled
		c.Assert(len(mockedRPC.transfers), chk.Equals, len(blobsToInclude))

		// validate that the right transfers were sent
		validateRemoveTransfersAreScheduled(c, true, blobsToInclude, mockedRPC)
	})
}

//TODO : func (s *cmdIntegrationSuite) TestSetPropertiesBlobsWithDirectoryStubs(c *chk.C)

func (s *cmdIntegrationSuite) TestSetPropertiesSingleBlobWithFromTo(c *chk.C) {
	bsu := getBSU()
	containerURL, containerName := createNewContainer(c, bsu)
	defer deleteContainer(c, containerURL)

	for _, blobName := range []string{"top/mid/low/singleblobisbest", "打麻将.txt", "%4509%4254$85140&"} {
		// set up the container with a single blob
		blobList := []string{blobName}
		scenarioHelper{}.generateBlobsFromListWithAccessTier(c, containerURL, blobList, blockBlobDefaultData, azblob.AccessTierHot)
		c.Assert(containerURL, chk.NotNil)

		// set up interceptor
		mockedRPC := interceptor{}
		Rpc = mockedRPC.intercept
		mockedRPC.init()

		// construct the raw input to simulate user input
		rawBlobURLWithSAS := scenarioHelper{}.getRawBlobURLWithSAS(c, containerName, blobList[0])
		raw := getDefaultSetPropertiesRawInput(rawBlobURLWithSAS.String(), common.EBlockBlobTier.Cool())
		raw.fromTo = "BlobNone"

		runCopyAndVerify(c, raw, func(err error) {
			c.Assert(err, chk.IsNil)

			// note that when we are targeting single blobs, the relative path is empty ("") since the root path already points to the blob
			validateRemoveTransfersAreScheduled(c, true, []string{""}, mockedRPC)
		})
	}
}

func (s *cmdIntegrationSuite) TestSetPropertiesBlobsUnderContainerWithFromTo(c *chk.C) {
	bsu := getBSU()

	// set up the container with numerous blobs
	containerURL, containerName := createNewContainer(c, bsu)
	defer deleteContainer(c, containerURL)
	blobList := scenarioHelper{}.generateCommonRemoteScenarioForBlobWithAccessTier(c, containerURL, "", azblob.AccessTierHot)

	c.Assert(containerURL, chk.NotNil)
	c.Assert(len(blobList), chk.Not(chk.Equals), 0)

	// set up interceptor
	mockedRPC := interceptor{}
	Rpc = mockedRPC.intercept
	mockedRPC.init()

	// construct the raw input to simulate user input
	rawContainerURLWithSAS := scenarioHelper{}.getRawContainerURLWithSAS(c, containerName)
	raw := getDefaultSetPropertiesRawInput(rawContainerURLWithSAS.String(), common.EBlockBlobTier.Cool())
	raw.fromTo = "BlobNone"
	raw.recursive = true
	raw.includeDirectoryStubs = false // The test target is a DFS account, which coincidentally created our directory stubs. Thus, we mustn't include them, since this is a test of blob.

	runCopyAndVerify(c, raw, func(err error) {
		c.Assert(err, chk.IsNil)

		// validate that the right number of transfers were scheduled
		c.Assert(len(mockedRPC.transfers), chk.Equals, len(blobList))

		// validate that the right transfers were sent
		validateRemoveTransfersAreScheduled(c, true, blobList, mockedRPC)
	})

	// turn off recursive, this time only top blobs should be deleted
	raw.recursive = false
	mockedRPC.reset()

	runCopyAndVerify(c, raw, func(err error) {
		c.Assert(err, chk.IsNil)
		c.Assert(len(mockedRPC.transfers), chk.Not(chk.Equals), len(blobList))

		for _, transfer := range mockedRPC.transfers {
			c.Assert(strings.Contains(transfer.Source, common.AZCOPY_PATH_SEPARATOR_STRING), chk.Equals, false)
		}
	})
}

func (s *cmdIntegrationSuite) TestSetPropertiesBlobsUnderVirtualDirWithFromTo(c *chk.C) {
	c.Skip("Enable after setting Account to non-HNS")
	bsu := getBSU()
	vdirName := "vdir1/vdir2/vdir3/"

	// set up the container with numerous blobs
	containerURL, containerName := createNewContainer(c, bsu)
	defer deleteContainer(c, containerURL)
	blobList := scenarioHelper{}.generateCommonRemoteScenarioForBlobWithAccessTier(c, containerURL, vdirName, azblob.AccessTierHot)

	c.Assert(containerURL, chk.NotNil)
	c.Assert(len(blobList), chk.Not(chk.Equals), 0)

	// set up interceptor
	mockedRPC := interceptor{}
	Rpc = mockedRPC.intercept
	mockedRPC.init()

	// construct the raw input to simulate user input
	rawVirtualDirectoryURLWithSAS := scenarioHelper{}.getRawBlobURLWithSAS(c, containerName, vdirName)
	raw := getDefaultSetPropertiesRawInput(rawVirtualDirectoryURLWithSAS.String(), common.EBlockBlobTier.Cool())
	raw.fromTo = "BlobNone"
	raw.recursive = true

	runCopyAndVerify(c, raw, func(err error) {
		c.Assert(err, chk.IsNil)

		// validate that the right number of transfers were scheduled
		c.Assert(len(mockedRPC.transfers), chk.Equals, len(blobList))

		// validate that the right transfers were sent
		expectedTransfers := scenarioHelper{}.shaveOffPrefix(blobList, vdirName)
		validateRemoveTransfersAreScheduled(c, true, expectedTransfers, mockedRPC)
	})

	// turn off recursive, this time only top blobs should be deleted
	raw.recursive = false
	mockedRPC.reset()

	runCopyAndVerify(c, raw, func(err error) {
		c.Assert(err, chk.IsNil)
		c.Assert(len(mockedRPC.transfers), chk.Not(chk.Equals), len(blobList))

		for _, transfer := range mockedRPC.transfers {
			c.Assert(strings.Contains(transfer.Source, common.AZCOPY_PATH_SEPARATOR_STRING), chk.Equals, false)
		}
	})
}
