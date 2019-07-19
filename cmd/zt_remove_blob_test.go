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
	"github.com/Azure/azure-storage-azcopy/common"
	chk "gopkg.in/check.v1"
	"net/url"
	"strings"
)

func (s *cmdIntegrationSuite) TestRemoveSingleBlob(c *chk.C) {
	bsu := getBSU()
	containerURL, containerName := createNewContainer(c, bsu)
	defer deleteContainer(c, containerURL)

	for _, blobName := range []string{"top/mid/low/singleblobisbest", "打麻将.txt", "%4509%4254$85140&"} {
		// set up the container with a single blob
		blobList := []string{blobName}
		scenarioHelper{}.generateBlobsFromList(c, containerURL, blobList, blockBlobDefaultData)
		c.Assert(containerURL, chk.NotNil)

		// set up interceptor
		mockedRPC := interceptor{}
		Rpc = mockedRPC.intercept
		mockedRPC.init()

		// construct the raw input to simulate user input
		rawBlobURLWithSAS := scenarioHelper{}.getRawBlobURLWithSAS(c, containerName, blobList[0])
		raw := getDefaultRemoveRawInput(rawBlobURLWithSAS.String())

		runCopyAndVerify(c, raw, func(err error) {
			c.Assert(err, chk.IsNil)

			// note that when we are targeting single blobs, the relative path is empty ("") since the root path already points to the blob
			validateRemoveTransfersAreScheduled(c, true, []string{""}, mockedRPC)
		})
	}
}

func (s *cmdIntegrationSuite) TestRemoveBlobsUnderContainer(c *chk.C) {
	bsu := getBSU()

	// set up the container with numerous blobs
	containerURL, containerName := createNewContainer(c, bsu)
	defer deleteContainer(c, containerURL)
	blobList := scenarioHelper{}.generateCommonRemoteScenarioForBlob(c, containerURL, "")
	c.Assert(containerURL, chk.NotNil)
	c.Assert(len(blobList), chk.Not(chk.Equals), 0)

	// set up interceptor
	mockedRPC := interceptor{}
	Rpc = mockedRPC.intercept
	mockedRPC.init()

	// construct the raw input to simulate user input
	rawContainerURLWithSAS := scenarioHelper{}.getRawContainerURLWithSAS(c, containerName)
	raw := getDefaultRemoveRawInput(rawContainerURLWithSAS.String())
	raw.recursive = true

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

func (s *cmdIntegrationSuite) TestRemoveBlobsUnderVirtualDir(c *chk.C) {
	bsu := getBSU()
	vdirName := "vdir1/vdir2/vdir3/"

	// set up the container with numerous blobs
	containerURL, containerName := createNewContainer(c, bsu)
	defer deleteContainer(c, containerURL)
	blobList := scenarioHelper{}.generateCommonRemoteScenarioForBlob(c, containerURL, vdirName)
	c.Assert(containerURL, chk.NotNil)
	c.Assert(len(blobList), chk.Not(chk.Equals), 0)

	// set up interceptor
	mockedRPC := interceptor{}
	Rpc = mockedRPC.intercept
	mockedRPC.init()

	// construct the raw input to simulate user input
	rawVirtualDirectoryURLWithSAS := scenarioHelper{}.getRawBlobURLWithSAS(c, containerName, vdirName)
	raw := getDefaultRemoveRawInput(rawVirtualDirectoryURLWithSAS.String())
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

// include flag limits the scope of the delete
func (s *cmdIntegrationSuite) TestRemoveWithIncludeFlag(c *chk.C) {
	bsu := getBSU()

	// set up the container with numerous blobs
	containerURL, containerName := createNewContainer(c, bsu)
	blobList := scenarioHelper{}.generateCommonRemoteScenarioForBlob(c, containerURL, "")
	defer deleteContainer(c, containerURL)
	c.Assert(containerURL, chk.NotNil)
	c.Assert(len(blobList), chk.Not(chk.Equals), 0)

	// add special blobs that we wish to include
	blobsToInclude := []string{"important.pdf", "includeSub/amazing.jpeg", "exactName"}
	scenarioHelper{}.generateBlobsFromList(c, containerURL, blobsToInclude, blockBlobDefaultData)
	includeString := "*.pdf;*.jpeg;exactName"

	// set up interceptor
	mockedRPC := interceptor{}
	Rpc = mockedRPC.intercept
	mockedRPC.init()

	// construct the raw input to simulate user input
	rawContainerURLWithSAS := scenarioHelper{}.getRawContainerURLWithSAS(c, containerName)
	raw := getDefaultRemoveRawInput(rawContainerURLWithSAS.String())
	raw.include = includeString
	raw.recursive = true

	runCopyAndVerify(c, raw, func(err error) {
		c.Assert(err, chk.IsNil)
		validateDownloadTransfersAreScheduled(c, "", "", blobsToInclude, mockedRPC)
	})
}

// exclude flag limits the scope of the delete
func (s *cmdIntegrationSuite) TestRemoveWithExcludeFlag(c *chk.C) {
	bsu := getBSU()

	// set up the container with numerous blobs
	containerURL, containerName := createNewContainer(c, bsu)
	blobList := scenarioHelper{}.generateCommonRemoteScenarioForBlob(c, containerURL, "")
	defer deleteContainer(c, containerURL)
	c.Assert(containerURL, chk.NotNil)
	c.Assert(len(blobList), chk.Not(chk.Equals), 0)

	// add special blobs that we wish to exclude
	blobsToExclude := []string{"notGood.pdf", "excludeSub/lame.jpeg", "exactName"}
	scenarioHelper{}.generateBlobsFromList(c, containerURL, blobsToExclude, blockBlobDefaultData)
	excludeString := "*.pdf;*.jpeg;exactName"

	// set up interceptor
	mockedRPC := interceptor{}
	Rpc = mockedRPC.intercept
	mockedRPC.init()

	// construct the raw input to simulate user input
	rawContainerURLWithSAS := scenarioHelper{}.getRawContainerURLWithSAS(c, containerName)
	raw := getDefaultRemoveRawInput(rawContainerURLWithSAS.String())
	raw.exclude = excludeString
	raw.recursive = true

	runCopyAndVerify(c, raw, func(err error) {
		c.Assert(err, chk.IsNil)
		validateDownloadTransfersAreScheduled(c, "", "", blobList, mockedRPC)
	})
}

// include and exclude flag can work together to limit the scope of the delete
func (s *cmdIntegrationSuite) TestRemoveWithIncludeAndExcludeFlag(c *chk.C) {
	bsu := getBSU()

	// set up the container with numerous blobs
	containerURL, containerName := createNewContainer(c, bsu)
	blobList := scenarioHelper{}.generateCommonRemoteScenarioForBlob(c, containerURL, "")
	defer deleteContainer(c, containerURL)
	c.Assert(containerURL, chk.NotNil)
	c.Assert(len(blobList), chk.Not(chk.Equals), 0)

	// add special blobs that we wish to include
	blobsToInclude := []string{"important.pdf", "includeSub/amazing.jpeg"}
	scenarioHelper{}.generateBlobsFromList(c, containerURL, blobsToInclude, blockBlobDefaultData)
	includeString := "*.pdf;*.jpeg;exactName"

	// add special blobs that we wish to exclude
	// note that the excluded files also match the include string
	blobsToExclude := []string{"sorry.pdf", "exclude/notGood.jpeg", "exactName", "sub/exactName"}
	scenarioHelper{}.generateBlobsFromList(c, containerURL, blobsToExclude, blockBlobDefaultData)
	excludeString := "so*;not*;exactName"

	// set up interceptor
	mockedRPC := interceptor{}
	Rpc = mockedRPC.intercept
	mockedRPC.init()

	// construct the raw input to simulate user input
	rawContainerURLWithSAS := scenarioHelper{}.getRawContainerURLWithSAS(c, containerName)
	raw := getDefaultRemoveRawInput(rawContainerURLWithSAS.String())
	raw.include = includeString
	raw.exclude = excludeString
	raw.recursive = true

	runCopyAndVerify(c, raw, func(err error) {
		c.Assert(err, chk.IsNil)
		validateDownloadTransfersAreScheduled(c, "", "", blobsToInclude, mockedRPC)
	})
}

// note: list-of-files flag is used
func (s *cmdIntegrationSuite) TestRemoveListOfBlobsAndVirtualDirs(c *chk.C) {
	bsu := getBSU()
	vdirName := "megadir"

	// set up the container with numerous blobs and a vdir
	containerURL, containerName := createNewContainer(c, bsu)
	defer deleteContainer(c, containerURL)
	c.Assert(containerURL, chk.NotNil)
	blobListPart1 := scenarioHelper{}.generateCommonRemoteScenarioForBlob(c, containerURL, "")
	blobListPart2 := scenarioHelper{}.generateCommonRemoteScenarioForBlob(c, containerURL, vdirName+"/")
	blobList := append(blobListPart1, blobListPart2...)
	c.Assert(len(blobList), chk.Not(chk.Equals), 0)

	// set up interceptor
	mockedRPC := interceptor{}
	Rpc = mockedRPC.intercept
	mockedRPC.init()

	// construct the raw input to simulate user input
	rawContainerURLWithSAS := scenarioHelper{}.getRawContainerURLWithSAS(c, containerName)
	raw := getDefaultRemoveRawInput(rawContainerURLWithSAS.String())
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

// note: list-of-files flag is used
func (s *cmdIntegrationSuite) TestRemoveListOfBlobsWithIncludeAndExclude(c *chk.C) {
	bsu := getBSU()
	vdirName := "megadir"

	// set up the container with numerous blobs and a vdir
	containerURL, containerName := createNewContainer(c, bsu)
	defer deleteContainer(c, containerURL)
	c.Assert(containerURL, chk.NotNil)
	blobListPart1 := scenarioHelper{}.generateCommonRemoteScenarioForBlob(c, containerURL, "")
	scenarioHelper{}.generateCommonRemoteScenarioForBlob(c, containerURL, vdirName+"/")

	// add special blobs that we wish to include
	blobsToInclude := []string{"important.pdf", "includeSub/amazing.jpeg"}
	scenarioHelper{}.generateBlobsFromList(c, containerURL, blobsToInclude, blockBlobDefaultData)
	includeString := "*.pdf;*.jpeg;exactName"

	// add special blobs that we wish to exclude
	// note that the excluded files also match the include string
	blobsToExclude := []string{"sorry.pdf", "exclude/notGood.jpeg", "exactName", "sub/exactName"}
	scenarioHelper{}.generateBlobsFromList(c, containerURL, blobsToExclude, blockBlobDefaultData)
	excludeString := "so*;not*;exactName"

	// set up interceptor
	mockedRPC := interceptor{}
	Rpc = mockedRPC.intercept
	mockedRPC.init()

	// construct the raw input to simulate user input
	rawContainerURLWithSAS := scenarioHelper{}.getRawContainerURLWithSAS(c, containerName)
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
	raw.listOfFilesToCopy = scenarioHelper{}.generateListOfFiles(c, listOfFiles)

	runCopyAndVerify(c, raw, func(err error) {
		c.Assert(err, chk.IsNil)

		// validate that the right number of transfers were scheduled
		c.Assert(len(mockedRPC.transfers), chk.Equals, len(blobsToInclude))

		// validate that the right transfers were sent
		validateRemoveTransfersAreScheduled(c, true, blobsToInclude, mockedRPC)
	})
}
