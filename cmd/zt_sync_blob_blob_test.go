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
	"strings"

	"github.com/Azure/azure-storage-azcopy/common"
	"github.com/Azure/azure-storage-blob-go/azblob"
	chk "gopkg.in/check.v1"
)

// regular blob->file sync
func (s *cmdIntegrationSuite) TestSyncS2SWithSingleBlob(c *chk.C) {
	bsu := getBSU()
	srcContainerURL, srcContainerName := createNewContainer(c, bsu)
	dstContainerURL, dstContainerName := createNewContainer(c, bsu)
	defer deleteContainer(c, srcContainerURL)
	defer deleteContainer(c, dstContainerURL)

	for _, blobName := range []string{"singleblobisbest", "打麻将.txt", "%4509%4254$85140&"} {
		// set up the source container with a single blob
		blobList := []string{blobName}
		scenarioHelper{}.generateBlobsFromList(c, srcContainerURL, blobList, blockBlobDefaultData)

		// set up the destination container with the same single blob
		scenarioHelper{}.generateBlobsFromList(c, dstContainerURL, blobList, blockBlobDefaultData)

		// set up interceptor
		mockedRPC := interceptor{}
		Rpc = mockedRPC.intercept
		mockedRPC.init()

		// construct the raw input to simulate user input
		srcBlobURLWithSAS := scenarioHelper{}.getRawBlobURLWithSAS(c, srcContainerName, blobList[0])
		dstBlobURLWithSAS := scenarioHelper{}.getRawBlobURLWithSAS(c, dstContainerName, blobList[0])
		raw := getDefaultSyncRawInput(srcBlobURLWithSAS.String(), dstBlobURLWithSAS.String())

		// the destination was created after the source, so no sync should happen
		runSyncAndVerify(c, raw, func(err error) {
			c.Assert(err, chk.IsNil)

			// validate that the right number of transfers were scheduled
			c.Assert(len(mockedRPC.transfers), chk.Equals, 0)
		})

		// recreate the source blob to have a later last modified time
		scenarioHelper{}.generateBlobsFromList(c, srcContainerURL, blobList, blockBlobDefaultData)
		mockedRPC.reset()

		runSyncAndVerify(c, raw, func(err error) {
			c.Assert(err, chk.IsNil)
			validateS2SSyncTransfersAreScheduled(c, "", "", []string{""}, mockedRPC)
		})
	}
}

// regular container->container sync but destination is empty, so everything has to be transferred
func (s *cmdIntegrationSuite) TestSyncS2SWithEmptyDestination(c *chk.C) {
	bsu := getBSU()
	srcContainerURL, srcContainerName := createNewContainer(c, bsu)
	dstContainerURL, dstContainerName := createNewContainer(c, bsu)
	defer deleteContainer(c, srcContainerURL)
	defer deleteContainer(c, dstContainerURL)

	// set up the source container with numerous blobs
	blobList := scenarioHelper{}.generateCommonRemoteScenarioForBlob(c, srcContainerURL, "")
	c.Assert(len(blobList), chk.Not(chk.Equals), 0)

	// set up interceptor
	mockedRPC := interceptor{}
	Rpc = mockedRPC.intercept
	mockedRPC.init()

	// construct the raw input to simulate user input
	srcContainerURLWithSAS := scenarioHelper{}.getRawContainerURLWithSAS(c, srcContainerName)
	dstContainerURLWithSAS := scenarioHelper{}.getRawContainerURLWithSAS(c, dstContainerName)
	raw := getDefaultSyncRawInput(srcContainerURLWithSAS.String(), dstContainerURLWithSAS.String())

	// all blobs at source should be synced to destination
	runSyncAndVerify(c, raw, func(err error) {
		c.Assert(err, chk.IsNil)

		// validate that the right number of transfers were scheduled
		c.Assert(len(mockedRPC.transfers), chk.Equals, len(blobList))

		// validate that the right transfers were sent
		validateS2SSyncTransfersAreScheduled(c, "", "", blobList, mockedRPC)
	})

	// turn off recursive, this time only top blobs should be transferred
	raw.recursive = false
	mockedRPC.reset()
	runSyncAndVerify(c, raw, func(err error) {
		c.Assert(err, chk.IsNil)
		c.Assert(len(mockedRPC.transfers), chk.Not(chk.Equals), len(blobList))

		for _, transfer := range mockedRPC.transfers {
			c.Assert(strings.Contains(transfer.Source, common.AZCOPY_PATH_SEPARATOR_STRING), chk.Equals, false)
		}
	})
}

// regular container->container sync but destination is identical to the source, transfers are scheduled based on lmt
func (s *cmdIntegrationSuite) TestSyncS2SWithIdenticalDestination(c *chk.C) {
	bsu := getBSU()
	srcContainerURL, srcContainerName := createNewContainer(c, bsu)
	dstContainerURL, dstContainerName := createNewContainer(c, bsu)
	defer deleteContainer(c, srcContainerURL)
	defer deleteContainer(c, dstContainerURL)

	// set up the source container with numerous blobs
	blobList := scenarioHelper{}.generateCommonRemoteScenarioForBlob(c, srcContainerURL, "")
	c.Assert(len(blobList), chk.Not(chk.Equals), 0)

	// set up the destination with the exact same files
	scenarioHelper{}.generateBlobsFromList(c, dstContainerURL, blobList, blockBlobDefaultData)

	// set up interceptor
	mockedRPC := interceptor{}
	Rpc = mockedRPC.intercept
	mockedRPC.init()

	// construct the raw input to simulate user input
	srcContainerURLWithSAS := scenarioHelper{}.getRawContainerURLWithSAS(c, srcContainerName)
	dstContainerURLWithSAS := scenarioHelper{}.getRawContainerURLWithSAS(c, dstContainerName)
	raw := getDefaultSyncRawInput(srcContainerURLWithSAS.String(), dstContainerURLWithSAS.String())

	// nothing should be sync since the source is older
	runSyncAndVerify(c, raw, func(err error) {
		c.Assert(err, chk.IsNil)

		// validate that the right number of transfers were scheduled
		c.Assert(len(mockedRPC.transfers), chk.Equals, 0)
	})

	// refresh the source blobs' last modified time so that they get synced
	scenarioHelper{}.generateBlobsFromList(c, srcContainerURL, blobList, blockBlobDefaultData)
	mockedRPC.reset()
	runSyncAndVerify(c, raw, func(err error) {
		c.Assert(err, chk.IsNil)
		validateS2SSyncTransfersAreScheduled(c, "", "", blobList, mockedRPC)
	})
}

// regular container->container sync where destination is missing some files from source, and also has some extra files
func (s *cmdIntegrationSuite) TestSyncS2SWithMismatchedDestination(c *chk.C) {
	bsu := getBSU()
	srcContainerURL, srcContainerName := createNewContainer(c, bsu)
	dstContainerURL, dstContainerName := createNewContainer(c, bsu)
	defer deleteContainer(c, srcContainerURL)
	defer deleteContainer(c, dstContainerURL)

	// set up the container with numerous blobs
	blobList := scenarioHelper{}.generateCommonRemoteScenarioForBlob(c, srcContainerURL, "")
	c.Assert(len(blobList), chk.Not(chk.Equals), 0)

	// set up the destination with half of the blobs from source
	scenarioHelper{}.generateBlobsFromList(c, dstContainerURL, blobList[0:len(blobList)/2], blockBlobDefaultData)
	expectedOutput := blobList[len(blobList)/2:] // the missing half of source blobs should be transferred

	// add some extra blobs that shouldn't be included
	scenarioHelper{}.generateCommonRemoteScenarioForBlob(c, dstContainerURL, "extra")

	// set up interceptor
	mockedRPC := interceptor{}
	Rpc = mockedRPC.intercept
	mockedRPC.init()

	// construct the raw input to simulate user input
	srcContainerURLWithSAS := scenarioHelper{}.getRawContainerURLWithSAS(c, srcContainerName)
	dstContainerURLWithSAS := scenarioHelper{}.getRawContainerURLWithSAS(c, dstContainerName)
	raw := getDefaultSyncRawInput(srcContainerURLWithSAS.String(), dstContainerURLWithSAS.String())

	runSyncAndVerify(c, raw, func(err error) {
		c.Assert(err, chk.IsNil)
		validateS2SSyncTransfersAreScheduled(c, "", "", expectedOutput, mockedRPC)

		// make sure the extra blobs were deleted
		extraFilesFound := false
		for marker := (azblob.Marker{}); marker.NotDone(); {
			listResponse, err := dstContainerURL.ListBlobsFlatSegment(ctx, marker, azblob.ListBlobsSegmentOptions{})
			c.Assert(err, chk.IsNil)
			marker = listResponse.NextMarker

			// if ever the extra blobs are found, note it down
			for _, blob := range listResponse.Segment.BlobItems {
				if strings.Contains(blob.Name, "extra") {
					extraFilesFound = true
				}
			}
		}

		c.Assert(extraFilesFound, chk.Equals, false)
	})
}

// include flag limits the scope of source/destination comparison
func (s *cmdIntegrationSuite) TestSyncS2SWithIncludeFlag(c *chk.C) {
	bsu := getBSU()
	srcContainerURL, srcContainerName := createNewContainer(c, bsu)
	dstContainerURL, dstContainerName := createNewContainer(c, bsu)
	defer deleteContainer(c, srcContainerURL)
	defer deleteContainer(c, dstContainerURL)

	// set up the source container with numerous blobs
	blobList := scenarioHelper{}.generateCommonRemoteScenarioForBlob(c, srcContainerURL, "")
	c.Assert(len(blobList), chk.Not(chk.Equals), 0)

	// add special blobs that we wish to include
	blobsToInclude := []string{"important.pdf", "includeSub/amazing.jpeg", "exactName"}
	scenarioHelper{}.generateBlobsFromList(c, srcContainerURL, blobsToInclude, blockBlobDefaultData)
	includeString := "*.pdf;*.jpeg;exactName"

	// set up interceptor
	mockedRPC := interceptor{}
	Rpc = mockedRPC.intercept
	mockedRPC.init()

	// construct the raw input to simulate user input
	srcContainerURLWithSAS := scenarioHelper{}.getRawContainerURLWithSAS(c, srcContainerName)
	dstContainerURLWithSAS := scenarioHelper{}.getRawContainerURLWithSAS(c, dstContainerName)
	raw := getDefaultSyncRawInput(srcContainerURLWithSAS.String(), dstContainerURLWithSAS.String())
	raw.include = includeString

	// verify that only the blobs specified by the include flag are synced
	runSyncAndVerify(c, raw, func(err error) {
		c.Assert(err, chk.IsNil)
		validateS2SSyncTransfersAreScheduled(c, "", "", blobsToInclude, mockedRPC)
	})
}

// exclude flag limits the scope of source/destination comparison
func (s *cmdIntegrationSuite) TestSyncS2SWithExcludeFlag(c *chk.C) {
	bsu := getBSU()
	srcContainerURL, srcContainerName := createNewContainer(c, bsu)
	dstContainerURL, dstContainerName := createNewContainer(c, bsu)
	defer deleteContainer(c, srcContainerURL)
	defer deleteContainer(c, dstContainerURL)

	// set up the source container with numerous blobs
	blobList := scenarioHelper{}.generateCommonRemoteScenarioForBlob(c, srcContainerURL, "")
	c.Assert(len(blobList), chk.Not(chk.Equals), 0)

	// add special blobs that we wish to exclude
	blobsToExclude := []string{"notGood.pdf", "excludeSub/lame.jpeg", "exactName"}
	scenarioHelper{}.generateBlobsFromList(c, srcContainerURL, blobsToExclude, blockBlobDefaultData)
	excludeString := "*.pdf;*.jpeg;exactName"

	// set up interceptor
	mockedRPC := interceptor{}
	Rpc = mockedRPC.intercept
	mockedRPC.init()

	// construct the raw input to simulate user input
	srcContainerURLWithSAS := scenarioHelper{}.getRawContainerURLWithSAS(c, srcContainerName)
	dstContainerURLWithSAS := scenarioHelper{}.getRawContainerURLWithSAS(c, dstContainerName)
	raw := getDefaultSyncRawInput(srcContainerURLWithSAS.String(), dstContainerURLWithSAS.String())
	raw.exclude = excludeString

	// make sure the list doesn't include the blobs specified by the exclude flag
	runSyncAndVerify(c, raw, func(err error) {
		c.Assert(err, chk.IsNil)
		validateS2SSyncTransfersAreScheduled(c, "", "", blobList, mockedRPC)
	})
}

// include and exclude flag can work together to limit the scope of source/destination comparison
func (s *cmdIntegrationSuite) TestSyncS2SWithIncludeAndExcludeFlag(c *chk.C) {
	bsu := getBSU()
	srcContainerURL, srcContainerName := createNewContainer(c, bsu)
	dstContainerURL, dstContainerName := createNewContainer(c, bsu)
	defer deleteContainer(c, srcContainerURL)
	defer deleteContainer(c, dstContainerURL)

	// set up the source container with numerous blobs
	blobList := scenarioHelper{}.generateCommonRemoteScenarioForBlob(c, srcContainerURL, "")
	c.Assert(len(blobList), chk.Not(chk.Equals), 0)

	// add special blobs that we wish to include
	blobsToInclude := []string{"important.pdf", "includeSub/amazing.jpeg"}
	scenarioHelper{}.generateBlobsFromList(c, srcContainerURL, blobsToInclude, blockBlobDefaultData)
	includeString := "*.pdf;*.jpeg;exactName"

	// add special blobs that we wish to exclude
	// note that the excluded files also match the include string
	blobsToExclude := []string{"sorry.pdf", "exclude/notGood.jpeg", "exactName", "sub/exactName"}
	scenarioHelper{}.generateBlobsFromList(c, srcContainerURL, blobsToExclude, blockBlobDefaultData)
	excludeString := "so*;not*;exactName"

	// set up interceptor
	mockedRPC := interceptor{}
	Rpc = mockedRPC.intercept
	mockedRPC.init()

	// construct the raw input to simulate user input
	srcContainerURLWithSAS := scenarioHelper{}.getRawContainerURLWithSAS(c, srcContainerName)
	dstContainerURLWithSAS := scenarioHelper{}.getRawContainerURLWithSAS(c, dstContainerName)
	raw := getDefaultSyncRawInput(srcContainerURLWithSAS.String(), dstContainerURLWithSAS.String())
	raw.include = includeString
	raw.exclude = excludeString

	// verify that only the blobs specified by the include flag are synced
	runSyncAndVerify(c, raw, func(err error) {
		c.Assert(err, chk.IsNil)
		validateS2SSyncTransfersAreScheduled(c, "", "", blobsToInclude, mockedRPC)
	})
}

// validate the bug fix for this scenario
func (s *cmdIntegrationSuite) TestSyncS2SWithMissingDestination(c *chk.C) {
	bsu := getBSU()
	srcContainerURL, srcContainerName := createNewContainer(c, bsu)
	dstContainerURL, dstContainerName := createNewContainer(c, bsu)
	defer deleteContainer(c, srcContainerURL)

	// delete the destination container to simulate non-existing destination, or recently removed destination
	deleteContainer(c, dstContainerURL)

	// set up the container with numerous blobs
	blobList := scenarioHelper{}.generateCommonRemoteScenarioForBlob(c, srcContainerURL, "")
	c.Assert(len(blobList), chk.Not(chk.Equals), 0)

	// set up interceptor
	mockedRPC := interceptor{}
	Rpc = mockedRPC.intercept
	mockedRPC.init()

	// construct the raw input to simulate user input
	srcContainerURLWithSAS := scenarioHelper{}.getRawContainerURLWithSAS(c, srcContainerName)
	dstContainerURLWithSAS := scenarioHelper{}.getRawContainerURLWithSAS(c, dstContainerName)
	raw := getDefaultSyncRawInput(srcContainerURLWithSAS.String(), dstContainerURLWithSAS.String())

	// verify error is thrown
	runSyncAndVerify(c, raw, func(err error) {
		// error should not be nil, but the app should not crash either
		c.Assert(err, chk.NotNil)

		// validate that the right number of transfers were scheduled
		c.Assert(len(mockedRPC.transfers), chk.Equals, 0)
	})
}

// there is a type mismatch between the source and destination
func (s *cmdIntegrationSuite) TestSyncS2SMismatchContainerAndBlob(c *chk.C) {
	bsu := getBSU()
	srcContainerURL, srcContainerName := createNewContainer(c, bsu)
	dstContainerURL, dstContainerName := createNewContainer(c, bsu)
	defer deleteContainer(c, srcContainerURL)
	defer deleteContainer(c, dstContainerURL)

	// set up the source container with numerous blobs
	blobList := scenarioHelper{}.generateCommonRemoteScenarioForBlob(c, srcContainerURL, "")
	c.Assert(len(blobList), chk.Not(chk.Equals), 0)

	// set up the destination container with a single blob
	singleBlobName := "single"
	scenarioHelper{}.generateBlobsFromList(c, dstContainerURL, []string{singleBlobName}, blockBlobDefaultData)

	// set up interceptor
	mockedRPC := interceptor{}
	Rpc = mockedRPC.intercept
	mockedRPC.init()

	// construct the raw input to simulate user input
	srcContainerURLWithSAS := scenarioHelper{}.getRawContainerURLWithSAS(c, srcContainerName)
	dstBlobURLWithSAS := scenarioHelper{}.getRawBlobURLWithSAS(c, dstContainerName, singleBlobName)
	raw := getDefaultSyncRawInput(srcContainerURLWithSAS.String(), dstBlobURLWithSAS.String())

	// type mismatch, we should get an error
	runSyncAndVerify(c, raw, func(err error) {
		c.Assert(err, chk.NotNil)

		// validate that the right number of transfers were scheduled
		c.Assert(len(mockedRPC.transfers), chk.Equals, 0)
	})

	// reverse the source and destination
	raw = getDefaultSyncRawInput(dstBlobURLWithSAS.String(), srcContainerURLWithSAS.String())

	// type mismatch again, we should also get an error
	runSyncAndVerify(c, raw, func(err error) {
		c.Assert(err, chk.NotNil)

		// validate that the right number of transfers were scheduled
		c.Assert(len(mockedRPC.transfers), chk.Equals, 0)
	})
}

// container <-> virtual dir sync
func (s *cmdIntegrationSuite) TestSyncS2SContainerAndEmptyVirtualDir(c *chk.C) {
	bsu := getBSU()
	srcContainerURL, srcContainerName := createNewContainer(c, bsu)
	dstContainerURL, dstContainerName := createNewContainer(c, bsu)
	defer deleteContainer(c, srcContainerURL)
	defer deleteContainer(c, dstContainerURL)

	// set up the source container with numerous blobs
	blobList := scenarioHelper{}.generateCommonRemoteScenarioForBlob(c, srcContainerURL, "")
	c.Assert(len(blobList), chk.Not(chk.Equals), 0)

	// set up interceptor
	mockedRPC := interceptor{}
	Rpc = mockedRPC.intercept
	mockedRPC.init()

	// construct the raw input to simulate user input
	srcContainerURLWithSAS := scenarioHelper{}.getRawContainerURLWithSAS(c, srcContainerName)
	dstVirtualDirURLWithSAS := scenarioHelper{}.getRawBlobURLWithSAS(c, dstContainerName, "emptydir")
	raw := getDefaultSyncRawInput(srcContainerURLWithSAS.String(), dstVirtualDirURLWithSAS.String())

	// verify that targeting a virtual directory works fine
	runSyncAndVerify(c, raw, func(err error) {
		c.Assert(err, chk.IsNil)

		// validate that the right number of transfers were scheduled
		c.Assert(len(mockedRPC.transfers), chk.Equals, len(blobList))

		// validate that the right transfers were sent
		validateS2SSyncTransfersAreScheduled(c, "", "", blobList, mockedRPC)
	})

	// turn off recursive, this time only top blobs should be transferred
	raw.recursive = false
	mockedRPC.reset()

	runSyncAndVerify(c, raw, func(err error) {
		c.Assert(err, chk.IsNil)
		c.Assert(len(mockedRPC.transfers), chk.Not(chk.Equals), len(blobList))

		for _, transfer := range mockedRPC.transfers {
			c.Assert(strings.Contains(transfer.Source, common.AZCOPY_PATH_SEPARATOR_STRING), chk.Equals, false)
		}
	})
}

// regular vdir -> vdir sync
func (s *cmdIntegrationSuite) TestSyncS2SBetweenVirtualDirs(c *chk.C) {
	bsu := getBSU()
	srcContainerURL, srcContainerName := createNewContainer(c, bsu)
	dstContainerURL, dstContainerName := createNewContainer(c, bsu)
	defer deleteContainer(c, srcContainerURL)
	defer deleteContainer(c, dstContainerURL)

	// set up the source container with numerous blobs
	vdirName := "vdir"
	blobList := scenarioHelper{}.generateCommonRemoteScenarioForBlob(c, srcContainerURL, vdirName+common.AZCOPY_PATH_SEPARATOR_STRING)
	c.Assert(len(blobList), chk.Not(chk.Equals), 0)

	// set up the destination with the exact same files
	scenarioHelper{}.generateBlobsFromList(c, dstContainerURL, blobList, blockBlobDefaultData)

	// set up interceptor
	mockedRPC := interceptor{}
	Rpc = mockedRPC.intercept
	mockedRPC.init()

	// construct the raw input to simulate user input
	srcContainerURLWithSAS := scenarioHelper{}.getRawContainerURLWithSAS(c, srcContainerName)
	dstContainerURLWithSAS := scenarioHelper{}.getRawContainerURLWithSAS(c, dstContainerName)
	srcContainerURLWithSAS.Path += common.AZCOPY_PATH_SEPARATOR_STRING + vdirName
	dstContainerURLWithSAS.Path += common.AZCOPY_PATH_SEPARATOR_STRING + vdirName
	raw := getDefaultSyncRawInput(srcContainerURLWithSAS.String(), dstContainerURLWithSAS.String())

	// nothing should be synced since the source is older
	runSyncAndVerify(c, raw, func(err error) {
		c.Assert(err, chk.IsNil)

		// validate that the right number of transfers were scheduled
		c.Assert(len(mockedRPC.transfers), chk.Equals, 0)
	})

	// refresh the blobs' last modified time so that they are newer
	scenarioHelper{}.generateBlobsFromList(c, srcContainerURL, blobList, blockBlobDefaultData)
	mockedRPC.reset()
	expectedList := scenarioHelper{}.shaveOffPrefix(blobList, vdirName+common.AZCOPY_PATH_SEPARATOR_STRING)
	runSyncAndVerify(c, raw, func(err error) {
		c.Assert(err, chk.IsNil)
		validateS2SSyncTransfersAreScheduled(c, "", "", expectedList, mockedRPC)
	})
}

// examine situation where a blob has the same name as virtual dir
// trailing slash is used to disambiguate the path as a vdir
func (s *cmdIntegrationSuite) TestSyncS2SBetweenVirtualDirsWithConflictingBlob(c *chk.C) {
	bsu := getBSU()
	srcContainerURL, srcContainerName := createNewContainer(c, bsu)
	dstContainerURL, dstContainerName := createNewContainer(c, bsu)
	defer deleteContainer(c, srcContainerURL)
	defer deleteContainer(c, dstContainerURL)

	// set up the source container with numerous blobs
	vdirName := "vdir"
	blobList := scenarioHelper{}.generateCommonRemoteScenarioForBlob(c, srcContainerURL,
		vdirName+common.AZCOPY_PATH_SEPARATOR_STRING)
	c.Assert(len(blobList), chk.Not(chk.Equals), 0)

	// set up the destination with the exact same files
	scenarioHelper{}.generateBlobsFromList(c, dstContainerURL, blobList, blockBlobDefaultData)

	// create a blob at the destination with the exact same name as the vdir
	scenarioHelper{}.generateBlobsFromList(c, dstContainerURL, []string{vdirName}, blockBlobDefaultData)

	// set up interceptor
	mockedRPC := interceptor{}
	Rpc = mockedRPC.intercept
	mockedRPC.init()

	// case 1: vdir -> blob sync: should fail
	srcContainerURLWithSAS := scenarioHelper{}.getRawContainerURLWithSAS(c, srcContainerName)
	dstContainerURLWithSAS := scenarioHelper{}.getRawContainerURLWithSAS(c, dstContainerName)
	srcContainerURLWithSAS.Path += common.AZCOPY_PATH_SEPARATOR_STRING + vdirName
	dstContainerURLWithSAS.Path += common.AZCOPY_PATH_SEPARATOR_STRING + vdirName
	// construct the raw input to simulate user input
	raw := getDefaultSyncRawInput(srcContainerURLWithSAS.String(), dstContainerURLWithSAS.String())
	runSyncAndVerify(c, raw, func(err error) {
		c.Assert(err, chk.NotNil)

		// validate that the right number of transfers were scheduled
		c.Assert(len(mockedRPC.transfers), chk.Equals, 0)
	})

	// case 2: blob -> vdir sync: simply swap src and dst, should fail too
	raw = getDefaultSyncRawInput(dstContainerURLWithSAS.String(), srcContainerURLWithSAS.String())
	runSyncAndVerify(c, raw, func(err error) {
		c.Assert(err, chk.NotNil)

		// validate that the right number of transfers were scheduled
		c.Assert(len(mockedRPC.transfers), chk.Equals, 0)
	})

	// case 3: blob -> blob: if source is also a blob, then single blob to blob sync happens
	// create a blob at the source with the exact same name as the vdir
	scenarioHelper{}.generateBlobsFromList(c, srcContainerURL, []string{vdirName}, blockBlobDefaultData)
	raw = getDefaultSyncRawInput(srcContainerURLWithSAS.String(), dstContainerURLWithSAS.String())
	runSyncAndVerify(c, raw, func(err error) {
		c.Assert(err, chk.IsNil)
		validateS2SSyncTransfersAreScheduled(c, "", "", []string{""}, mockedRPC)
	})

	// refresh the dst blobs' last modified time so that they are newer
	scenarioHelper{}.generateBlobsFromList(c, srcContainerURL, blobList, blockBlobDefaultData)
	mockedRPC.reset()

	// case 4: vdir -> vdir: adding a trailing slash helps to clarify it should be treated as virtual dir
	srcContainerURLWithSAS.Path += common.AZCOPY_PATH_SEPARATOR_STRING
	dstContainerURLWithSAS.Path += common.AZCOPY_PATH_SEPARATOR_STRING
	raw = getDefaultSyncRawInput(srcContainerURLWithSAS.String(), dstContainerURLWithSAS.String())
	expectedList := scenarioHelper{}.shaveOffPrefix(blobList, vdirName+common.AZCOPY_PATH_SEPARATOR_STRING)
	runSyncAndVerify(c, raw, func(err error) {
		c.Assert(err, chk.IsNil)
		validateS2SSyncTransfersAreScheduled(c, "", "", expectedList, mockedRPC)
	})
}

// sync a vdir with a blob representing an ADLS directory
// we should recognize this and sync with the virtual directory instead
func (s *cmdIntegrationSuite) TestSyncS2SADLSDirectory(c *chk.C) {
	bsu := getBSU()
	srcContainerURL, srcContainerName := createNewContainer(c, bsu)
	dstContainerURL, dstContainerName := createNewContainer(c, bsu)
	defer deleteContainer(c, srcContainerURL)
	defer deleteContainer(c, dstContainerURL)

	// set up the source container with numerous blobs
	vdirName := "vdir"
	blobList := scenarioHelper{}.generateCommonRemoteScenarioForBlob(c, srcContainerURL,
		vdirName+common.AZCOPY_PATH_SEPARATOR_STRING)
	c.Assert(len(blobList), chk.Not(chk.Equals), 0)

	// set up the destination with the exact same files
	scenarioHelper{}.generateBlobsFromList(c, dstContainerURL, blobList, blockBlobDefaultData)

	// create an ADLS Gen2 directory at the source with the exact same name as the vdir
	_, err := srcContainerURL.NewBlockBlobURL(vdirName).Upload(context.Background(), bytes.NewReader(nil),
		azblob.BlobHTTPHeaders{}, azblob.Metadata{"hdi_isfolder": "true"}, azblob.BlobAccessConditions{})
	c.Assert(err, chk.IsNil)

	// set up interceptor
	mockedRPC := interceptor{}
	Rpc = mockedRPC.intercept
	mockedRPC.init()

	// ADLS Gen2 directory -> vdir sync: should work
	// but since the src files are older, nothing should be synced
	srcContainerURLWithSAS := scenarioHelper{}.getRawContainerURLWithSAS(c, srcContainerName)
	dstContainerURLWithSAS := scenarioHelper{}.getRawContainerURLWithSAS(c, dstContainerName)
	srcContainerURLWithSAS.Path += common.AZCOPY_PATH_SEPARATOR_STRING + vdirName
	dstContainerURLWithSAS.Path += common.AZCOPY_PATH_SEPARATOR_STRING + vdirName
	// construct the raw input to simulate user input
	raw := getDefaultSyncRawInput(srcContainerURLWithSAS.String(), dstContainerURLWithSAS.String())
	runSyncAndVerify(c, raw, func(err error) {
		c.Assert(err, chk.IsNil)

		// validate that the right number of transfers were scheduled
		c.Assert(len(mockedRPC.transfers), chk.Equals, 0)
	})

	// refresh the sources blobs' last modified time so that they are newer
	scenarioHelper{}.generateBlobsFromList(c, srcContainerURL, blobList, blockBlobDefaultData)
	mockedRPC.reset()

	expectedTransfers := scenarioHelper{}.shaveOffPrefix(blobList, vdirName+common.AZCOPY_PATH_SEPARATOR_STRING)
	runSyncAndVerify(c, raw, func(err error) {
		c.Assert(err, chk.IsNil)
		validateS2SSyncTransfersAreScheduled(c, "", "", expectedTransfers, mockedRPC)
	})
}
