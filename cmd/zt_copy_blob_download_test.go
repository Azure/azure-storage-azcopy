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
	"path/filepath"
	"strings"
)

// regular blob->local file download
func (s *cmdIntegrationSuite) TestDownloadSingleBlobToFile(c *chk.C) {
	bsu := getBSU()
	containerURL, containerName := createNewContainer(c, bsu)
	defer deleteContainer(c, containerURL)

	for _, blobName := range []string{"singleblobisbest", "打麻将.txt", "%4509%4254$85140&"} {
		// set up the container with a single blob
		blobList := []string{blobName}
		scenarioHelper{}.generateBlobsFromList(c, containerURL, blobList, blockBlobDefaultData)
		c.Assert(containerURL, chk.NotNil)

		// set up the destination as a single file
		dstDirName := scenarioHelper{}.generateLocalDirectory(c)
		dstFileName := "whatever"
		scenarioHelper{}.generateLocalFilesFromList(c, dstDirName, blobList)

		// set up interceptor
		mockedRPC := interceptor{}
		Rpc = mockedRPC.intercept
		mockedRPC.init()

		// construct the raw input to simulate user input
		rawBlobURLWithSAS := scenarioHelper{}.getRawBlobURLWithSAS(c, containerName, blobList[0])
		raw := getDefaultCopyRawInput(rawBlobURLWithSAS.String(), filepath.Join(dstDirName, dstFileName))

		// the file was created after the blob, so no sync should happen
		runCopyAndVerify(c, raw, func(err error) {
			c.Assert(err, chk.IsNil)

			validateDownloadTransfersAreScheduled(c, "", "", []string{""}, mockedRPC)
		})

		// clean the RPC for the next test
		mockedRPC.reset()

		// now target the destination directory, the result should be the same
		raw = getDefaultCopyRawInput(rawBlobURLWithSAS.String(), dstDirName)

		// the file was created after the blob, so no sync should happen
		runCopyAndVerify(c, raw, func(err error) {
			c.Assert(err, chk.IsNil)

			// verify explicitly since the source and destination names will be different:
			// the source is "" since the given URL points to the blob itself
			// the destination should be the blob name, since the given local path points to the parent dir
			c.Assert(len(mockedRPC.transfers), chk.Equals, 1)
			c.Assert(mockedRPC.transfers[0].Source, chk.Equals, "")
			c.Assert(mockedRPC.transfers[0].Destination, chk.Equals, common.AZCOPY_PATH_SEPARATOR_STRING+blobName)
		})
	}
}

// regular container->directory download
func (s *cmdIntegrationSuite) TestDownloadBlobContainer(c *chk.C) {
	bsu := getBSU()

	// set up the container with numerous blobs
	containerURL, containerName := createNewContainer(c, bsu)
	blobList := scenarioHelper{}.generateCommonRemoteScenarioForBlob(c, containerURL, "")
	defer deleteContainer(c, containerURL)
	c.Assert(containerURL, chk.NotNil)
	c.Assert(len(blobList), chk.Not(chk.Equals), 0)

	// set up the destination with an empty folder
	dstDirName := scenarioHelper{}.generateLocalDirectory(c)

	// set up interceptor
	mockedRPC := interceptor{}
	Rpc = mockedRPC.intercept
	mockedRPC.init()

	// construct the raw input to simulate user input
	rawContainerURLWithSAS := scenarioHelper{}.getRawContainerURLWithSAS(c, containerName)
	raw := getDefaultCopyRawInput(rawContainerURLWithSAS.String(), dstDirName)
	raw.recursive = true

	runCopyAndVerify(c, raw, func(err error) {
		c.Assert(err, chk.IsNil)

		// validate that the right number of transfers were scheduled
		c.Assert(len(mockedRPC.transfers), chk.Equals, len(blobList))

		// validate that the right transfers were sent
		validateDownloadTransfersAreScheduled(c, common.AZCOPY_PATH_SEPARATOR_STRING, common.AZCOPY_PATH_SEPARATOR_STRING+containerName+common.AZCOPY_PATH_SEPARATOR_STRING, blobList, mockedRPC)
	})

	// turn off recursive, this time nothing should be transferred
	raw.recursive = false
	mockedRPC.reset()

	runCopyAndVerify(c, raw, func(err error) {
		c.Assert(err, chk.NotNil)
		c.Assert(len(mockedRPC.transfers), chk.Equals, 0)
	})
}

// regular vdir->dir download
func (s *cmdIntegrationSuite) TestDownloadBlobVirtualDirectory(c *chk.C) {
	bsu := getBSU()
	vdirName := "vdir1"

	// set up the container with numerous blobs
	containerURL, containerName := createNewContainer(c, bsu)
	blobList := scenarioHelper{}.generateCommonRemoteScenarioForBlob(c, containerURL, vdirName+common.AZCOPY_PATH_SEPARATOR_STRING)
	defer deleteContainer(c, containerURL)
	c.Assert(containerURL, chk.NotNil)
	c.Assert(len(blobList), chk.Not(chk.Equals), 0)

	// set up the destination with an empty folder
	dstDirName := scenarioHelper{}.generateLocalDirectory(c)

	// set up interceptor
	mockedRPC := interceptor{}
	Rpc = mockedRPC.intercept
	mockedRPC.init()

	// construct the raw input to simulate user input
	rawContainerURLWithSAS := scenarioHelper{}.getRawBlobURLWithSAS(c, containerName, vdirName)
	raw := getDefaultCopyRawInput(rawContainerURLWithSAS.String(), dstDirName)
	raw.recursive = true

	runCopyAndVerify(c, raw, func(err error) {
		c.Assert(err, chk.IsNil)

		// validate that the right number of transfers were scheduled
		c.Assert(len(mockedRPC.transfers), chk.Equals, len(blobList))

		// validate that the right transfers were sent
		expectedTransfers := scenarioHelper{}.shaveOffPrefix(blobList, vdirName+common.AZCOPY_PATH_SEPARATOR_STRING)
		validateDownloadTransfersAreScheduled(c, common.AZCOPY_PATH_SEPARATOR_STRING,
			common.AZCOPY_PATH_SEPARATOR_STRING+vdirName+common.AZCOPY_PATH_SEPARATOR_STRING, expectedTransfers, mockedRPC)
	})

	// turn off recursive, this time nothing should be transferred
	raw.recursive = false
	mockedRPC.reset()

	runCopyAndVerify(c, raw, func(err error) {
		c.Assert(err, chk.NotNil)
		c.Assert(len(mockedRPC.transfers), chk.Equals, 0)
	})
}

// blobs(from pattern)->directory download
// TODO the current pattern matching behavior is inconsistent with the posix filesystem
//   update test after re-writing copy enumerators
func (s *cmdIntegrationSuite) TestDownloadBlobContainerWithPattern(c *chk.C) {
	bsu := getBSU()

	// set up the container with numerous blobs
	containerURL, containerName := createNewContainer(c, bsu)
	blobsToIgnore := scenarioHelper{}.generateCommonRemoteScenarioForBlob(c, containerURL, "")
	defer deleteContainer(c, containerURL)
	c.Assert(containerURL, chk.NotNil)
	c.Assert(len(blobsToIgnore), chk.Not(chk.Equals), 0)

	// add special blobs that we wish to include
	blobsToInclude := []string{"important.pdf", "includeSub/amazing.pdf", "includeSub/wow/amazing.pdf"}
	scenarioHelper{}.generateBlobsFromList(c, containerURL, blobsToInclude, blockBlobDefaultData)

	// set up the destination with an empty folder
	dstDirName := scenarioHelper{}.generateLocalDirectory(c)

	// set up interceptor
	mockedRPC := interceptor{}
	Rpc = mockedRPC.intercept
	mockedRPC.init()

	// construct the raw input to simulate user input
	rawContainerURLWithSAS := scenarioHelper{}.getRawContainerURLWithSAS(c, containerName)
	rawContainerURLWithSAS.Path += "/*.pdf"
	raw := getDefaultCopyRawInput(rawContainerURLWithSAS.String(), dstDirName)
	raw.recursive = true

	runCopyAndVerify(c, raw, func(err error) {
		c.Assert(err, chk.IsNil)

		// validate that the right number of transfers were scheduled
		c.Assert(len(mockedRPC.transfers), chk.Equals, len(blobsToInclude))

		// validate that the right transfers were sent
		validateDownloadTransfersAreScheduled(c, common.AZCOPY_PATH_SEPARATOR_STRING, common.AZCOPY_PATH_SEPARATOR_STRING,
			blobsToInclude, mockedRPC)
	})

	// turn off recursive, this time nothing should be transferred
	raw.recursive = false
	mockedRPC.reset()

	runCopyAndVerify(c, raw, func(err error) {
		c.Assert(err, chk.IsNil)

		// only the top pdf should be included
		c.Assert(len(mockedRPC.transfers), chk.Equals, 1)
		c.Assert(mockedRPC.transfers[0].Source, chk.Equals, mockedRPC.transfers[0].Destination)
		c.Assert(strings.HasSuffix(mockedRPC.transfers[0].Source, ".pdf"), chk.Equals, true)
		c.Assert(strings.Contains(mockedRPC.transfers[0].Source[1:], common.AZCOPY_PATH_SEPARATOR_STRING), chk.Equals, false)
	})
}
