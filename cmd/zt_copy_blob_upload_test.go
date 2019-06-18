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
	"path/filepath"
	"strings"
)

// regular local file->blob upload
func (s *cmdIntegrationSuite) TestUploadSingleFileToBlobVirtualDirectory(c *chk.C) {
	bsu := getBSU()
	containerURL, containerName := createNewContainer(c, bsu)
	defer deleteContainer(c, containerURL)

	for _, srcFileName := range []string{"singleblobisbest", "打麻将.txt", "%4509%4254$85140&"} {
		// set up the source as a single file
		srcDirName := scenarioHelper{}.generateLocalDirectory(c)
		fileList := []string{srcFileName}
		scenarioHelper{}.generateLocalFilesFromList(c, srcDirName, fileList)

		// set up the destination container with a single blob
		dstBlobName := "testfolder/"

		// set up interceptor
		mockedRPC := interceptor{}
		Rpc = mockedRPC.intercept
		mockedRPC.init()

		// construct the raw input to simulate user input
		rawBlobURLWithSAS := scenarioHelper{}.getRawBlobURLWithSAS(c, containerName, dstBlobName)
		raw := getDefaultCopyRawInput(filepath.Join(srcDirName, srcFileName), rawBlobURLWithSAS.String())

		// the blob was created after the file, so no sync should happen
		runCopyAndVerify(c, raw, func(err error) {
			c.Assert(err, chk.IsNil)

			// Validate that the destination is the file name (within the folder).
			// The destination being the folder *was* the issue in the past.
			// The service would just name the file as the folder if we didn't explicitly specify it.
			c.Assert(len(mockedRPC.transfers), chk.Equals, 1)
			d, err := url.PathUnescape(mockedRPC.transfers[0].Destination) //Unescape the destination, as we have special characters.
			c.Assert(err, chk.IsNil)
			c.Assert(d, chk.Equals, srcFileName)
		})

		// clean the RPC for the next test
		mockedRPC.reset()

		// now target the destination container, the result should be the same
		rawContainerURLWithSAS := scenarioHelper{}.getRawContainerURLWithSAS(c, containerName)
		raw = getDefaultCopyRawInput(filepath.Join(srcDirName, srcFileName), rawContainerURLWithSAS.String())

		// the file was created after the blob, so no sync should happen
		runCopyAndVerify(c, raw, func(err error) {
			c.Assert(err, chk.IsNil)

			// verify explicitly since the source and destination names will be different:
			// the source is "" since the given URL points to the blob itself
			// the destination should be the blob name, since the given local path points to the parent dir
			c.Assert(len(mockedRPC.transfers), chk.Equals, 1)

			c.Assert(mockedRPC.transfers[0].Source, chk.Equals, "")
			c.Assert(mockedRPC.transfers[0].Destination, chk.Equals, common.AZCOPY_PATH_SEPARATOR_STRING+url.PathEscape(srcFileName))
		})
	}
}

// regular local file->blob upload
func (s *cmdIntegrationSuite) TestUploadSingleFileToBlob(c *chk.C) {
	bsu := getBSU()
	containerURL, containerName := createNewContainer(c, bsu)
	defer deleteContainer(c, containerURL)

	for _, srcFileName := range []string{"singleblobisbest", "打麻将.txt", "%4509%4254$85140&"} {
		// set up the source as a single file
		srcDirName := scenarioHelper{}.generateLocalDirectory(c)
		fileList := []string{srcFileName}
		scenarioHelper{}.generateLocalFilesFromList(c, srcDirName, fileList)

		// set up the destination container with a single blob
		dstBlobName := "whatever"
		scenarioHelper{}.generateBlobsFromList(c, containerURL, []string{dstBlobName})
		c.Assert(containerURL, chk.NotNil)

		// set up interceptor
		mockedRPC := interceptor{}
		Rpc = mockedRPC.intercept
		mockedRPC.init()

		// construct the raw input to simulate user input
		rawBlobURLWithSAS := scenarioHelper{}.getRawBlobURLWithSAS(c, containerName, dstBlobName)
		raw := getDefaultCopyRawInput(filepath.Join(srcDirName, srcFileName), rawBlobURLWithSAS.String())

		// the blob was created after the file, so no sync should happen
		runCopyAndVerify(c, raw, func(err error) {
			c.Assert(err, chk.IsNil)

			// validate that the right number of transfers were scheduled
			validateUploadTransfersAreScheduled(c, "", "", []string{""}, mockedRPC)
		})

		// clean the RPC for the next test
		mockedRPC.reset()

		// now target the destination container, the result should be the same
		rawContainerURLWithSAS := scenarioHelper{}.getRawContainerURLWithSAS(c, containerName)
		raw = getDefaultCopyRawInput(filepath.Join(srcDirName, srcFileName), rawContainerURLWithSAS.String())

		// the file was created after the blob, so no sync should happen
		runCopyAndVerify(c, raw, func(err error) {
			c.Assert(err, chk.IsNil)

			// verify explicitly since the source and destination names will be different:
			// the source is "" since the given URL points to the blob itself
			// the destination should be the blob name, since the given local path points to the parent dir
			c.Assert(len(mockedRPC.transfers), chk.Equals, 1)

			c.Assert(mockedRPC.transfers[0].Source, chk.Equals, "")
			c.Assert(mockedRPC.transfers[0].Destination, chk.Equals, common.AZCOPY_PATH_SEPARATOR_STRING+url.PathEscape(srcFileName))
		})
	}
}

// regular directory->container upload
func (s *cmdIntegrationSuite) TestUploadDirectoryToContainer(c *chk.C) {
	bsu := getBSU()

	// set up the source with numerous files
	srcDirPath := scenarioHelper{}.generateLocalDirectory(c)
	fileList := scenarioHelper{}.generateCommonRemoteScenarioForLocal(c, srcDirPath, "")

	// set up an empty container
	containerURL, containerName := createNewContainer(c, bsu)
	defer deleteContainer(c, containerURL)

	// set up interceptor
	mockedRPC := interceptor{}
	Rpc = mockedRPC.intercept
	mockedRPC.init()

	// construct the raw input to simulate user input
	rawContainerURLWithSAS := scenarioHelper{}.getRawContainerURLWithSAS(c, containerName)
	raw := getDefaultCopyRawInput(srcDirPath, rawContainerURLWithSAS.String())
	raw.recursive = true

	runCopyAndVerify(c, raw, func(err error) {
		c.Assert(err, chk.IsNil)

		// validate that the right number of transfers were scheduled
		c.Assert(len(mockedRPC.transfers), chk.Equals, len(fileList))

		// validate that the right transfers were sent
		validateUploadTransfersAreScheduled(c, common.AZCOPY_PATH_SEPARATOR_STRING,
			common.AZCOPY_PATH_SEPARATOR_STRING+filepath.Base(srcDirPath)+common.AZCOPY_PATH_SEPARATOR_STRING, fileList, mockedRPC)
	})

	// turn off recursive, this time nothing should be transferred
	raw.recursive = false
	mockedRPC.reset()

	runCopyAndVerify(c, raw, func(err error) {
		c.Assert(err, chk.NotNil)
		c.Assert(len(mockedRPC.transfers), chk.Equals, 0)
	})
}

// regular directory->virtual dir upload
func (s *cmdIntegrationSuite) TestUploadDirectoryToVirtualDirectory(c *chk.C) {
	bsu := getBSU()
	vdirName := "vdir"

	// set up the source with numerous files
	srcDirPath := scenarioHelper{}.generateLocalDirectory(c)
	fileList := scenarioHelper{}.generateCommonRemoteScenarioForLocal(c, srcDirPath, "")

	// set up an empty container
	containerURL, containerName := createNewContainer(c, bsu)
	defer deleteContainer(c, containerURL)

	// set up interceptor
	mockedRPC := interceptor{}
	Rpc = mockedRPC.intercept
	mockedRPC.init()

	// construct the raw input to simulate user input
	rawContainerURLWithSAS := scenarioHelper{}.getRawBlobURLWithSAS(c, containerName, vdirName)
	raw := getDefaultCopyRawInput(srcDirPath, rawContainerURLWithSAS.String())
	raw.recursive = true

	runCopyAndVerify(c, raw, func(err error) {
		c.Assert(err, chk.IsNil)

		// validate that the right number of transfers were scheduled
		c.Assert(len(mockedRPC.transfers), chk.Equals, len(fileList))

		// validate that the right transfers were sent
		expectedTransfers := scenarioHelper{}.shaveOffPrefix(fileList, filepath.Base(srcDirPath)+common.AZCOPY_PATH_SEPARATOR_STRING)
		validateUploadTransfersAreScheduled(c, common.AZCOPY_PATH_SEPARATOR_STRING,
			common.AZCOPY_PATH_SEPARATOR_STRING+filepath.Base(srcDirPath)+common.AZCOPY_PATH_SEPARATOR_STRING, expectedTransfers, mockedRPC)
	})

	// turn off recursive, this time nothing should be transferred
	raw.recursive = false
	mockedRPC.reset()

	runCopyAndVerify(c, raw, func(err error) {
		c.Assert(err, chk.NotNil)
		c.Assert(len(mockedRPC.transfers), chk.Equals, 0)
	})
}

// files(from pattern)->container upload
func (s *cmdIntegrationSuite) TestUploadDirectoryToContainerWithPattern(c *chk.C) {
	bsu := getBSU()

	// set up the source with numerous files
	srcDirPath := scenarioHelper{}.generateLocalDirectory(c)
	scenarioHelper{}.generateCommonRemoteScenarioForLocal(c, srcDirPath, "")

	// add special files that we wish to include
	filesToInclude := []string{"important.pdf", "includeSub/amazing.pdf", "includeSub/wow/amazing.pdf"}
	scenarioHelper{}.generateLocalFilesFromList(c, srcDirPath, filesToInclude)

	// set up an empty container
	containerURL, containerName := createNewContainer(c, bsu)
	defer deleteContainer(c, containerURL)

	// set up interceptor
	mockedRPC := interceptor{}
	Rpc = mockedRPC.intercept
	mockedRPC.init()

	// construct the raw input to simulate user input
	rawContainerURLWithSAS := scenarioHelper{}.getRawContainerURLWithSAS(c, containerName)
	raw := getDefaultCopyRawInput(filepath.Join(srcDirPath, "/*.pdf"), rawContainerURLWithSAS.String())
	raw.recursive = true

	runCopyAndVerify(c, raw, func(err error) {
		c.Assert(err, chk.IsNil)

		// validate that the right number of transfers were scheduled
		c.Assert(len(mockedRPC.transfers), chk.Equals, 1)

		// only the top pdf should be included
		c.Assert(len(mockedRPC.transfers), chk.Equals, 1)
		c.Assert(mockedRPC.transfers[0].Source, chk.Equals, mockedRPC.transfers[0].Destination)
		c.Assert(strings.HasSuffix(mockedRPC.transfers[0].Source, ".pdf"), chk.Equals, true)
		c.Assert(strings.Contains(mockedRPC.transfers[0].Source[1:], common.AZCOPY_PATH_SEPARATOR_STRING), chk.Equals, false)
	})
}
