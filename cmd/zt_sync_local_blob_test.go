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
	"strings"

	"github.com/Azure/azure-storage-azcopy/common"
	"github.com/Azure/azure-storage-blob-go/azblob"
	chk "gopkg.in/check.v1"
)

// regular file->blob sync
func (s *cmdIntegrationSuite) TestSyncUploadWithSingleFile(c *chk.C) {
	bsu := getBSU()
	containerURL, containerName := createNewContainer(c, bsu)
	defer deleteContainer(c, containerURL)

	for _, srcFileName := range []string{"singlefileisbest", "打麻将.txt", "%4509%4254$85140&"} {
		// set up the source as a single file
		srcDirName := scenarioHelper{}.generateLocalDirectory(c)
		defer os.RemoveAll(srcDirName)
		fileList := []string{srcFileName}
		scenarioHelper{}.generateLocalFilesFromList(c, srcDirName, fileList)

		// set up the destination container with a single blob
		dstBlobName := srcFileName
		scenarioHelper{}.generateBlobsFromList(c, containerURL, []string{dstBlobName}, blockBlobDefaultData)
		c.Assert(containerURL, chk.NotNil)

		// set up interceptor
		mockedRPC := interceptor{}
		Rpc = mockedRPC.intercept
		mockedRPC.init()

		// construct the raw input to simulate user input
		rawBlobURLWithSAS := scenarioHelper{}.getRawBlobURLWithSAS(c, containerName, dstBlobName)
		raw := getDefaultSyncRawInput(filepath.Join(srcDirName, srcFileName), rawBlobURLWithSAS.String())

		// the blob was created after the file, so no sync should happen
		runSyncAndVerify(c, raw, func(err error) {
			c.Assert(err, chk.IsNil)

			// validate that the right number of transfers were scheduled
			c.Assert(len(mockedRPC.transfers), chk.Equals, 0)
		})

		// recreate the file to have a later last modified time
		scenarioHelper{}.generateLocalFilesFromList(c, srcDirName, []string{srcFileName})
		mockedRPC.reset()

		// the file was created after the blob, so the sync should happen
		runSyncAndVerify(c, raw, func(err error) {
			c.Assert(err, chk.IsNil)

			// if source and destination already point to files, the relative path is an empty string ""
			validateUploadTransfersAreScheduled(c, "", "", []string{""}, mockedRPC)
		})
	}
}

// regular directory->container sync but destination is empty, so everything has to be transferred
func (s *cmdIntegrationSuite) TestSyncUploadWithEmptyDestination(c *chk.C) {
	bsu := getBSU()

	// set up the source with numerous files
	srcDirName := scenarioHelper{}.generateLocalDirectory(c)
	defer os.RemoveAll(srcDirName)
	fileList := scenarioHelper{}.generateCommonRemoteScenarioForLocal(c, srcDirName, "")

	// set up an empty container
	containerURL, containerName := createNewContainer(c, bsu)
	defer deleteContainer(c, containerURL)

	// set up interceptor
	mockedRPC := interceptor{}
	Rpc = mockedRPC.intercept
	mockedRPC.init()

	// construct the raw input to simulate user input
	rawContainerURLWithSAS := scenarioHelper{}.getRawContainerURLWithSAS(c, containerName)
	raw := getDefaultSyncRawInput(srcDirName, rawContainerURLWithSAS.String())

	runSyncAndVerify(c, raw, func(err error) {
		c.Assert(err, chk.IsNil)

		// validate that the right number of transfers were scheduled
		c.Assert(len(mockedRPC.transfers), chk.Equals, len(fileList))

		// validate that the right transfers were sent
		validateUploadTransfersAreScheduled(c, "", "", fileList, mockedRPC)
	})

	// turn off recursive, this time only top blobs should be transferred
	raw.recursive = false
	mockedRPC.reset()

	runSyncAndVerify(c, raw, func(err error) {
		c.Assert(err, chk.IsNil)
		c.Assert(len(mockedRPC.transfers), chk.Not(chk.Equals), len(fileList))

		for _, transfer := range mockedRPC.transfers {
			c.Assert(strings.Contains(transfer.Source, common.AZCOPY_PATH_SEPARATOR_STRING), chk.Equals, false)
		}
	})
}

// regular directory->container sync but destination is identical to the source, transfers are scheduled based on lmt
func (s *cmdIntegrationSuite) TestSyncUploadWithIdenticalDestination(c *chk.C) {
	bsu := getBSU()

	// set up the source with numerous files
	srcDirName := scenarioHelper{}.generateLocalDirectory(c)
	defer os.RemoveAll(srcDirName)
	fileList := scenarioHelper{}.generateCommonRemoteScenarioForLocal(c, srcDirName, "")

	// set up an the container with the exact same files, but later lmts
	containerURL, containerName := createNewContainer(c, bsu)
	defer deleteContainer(c, containerURL)

	// wait for 1 second so that the last modified times of the blobs are guaranteed to be newer
	scenarioHelper{}.generateBlobsFromList(c, containerURL, fileList, blockBlobDefaultData)

	// set up interceptor
	mockedRPC := interceptor{}
	Rpc = mockedRPC.intercept
	mockedRPC.init()

	// construct the raw input to simulate user input
	rawContainerURLWithSAS := scenarioHelper{}.getRawContainerURLWithSAS(c, containerName)
	raw := getDefaultSyncRawInput(srcDirName, rawContainerURLWithSAS.String())

	runSyncAndVerify(c, raw, func(err error) {
		c.Assert(err, chk.IsNil)

		// validate that the right number of transfers were scheduled
		c.Assert(len(mockedRPC.transfers), chk.Equals, 0)
	})

	// refresh the files' last modified time so that they are newer
	scenarioHelper{}.generateLocalFilesFromList(c, srcDirName, fileList)
	mockedRPC.reset()

	runSyncAndVerify(c, raw, func(err error) {
		c.Assert(err, chk.IsNil)
		validateUploadTransfersAreScheduled(c, "", "", fileList, mockedRPC)
	})
}

// regular container->directory sync where destination is missing some files from source, and also has some extra files
func (s *cmdIntegrationSuite) TestSyncUploadWithMismatchedDestination(c *chk.C) {
	bsu := getBSU()

	// set up the source with numerous files
	srcDirName := scenarioHelper{}.generateLocalDirectory(c)
	defer os.RemoveAll(srcDirName)
	fileList := scenarioHelper{}.generateCommonRemoteScenarioForLocal(c, srcDirName, "")

	// set up an the container with half of the files, but later lmts
	// also add some extra blobs that are not present at the source
	extraBlobs := []string{"extraFile1.pdf, extraFile2.txt"}
	containerURL, containerName := createNewContainer(c, bsu)
	defer deleteContainer(c, containerURL)
	scenarioHelper{}.generateBlobsFromList(c, containerURL, fileList[0:len(fileList)/2], blockBlobDefaultData)
	scenarioHelper{}.generateBlobsFromList(c, containerURL, extraBlobs, blockBlobDefaultData)
	expectedOutput := fileList[len(fileList)/2:]

	// set up interceptor
	mockedRPC := interceptor{}
	Rpc = mockedRPC.intercept
	mockedRPC.init()

	// construct the raw input to simulate user input
	rawContainerURLWithSAS := scenarioHelper{}.getRawContainerURLWithSAS(c, containerName)
	raw := getDefaultSyncRawInput(srcDirName, rawContainerURLWithSAS.String())

	runSyncAndVerify(c, raw, func(err error) {
		c.Assert(err, chk.IsNil)
		validateUploadTransfersAreScheduled(c, "", "", expectedOutput, mockedRPC)

		// make sure the extra blobs were deleted
		for _, blobName := range extraBlobs {
			exists := scenarioHelper{}.blobExists(containerURL.NewBlobURL(blobName))
			c.Assert(exists, chk.Equals, false)
		}
	})
}

// include flag limits the scope of source/destination comparison
func (s *cmdIntegrationSuite) TestSyncUploadWithIncludeFlag(c *chk.C) {
	bsu := getBSU()

	// set up the source with numerous files
	srcDirName := scenarioHelper{}.generateLocalDirectory(c)
	defer os.RemoveAll(srcDirName)
	scenarioHelper{}.generateCommonRemoteScenarioForLocal(c, srcDirName, "")

	// add special files that we wish to include
	filesToInclude := []string{"important.pdf", "includeSub/amazing.jpeg", "exactName"}
	scenarioHelper{}.generateLocalFilesFromList(c, srcDirName, filesToInclude)
	includeString := "*.pdf;*.jpeg;exactName"

	// set up the destination as an empty container
	containerURL, containerName := createNewContainer(c, bsu)
	defer deleteContainer(c, containerURL)

	// set up interceptor
	mockedRPC := interceptor{}
	Rpc = mockedRPC.intercept
	mockedRPC.init()

	// construct the raw input to simulate user input
	rawContainerURLWithSAS := scenarioHelper{}.getRawContainerURLWithSAS(c, containerName)
	raw := getDefaultSyncRawInput(srcDirName, rawContainerURLWithSAS.String())
	raw.include = includeString

	runSyncAndVerify(c, raw, func(err error) {
		c.Assert(err, chk.IsNil)
		validateUploadTransfersAreScheduled(c, "", "", filesToInclude, mockedRPC)
	})
}

// exclude flag limits the scope of source/destination comparison
func (s *cmdIntegrationSuite) TestSyncUploadWithExcludeFlag(c *chk.C) {
	bsu := getBSU()

	// set up the source with numerous files
	srcDirName := scenarioHelper{}.generateLocalDirectory(c)
	defer os.RemoveAll(srcDirName)
	fileList := scenarioHelper{}.generateCommonRemoteScenarioForLocal(c, srcDirName, "")

	// add special files that we wish to exclude
	filesToExclude := []string{"notGood.pdf", "excludeSub/lame.jpeg", "exactName"}
	scenarioHelper{}.generateLocalFilesFromList(c, srcDirName, filesToExclude)
	excludeString := "*.pdf;*.jpeg;exactName"

	// set up the destination as an empty container
	containerURL, containerName := createNewContainer(c, bsu)
	defer deleteContainer(c, containerURL)

	// set up interceptor
	mockedRPC := interceptor{}
	Rpc = mockedRPC.intercept
	mockedRPC.init()

	// construct the raw input to simulate user input
	rawContainerURLWithSAS := scenarioHelper{}.getRawContainerURLWithSAS(c, containerName)
	raw := getDefaultSyncRawInput(srcDirName, rawContainerURLWithSAS.String())
	raw.exclude = excludeString

	runSyncAndVerify(c, raw, func(err error) {
		c.Assert(err, chk.IsNil)
		validateUploadTransfersAreScheduled(c, "", "", fileList, mockedRPC)
	})
}

// include and exclude flag can work together to limit the scope of source/destination comparison
func (s *cmdIntegrationSuite) TestSyncUploadWithIncludeAndExcludeFlag(c *chk.C) {
	bsu := getBSU()

	// set up the source with numerous files
	srcDirName := scenarioHelper{}.generateLocalDirectory(c)
	defer os.RemoveAll(srcDirName)
	scenarioHelper{}.generateCommonRemoteScenarioForLocal(c, srcDirName, "")

	// add special files that we wish to include
	filesToInclude := []string{"important.pdf", "includeSub/amazing.jpeg"}
	scenarioHelper{}.generateLocalFilesFromList(c, srcDirName, filesToInclude)
	includeString := "*.pdf;*.jpeg;exactName"

	// add special files that we wish to exclude
	// note that the excluded files also match the include string
	filesToExclude := []string{"sorry.pdf", "exclude/notGood.jpeg", "exactName", "sub/exactName"}
	scenarioHelper{}.generateLocalFilesFromList(c, srcDirName, filesToExclude)
	excludeString := "so*;not*;exactName"

	// set up the destination as an empty container
	containerURL, containerName := createNewContainer(c, bsu)
	defer deleteContainer(c, containerURL)

	// set up interceptor
	mockedRPC := interceptor{}
	Rpc = mockedRPC.intercept
	mockedRPC.init()

	// construct the raw input to simulate user input
	rawContainerURLWithSAS := scenarioHelper{}.getRawContainerURLWithSAS(c, containerName)
	raw := getDefaultSyncRawInput(srcDirName, rawContainerURLWithSAS.String())
	raw.include = includeString
	raw.exclude = excludeString

	runSyncAndVerify(c, raw, func(err error) {
		c.Assert(err, chk.IsNil)
		validateUploadTransfersAreScheduled(c, "", "", filesToInclude, mockedRPC)
	})
}

// validate the bug fix for this scenario
func (s *cmdIntegrationSuite) TestSyncUploadWithMissingDestination(c *chk.C) {
	bsu := getBSU()

	// set up the source with numerous files
	srcDirName := scenarioHelper{}.generateLocalDirectory(c)
	defer os.RemoveAll(srcDirName)
	scenarioHelper{}.generateCommonRemoteScenarioForLocal(c, srcDirName, "")

	// set up the destination as an non-existent container
	containerURL, containerName := getContainerURL(c, bsu)

	// validate that the container does not exist
	_, err := containerURL.GetProperties(context.Background(), azblob.LeaseAccessConditions{})
	c.Assert(err, chk.NotNil)

	// set up interceptor
	mockedRPC := interceptor{}
	Rpc = mockedRPC.intercept
	mockedRPC.init()

	// construct the raw input to simulate user input
	rawContainerURLWithSAS := scenarioHelper{}.getRawContainerURLWithSAS(c, containerName)
	raw := getDefaultSyncRawInput(srcDirName, rawContainerURLWithSAS.String())

	runSyncAndVerify(c, raw, func(err error) {
		// error should not be nil, but the app should not crash either
		c.Assert(err, chk.NotNil)

		// validate that the right number of transfers were scheduled
		c.Assert(len(mockedRPC.transfers), chk.Equals, 0)
	})
}
