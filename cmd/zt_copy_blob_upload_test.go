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
	"os"
	"path/filepath"
	"strings"
	"time"
)

func (s *cmdIntegrationSuite) TestIncludeDirSimple(c *chk.C) {
	bsu := getBSU()
	containerURL, containerName := createNewContainer(c, bsu)
	defer deleteContainer(c, containerURL)

	files := []string{
		"filea",
		"fileb",
		"filec",
		"other/sub/subsub/filey", // should not be included because sub/subsub is not at root here
		"other2/sub",             // ditto
		"sub/filea",
		"sub/fileb",
		"sub/child/filec",
	}

	dirPath := scenarioHelper{}.generateLocalDirectory(c)
	defer os.RemoveAll(dirPath)
	scenarioHelper{}.generateLocalFilesFromList(c, dirPath, files)

	mockedRPC := interceptor{}
	Rpc = mockedRPC.intercept
	mockedRPC.init()

	rawBlobURLWithSAS := scenarioHelper{}.getRawContainerURLWithSAS(c, containerName)
	raw := getDefaultRawCopyInput(dirPath, rawBlobURLWithSAS.String())
	raw.recursive = true
	raw.includePath = "sub"

	runCopyAndVerify(c, raw, func(err error) {
		c.Assert(err, chk.IsNil)

		c.Assert(len(mockedRPC.transfers), chk.Equals, 3)
		// trim / and /folder/ off
		validateDownloadTransfersAreScheduled(c, "/", "/"+filepath.Base(dirPath)+"/", files[5:], mockedRPC)
	})
}

func (s *cmdIntegrationSuite) TestIncludeDir(c *chk.C) {
	bsu := getBSU()
	containerURL, containerName := createNewContainer(c, bsu)
	defer deleteContainer(c, containerURL)

	files := []string{
		"filea",
		"fileb",
		"filec",
		"sub/filea",
		"sub/fileb",
		"sub/filec",
		"sub/somethingelse/subsub/filex", // should not be included because sub/subsub is not contiguous here
		"othersub/sub/subsub/filey",      // should not be included because sub/subsub is not at root here
		"sub/subsub/filea",
		"sub/subsub/fileb",
		"sub/subsub/filec",
	}

	dirPath := scenarioHelper{}.generateLocalDirectory(c)
	defer os.RemoveAll(dirPath)
	scenarioHelper{}.generateLocalFilesFromList(c, dirPath, files)

	mockedRPC := interceptor{}
	Rpc = mockedRPC.intercept
	mockedRPC.init()

	rawBlobURLWithSAS := scenarioHelper{}.getRawContainerURLWithSAS(c, containerName)
	raw := getDefaultRawCopyInput(dirPath, rawBlobURLWithSAS.String())
	raw.recursive = true
	raw.includePath = "sub/subsub"

	runCopyAndVerify(c, raw, func(err error) {
		c.Assert(err, chk.IsNil)

		c.Assert(len(mockedRPC.transfers), chk.Equals, 3)
		// trim / and /folder/ off
		validateDownloadTransfersAreScheduled(c, "/", "/"+filepath.Base(dirPath)+"/", files[8:], mockedRPC)
	})
}

func (s *cmdIntegrationSuite) TestExcludeDir(c *chk.C) {
	bsu := getBSU()
	containerURL, containerName := createNewContainer(c, bsu)
	defer deleteContainer(c, containerURL)

	files := []string{
		"filea",
		"fileb",
		"filec",
		"sub/filea",
		"sub/fileb",
		"sub/filec",
		"sub/somethingelse/subsub/filex", // should not be excluded, since sub/subsub is not contiguous here
		"othersub/sub/subsub/filey",      // should not be excluded, since sub/subsub is not a root level here
		"sub/subsub/filea",
		"sub/subsub/fileb",
		"sub/subsub/filec",
	}

	dirPath := scenarioHelper{}.generateLocalDirectory(c)
	defer os.RemoveAll(dirPath)
	scenarioHelper{}.generateLocalFilesFromList(c, dirPath, files)

	mockedRPC := interceptor{}
	Rpc = mockedRPC.intercept
	mockedRPC.init()

	rawBlobURLWithSAS := scenarioHelper{}.getRawContainerURLWithSAS(c, containerName)
	raw := getDefaultRawCopyInput(dirPath, rawBlobURLWithSAS.String())
	raw.recursive = true
	raw.excludePath = "sub/subsub"

	runCopyAndVerify(c, raw, func(err error) {
		c.Assert(err, chk.IsNil)

		c.Assert(len(mockedRPC.transfers), chk.Equals, 8)
		// Trim / and /folder/ off
		validateDownloadTransfersAreScheduled(c, "/", "/"+filepath.Base(dirPath)+"/", files[:8], mockedRPC)
	})
}

func (s *cmdIntegrationSuite) TestIncludeAndExcludeDir(c *chk.C) {
	bsu := getBSU()
	containerURL, containerName := createNewContainer(c, bsu)
	defer deleteContainer(c, containerURL)

	files := []string{
		"xyz/aaa",
		"xyz/def", // should be included, because although we are excluding "def", here it is not at the root
		"def",     // should be excluded because here it is at root
		"filea",
		"fileb",
		"filec",
	}

	dirPath := scenarioHelper{}.generateLocalDirectory(c)
	defer os.RemoveAll(dirPath)
	scenarioHelper{}.generateLocalFilesFromList(c, dirPath, files)

	mockedRPC := interceptor{}
	Rpc = mockedRPC.intercept
	mockedRPC.init()

	rawBlobURLWithSAS := scenarioHelper{}.getRawContainerURLWithSAS(c, containerName)
	raw := getDefaultRawCopyInput(dirPath, rawBlobURLWithSAS.String())
	raw.recursive = true
	raw.includePath = "xyz"
	raw.excludePath = "def"

	runCopyAndVerify(c, raw, func(err error) {
		c.Assert(err, chk.IsNil)

		c.Assert(len(mockedRPC.transfers), chk.Equals, 2)
		// Trim / and /folder/ off
		validateDownloadTransfersAreScheduled(c, "/", "/"+filepath.Base(dirPath)+"/", files[:2], mockedRPC)
	})
}

// regular local file->blob upload
func (s *cmdIntegrationSuite) TestUploadSingleFileToBlobVirtualDirectory(c *chk.C) {
	bsu := getBSU()
	containerURL, containerName := createNewContainer(c, bsu)
	defer deleteContainer(c, containerURL)

	for _, srcFileName := range []string{"singleblobisbest", "打麻将.txt", "%4509%4254$85140&"} {
		// set up the source as a single file
		srcDirName := scenarioHelper{}.generateLocalDirectory(c)
		defer os.RemoveAll(srcDirName)
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
			c.Assert(d, chk.Equals, common.AZCOPY_PATH_SEPARATOR_STRING+srcFileName)
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
		defer os.RemoveAll(srcDirName)
		fileList := []string{srcFileName}
		scenarioHelper{}.generateLocalFilesFromList(c, srcDirName, fileList)

		// set up the destination container with a single blob
		dstBlobName := "whatever"
		scenarioHelper{}.generateBlobsFromList(c, containerURL, []string{dstBlobName}, blockBlobDefaultData)
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
	defer os.RemoveAll(srcDirPath)
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
	defer os.RemoveAll(srcDirPath)
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
	defer os.RemoveAll(srcDirPath)
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

func (s *cmdIntegrationSuite) TestUploadDirectoryToContainerWithIncludeAfter_UTC(c *chk.C) {
	s.doTestUploadDirectoryToContainerWithIncludeAfter(true, c)
}

func (s *cmdIntegrationSuite) TestUploadDirectoryToContainerWithIncludeAfter_LocalTime(c *chk.C) {
	s.doTestUploadDirectoryToContainerWithIncludeAfter(false, c)
}

func (s *cmdIntegrationSuite) doTestUploadDirectoryToContainerWithIncludeAfter(useUtc bool, c *chk.C) {
	bsu := getBSU()

	// set up the source with numerous files
	srcDirPath := scenarioHelper{}.generateLocalDirectory(c)
	defer os.RemoveAll(srcDirPath)
	scenarioHelper{}.generateCommonRemoteScenarioForLocal(c, srcDirPath, "")

	// sleep a little longer, to give clear LMT separation between the files above and those below
	time.Sleep(1500 * time.Millisecond)
	includeFrom := time.Now()

	// add newer files, which we wish to include
	filesToInclude := []string{"important.txt", "includeSub/amazing.txt", "includeSub/wow/amazing.txt"}
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
	raw := getDefaultCopyRawInput(srcDirPath, rawContainerURLWithSAS.String())
	raw.recursive = true
	if useUtc {
		raw.includeAfter = includeFrom.UTC().Format(time.RFC3339)
	} else {
		raw.includeAfter = includeFrom.Format("2006-01-02T15:04:05") // local time, no timezone
	}

	runCopyAndVerify(c, raw, func(err error) {
		c.Assert(err, chk.IsNil)

		// validate that the right number of transfers were scheduled
		c.Assert(len(mockedRPC.transfers), chk.Equals, 3)

		// validate that the right transfers were sent
		expectedTransfers := scenarioHelper{}.shaveOffPrefix(filesToInclude, filepath.Base(srcDirPath)+common.AZCOPY_PATH_SEPARATOR_STRING)
		validateUploadTransfersAreScheduled(c, common.AZCOPY_PATH_SEPARATOR_STRING,
			common.AZCOPY_PATH_SEPARATOR_STRING+filepath.Base(srcDirPath)+common.AZCOPY_PATH_SEPARATOR_STRING, expectedTransfers, mockedRPC)
	})
}
