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
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/Azure/azure-storage-azcopy/v10/common"
	"github.com/Azure/azure-storage-azcopy/v10/jobsAdmin"
	"github.com/stretchr/testify/assert"
)

func TestIncludeDirSimple(t *testing.T) {
	a := assert.New(t)
	bsc := getBlobServiceClient()
	cc, containerName := createNewContainer(a, bsc)
	defer deleteContainer(a, cc)

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

	dirPath := scenarioHelper{}.generateLocalDirectory(a)
	defer os.RemoveAll(dirPath)
	scenarioHelper{}.generateLocalFilesFromList(a, dirPath, files)

	mockedRPC := interceptor{}
	jobsAdmin.ExecuteNewCopyJobPartOrder = func(order common.CopyJobPartOrderRequest) common.CopyJobPartOrderResponse {
		return mockedRPC.intercept(order)
	}
	mockedRPC.init()

	rawBlobURLWithSAS := scenarioHelper{}.getRawContainerURLWithSAS(a, containerName)
	raw := getDefaultRawCopyInput(dirPath, rawBlobURLWithSAS.String())
	raw.recursive = true
	raw.includePath = "sub"

	runCopyAndVerify(a, raw, func(err error) {
		a.Nil(err)

		a.Equal(3, len(mockedRPC.transfers))
		// trim / and /folder/ off
		validateDownloadTransfersAreScheduled(a, "/", "/"+filepath.Base(dirPath)+"/", files[5:], mockedRPC)
	})
}

func TestIncludeDir(t *testing.T) {
	a := assert.New(t)
	bsc := getBlobServiceClient()
	cc, containerName := createNewContainer(a, bsc)
	defer deleteContainer(a, cc)

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

	dirPath := scenarioHelper{}.generateLocalDirectory(a)
	defer os.RemoveAll(dirPath)
	scenarioHelper{}.generateLocalFilesFromList(a, dirPath, files)

	mockedRPC := interceptor{}
	jobsAdmin.ExecuteNewCopyJobPartOrder = func(order common.CopyJobPartOrderRequest) common.CopyJobPartOrderResponse {
		return mockedRPC.intercept(order)
	}
	mockedRPC.init()

	rawBlobURLWithSAS := scenarioHelper{}.getRawContainerURLWithSAS(a, containerName)
	raw := getDefaultRawCopyInput(dirPath, rawBlobURLWithSAS.String())
	raw.recursive = true
	raw.includePath = "sub/subsub"

	runCopyAndVerify(a, raw, func(err error) {
		a.Nil(err)

		a.Equal(3, len(mockedRPC.transfers))
		// trim / and /folder/ off
		validateDownloadTransfersAreScheduled(a, "/", "/"+filepath.Base(dirPath)+"/", files[8:], mockedRPC)
	})
}

func TestExcludeDir(t *testing.T) {
	a := assert.New(t)
	bsc := getBlobServiceClient()
	cc, containerName := createNewContainer(a, bsc)
	defer deleteContainer(a, cc)

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

	dirPath := scenarioHelper{}.generateLocalDirectory(a)
	defer os.RemoveAll(dirPath)
	scenarioHelper{}.generateLocalFilesFromList(a, dirPath, files)

	mockedRPC := interceptor{}
	jobsAdmin.ExecuteNewCopyJobPartOrder = func(order common.CopyJobPartOrderRequest) common.CopyJobPartOrderResponse {
		return mockedRPC.intercept(order)
	}
	mockedRPC.init()

	rawBlobURLWithSAS := scenarioHelper{}.getRawContainerURLWithSAS(a, containerName)
	raw := getDefaultRawCopyInput(dirPath, rawBlobURLWithSAS.String())
	raw.recursive = true
	raw.excludePath = "sub/subsub"

	runCopyAndVerify(a, raw, func(err error) {
		a.Nil(err)

		a.Equal(8, len(mockedRPC.transfers))
		// Trim / and /folder/ off
		validateDownloadTransfersAreScheduled(a, "/", "/"+filepath.Base(dirPath)+"/", files[:8], mockedRPC)
	})
}

func TestIncludeAndExcludeDir(t *testing.T) {
	a := assert.New(t)
	bsc := getBlobServiceClient()
	cc, containerName := createNewContainer(a, bsc)
	defer deleteContainer(a, cc)

	files := []string{
		"xyz/aaa",
		"xyz/def", // should be included, because although we are excluding "def", here it is not at the root
		"def",     // should be excluded because here it is at root
		"filea",
		"fileb",
		"filec",
	}

	dirPath := scenarioHelper{}.generateLocalDirectory(a)
	defer os.RemoveAll(dirPath)
	scenarioHelper{}.generateLocalFilesFromList(a, dirPath, files)

	mockedRPC := interceptor{}
	jobsAdmin.ExecuteNewCopyJobPartOrder = func(order common.CopyJobPartOrderRequest) common.CopyJobPartOrderResponse {
		return mockedRPC.intercept(order)
	}
	mockedRPC.init()

	rawBlobURLWithSAS := scenarioHelper{}.getRawContainerURLWithSAS(a, containerName)
	raw := getDefaultRawCopyInput(dirPath, rawBlobURLWithSAS.String())
	raw.recursive = true
	raw.includePath = "xyz"
	raw.excludePath = "def"

	runCopyAndVerify(a, raw, func(err error) {
		a.Nil(err)

		a.Equal(2, len(mockedRPC.transfers))
		// Trim / and /folder/ off
		validateDownloadTransfersAreScheduled(a, "/", "/"+filepath.Base(dirPath)+"/", files[:2], mockedRPC)
	})
}

// regular local file->blob upload
func TestUploadSingleFileToBlobVirtualDirectory(t *testing.T) {
	a := assert.New(t)
	bsc := getBlobServiceClient()
	cc, containerName := createNewContainer(a, bsc)
	defer deleteContainer(a, cc)

	for _, srcFileName := range []string{"singleblobisbest", "打麻将.txt", "%4509%4254$85140&"} {
		// set up the source as a single file
		srcDirName := scenarioHelper{}.generateLocalDirectory(a)
		defer os.RemoveAll(srcDirName)
		fileList := []string{srcFileName}
		scenarioHelper{}.generateLocalFilesFromList(a, srcDirName, fileList)

		// set up the destination container with a single blob
		dstBlobName := "testfolder/"

		// set up interceptor
		mockedRPC := interceptor{}
		jobsAdmin.ExecuteNewCopyJobPartOrder = func(order common.CopyJobPartOrderRequest) common.CopyJobPartOrderResponse {
			return mockedRPC.intercept(order)
		}
		mockedRPC.init()

		// construct the raw input to simulate user input
		rawBlobURLWithSAS := scenarioHelper{}.getRawBlobURLWithSAS(a, containerName, dstBlobName)
		raw := getDefaultCopyRawInput(filepath.Join(srcDirName, srcFileName), rawBlobURLWithSAS.String())

		// the blob was created after the file, so no sync should happen
		runCopyAndVerify(a, raw, func(err error) {
			a.Nil(err)

			// Validate that the destination is the file name (within the folder).
			// The destination being the folder *was* the issue in the past.
			// The service would just name the file as the folder if we didn't explicitly specify it.
			a.Equal(1, len(mockedRPC.transfers))
			d, err := url.PathUnescape(mockedRPC.transfers[0].Destination) //Unescape the destination, as we have special characters.
			a.Nil(err)
			a.Equal(common.AZCOPY_PATH_SEPARATOR_STRING+srcFileName, d)
		})

		// clean the RPC for the next test
		mockedRPC.reset()

		// now target the destination container, the result should be the same
		rawContainerURLWithSAS := scenarioHelper{}.getRawContainerURLWithSAS(a, containerName)
		raw = getDefaultCopyRawInput(filepath.Join(srcDirName, srcFileName), rawContainerURLWithSAS.String())

		// the file was created after the blob, so no sync should happen
		runCopyAndVerify(a, raw, func(err error) {
			a.Nil(err)

			// verify explicitly since the source and destination names will be different:
			// the source is "" since the given URL points to the blob itself
			// the destination should be the blob name, since the given local path points to the parent dir
			a.Equal(1, len(mockedRPC.transfers))

			a.Equal("", mockedRPC.transfers[0].Source)
			a.Equal(common.AZCOPY_PATH_SEPARATOR_STRING+url.PathEscape(srcFileName), mockedRPC.transfers[0].Destination)
		})
	}
}

// regular local file->blob upload
func TestUploadSingleFileToBlob(t *testing.T) {
	a := assert.New(t)
	bsc := getBlobServiceClient()
	cc, containerName := createNewContainer(a, bsc)
	defer deleteContainer(a, cc)

	for _, srcFileName := range []string{"singleblobisbest", "打麻将.txt", "%4509%4254$85140&"} {
		// set up the source as a single file
		srcDirName := scenarioHelper{}.generateLocalDirectory(a)
		defer os.RemoveAll(srcDirName)
		fileList := []string{srcFileName}
		scenarioHelper{}.generateLocalFilesFromList(a, srcDirName, fileList)

		// set up the destination container with a single blob
		dstBlobName := "whatever"
		scenarioHelper{}.generateBlobsFromList(a, cc, []string{dstBlobName}, blockBlobDefaultData)
		a.NotNil(cc)

		// set up interceptor
		mockedRPC := interceptor{}
		jobsAdmin.ExecuteNewCopyJobPartOrder = func(order common.CopyJobPartOrderRequest) common.CopyJobPartOrderResponse {
			return mockedRPC.intercept(order)
		}
		mockedRPC.init()

		// construct the raw input to simulate user input
		rawBlobURLWithSAS := scenarioHelper{}.getRawBlobURLWithSAS(a, containerName, dstBlobName)
		raw := getDefaultCopyRawInput(filepath.Join(srcDirName, srcFileName), rawBlobURLWithSAS.String())

		// the blob was created after the file, so no sync should happen
		runCopyAndVerify(a, raw, func(err error) {
			a.Nil(err)

			// validate that the right number of transfers were scheduled
			validateUploadTransfersAreScheduled(a, "", "", []string{""}, mockedRPC)
		})

		// clean the RPC for the next test
		mockedRPC.reset()

		// now target the destination container, the result should be the same
		rawContainerURLWithSAS := scenarioHelper{}.getRawContainerURLWithSAS(a, containerName)
		raw = getDefaultCopyRawInput(filepath.Join(srcDirName, srcFileName), rawContainerURLWithSAS.String())

		// the file was created after the blob, so no sync should happen
		runCopyAndVerify(a, raw, func(err error) {
			a.Nil(err)

			// verify explicitly since the source and destination names will be different:
			// the source is "" since the given URL points to the blob itself
			// the destination should be the blob name, since the given local path points to the parent dir
			a.Equal(1, len(mockedRPC.transfers))

			a.Equal("", mockedRPC.transfers[0].Source)
			a.Equal(common.AZCOPY_PATH_SEPARATOR_STRING+url.PathEscape(srcFileName), mockedRPC.transfers[0].Destination)
		})
	}
}

// regular directory->container upload
func TestUploadDirectoryToContainer(t *testing.T) {
	a := assert.New(t)
	bsc := getBlobServiceClient()

	// set up the source with numerous files
	srcDirPath := scenarioHelper{}.generateLocalDirectory(a)
	defer os.RemoveAll(srcDirPath)
	fileList := scenarioHelper{}.generateCommonRemoteScenarioForLocal(a, srcDirPath, "")

	// set up an empty container

	cc, containerName := createNewContainer(a, bsc)
	defer deleteContainer(a, cc)

	// set up interceptor
	mockedRPC := interceptor{}
	jobsAdmin.ExecuteNewCopyJobPartOrder = func(order common.CopyJobPartOrderRequest) common.CopyJobPartOrderResponse {
		return mockedRPC.intercept(order)
	}
	mockedRPC.init()

	// construct the raw input to simulate user input
	rawContainerURLWithSAS := scenarioHelper{}.getRawContainerURLWithSAS(a, containerName)
	raw := getDefaultCopyRawInput(srcDirPath, rawContainerURLWithSAS.String())
	raw.recursive = true

	runCopyAndVerify(a, raw, func(err error) {
		a.Nil(err)

		// validate that the right number of transfers were scheduled
		a.Equal(len(fileList), len(mockedRPC.transfers))

		// validate that the right transfers were sent
		validateUploadTransfersAreScheduled(a, common.AZCOPY_PATH_SEPARATOR_STRING,
			common.AZCOPY_PATH_SEPARATOR_STRING+filepath.Base(srcDirPath)+common.AZCOPY_PATH_SEPARATOR_STRING, fileList, mockedRPC)
	})

	// turn off recursive, this time nothing should be transferred
	raw.recursive = false
	mockedRPC.reset()

	runCopyAndVerify(a, raw, func(err error) {
		a.NotNil(err)
		a.Zero(len(mockedRPC.transfers))
	})
}

// regular directory->virtual dir upload
func TestUploadDirectoryToVirtualDirectory(t *testing.T) {
	a := assert.New(t)
	bsc := getBlobServiceClient()
	vdirName := "vdir"

	// set up the source with numerous files
	srcDirPath := scenarioHelper{}.generateLocalDirectory(a)
	defer os.RemoveAll(srcDirPath)
	fileList := scenarioHelper{}.generateCommonRemoteScenarioForLocal(a, srcDirPath, "")

	// set up an empty container
	cc, containerName := createNewContainer(a, bsc)
	defer deleteContainer(a, cc)

	// set up interceptor
	mockedRPC := interceptor{}
	jobsAdmin.ExecuteNewCopyJobPartOrder = func(order common.CopyJobPartOrderRequest) common.CopyJobPartOrderResponse {
		return mockedRPC.intercept(order)
	}
	mockedRPC.init()

	// construct the raw input to simulate user input
	rawContainerURLWithSAS := scenarioHelper{}.getRawBlobURLWithSAS(a, containerName, vdirName)
	raw := getDefaultCopyRawInput(srcDirPath, rawContainerURLWithSAS.String())
	raw.recursive = true

	runCopyAndVerify(a, raw, func(err error) {
		a.Nil(err)

		// validate that the right number of transfers were scheduled
		a.Equal(len(fileList), len(mockedRPC.transfers))

		// validate that the right transfers were sent
		expectedTransfers := scenarioHelper{}.shaveOffPrefix(fileList, filepath.Base(srcDirPath)+common.AZCOPY_PATH_SEPARATOR_STRING)
		validateUploadTransfersAreScheduled(a, common.AZCOPY_PATH_SEPARATOR_STRING,
			common.AZCOPY_PATH_SEPARATOR_STRING+filepath.Base(srcDirPath)+common.AZCOPY_PATH_SEPARATOR_STRING, expectedTransfers, mockedRPC)
	})

	// turn off recursive, this time nothing should be transferred
	raw.recursive = false
	mockedRPC.reset()

	runCopyAndVerify(a, raw, func(err error) {
		a.NotNil(err)
		a.Zero(len(mockedRPC.transfers))
	})
}

// files(from pattern)->container upload
func TestUploadDirectoryToContainerWithPattern(t *testing.T) {
	a := assert.New(t)
	bsc := getBlobServiceClient()

	// set up the source with numerous files
	srcDirPath := scenarioHelper{}.generateLocalDirectory(a)
	defer os.RemoveAll(srcDirPath)
	scenarioHelper{}.generateCommonRemoteScenarioForLocal(a, srcDirPath, "")

	// add special files that we wish to include
	filesToInclude := []string{"important.pdf", "includeSub/amazing.pdf", "includeSub/wow/amazing.pdf"}
	scenarioHelper{}.generateLocalFilesFromList(a, srcDirPath, filesToInclude)

	// set up an empty container
	cc, containerName := createNewContainer(a, bsc)
	defer deleteContainer(a, cc)

	// set up interceptor
	mockedRPC := interceptor{}
	jobsAdmin.ExecuteNewCopyJobPartOrder = func(order common.CopyJobPartOrderRequest) common.CopyJobPartOrderResponse {
		return mockedRPC.intercept(order)
	}
	mockedRPC.init()

	// construct the raw input to simulate user input
	rawContainerURLWithSAS := scenarioHelper{}.getRawContainerURLWithSAS(a, containerName)
	raw := getDefaultCopyRawInput(filepath.Join(srcDirPath, "/*.pdf"), rawContainerURLWithSAS.String())
	raw.recursive = true

	runCopyAndVerify(a, raw, func(err error) {
		a.Nil(err)

		// validate that the right number of transfers were scheduled
		a.Equal(1, len(mockedRPC.transfers))

		// only the top pdf should be included
		a.Equal(1, len(mockedRPC.transfers))
		a.Equal(mockedRPC.transfers[0].Destination, mockedRPC.transfers[0].Source)
		a.True(strings.HasSuffix(mockedRPC.transfers[0].Source, ".pdf"))
		a.False(strings.Contains(mockedRPC.transfers[0].Source[1:], common.AZCOPY_PATH_SEPARATOR_STRING))
	})
}

func TestUploadDirectoryToContainerWithIncludeBefore_UTC(t *testing.T) {
	a := assert.New(t)
	doTestUploadDirectoryToContainerWithIncludeBefore(true, a)
}

func TestUploadDirectoryToContainerWithIncludeBefore_LocalTime(t *testing.T) {
	a := assert.New(t)
	doTestUploadDirectoryToContainerWithIncludeBefore(false, a)
}

func doTestUploadDirectoryToContainerWithIncludeBefore(useUtc bool, a *assert.Assertions) {
	bsc := getBlobServiceClient()

	// set up the source directory
	srcDirPath := scenarioHelper{}.generateLocalDirectory(a)
	defer os.RemoveAll(srcDirPath)

	// add newer files, which we wish to include
	filesToInclude := []string{"important.txt", "includeSub/amazing.txt", "includeSub/wow/amazing.txt"}
	scenarioHelper{}.generateLocalFilesFromList(a, srcDirPath, filesToInclude)

	// sleep a little longer, to give clear LMT separation between the files above and those below (should not be copied)
	time.Sleep(1500 * time.Millisecond)
	includeFrom := time.Now()
	extraIgnoredFiles := []string{"ignored.txt", "includeSub/ignored.txt", "includeSub/wow/ignored.txt"}
	scenarioHelper{}.generateLocalFilesFromList(a, srcDirPath, extraIgnoredFiles)

	// set up an empty container
	cc, containerName := createNewContainer(a, bsc)
	defer deleteContainer(a, cc)

	// set up interceptor
	mockedRPC := interceptor{}
	jobsAdmin.ExecuteNewCopyJobPartOrder = func(order common.CopyJobPartOrderRequest) common.CopyJobPartOrderResponse {
		return mockedRPC.intercept(order)
	}
	mockedRPC.init()

	// construct the raw input to simulate user input
	rawContainerURLWithSAS := scenarioHelper{}.getRawContainerURLWithSAS(a, containerName)
	raw := getDefaultCopyRawInput(srcDirPath, rawContainerURLWithSAS.String())
	raw.recursive = true
	if useUtc {
		raw.includeBefore = includeFrom.UTC().Format(time.RFC3339)
	} else {
		raw.includeBefore = includeFrom.Format("2006-01-02T15:04:05") // local time, no timezone
	}

	runCopyAndVerify(a, raw, func(err error) {
		a.Nil(err)

		// validate that the right number of transfers were scheduled
		a.Equal(len(filesToInclude), len(mockedRPC.transfers))

		// validate that the right transfers were sent
		expectedTransfers := scenarioHelper{}.shaveOffPrefix(filesToInclude, filepath.Base(srcDirPath)+common.AZCOPY_PATH_SEPARATOR_STRING)
		validateUploadTransfersAreScheduled(a, common.AZCOPY_PATH_SEPARATOR_STRING,
			common.AZCOPY_PATH_SEPARATOR_STRING+filepath.Base(srcDirPath)+common.AZCOPY_PATH_SEPARATOR_STRING, expectedTransfers, mockedRPC)
	})
}

func TestUploadDirectoryToContainerWithIncludeAfter_UTC(t *testing.T) {
	a := assert.New(t)
	doTestUploadDirectoryToContainerWithIncludeAfter(true, a)
}

func TestUploadDirectoryToContainerWithIncludeAfter_LocalTime(t *testing.T) {
	a := assert.New(t)
	doTestUploadDirectoryToContainerWithIncludeAfter(false, a)
}

func doTestUploadDirectoryToContainerWithIncludeAfter(useUtc bool, a *assert.Assertions) {
	bsc := getBlobServiceClient()

	// set up the source with numerous files
	srcDirPath := scenarioHelper{}.generateLocalDirectory(a)
	defer os.RemoveAll(srcDirPath)
	scenarioHelper{}.generateCommonRemoteScenarioForLocal(a, srcDirPath, "")

	// sleep a little longer, to give clear LMT separation between the files above and those below
	time.Sleep(1500 * time.Millisecond)
	includeFrom := time.Now()

	// add newer files, which we wish to include
	filesToInclude := []string{"important.txt", "includeSub/amazing.txt", "includeSub/wow/amazing.txt"}
	scenarioHelper{}.generateLocalFilesFromList(a, srcDirPath, filesToInclude)

	// set up an empty container
	cc, containerName := createNewContainer(a, bsc)
	defer deleteContainer(a, cc)

	// set up interceptor
	mockedRPC := interceptor{}
	jobsAdmin.ExecuteNewCopyJobPartOrder = func(order common.CopyJobPartOrderRequest) common.CopyJobPartOrderResponse {
		return mockedRPC.intercept(order)
	}
	mockedRPC.init()

	// construct the raw input to simulate user input
	rawContainerURLWithSAS := scenarioHelper{}.getRawContainerURLWithSAS(a, containerName)
	raw := getDefaultCopyRawInput(srcDirPath, rawContainerURLWithSAS.String())
	raw.recursive = true
	if useUtc {
		raw.includeAfter = includeFrom.UTC().Format(time.RFC3339)
	} else {
		raw.includeAfter = includeFrom.Format("2006-01-02T15:04:05") // local time, no timezone
	}

	runCopyAndVerify(a, raw, func(err error) {
		a.Nil(err)

		// validate that the right number of transfers were scheduled
		a.Equal(3, len(mockedRPC.transfers))

		// validate that the right transfers were sent
		expectedTransfers := scenarioHelper{}.shaveOffPrefix(filesToInclude, filepath.Base(srcDirPath)+common.AZCOPY_PATH_SEPARATOR_STRING)
		validateUploadTransfersAreScheduled(a, common.AZCOPY_PATH_SEPARATOR_STRING,
			common.AZCOPY_PATH_SEPARATOR_STRING+filepath.Base(srcDirPath)+common.AZCOPY_PATH_SEPARATOR_STRING, expectedTransfers, mockedRPC)
	})
}

func TestDisableAutoDecoding(t *testing.T) {
	a := assert.New(t)
	bsc := getBlobServiceClient()
	cc, containerName := createNewContainer(a, bsc)
	defer deleteContainer(a, cc)

	// Encoded file name since Windows won't create name with invalid chars
	srcFileName := `%3C %3E %5C %2F %3A %22 %7C %3F %2A invalidcharsfile`

	// set up the source as a single file
	srcDirName := scenarioHelper{}.generateLocalDirectory(a)
	defer os.RemoveAll(srcDirName)
	_, err := scenarioHelper{}.generateLocalFile(filepath.Join(srcDirName, srcFileName), defaultFileSize)
	a.Nil(err)

	// set up interceptor
	mockedRPC := interceptor{}
	jobsAdmin.ExecuteNewCopyJobPartOrder = func(order common.CopyJobPartOrderRequest) common.CopyJobPartOrderResponse {
		return mockedRPC.intercept(order)
	}
	mockedRPC.init()

	// clean the RPC for the next test
	mockedRPC.reset()

	// now target the destination container, the result should be the same
	rawContainerURLWithSAS := scenarioHelper{}.getRawContainerURLWithSAS(a, containerName)
	raw := getDefaultCopyRawInput(filepath.Join(srcDirName, srcFileName), rawContainerURLWithSAS.String())
	raw.disableAutoDecoding = true

	// the file was created after the blob, so no sync should happen
	runCopyAndVerify(a, raw, func(err error) {
		a.Nil(err)

		// verify explicitly since the source and destination names will be different:
		// the source is "" since the given URL points to the blob itself
		// the destination should be the source file name, since decoding has been disabled
		a.Equal(1, len(mockedRPC.transfers))

		a.Equal("", mockedRPC.transfers[0].Source)
		a.Equal(common.AZCOPY_PATH_SEPARATOR_STRING+url.PathEscape(srcFileName), mockedRPC.transfers[0].Destination)
	})
}
