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
	"encoding/json"
	"fmt"
	"os"
	"path"
	"path/filepath"
	"sort"
	"strings"
	"testing"

	"github.com/Azure/azure-sdk-for-go/sdk/azcore/streaming"
	"github.com/Azure/azure-sdk-for-go/sdk/azcore/to"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/container"
	"github.com/Azure/azure-storage-azcopy/v10/jobsAdmin"
	"github.com/Azure/azure-storage-azcopy/v10/traverser"
	"github.com/stretchr/testify/assert"

	"github.com/Azure/azure-storage-azcopy/v10/common"
)

func TestInferredStripTopDirDownload(t *testing.T) {
	a := assert.New(t)
	bsc := getBlobServiceClient()
	cc, cName := createNewContainer(a, bsc)
	defer deleteContainer(a, cc)

	blobNames := []string{
		"*", // File name that we want to retain compatibility with
		"testFile",
		"DoYouPronounceItDataOrData",
		"sub*dir/Help I cannot so much into computer",
	}

	// ----- TEST # 1: Test inferred as false by using escaped * -----

	// set up container name
	scenarioHelper{}.generateBlobsFromList(a, cc, blobNames, blockBlobDefaultData)

	dstDirName := scenarioHelper{}.generateLocalDirectory(a)

	rawContainerURL := scenarioHelper{}.getRawContainerURLWithSAS(a, cName)

	// Don't add /* while still in URL form-- it will get improperly encoded, and azcopy will ignore it.
	rawContainerString := rawContainerURL.String()
	rawContainerStringSplit := strings.Split(rawContainerString, "?")
	rawContainerStringSplit[0] += "/%2A"
	// now in theory: https://ciblobaccount.blob.core.windows.net/container/%2A
	// %2A is set to magic number %00 and not stripped
	// striptopdir should not be set

	// re join strings and create raw input
	raw := getDefaultRawCopyInput(strings.Join(rawContainerStringSplit, "?"), dstDirName)
	raw.recursive = false // default recursive is true in testing framework

	// set up interceptor
	mockedRPC := interceptor{}
	jobsAdmin.ExecuteNewCopyJobPartOrder = func(order common.CopyJobPartOrderRequest) common.CopyJobPartOrderResponse {
		return mockedRPC.intercept(order)
	}
	mockedRPC.init()

	// Test inference of striptopdir
	cooked, err := raw.cook()
	a.Nil(err)
	a.False(cooked.StripTopDir)

	// Test and ensure only one file is being downloaded
	runCopyAndVerify(a, raw, func(err error) {
		a.Nil(err)

		a.Equal(1, len(mockedRPC.transfers))
	})

	// ----- TEST # 2: Test inferred as true by using unescaped * -----

	rawContainerStringSplit = strings.Split(rawContainerString, "?")
	rawContainerStringSplit[0] += "/*"
	// now in theory: https://ciblobaccount.blob.core.windows.net/container/*
	// * is not set to magic number %00, * gets stripped
	// striptopdir should be set.

	// re join strings and create raw input
	raw = getDefaultRawCopyInput(strings.Join(rawContainerStringSplit, "?"), dstDirName)
	raw.recursive = false // default recursive is true in testing framework

	// reset RPC
	mockedRPC.reset()

	// Test inference of striptopdir
	cooked, err = raw.cook()
	a.Nil(err)
	a.True(cooked.StripTopDir)

	// Test and ensure only 3 files get scheduled, nothing under the sub-directory
	runCopyAndVerify(a, raw, func(err error) {
		a.Nil(err)

		a.Equal(3, len(mockedRPC.transfers))
	})

	// ----- TEST # 3: Attempt to use the * in the folder name without encoding ----

	rawContainerStringSplit = strings.Split(rawContainerString, "?")
	rawContainerStringSplit[0] += "/sub*dir/*"
	// now in theory: https://ciblobaccount.blob.core.windows.net/container/sub*dir/*
	// *s are not replaced with magic number %00
	// should error out due to extra * in dir name

	// reset RPC
	mockedRPC.reset()

	// re join strings and create raw input
	raw = getDefaultRawCopyInput(strings.Join(rawContainerStringSplit, "?"), dstDirName)
	raw.recursive = false // default recursive is true in testing framework

	// test error
	cooked, err = raw.cook()
	a.NotNil(err)
	a.Contains(err.Error(), "cannot use wildcards")

	// no actual test needed-- this is where the error lives.

	// ----- TEST # 4: Encode %2A in the folder name and still use stripTopDir ----

	rawContainerStringSplit = strings.Split(rawContainerString, "?")
	rawContainerStringSplit[0] += "/sub%2Adir/*"
	// now in theory: https://ciblobaccount.blob.core.windows.net/container/sub%2Adir/*
	// %2A is replaced with magic number %00
	// should not error out; striptopdir should be true

	// reset RPC
	mockedRPC.reset()

	// re join strings and create raw input
	raw = getDefaultRawCopyInput(strings.Join(rawContainerStringSplit, "?"), dstDirName)
	raw.recursive = false // default recursive is true in testing framework

	// test cook
	cooked, err = raw.cook()
	a.Nil(err)
	a.True(cooked.StripTopDir)

	// Test and ensure only one file got scheduled
	runCopyAndVerify(a, raw, func(err error) {
		a.Nil(err)

		a.Equal(1, len(mockedRPC.transfers))
	})
}

// Test downloading the entire account.
func TestDownloadAccount(t *testing.T) {
	a := assert.New(t)
	bsc := getBlobServiceClient()
	rawBSC := scenarioHelper{}.getBlobServiceClientWithSAS(a)

	// Just in case there are no existing containers...
	cc, _ := createNewContainer(a, bsc)
	defer deleteContainer(a, cc)
	scenarioHelper{}.generateCommonRemoteScenarioForBlob(a, cc, "")

	// Traverse the account ahead of time and determine the relative paths for testing.
	relPaths := make([]string, 0) // Use a map for easy lookup
	blobTraverser := traverser.NewBlobAccountTraverser(rawBSC, "", ctx, traverser.InitResourceTraverserOptions{})
	processor := func(object traverser.StoredObject) error {
		// Skip non-file types
		_, ok := object.Metadata[common.POSIXSymlinkMeta]
		if ok {
			return nil
		}

		// Append the container name to the relative path
		relPath := "/" + object.ContainerName + "/" + object.RelativePath
		relPaths = append(relPaths, relPath)

		return nil
	}
	err := blobTraverser.Traverse(traverser.NoPreProccessor, processor, []traverser.ObjectFilter{})
	a.Nil(err)

	// set up a destination
	dstDirName := scenarioHelper{}.generateLocalDirectory(a)
	defer os.RemoveAll(dstDirName)

	// set up interceptor
	mockedRPC := interceptor{}
	jobsAdmin.ExecuteNewCopyJobPartOrder = func(order common.CopyJobPartOrderRequest) common.CopyJobPartOrderResponse {
		return mockedRPC.intercept(order)
	}
	mockedRPC.init()

	raw := getDefaultCopyRawInput(rawBSC.URL(), dstDirName)
	raw.recursive = true

	runCopyAndVerify(a, raw, func(err error) {
		a.Nil(err)

		validateDownloadTransfersAreScheduled(a, "", "", relPaths, mockedRPC)
	})
}

// Test downloading the entire account.
func TestDownloadAccountWildcard(t *testing.T) {
	a := assert.New(t)
	bsc := getBlobServiceClient()
	rawBSC := scenarioHelper{}.getBlobServiceClientWithSAS(a)

	// Create a unique container to be targeted.
	cname := generateName("blah-unique-blah", 63)
	curl := bsc.NewContainerClient(cname)
	_, err := curl.Create(ctx, nil)
	a.Nil(err)
	defer deleteContainer(a, curl)
	scenarioHelper{}.generateCommonRemoteScenarioForBlob(a, curl, "")

	// update the raw BSU to match the unique container name
	container := "blah-unique-blah*"

	// Traverse the account ahead of time and determine the relative paths for testing.
	relPaths := make([]string, 0) // Use a map for easy lookup
	blobTraverser := traverser.NewBlobAccountTraverser(rawBSC, container, ctx, traverser.InitResourceTraverserOptions{})
	processor := func(object traverser.StoredObject) error {
		// Skip non-file types
		_, ok := object.Metadata[common.POSIXSymlinkMeta]
		if ok {
			return nil
		}

		// Append the container name to the relative path
		relPath := "/" + object.ContainerName + "/" + object.RelativePath
		relPaths = append(relPaths, relPath)
		return nil
	}
	err = blobTraverser.Traverse(traverser.NoPreProccessor, processor, []traverser.ObjectFilter{})
	a.Nil(err)

	// set up a destination
	dstDirName := scenarioHelper{}.generateLocalDirectory(a)
	defer os.RemoveAll(dstDirName)

	// set up interceptor
	mockedRPC := interceptor{}
	jobsAdmin.ExecuteNewCopyJobPartOrder = func(order common.CopyJobPartOrderRequest) common.CopyJobPartOrderResponse {
		return mockedRPC.intercept(order)
	}
	mockedRPC.init()

	raw := getDefaultCopyRawInput(rawBSC.NewContainerClient(container).URL(), dstDirName)
	raw.recursive = true

	runCopyAndVerify(a, raw, func(err error) {
		a.Nil(err)

		validateDownloadTransfersAreScheduled(a, "", "", relPaths, mockedRPC)
	})
}

// regular blob->local file download
func TestDownloadSingleBlobToFile(t *testing.T) {
	a := assert.New(t)
	bsc := getBlobServiceClient()
	cc, containerName := createNewContainer(a, bsc)
	defer deleteContainer(a, cc)

	for _, blobName := range []string{"singleblobisbest", "打麻将.txt", "%4509%4254$85140&"} {
		// set up the container with a single blob
		blobList := []string{blobName}
		scenarioHelper{}.generateBlobsFromList(a, cc, blobList, blockBlobDefaultData)
		a.NotNil(cc)

		// set up the destination as a single file
		dstDirName := scenarioHelper{}.generateLocalDirectory(a)
		defer os.RemoveAll(dstDirName)
		dstFileName := "whatever"
		scenarioHelper{}.generateLocalFilesFromList(a, dstDirName, blobList)

		// set up interceptor
		mockedRPC := interceptor{}
		jobsAdmin.ExecuteNewCopyJobPartOrder = func(order common.CopyJobPartOrderRequest) common.CopyJobPartOrderResponse {
			return mockedRPC.intercept(order)
		}
		mockedRPC.init()

		// construct the raw input to simulate user input
		rawBlobURLWithSAS := scenarioHelper{}.getRawBlobURLWithSAS(a, containerName, blobList[0])
		raw := getDefaultCopyRawInput(rawBlobURLWithSAS.String(), filepath.Join(dstDirName, dstFileName))

		// the file was created after the blob, so no sync should happen
		runCopyAndVerify(a, raw, func(err error) {
			a.Nil(err)

			validateDownloadTransfersAreScheduled(a, "", "", []string{""}, mockedRPC)
		})

		// clean the RPC for the next test
		mockedRPC.reset()

		// now target the destination directory, the result should be the same
		raw = getDefaultCopyRawInput(rawBlobURLWithSAS.String(), dstDirName)

		// the file was created after the blob, so no sync should happen
		runCopyAndVerify(a, raw, func(err error) {
			a.Nil(err)

			// verify explicitly since the source and destination names will be different:
			// the source is "" since the given URL points to the blob itself
			// the destination should be the blob name, since the given local path points to the parent dir
			a.Equal(1, len(mockedRPC.transfers))
			a.Equal("", mockedRPC.transfers[0].Source)
			a.Equal(common.AZCOPY_PATH_SEPARATOR_STRING+blobName, mockedRPC.transfers[0].Destination)
		})
	}
}

// regular container->directory download
func TestDownloadBlobContainer(t *testing.T) {
	a := assert.New(t)
	bsc := getBlobServiceClient()

	// set up the container with numerous blobs
	cc, containerName := createNewContainer(a, bsc)
	blobList := scenarioHelper{}.generateCommonRemoteScenarioForBlob(a, cc, "")
	defer deleteContainer(a, cc)
	a.NotNil(cc)
	a.NotEqual(0, len(blobList))

	// set up the destination with an empty folder
	dstDirName := scenarioHelper{}.generateLocalDirectory(a)
	defer os.RemoveAll(dstDirName)

	// set up interceptor
	mockedRPC := interceptor{}
	jobsAdmin.ExecuteNewCopyJobPartOrder = func(order common.CopyJobPartOrderRequest) common.CopyJobPartOrderResponse {
		return mockedRPC.intercept(order)
	}
	mockedRPC.init()

	// construct the raw input to simulate user input
	rawContainerURLWithSAS := scenarioHelper{}.getRawContainerURLWithSAS(a, containerName)
	raw := getDefaultCopyRawInput(rawContainerURLWithSAS.String(), dstDirName)
	raw.recursive = true

	runCopyAndVerify(a, raw, func(err error) {
		a.Nil(err)

		// validate that the right number of transfers were scheduled
		a.Equal(len(blobList), len(mockedRPC.transfers))

		// validate that the right transfers were sent
		validateDownloadTransfersAreScheduled(a, common.AZCOPY_PATH_SEPARATOR_STRING, common.AZCOPY_PATH_SEPARATOR_STRING+containerName+common.AZCOPY_PATH_SEPARATOR_STRING, blobList, mockedRPC)
	})

	// turn off recursive, this time nothing should be transferred
	raw.recursive = false
	mockedRPC.reset()

	runCopyAndVerify(a, raw, func(err error) {
		a.NotNil(err)
		a.Zero(len(mockedRPC.transfers))
	})
}

// regular vdir->dir download
func TestDownloadBlobVirtualDirectory(t *testing.T) {
	a := assert.New(t)
	bsc := getBlobServiceClient()
	vdirName := "vdir1"

	// set up the container with numerous blobs
	cc, containerName := createNewContainer(a, bsc)
	blobList := scenarioHelper{}.generateCommonRemoteScenarioForBlob(a, cc, vdirName+common.AZCOPY_PATH_SEPARATOR_STRING)
	defer deleteContainer(a, cc)
	a.NotNil(cc)
	a.NotEqual(0, len(blobList))

	// set up the destination with an empty folder
	dstDirName := scenarioHelper{}.generateLocalDirectory(a)
	defer os.RemoveAll(dstDirName)

	// set up interceptor
	mockedRPC := interceptor{}
	jobsAdmin.ExecuteNewCopyJobPartOrder = func(order common.CopyJobPartOrderRequest) common.CopyJobPartOrderResponse {
		return mockedRPC.intercept(order)
	}
	mockedRPC.init()

	// construct the raw input to simulate user input
	rawContainerURLWithSAS := scenarioHelper{}.getRawBlobURLWithSAS(a, containerName, vdirName)
	raw := getDefaultCopyRawInput(rawContainerURLWithSAS.String(), dstDirName)
	raw.recursive = true

	runCopyAndVerify(a, raw, func(err error) {
		a.Nil(err)

		// validate that the right number of transfers were scheduled
		a.Equal(len(blobList), len(mockedRPC.transfers))

		// validate that the right transfers were sent
		expectedTransfers := scenarioHelper{}.shaveOffPrefix(blobList, vdirName+common.AZCOPY_PATH_SEPARATOR_STRING)
		validateDownloadTransfersAreScheduled(a, common.AZCOPY_PATH_SEPARATOR_STRING,
			common.AZCOPY_PATH_SEPARATOR_STRING+vdirName+common.AZCOPY_PATH_SEPARATOR_STRING, expectedTransfers, mockedRPC)
	})

	// turn off recursive, this time nothing should be transferred
	raw.recursive = false
	mockedRPC.reset()

	runCopyAndVerify(a, raw, func(err error) {
		a.NotNil(err)
		a.Zero(len(mockedRPC.transfers))
	})
}

// blobs(from pattern)->directory download
// TODO the current pattern matching behavior is inconsistent with the posix filesystem
//
//	update test after re-writing copy enumerators
func TestDownloadBlobContainerWithPattern(t *testing.T) {
	a := assert.New(t)
	bsc := getBlobServiceClient()

	// set up the container with numerous blobs
	cc, containerName := createNewContainer(a, bsc)
	blobsToIgnore := scenarioHelper{}.generateCommonRemoteScenarioForBlob(a, cc, "")
	defer deleteContainer(a, cc)
	a.NotNil(cc)
	a.NotEqual(0, len(blobsToIgnore))

	// add special blobs that we wish to include
	blobsToInclude := []string{"important.pdf", "includeSub/amazing.pdf", "includeSub/wow/amazing.pdf"}
	scenarioHelper{}.generateBlobsFromList(a, cc, blobsToInclude, blockBlobDefaultData)

	// set up the destination with an empty folder
	dstDirName := scenarioHelper{}.generateLocalDirectory(a)
	defer os.RemoveAll(dstDirName)

	// set up interceptor
	mockedRPC := interceptor{}
	jobsAdmin.ExecuteNewCopyJobPartOrder = func(order common.CopyJobPartOrderRequest) common.CopyJobPartOrderResponse {
		return mockedRPC.intercept(order)
	}
	mockedRPC.init()

	// construct the raw input to simulate user input
	rawContainerURLWithSAS := scenarioHelper{}.getRawContainerURLWithSAS(a, containerName)
	rawContainerURLWithSAS.Path = path.Join(rawContainerURLWithSAS.Path, string([]byte{0x00}))
	containerString := strings.ReplaceAll(rawContainerURLWithSAS.String(), "%00", "*")
	raw := getDefaultCopyRawInput(containerString, dstDirName)
	raw.recursive = true
	raw.include = "*.pdf"

	runCopyAndVerify(a, raw, func(err error) {
		a.Nil(err)

		// validate that the right number of transfers were scheduled
		a.Equal(len(blobsToInclude), len(mockedRPC.transfers))

		// validate that the right transfers were sent
		validateDownloadTransfersAreScheduled(a, common.AZCOPY_PATH_SEPARATOR_STRING, common.AZCOPY_PATH_SEPARATOR_STRING,
			blobsToInclude, mockedRPC)
	})

	// turn off recursive, this time nothing should be transferred
	raw.recursive = false
	mockedRPC.reset()

	runCopyAndVerify(a, raw, func(err error) {
		a.Nil(err)

		// only the top pdf should be included
		a.Equal(1, len(mockedRPC.transfers))
		a.Equal(mockedRPC.transfers[0].Destination, mockedRPC.transfers[0].Source)
		a.True(strings.HasSuffix(mockedRPC.transfers[0].Source, ".pdf"))
		a.False(strings.Contains(mockedRPC.transfers[0].Source[1:], common.AZCOPY_PATH_SEPARATOR_STRING))
	})
}

// test for include with one regular expression
func TestDownloadBlobContainerWithRegexInclude(t *testing.T) {
	a := assert.New(t)
	bsc := getBlobServiceClient()

	// set up the container with  blobs
	cc, containerName := createNewContainer(a, bsc)
	blobsToIgnore := scenarioHelper{}.generateCommonRemoteScenarioForBlob(a, cc, "")
	defer deleteContainer(a, cc)
	a.NotNil(cc)
	a.NotEqual(0, len(blobsToIgnore))

	// add blobs that we wish to include
	blobsToInclude := []string{"tessssssssssssst.txt", "subOne/tetingessssss.jpeg", "subOne/tessssst/hi.pdf"}
	scenarioHelper{}.generateBlobsFromList(a, cc, blobsToInclude, blockBlobDefaultData)

	// set up the destination with an empty folder
	dstDirName := scenarioHelper{}.generateLocalDirectory(a)
	defer os.RemoveAll(dstDirName)

	// set up interceptor
	mockedRPC := interceptor{}
	jobsAdmin.ExecuteNewCopyJobPartOrder = func(order common.CopyJobPartOrderRequest) common.CopyJobPartOrderResponse {
		return mockedRPC.intercept(order)
	}
	mockedRPC.init()

	// construct the raw input to simulate user input
	rawContainerURLWithSAS := scenarioHelper{}.getRawContainerURLWithSAS(a, containerName)
	rawContainerURLWithSAS.Path = path.Join(rawContainerURLWithSAS.Path, string([]byte{0x00}))
	containerString := strings.ReplaceAll(rawContainerURLWithSAS.String(), "%00", "*")
	raw := getDefaultCopyRawInput(containerString, dstDirName)
	raw.recursive = true
	raw.includeRegex = "es{4,}"

	runCopyAndVerify(a, raw, func(err error) {
		a.Nil(err)
		// validate that the right number of transfers were scheduled
		a.Equal(len(blobsToInclude), len(mockedRPC.transfers))
		// comparing is names of files match
		actualTransfer := []string{}
		for i := 0; i < len(mockedRPC.transfers); i++ {
			actualTransfer = append(actualTransfer, strings.Trim(mockedRPC.transfers[i].Source, "/"))
		}
		sort.Strings(actualTransfer)
		sort.Strings(blobsToInclude)
		a.Equal(blobsToInclude, actualTransfer)

		// validate that the right transfers were sent
		validateDownloadTransfersAreScheduled(a, common.AZCOPY_PATH_SEPARATOR_STRING, common.AZCOPY_PATH_SEPARATOR_STRING,
			blobsToInclude, mockedRPC)
	})
}

// test multiple regular expression with include
func TestDownloadBlobContainerWithMultRegexInclude(t *testing.T) {
	a := assert.New(t)
	bsc := getBlobServiceClient()

	// set up the container with  blobs
	cc, containerName := createNewContainer(a, bsc)
	blobsToIgnore := scenarioHelper{}.generateCommonRemoteScenarioForBlob(a, cc, "")
	defer deleteContainer(a, cc)
	a.NotNil(cc)
	a.NotEqual(0, len(blobsToIgnore))

	// add blobs that we wish to include
	blobsToInclude := []string{"tessssssssssssst.txt", "zxcfile.txt", "subOne/tetingessssss.jpeg", "subOne/subTwo/tessssst.pdf"}
	scenarioHelper{}.generateBlobsFromList(a, cc, blobsToInclude, blockBlobDefaultData)

	// set up the destination with an empty folder
	dstDirName := scenarioHelper{}.generateLocalDirectory(a)
	defer os.RemoveAll(dstDirName)

	// set up interceptor
	mockedRPC := interceptor{}
	jobsAdmin.ExecuteNewCopyJobPartOrder = func(order common.CopyJobPartOrderRequest) common.CopyJobPartOrderResponse {
		return mockedRPC.intercept(order)
	}
	mockedRPC.init()

	// construct the raw input to simulate user input
	rawContainerURLWithSAS := scenarioHelper{}.getRawContainerURLWithSAS(a, containerName)
	rawContainerURLWithSAS.Path = path.Join(rawContainerURLWithSAS.Path, string([]byte{0x00}))
	containerString := strings.ReplaceAll(rawContainerURLWithSAS.String(), "%00", "*")
	raw := getDefaultCopyRawInput(containerString, dstDirName)
	raw.recursive = true
	raw.includeRegex = "es{4,};^zxc"

	runCopyAndVerify(a, raw, func(err error) {
		a.Nil(err)
		// validate that the right number of transfers were scheduled
		a.Equal(len(blobsToInclude), len(mockedRPC.transfers))
		// validate that the right transfers were sent

		// comparing is names of files, since not in order need to sort each string and the compare them
		actualTransfer := []string{}
		for i := 0; i < len(mockedRPC.transfers); i++ {
			actualTransfer = append(actualTransfer, strings.Trim(mockedRPC.transfers[i].Source, "/"))
		}
		sort.Strings(actualTransfer)
		sort.Strings(blobsToInclude)
		a.Equal(blobsToInclude, actualTransfer)

		validateDownloadTransfersAreScheduled(a, common.AZCOPY_PATH_SEPARATOR_STRING, common.AZCOPY_PATH_SEPARATOR_STRING,
			blobsToInclude, mockedRPC)
	})
}

// testing empty expressions for both include and exclude
func TestDownloadBlobContainerWithEmptyRegex(t *testing.T) {
	a := assert.New(t)
	bsc := getBlobServiceClient()

	// set up the container with  blobs
	cc, containerName := createNewContainer(a, bsc)
	// test empty regex flag so all blobs will be included since there is no filter
	blobsToInclude := scenarioHelper{}.generateCommonRemoteScenarioForBlob(a, cc, "")
	defer deleteContainer(a, cc)
	a.NotNil(cc)
	a.NotEqual(0, len(blobsToInclude))

	// set up the destination with an empty folder
	dstDirName := scenarioHelper{}.generateLocalDirectory(a)
	defer os.RemoveAll(dstDirName)

	// set up interceptor
	mockedRPC := interceptor{}
	jobsAdmin.ExecuteNewCopyJobPartOrder = func(order common.CopyJobPartOrderRequest) common.CopyJobPartOrderResponse {
		return mockedRPC.intercept(order)
	}
	mockedRPC.init()

	// construct the raw input to simulate user input
	rawContainerURLWithSAS := scenarioHelper{}.getRawContainerURLWithSAS(a, containerName)
	rawContainerURLWithSAS.Path = path.Join(rawContainerURLWithSAS.Path, string([]byte{0x00}))
	containerString := strings.ReplaceAll(rawContainerURLWithSAS.String(), "%00", "*")
	raw := getDefaultCopyRawInput(containerString, dstDirName)
	raw.recursive = true
	raw.includeRegex = ""
	raw.excludeRegex = ""

	runCopyAndVerify(a, raw, func(err error) {
		a.Nil(err)
		// validate that the right number of transfers were scheduled
		a.Equal(len(blobsToInclude), len(mockedRPC.transfers))
		// do not need to check file names since all files for blobsToInclude are passed bc flags are empty
		// validate that the right transfers were sent
		validateDownloadTransfersAreScheduled(a, common.AZCOPY_PATH_SEPARATOR_STRING, common.AZCOPY_PATH_SEPARATOR_STRING,
			blobsToInclude, mockedRPC)
	})
}

// testing exclude with one regular expression
func TestDownloadBlobContainerWithRegexExclude(t *testing.T) {
	a := assert.New(t)
	bsc := getBlobServiceClient()

	// set up the container with  blobs
	cc, containerName := createNewContainer(a, bsc)
	blobsToInclude := scenarioHelper{}.generateCommonRemoteScenarioForBlob(a, cc, "")
	defer deleteContainer(a, cc)
	a.NotNil(cc)
	a.NotEqual(0, len(blobsToInclude))

	// add blobs that we wish to exclude
	blobsToIgnore := []string{"tessssssssssssst.txt", "subOne/tetingessssss.jpeg", "subOne/subTwo/tessssst.pdf"}
	scenarioHelper{}.generateBlobsFromList(a, cc, blobsToIgnore, blockBlobDefaultData)

	// set up the destination with an empty folder
	dstDirName := scenarioHelper{}.generateLocalDirectory(a)
	defer os.RemoveAll(dstDirName)

	// set up interceptor
	mockedRPC := interceptor{}
	jobsAdmin.ExecuteNewCopyJobPartOrder = func(order common.CopyJobPartOrderRequest) common.CopyJobPartOrderResponse {
		return mockedRPC.intercept(order)
	}
	mockedRPC.init()

	// construct the raw input to simulate user input
	rawContainerURLWithSAS := scenarioHelper{}.getRawContainerURLWithSAS(a, containerName)
	rawContainerURLWithSAS.Path = path.Join(rawContainerURLWithSAS.Path, string([]byte{0x00}))
	containerString := strings.ReplaceAll(rawContainerURLWithSAS.String(), "%00", "*")
	raw := getDefaultCopyRawInput(containerString, dstDirName)
	raw.recursive = true
	raw.excludeRegex = "es{4,}"

	runCopyAndVerify(a, raw, func(err error) {
		a.Nil(err)
		// validate that only blobsTo
		a.Equal(len(blobsToInclude), len(mockedRPC.transfers))
		// comparing is names of files, since not in order need to sort each string and the compare them
		actualTransfer := []string{}
		for i := 0; i < len(mockedRPC.transfers); i++ {
			actualTransfer = append(actualTransfer, strings.Trim(mockedRPC.transfers[i].Destination, "/"))
		}
		sort.Strings(actualTransfer)
		sort.Strings(blobsToInclude)
		a.Equal(blobsToInclude, actualTransfer)

		// validate that the right transfers were sent
		validateDownloadTransfersAreScheduled(a, common.AZCOPY_PATH_SEPARATOR_STRING, common.AZCOPY_PATH_SEPARATOR_STRING,
			blobsToInclude, mockedRPC)
	})
}

// testing exclude with multiple regular expressions
func TestDownloadBlobContainerWithMultRegexExclude(t *testing.T) {
	a := assert.New(t)
	bsc := getBlobServiceClient()

	// set up the container with  blobs
	cc, containerName := createNewContainer(a, bsc)
	blobsToInclude := scenarioHelper{}.generateCommonRemoteScenarioForBlob(a, cc, "")
	defer deleteContainer(a, cc)
	a.NotNil(cc)
	a.NotEqual(0, len(blobsToInclude))

	// add blobs that we wish to exclude
	blobsToIgnore := []string{"tessssssssssssst.txt", "subOne/dogs.jpeg", "subOne/subTwo/tessssst.pdf"}
	scenarioHelper{}.generateBlobsFromList(a, cc, blobsToIgnore, blockBlobDefaultData)

	// set up the destination with an empty folder
	dstDirName := scenarioHelper{}.generateLocalDirectory(a)
	defer os.RemoveAll(dstDirName)

	// set up interceptor
	mockedRPC := interceptor{}
	jobsAdmin.ExecuteNewCopyJobPartOrder = func(order common.CopyJobPartOrderRequest) common.CopyJobPartOrderResponse {
		return mockedRPC.intercept(order)
	}
	mockedRPC.init()

	// construct the raw input to simulate user input
	rawContainerURLWithSAS := scenarioHelper{}.getRawContainerURLWithSAS(a, containerName)
	rawContainerURLWithSAS.Path = path.Join(rawContainerURLWithSAS.Path, string([]byte{0x00}))
	containerString := strings.ReplaceAll(rawContainerURLWithSAS.String(), "%00", "*")
	raw := getDefaultCopyRawInput(containerString, dstDirName)
	raw.recursive = true
	raw.excludeRegex = "es{4,};o(g)"

	runCopyAndVerify(a, raw, func(err error) {
		a.Nil(err)
		// validate that the right number of transfers were scheduled
		a.Equal(len(blobsToInclude), len(mockedRPC.transfers))
		// comparing is names of files, since not in order need to sort each string and the compare them
		actualTransfer := []string{}
		for i := 0; i < len(mockedRPC.transfers); i++ {
			actualTransfer = append(actualTransfer, strings.Trim(mockedRPC.transfers[i].Destination, "/"))
		}
		sort.Strings(actualTransfer)
		sort.Strings(blobsToInclude)
		a.Equal(blobsToInclude, actualTransfer)

		// validate that the right transfers were sent
		validateDownloadTransfersAreScheduled(a, common.AZCOPY_PATH_SEPARATOR_STRING, common.AZCOPY_PATH_SEPARATOR_STRING,
			blobsToInclude, mockedRPC)
	})
}

func TestDryrunCopyLocalToBlob(t *testing.T) {
	a := assert.New(t)
	bsc := getBlobServiceClient()

	// set up the local source
	blobsToInclude := []string{"AzURE2021.jpeg", "sub1/dir2/HELLO-4.txt", "sub1/test/testing.txt"}
	srcDirName := scenarioHelper{}.generateLocalDirectory(a)
	defer os.RemoveAll(srcDirName)
	scenarioHelper{}.generateLocalFilesFromList(a, srcDirName, blobsToInclude)
	a.NotNil(srcDirName)

	// set up the destination container
	dstContainerClient, dstContainerName := createNewContainer(a, bsc)
	defer deleteContainer(a, dstContainerClient)
	a.NotNil(dstContainerClient)

	// set up interceptor
	mockedRPC := interceptor{}
	jobsAdmin.ExecuteNewCopyJobPartOrder = func(order common.CopyJobPartOrderRequest) common.CopyJobPartOrderResponse {
		return mockedRPC.intercept(order)
	}
	mockedLcm := mockedLifecycleManager{dryrunLog: make(chan string, 50)}
	mockedLcm.SetOutputFormat(EOutputFormat.Text()) // text format
	glcm = &mockedLcm

	// construct the raw input to simulate user input
	rawContainerURLWithSAS := scenarioHelper{}.getRawContainerURLWithSAS(a, dstContainerName)
	raw := getDefaultCopyRawInput(srcDirName, rawContainerURLWithSAS.String())
	raw.dryrun = true
	raw.recursive = true

	runCopyAndVerify(a, raw, func(err error) {
		a.Nil(err)
		// validate that none where transferred
		a.Zero(len(mockedRPC.transfers))

		msg := mockedLcm.GatherAllLogs(mockedLcm.dryrunLog)
		for i := 0; i < len(blobsToInclude); i++ {
			a.True(strings.Contains(msg[i], "DRYRUN: copy"))
			a.True(strings.Contains(msg[i], srcDirName))
			a.True(strings.Contains(msg[i], dstContainerClient.URL()))
		}

		a.True(testDryrunStatements(blobsToInclude, msg))
	})
}

func TestDryrunCopyBlobToBlob(t *testing.T) {
	a := assert.New(t)
	bsc := getBlobServiceClient()

	// set up src container
	srcContainerClient, srcContainerName := createNewContainer(a, bsc)
	defer deleteContainer(a, srcContainerClient)
	blobsToInclude := []string{"AzURE2021.jpeg", "sub1/dir2/HELLO-4.txt", "sub1/test/testing.txt"}
	scenarioHelper{}.generateBlobsFromList(a, srcContainerClient, blobsToInclude, blockBlobDefaultData)
	a.NotNil(srcContainerClient)

	// set up the destination
	dstContainerClient, dstContainerName := createNewContainer(a, bsc)
	defer deleteContainer(a, dstContainerClient)
	a.NotNil(dstContainerClient)

	// set up interceptor
	mockedRPC := interceptor{}
	jobsAdmin.ExecuteNewCopyJobPartOrder = func(order common.CopyJobPartOrderRequest) common.CopyJobPartOrderResponse {
		return mockedRPC.intercept(order)
	}
	mockedLcm := mockedLifecycleManager{dryrunLog: make(chan string, 50)}
	mockedLcm.SetOutputFormat(EOutputFormat.Text()) // text format
	glcm = &mockedLcm

	// construct the raw input to simulate user input
	rawContainerURLWithSAS := scenarioHelper{}.getRawContainerURLWithSAS(a, srcContainerName)
	rawDstContainerURLWithSAS := scenarioHelper{}.getRawContainerURLWithSAS(a, dstContainerName)
	raw := getDefaultCopyRawInput(rawContainerURLWithSAS.String(), rawDstContainerURLWithSAS.String())
	raw.dryrun = true
	raw.recursive = true

	runCopyAndVerify(a, raw, func(err error) {
		a.Nil(err)
		// validate that none where transferred
		a.Zero(len(mockedRPC.transfers))

		msg := mockedLcm.GatherAllLogs(mockedLcm.dryrunLog)
		for i := 0; i < len(blobsToInclude); i++ {
			a.True(strings.Contains(msg[i], "DRYRUN: copy"))
			a.True(strings.Contains(msg[i], srcContainerClient.URL()))
			a.True(strings.Contains(msg[i], dstContainerClient.URL()))
		}

		a.True(testDryrunStatements(blobsToInclude, msg))
	})
}

func TestDryrunCopyBlobToBlobJson(t *testing.T) {
	a := assert.New(t)
	bsc := getBlobServiceClient()
	// set up src container
	srcContainerClient, srcContainerName := createNewContainer(a, bsc)
	defer deleteContainer(a, srcContainerClient)
	blobsToInclude := []string{"AzURE2021.jpeg"}
	scenarioHelper{}.generateBlobsFromList(a, srcContainerClient, blobsToInclude, blockBlobDefaultData)
	a.NotNil(srcContainerClient)

	// set up the destination
	dstContainerClient, dstContainerName := createNewContainer(a, bsc)
	defer deleteContainer(a, dstContainerClient)
	a.NotNil(dstContainerClient)

	// set up interceptor
	mockedRPC := interceptor{}
	jobsAdmin.ExecuteNewCopyJobPartOrder = func(order common.CopyJobPartOrderRequest) common.CopyJobPartOrderResponse {
		return mockedRPC.intercept(order)
	}
	mockedLcm := mockedLifecycleManager{dryrunLog: make(chan string, 50)}
	mockedLcm.SetOutputFormat(EOutputFormat.Json()) // json format
	glcm = &mockedLcm

	// construct the raw input to simulate user input
	rawSrcContainerURLWithSAS := scenarioHelper{}.getRawContainerURLWithSAS(a, srcContainerName)
	rawDstContainerURLWithSAS := scenarioHelper{}.getRawContainerURLWithSAS(a, dstContainerName)
	raw := getDefaultCopyRawInput(rawSrcContainerURLWithSAS.String(), rawDstContainerURLWithSAS.String())
	raw.dryrun = true
	raw.recursive = true

	runCopyAndVerify(a, raw, func(err error) {
		a.Nil(err)
		// validate that none where transferred
		a.Zero(len(mockedRPC.transfers))

		msg := <-mockedLcm.dryrunLog
		copyMessage := DryrunTransfer{}
		errMarshal := json.Unmarshal([]byte(msg), &copyMessage)
		a.Nil(errMarshal)

		// comparing some values of copyMessage
		srcRel := strings.TrimPrefix(copyMessage.Source, srcContainerClient.URL())
		dstRel := strings.TrimPrefix(copyMessage.Destination, dstContainerClient.URL())
		a.Equal(blobsToInclude[0], strings.Trim(srcRel, "/"))
		a.Equal(blobsToInclude[0], strings.Trim(dstRel, "/"))
		a.Equal(common.EEntityType.File(), copyMessage.EntityType)
		a.Equal(common.EBlobType.BlockBlob(), copyMessage.BlobType)
	})
}

func TestDryrunCopyS3toBlob(t *testing.T) {
	a := assert.New(t)
	skipIfS3Disabled(t)
	s3Client, err := createS3ClientWithMinio(createS3ResOptions{})
	if err != nil {
		t.Skip("S3 client credentials not supplied")
	}

	// set up src s3 bucket
	bucketName := generateBucketName()
	createNewBucketWithName(a, s3Client, bucketName, createS3ResOptions{})
	defer deleteBucket(s3Client, bucketName, true)
	objectList := []string{"AzURE2021.jpeg"}
	scenarioHelper{}.generateObjects(a, s3Client, bucketName, objectList)

	// initialize dst container
	dstContainerName := generateContainerName()

	// set up interceptor
	mockedRPC := interceptor{}
	jobsAdmin.ExecuteNewCopyJobPartOrder = func(order common.CopyJobPartOrderRequest) common.CopyJobPartOrderResponse {
		return mockedRPC.intercept(order)
	}
	mockedLcm := mockedLifecycleManager{dryrunLog: make(chan string, 50)}
	mockedLcm.SetOutputFormat(EOutputFormat.Text()) // text format
	glcm = &mockedLcm

	// construct the raw input to simulate user input
	rawSrcS3ObjectURL := scenarioHelper{}.getRawS3ObjectURL(a, "", bucketName, "AzURE2021.jpeg")
	rawDstContainerURLWithSAS := scenarioHelper{}.getRawContainerURLWithSAS(a, dstContainerName)
	raw := getDefaultRawCopyInput(rawSrcS3ObjectURL.String(), rawDstContainerURLWithSAS.String())
	raw.dryrun = true
	raw.recursive = true

	runCopyAndVerify(a, raw, func(err error) {
		a.Nil(err)
		// validate that none where transferred
		a.Zero(len(mockedRPC.transfers))

		msg := mockedLcm.GatherAllLogs(mockedLcm.dryrunLog)
		dstPath := strings.Split(rawDstContainerURLWithSAS.String(), "?")
		a.True(strings.Contains(msg[0], "DRYRUN: copy"))
		a.True(strings.Contains(msg[0], rawSrcS3ObjectURL.String()))
		a.True(strings.Contains(msg[0], dstPath[0]))
		a.True(testDryrunStatements(objectList, msg))
	})
}

func TestDryrunCopyGCPtoBlob(t *testing.T) {
	a := assert.New(t)
	skipIfGCPDisabled(t)
	gcpClient, err := createGCPClientWithGCSSDK()
	if err != nil {
		t.Skip("GCP client credentials not supplied")
	}
	// set up src gcp bucket
	bucketName := generateBucketName()
	createNewGCPBucketWithName(a, gcpClient, bucketName)
	defer deleteGCPBucket(gcpClient, bucketName, true)
	blobsToInclude := []string{"AzURE2021.jpeg"}
	scenarioHelper{}.generateGCPObjects(a, gcpClient, bucketName, blobsToInclude)
	a.NotNil(gcpClient)

	// initialize dst container
	dstContainerName := generateContainerName()

	// set up interceptor
	mockedRPC := interceptor{}
	jobsAdmin.ExecuteNewCopyJobPartOrder = func(order common.CopyJobPartOrderRequest) common.CopyJobPartOrderResponse {
		return mockedRPC.intercept(order)
	}
	mockedLcm := mockedLifecycleManager{dryrunLog: make(chan string, 50)}
	mockedLcm.SetOutputFormat(EOutputFormat.Text()) // text format
	glcm = &mockedLcm

	// construct the raw input to simulate user input
	rawSrcGCPObjectURL := scenarioHelper{}.getRawGCPObjectURL(a, bucketName, "AzURE2021.jpeg") // Use default region
	rawDstContainerURLWithSAS := scenarioHelper{}.getRawContainerURLWithSAS(a, dstContainerName)
	raw := getDefaultRawCopyInput(rawSrcGCPObjectURL.String(), rawDstContainerURLWithSAS.String())
	raw.dryrun = true
	raw.recursive = true

	runCopyAndVerify(a, raw, func(err error) {
		a.Nil(err)
		// validate that none where transferred
		a.Zero(len(mockedRPC.transfers))

		msg := mockedLcm.GatherAllLogs(mockedLcm.dryrunLog)
		dstPath := strings.Split(rawDstContainerURLWithSAS.String(), "?")
		a.True(strings.Contains(msg[0], "DRYRUN: copy"))
		a.True(strings.Contains(msg[0], rawSrcGCPObjectURL.String()))
		a.True(strings.Contains(msg[0], dstPath[0]))
		a.True(testDryrunStatements(blobsToInclude, msg))
	})
}

func TestListOfVersions(t *testing.T) {
	a := assert.New(t)
	bsc := getSecondaryBlobServiceClient()
	// set up the container with single blob with 2 versions
	containerClient, containerName := createNewContainer(a, bsc)
	defer deleteContainer(a, containerClient)

	bbClient, blobName := getBlockBlobClient(a, containerClient, "")
	// initial upload
	_, err := bbClient.Upload(ctx, streaming.NopCloser(strings.NewReader(blockBlobDefaultData)), nil)
	a.NoError(err)

	blobProp, err := bbClient.GetProperties(ctx, nil)
	a.NoError(err)

	// second upload to create 1st version
	uploadResp, err := bbClient.Upload(ctx, streaming.NopCloser(strings.NewReader("Random random")), nil)
	a.NoError(err)
	a.NotNil(uploadResp.VersionID)
	a.NotEqual(blobProp.VersionID, uploadResp.VersionID)

	// second upload to create 2nd version
	uploadResp2, err := bbClient.Upload(ctx, streaming.NopCloser(strings.NewReader("Random stuff again")), nil)
	a.NoError(err)
	a.NotNil(uploadResp2.VersionID)
	a.NotEqual(blobProp.VersionID, uploadResp2.VersionID)
	a.NotEqual(uploadResp.VersionID, uploadResp2.VersionID)

	// creating list of version files
	versions := [2]string{*uploadResp.VersionID, *uploadResp2.VersionID}

	tmpDir, err := os.MkdirTemp("", "tmpdir")
	defer os.RemoveAll(tmpDir)
	a.NoError(err)

	fileName := "listofversions.txt"
	file, err := os.CreateTemp(tmpDir, fileName)
	a.NoError(err)
	defer os.Remove(file.Name())
	defer file.Close()

	for _, ver := range versions {
		fmt.Fprintln(file, ver)
	}

	// confirm that base blob has 2 versions
	pager := containerClient.NewListBlobsFlatPager(&container.ListBlobsFlatOptions{
		Prefix:  to.Ptr(blobName),
		Include: container.ListBlobsInclude{Versions: true},
	})
	list, err := pager.NextPage(ctx)
	a.NoError(err)
	a.NotNil(list.Segment.BlobItems)
	a.Equal(3, len(list.Segment.BlobItems))

	// set up interceptor
	mockedRPC := interceptor{}
	jobsAdmin.ExecuteNewCopyJobPartOrder = func(order common.CopyJobPartOrderRequest) common.CopyJobPartOrderResponse {
		return mockedRPC.intercept(order)
	}
	mockedRPC.init()

	// construct the raw input to simulate user input
	rawBlobURLWithSAS := scenarioHelper{}.getSecondaryRawBlobURLWithSAS(a, containerName, blobName)
	raw := getDefaultRemoveRawInput(rawBlobURLWithSAS.String())
	raw.recursive = true
	raw.listOfVersionIDs = file.Name()
	runCopyAndVerify(a, raw, func(err error) {
		a.Nil(err)

		// validate that the right number of transfers were scheduled
		a.Equal(2, len(mockedRPC.transfers))
		versionsTransfer := [2]string{mockedRPC.transfers[0].BlobVersionID, mockedRPC.transfers[1].BlobVersionID}
		a.Equal(versions, versionsTransfer)
	})
}

func TestListOfVersionsNegative(t *testing.T) {
	a := assert.New(t)
	bsc := getBlobServiceClient()
	// set up the container with single blob with 2 versions
	containerClient, containerName := createNewContainer(a, bsc)
	defer deleteContainer(a, containerClient)

	bbClient, blobName := getBlockBlobClient(a, containerClient, "")
	// initial upload
	_, err := bbClient.Upload(ctx, streaming.NopCloser(strings.NewReader(blockBlobDefaultData)), nil)
	a.NoError(err)

	// creating list of version files
	versions := [1]string{"fakeversionid"}

	tmpDir, err := os.MkdirTemp("", "tmpdir")
	defer os.RemoveAll(tmpDir)
	a.NoError(err)

	fileName := "listofversions.txt"
	file, err := os.CreateTemp(tmpDir, fileName)
	a.NoError(err)
	defer os.Remove(file.Name())
	defer file.Close()

	for _, ver := range versions {
		fmt.Fprintln(file, ver)
	}

	// set up interceptor
	mockedRPC := interceptor{}
	jobsAdmin.ExecuteNewCopyJobPartOrder = func(order common.CopyJobPartOrderRequest) common.CopyJobPartOrderResponse {
		return mockedRPC.intercept(order)
	}
	mockedRPC.init()

	// construct the raw input to simulate user input
	rawBlobURLWithSAS := scenarioHelper{}.getRawBlobURLWithSAS(a, containerName, blobName)
	raw := getDefaultRemoveRawInput(rawBlobURLWithSAS.String())
	raw.recursive = true
	raw.listOfVersionIDs = file.Name()
	runCopyAndVerify(a, raw, func(err error) {
		a.Error(err)
	})
}
