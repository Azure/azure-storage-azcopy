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
	"github.com/Azure/azure-pipeline-go/pipeline"
	"github.com/stretchr/testify/assert"
	"os"
	"path"
	"path/filepath"
	"sort"
	"strings"
	"testing"

	"github.com/Azure/azure-storage-azcopy/v10/common"
	"github.com/Azure/azure-storage-blob-go/azblob"
)

func TestInferredStripTopDirDownload(t *testing.T) {
	a := assert.New(t)
	bsu := getBSU()
	cURL, cName := createNewContainer(a, bsu)

	blobNames := []string{
		"*", // File name that we want to retain compatibility with
		"testFile",
		"DoYouPronounceItDataOrData",
		"sub*dir/Help I cannot so much into computer",
	}

	// ----- TEST # 1: Test inferred as false by using escaped * -----

	// set up container name
	scenarioHelper{}.generateBlobsFromList(a, cURL, blobNames, blockBlobDefaultData)

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
	Rpc = mockedRPC.intercept
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
	bsu := getBSU()
	rawBSU := scenarioHelper{}.getRawBlobServiceURLWithSAS(a)
	p, err := InitPipeline(ctx, common.ELocation.Blob(), common.CredentialInfo{CredentialType: common.ECredentialType.Anonymous()}, pipeline.LogNone, common.ETrailingDotOption.Enable(), common.ELocation.Blob())
	a.Nil(err)

	// Just in case there are no existing containers...
	curl, _ := createNewContainer(a, bsu)
	scenarioHelper{}.generateCommonRemoteScenarioForBlob(a, curl, "")

	// Traverse the account ahead of time and determine the relative paths for testing.
	relPaths := make([]string, 0) // Use a map for easy lookup
	blobTraverser := newBlobAccountTraverser(&rawBSU, p, ctx, false, func(common.EntityType) {}, false, common.CpkOptions{}, common.EPreservePermissionsOption.None(), false)
	processor := func(object StoredObject) error {
		// Append the container name to the relative path
		relPath := "/" + object.ContainerName + "/" + object.relativePath
		relPaths = append(relPaths, relPath)
		return nil
	}
	err = blobTraverser.Traverse(noPreProccessor, processor, []ObjectFilter{})
	a.Nil(err)

	// set up a destination
	dstDirName := scenarioHelper{}.generateLocalDirectory(a)
	defer os.RemoveAll(dstDirName)

	// set up interceptor
	mockedRPC := interceptor{}
	Rpc = mockedRPC.intercept
	mockedRPC.init()

	raw := getDefaultCopyRawInput(rawBSU.String(), dstDirName)
	raw.recursive = true

	runCopyAndVerify(a, raw, func(err error) {
		a.Nil(err)

		validateDownloadTransfersAreScheduled(a, "", "", relPaths, mockedRPC)
	})
}

// Test downloading the entire account.
func TestDownloadAccountWildcard(t *testing.T) {
	a := assert.New(t)
	bsu := getBSU()
	rawBSU := scenarioHelper{}.getRawBlobServiceURLWithSAS(a)
	p, err := InitPipeline(ctx, common.ELocation.Blob(), common.CredentialInfo{CredentialType: common.ECredentialType.Anonymous()}, pipeline.LogNone, common.ETrailingDotOption.Enable(), common.ELocation.Blob())
	a.Nil(err)

	// Create a unique container to be targeted.
	cname := generateName("blah-unique-blah", 63)
	curl := bsu.NewContainerURL(cname)
	_, err = curl.Create(ctx, azblob.Metadata{}, azblob.PublicAccessNone)
	a.Nil(err)
	scenarioHelper{}.generateCommonRemoteScenarioForBlob(a, curl, "")

	// update the raw BSU to match the unique container name
	rawBSU.Path = "/blah-unique-blah*"

	// Traverse the account ahead of time and determine the relative paths for testing.
	relPaths := make([]string, 0) // Use a map for easy lookup
	blobTraverser := newBlobAccountTraverser(&rawBSU, p, ctx, false, func(common.EntityType) {}, false, common.CpkOptions{}, common.EPreservePermissionsOption.None(), false)
	processor := func(object StoredObject) error {
		// Append the container name to the relative path
		relPath := "/" + object.ContainerName + "/" + object.relativePath
		relPaths = append(relPaths, relPath)
		return nil
	}
	err = blobTraverser.Traverse(noPreProccessor, processor, []ObjectFilter{})
	a.Nil(err)

	// set up a destination
	dstDirName := scenarioHelper{}.generateLocalDirectory(a)
	defer os.RemoveAll(dstDirName)

	// set up interceptor
	mockedRPC := interceptor{}
	Rpc = mockedRPC.intercept
	mockedRPC.init()

	raw := getDefaultCopyRawInput(rawBSU.String(), dstDirName)
	raw.recursive = true

	runCopyAndVerify(a, raw, func(err error) {
		a.Nil(err)

		validateDownloadTransfersAreScheduled(a, "", "", relPaths, mockedRPC)
	})
}

// regular blob->local file download
func TestDownloadSingleBlobToFile(t *testing.T) {
	a := assert.New(t)
	bsu := getBSU()
	containerURL, containerName := createNewContainer(a, bsu)
	defer deleteContainer(a, containerURL)

	for _, blobName := range []string{"singleblobisbest", "打麻将.txt", "%4509%4254$85140&"} {
		// set up the container with a single blob
		blobList := []string{blobName}
		scenarioHelper{}.generateBlobsFromList(a, containerURL, blobList, blockBlobDefaultData)
		a.NotNil(containerURL)

		// set up the destination as a single file
		dstDirName := scenarioHelper{}.generateLocalDirectory(a)
		defer os.RemoveAll(dstDirName)
		dstFileName := "whatever"
		scenarioHelper{}.generateLocalFilesFromList(a, dstDirName, blobList)

		// set up interceptor
		mockedRPC := interceptor{}
		Rpc = mockedRPC.intercept
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
	bsu := getBSU()

	// set up the container with numerous blobs
	containerURL, containerName := createNewContainer(a, bsu)
	blobList := scenarioHelper{}.generateCommonRemoteScenarioForBlob(a, containerURL, "")
	defer deleteContainer(a, containerURL)
	a.NotNil(containerURL)
	a.NotEqual(0, len(blobList))

	// set up the destination with an empty folder
	dstDirName := scenarioHelper{}.generateLocalDirectory(a)
	defer os.RemoveAll(dstDirName)

	// set up interceptor
	mockedRPC := interceptor{}
	Rpc = mockedRPC.intercept
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
	bsu := getBSU()
	vdirName := "vdir1"

	// set up the container with numerous blobs
	containerURL, containerName := createNewContainer(a, bsu)
	blobList := scenarioHelper{}.generateCommonRemoteScenarioForBlob(a, containerURL, vdirName+common.AZCOPY_PATH_SEPARATOR_STRING)
	defer deleteContainer(a, containerURL)
	a.NotNil(containerURL)
	a.NotEqual(0, len(blobList))

	// set up the destination with an empty folder
	dstDirName := scenarioHelper{}.generateLocalDirectory(a)
	defer os.RemoveAll(dstDirName)

	// set up interceptor
	mockedRPC := interceptor{}
	Rpc = mockedRPC.intercept
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
//   update test after re-writing copy enumerators
func TestDownloadBlobContainerWithPattern(t *testing.T) {
	a := assert.New(t)
	bsu := getBSU()

	// set up the container with numerous blobs
	containerURL, containerName := createNewContainer(a, bsu)
	blobsToIgnore := scenarioHelper{}.generateCommonRemoteScenarioForBlob(a, containerURL, "")
	defer deleteContainer(a, containerURL)
	a.NotNil(containerURL)
	a.NotEqual(0, len(blobsToIgnore))

	// add special blobs that we wish to include
	blobsToInclude := []string{"important.pdf", "includeSub/amazing.pdf", "includeSub/wow/amazing.pdf"}
	scenarioHelper{}.generateBlobsFromList(a, containerURL, blobsToInclude, blockBlobDefaultData)

	// set up the destination with an empty folder
	dstDirName := scenarioHelper{}.generateLocalDirectory(a)
	defer os.RemoveAll(dstDirName)

	// set up interceptor
	mockedRPC := interceptor{}
	Rpc = mockedRPC.intercept
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
	bsu := getBSU()

	// set up the container with  blobs
	containerURL, containerName := createNewContainer(a, bsu)
	blobsToIgnore := scenarioHelper{}.generateCommonRemoteScenarioForBlob(a, containerURL, "")
	defer deleteContainer(a, containerURL)
	a.NotNil(containerURL)
	a.NotEqual(0, len(blobsToIgnore))

	// add blobs that we wish to include
	blobsToInclude := []string{"tessssssssssssst.txt", "subOne/tetingessssss.jpeg", "subOne/tessssst/hi.pdf"}
	scenarioHelper{}.generateBlobsFromList(a, containerURL, blobsToInclude, blockBlobDefaultData)

	// set up the destination with an empty folder
	dstDirName := scenarioHelper{}.generateLocalDirectory(a)
	defer os.RemoveAll(dstDirName)

	// set up interceptor
	mockedRPC := interceptor{}
	Rpc = mockedRPC.intercept
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
	bsu := getBSU()

	// set up the container with  blobs
	containerURL, containerName := createNewContainer(a, bsu)
	blobsToIgnore := scenarioHelper{}.generateCommonRemoteScenarioForBlob(a, containerURL, "")
	defer deleteContainer(a, containerURL)
	a.NotNil(containerURL)
	a.NotEqual(0, len(blobsToIgnore))

	// add blobs that we wish to include
	blobsToInclude := []string{"tessssssssssssst.txt", "zxcfile.txt", "subOne/tetingessssss.jpeg", "subOne/subTwo/tessssst.pdf"}
	scenarioHelper{}.generateBlobsFromList(a, containerURL, blobsToInclude, blockBlobDefaultData)

	// set up the destination with an empty folder
	dstDirName := scenarioHelper{}.generateLocalDirectory(a)
	defer os.RemoveAll(dstDirName)

	// set up interceptor
	mockedRPC := interceptor{}
	Rpc = mockedRPC.intercept
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
	bsu := getBSU()

	// set up the container with  blobs
	containerURL, containerName := createNewContainer(a, bsu)
	// test empty regex flag so all blobs will be included since there is no filter
	blobsToInclude := scenarioHelper{}.generateCommonRemoteScenarioForBlob(a, containerURL, "")
	defer deleteContainer(a, containerURL)
	a.NotNil(containerURL)
	a.NotEqual(0, len(blobsToInclude))

	// set up the destination with an empty folder
	dstDirName := scenarioHelper{}.generateLocalDirectory(a)
	defer os.RemoveAll(dstDirName)

	// set up interceptor
	mockedRPC := interceptor{}
	Rpc = mockedRPC.intercept
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
	bsu := getBSU()

	// set up the container with  blobs
	containerURL, containerName := createNewContainer(a, bsu)
	blobsToInclude := scenarioHelper{}.generateCommonRemoteScenarioForBlob(a, containerURL, "")
	defer deleteContainer(a, containerURL)
	a.NotNil(containerURL)
	a.NotEqual(0, len(blobsToInclude))

	// add blobs that we wish to exclude
	blobsToIgnore := []string{"tessssssssssssst.txt", "subOne/tetingessssss.jpeg", "subOne/subTwo/tessssst.pdf"}
	scenarioHelper{}.generateBlobsFromList(a, containerURL, blobsToIgnore, blockBlobDefaultData)

	// set up the destination with an empty folder
	dstDirName := scenarioHelper{}.generateLocalDirectory(a)
	defer os.RemoveAll(dstDirName)

	// set up interceptor
	mockedRPC := interceptor{}
	Rpc = mockedRPC.intercept
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
	bsu := getBSU()

	// set up the container with  blobs
	containerURL, containerName := createNewContainer(a, bsu)
	blobsToInclude := scenarioHelper{}.generateCommonRemoteScenarioForBlob(a, containerURL, "")
	defer deleteContainer(a, containerURL)
	a.NotNil(containerURL)
	a.NotEqual(0, len(blobsToInclude))

	// add blobs that we wish to exclude
	blobsToIgnore := []string{"tessssssssssssst.txt", "subOne/dogs.jpeg", "subOne/subTwo/tessssst.pdf"}
	scenarioHelper{}.generateBlobsFromList(a, containerURL, blobsToIgnore, blockBlobDefaultData)

	// set up the destination with an empty folder
	dstDirName := scenarioHelper{}.generateLocalDirectory(a)
	defer os.RemoveAll(dstDirName)

	// set up interceptor
	mockedRPC := interceptor{}
	Rpc = mockedRPC.intercept
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
	bsu := getBSU()

	// set up the local source
	blobsToInclude := []string{"AzURE2021.jpeg", "sub1/dir2/HELLO-4.txt", "sub1/test/testing.txt"}
	srcDirName := scenarioHelper{}.generateLocalDirectory(a)
	defer os.RemoveAll(srcDirName)
	scenarioHelper{}.generateLocalFilesFromList(a, srcDirName, blobsToInclude)
	a.NotNil(srcDirName)

	// set up the destination container
	dstContainerURL, dstContainerName := createNewContainer(a, bsu)
	defer deleteContainer(a, dstContainerURL)
	a.NotNil(dstContainerURL)

	// set up interceptor
	mockedRPC := interceptor{}
	Rpc = mockedRPC.intercept
	mockedLcm := mockedLifecycleManager{dryrunLog: make(chan string, 50)}
	mockedLcm.SetOutputFormat(common.EOutputFormat.Text()) // text format
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
			a.True(strings.Contains(msg[i], dstContainerURL.String()))
		}

		a.True(testDryrunStatements(blobsToInclude, msg))
	})
}

func TestDryrunCopyBlobToBlob(t *testing.T) {
	a := assert.New(t)
	bsu := getBSU()

	// set up src container
	srcContainerURL, srcContainerName := createNewContainer(a, bsu)
	defer deleteContainer(a, srcContainerURL)
	blobsToInclude := []string{"AzURE2021.jpeg", "sub1/dir2/HELLO-4.txt", "sub1/test/testing.txt"}
	scenarioHelper{}.generateBlobsFromList(a, srcContainerURL, blobsToInclude, blockBlobDefaultData)
	a.NotNil(srcContainerURL)

	// set up the destination
	dstContainerURL, dstContainerName := createNewContainer(a, bsu)
	defer deleteContainer(a, dstContainerURL)
	a.NotNil(dstContainerURL)

	// set up interceptor
	mockedRPC := interceptor{}
	Rpc = mockedRPC.intercept
	mockedLcm := mockedLifecycleManager{dryrunLog: make(chan string, 50)}
	mockedLcm.SetOutputFormat(common.EOutputFormat.Text()) // text format
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
			a.True(strings.Contains(msg[i], srcContainerURL.String()))
			a.True(strings.Contains(msg[i], dstContainerURL.String()))
		}

		a.True(testDryrunStatements(blobsToInclude, msg))
	})
}

func TestDryrunCopyBlobToBlobJson(t *testing.T) {
	a := assert.New(t)
	bsu := getBSU()
	// set up src container
	srcContainerURL, srcContainerName := createNewContainer(a, bsu)
	defer deleteContainer(a, srcContainerURL)
	blobsToInclude := []string{"AzURE2021.jpeg"}
	scenarioHelper{}.generateBlobsFromList(a, srcContainerURL, blobsToInclude, blockBlobDefaultData)
	a.NotNil(srcContainerURL)

	// set up the destination
	dstContainerURL, dstContainerName := createNewContainer(a, bsu)
	defer deleteContainer(a, dstContainerURL)
	a.NotNil(dstContainerURL)

	// set up interceptor
	mockedRPC := interceptor{}
	Rpc = mockedRPC.intercept
	mockedLcm := mockedLifecycleManager{dryrunLog: make(chan string, 50)}
	mockedLcm.SetOutputFormat(common.EOutputFormat.Json()) // json format
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
		copyMessage := common.CopyTransfer{}
		errMarshal := json.Unmarshal([]byte(msg), &copyMessage)
		a.Nil(errMarshal)
		// comparing some values of copyMessage
		a.Zero(strings.Compare(strings.Trim(copyMessage.Source, "/"), blobsToInclude[0]))
		a.Zero(strings.Compare(strings.Trim(copyMessage.Destination, "/"), blobsToInclude[0]))
		a.Zero(strings.Compare(copyMessage.EntityType.String(), common.EEntityType.File().String()))
		a.Zero(strings.Compare(string(copyMessage.BlobType), "BlockBlob"))
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
	Rpc = mockedRPC.intercept
	mockedLcm := mockedLifecycleManager{dryrunLog: make(chan string, 50)}
	mockedLcm.SetOutputFormat(common.EOutputFormat.Text()) // text format
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
	Rpc = mockedRPC.intercept
	mockedLcm := mockedLifecycleManager{dryrunLog: make(chan string, 50)}
	mockedLcm.SetOutputFormat(common.EOutputFormat.Text()) // text format
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