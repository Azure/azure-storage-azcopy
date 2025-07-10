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
	"fmt"
	"strings"
	"testing"

	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/container"
	"github.com/stretchr/testify/assert"
)

// TestBlobAccountCopyToFileShareS2S actually ends up testing the entire account->container scenario as that is not dependent on destination or source.
func TestBlobAccountCopyToFileShareS2S(t *testing.T) {
	a := assert.New(t)
	bsc := getBlobServiceClient()
	fsc := getFileServiceClient()

	// Ensure no containers with similar naming schemes exist
	cleanBlobAccount(a, bsc)

	containerSources := map[string]*container.Client{}
	expectedTransfers := make([]string, 0)

	for k := range make([]bool, 5) {
		name := generateName(fmt.Sprintf("blobacc-file%dcontainer", k), 63)

		// create the container
		containerSources[name] = bsc.NewContainerClient(name)
		_, err := containerSources[name].Create(ctx, nil)
		a.Nil(err)

		// Generate the remote scenario
		fileNames := scenarioHelper{}.generateCommonRemoteScenarioForBlob(a, containerSources[name], "")
		fileNames = scenarioHelper{}.addPrefix(fileNames, name+"/")
		expectedTransfers = append(expectedTransfers, fileNames...)

		// Prepare to delete all 5 containers
		//noinspection GoDeferInLoop
		defer deleteContainer(a, containerSources[name])
	}

	// generate destination share
	dstShareURL, dstShareName := createNewShare(a, fsc)
	defer deleteShare(a, dstShareURL)

	// initialize mocked RPC
	mockedRPC := interceptor{}
	Rpc = mockedRPC.intercept
	mockedRPC.init()

	// generate raw input
	blobServiceURLWithSAS := scenarioHelper{}.getRawBlobServiceURLWithSAS(a)
	blobServiceURLWithSAS.Path = "/blobacc-file*container*" // wildcard the container name
	dstShareURLWithSAS := scenarioHelper{}.getRawShareURLWithSAS(a, dstShareName)
	raw := getDefaultRawCopyInput(blobServiceURLWithSAS.String(), dstShareURLWithSAS.String())

	runCopyAndVerify(a, raw, func(err error) {
		a.Nil(err)

		a.Equal(len(expectedTransfers), len(mockedRPC.transfers))

		validateS2STransfersAreScheduled(a, "/", "/", expectedTransfers, mockedRPC)
	})
}

// TestBlobCopyToFileS2SImplicitDstShare uses a service-level URL on the destination to implicitly create the destination share.
// This test case is no longer valid because the logic to create the destination share
// has been removed from AzCopy. If the destination share does not exist, AzCopy will not create it.
// func TestBlobCopyToFileS2SImplicitDstShare(t *testing.T) {
// 	a := assert.New(t)
// 	bsc := getBlobServiceClient()
// 	fsc := getFileServiceClient()

// 	// create source container
// 	srcContainerClient, srcContainerName := createNewContainer(a, bsc)
// 	defer deleteContainer(a, srcContainerClient)

// 	// prepare a destination container URL to be deleted.
// 	dstShareClient := fsc.NewShareClient(srcContainerName)
// 	// _, err := dstShareClient.Create(ctx, nil)
// 	// a.Nil(err)
// 	defer deleteShare(a, dstShareClient)

// 	// create a scenario on the source container
// 	fileList := scenarioHelper{}.generateCommonRemoteScenarioForBlob(a, srcContainerClient, "blobFileImplicitDest")
// 	a.NotZero(len(fileList)) // Ensure that at least one blob is present

// 	// initialize the mocked RPC
// 	mockedRPC := interceptor{}
// 	Rpc = mockedRPC.intercept
// 	mockedRPC.init()

// 	// Create raw arguments
// 	srcContainerURLWithSAS := scenarioHelper{}.getRawContainerURLWithSAS(a, srcContainerName)
// 	dstServiceURLWithSAS := scenarioHelper{}.getRawFileServiceURLWithSAS(a)
// 	raw := getDefaultRawCopyInput(srcContainerURLWithSAS.String(), dstServiceURLWithSAS.String())
// 	// recursive is enabled by default

// 	// run the copy, check the container, and check the transfer success.
// 	runCopyAndVerify(a, raw, func(err error) {
// 		a.Nil(err) // Check there was no error

// 		_, err = dstShareClient.GetProperties(ctx, nil)
// 		a.Nil(err) // Ensure the destination share exists

// 		// Ensure the transfers were scheduled
// 		validateS2STransfersAreScheduled(a, "/", "/"+srcContainerName+"/", fileList, mockedRPC)
// 	})
// }

func TestBlobCopyToFileS2SWithSingleFile(t *testing.T) {
	a := assert.New(t)
	bsc := getBlobServiceClient()
	fsu := getFileServiceClient()

	srcContainerClient, srcContainerName := createNewContainer(a, bsc)
	dstShareClient, dstShareName := createNewShare(a, fsu)
	defer deleteContainer(a, srcContainerClient)
	defer deleteShare(a, dstShareClient)

	// copy to explicit destination
	for _, fileName := range []string{"singlefileisbest", "打麻将.txt", "%4509%4254$85140&"} {
		// set up the source container with a single file
		scenarioHelper{}.generateBlobsFromList(a, srcContainerClient, []string{fileName}, blockBlobDefaultData)

		// set up the interceptor
		mockedRPC := interceptor{}
		Rpc = mockedRPC.intercept
		mockedRPC.init()

		// construct the raw input for explicit destination
		srcBlobURLWithSAS := scenarioHelper{}.getRawBlobURLWithSAS(a, srcContainerName, fileName)
		dstFileURLWithSAS := scenarioHelper{}.getRawFileURLWithSAS(a, dstShareName, fileName)
		raw := getDefaultRawCopyInput(srcBlobURLWithSAS.String(), dstFileURLWithSAS.String())

		runCopyAndVerify(a, raw, func(err error) {
			a.Nil(err)

			validateS2STransfersAreScheduled(a, "", "", []string{""}, mockedRPC)
		})
	}

	// copy to an implicit destination
	for _, fileName := range []string{"singlefileisbest", "打麻将.txt", "%4509%4254$85140&"} {
		// Because we're using the same files, we don't need to re-generate them.

		// set up the interceptor
		mockedRPC := interceptor{}
		Rpc = mockedRPC.intercept
		mockedRPC.init()

		// construct the raw input for implicit destination
		srcBlobURLWithSAS := scenarioHelper{}.getRawBlobURLWithSAS(a, srcContainerName, fileName)
		dstShareURLWithSAS := scenarioHelper{}.getRawShareURLWithSAS(a, dstShareName)
		raw := getDefaultRawCopyInput(srcBlobURLWithSAS.String(), dstShareURLWithSAS.String())

		runCopyAndVerify(a, raw, func(err error) {
			a.Nil(err)

			// put the filename in the destination dir name
			// this is because validateS2STransfersAreScheduled dislikes when the relative paths differ
			// In this case, the relative path should absolutely differ. (explicit file path -> implicit)
			validateS2STransfersAreScheduled(a, "", "/"+strings.ReplaceAll(fileName, "%", "%25"), []string{""}, mockedRPC)
		})
	}
}

func TestContainerToShareCopyS2S(t *testing.T) {
	a := assert.New(t)
	bsc := getBlobServiceClient()
	fsc := getFileServiceClient()

	// Create source container and destination share, schedule their deletion
	srcContainerClient, srcContainerName := createNewContainer(a, bsc)
	dstShareClient, dstShareName := createNewShare(a, fsc)
	defer deleteContainer(a, srcContainerClient)
	defer deleteShare(a, dstShareClient)

	// set up the source container with numerous files
	fileList := scenarioHelper{}.generateCommonRemoteScenarioForBlob(a, srcContainerClient, "")
	a.NotZero(len(fileList))

	// set up the interceptor
	mockedRPC := interceptor{}
	Rpc = mockedRPC.intercept
	mockedRPC.init()

	// set up the raw input with recursive = true to copy
	srcContainerURLWithSAS := scenarioHelper{}.getRawContainerURLWithSAS(a, srcContainerName)
	dstShareURLWithSAS := scenarioHelper{}.getRawShareURLWithSAS(a, dstShareName)
	raw := getDefaultRawCopyInput(srcContainerURLWithSAS.String(), dstShareURLWithSAS.String())

	runCopyAndVerify(a, raw, func(err error) {
		a.Nil(err)

		// validate the transfer count is correct
		a.Equal(len(fileList), len(mockedRPC.transfers))

		validateS2STransfersAreScheduled(a, "/", "/", fileList, mockedRPC)
	})

	// turn off recursive and set recursive to false
	raw.recursive = false
	mockedRPC.reset()

	runCopyAndVerify(a, raw, func(err error) {
		a.NotNil(err)
		// make sure that the failure was due to the recursive flag
		a.Contains(err.Error(), "recursive")
	})
}

func TestBlobFileCopyS2SWithIncludeAndIncludeDirFlag(t *testing.T) {
	a := assert.New(t)
	bsc := getBlobServiceClient()
	fsc := getFileServiceClient()

	// generate source container and destination fileshare
	srcContainerClient, srcContainerName := createNewContainer(a, bsc)
	dstShareClient, dstShareName := createNewShare(a, fsc)
	defer deleteContainer(a, srcContainerClient)
	defer deleteShare(a, dstShareClient)

	// create file list to include against
	fileList := []string{
		"spooktober.pdf",
		"exactName",
		"area51map.pdf.png", // False flag the filter
		"subdir/spookyscaryskeletons.pdf",
		"subdir/exactName",
		"subdir/maybeIsJustAsynchronousNo.pdf.png",
		"subdir/surprisedpikachu.jpeg",
		"subdir2/forever box.jpeg",
	}
	loneIncludeList := []string{ // define correct list for use of --include
		fileList[0],
		fileList[1],
		fileList[3],
		fileList[4],
		fileList[6],
		fileList[7],
	}
	includePathAndIncludeList := []string{ // define correct list for use of --include-path and --include
		fileList[3],
		fileList[4],
		fileList[6],
	}

	// set up filters and generate blobs
	includeString := "*.pdf;*.jpeg;exactName"
	includePathString := "subdir/"
	scenarioHelper{}.generateBlobsFromList(a, srcContainerClient, fileList, blockBlobDefaultData)

	// set up the interceptor
	mockedRPC := interceptor{}
	Rpc = mockedRPC.intercept
	mockedRPC.init()

	// construct the raw input
	srcContainerURLWithSAS := scenarioHelper{}.getRawContainerURLWithSAS(a, srcContainerName)
	dstShareURLWithSAS := scenarioHelper{}.getRawShareURLWithSAS(a, dstShareName)
	raw := getDefaultRawCopyInput(srcContainerURLWithSAS.String(), dstShareURLWithSAS.String())
	raw.include = includeString
	raw.recursive = true

	runCopyAndVerify(a, raw, func(err error) {
		a.Nil(err)
		validateS2STransfersAreScheduled(a, "/", "/", loneIncludeList, mockedRPC)
	})

	mockedRPC.reset()
	raw.includePath = includePathString
	runCopyAndVerify(a, raw, func(err error) {
		a.Nil(err)
		validateS2STransfersAreScheduled(a, "/", "/", includePathAndIncludeList, mockedRPC)
	})
}

func TestBlobToFileCopyS2SWithExcludeAndExcludeDirFlag(t *testing.T) {
	a := assert.New(t)
	bsc := getBlobServiceClient()
	fsc := getFileServiceClient()

	// generate source container and destination fileshare
	srcContainerClient, srcContainerName := createNewContainer(a, bsc)
	dstShareClient, dstShareName := createNewShare(a, fsc)
	defer deleteContainer(a, srcContainerClient)
	defer deleteShare(a, dstShareClient)

	// create file list to include against
	fileList := []string{
		"spooktober.pdf",
		"exactName",
		"area51map.pdf.png", // False flag the filter
		"subdir/spookyscaryskeletons.pdf",
		"subdir/exactName",
		"subdir/maybeIsJustAsynchronousNo.pdf.png",
		"subdir/surprisedpikachu.jpeg",
		"subdir2/forever box.jpeg",
		"subdir2/includedfilebyexclude",
	}
	loneExcludeList := []string{ // define correct list for use of --exclude
		fileList[2],
		fileList[5],
		fileList[8],
	}
	excludePathAndExcludeList := []string{ // define correct list for use of --exclude-path and --exclude
		fileList[2],
		fileList[8],
	}

	// set up filters and generate blobs
	excludeString := "*.pdf;*.jpeg;exactName"
	excludePathString := "subdir/"
	scenarioHelper{}.generateBlobsFromList(a, srcContainerClient, fileList, blockBlobDefaultData)

	// set up the interceptor
	mockedRPC := interceptor{}
	Rpc = mockedRPC.intercept
	mockedRPC.init()

	// construct the raw input
	srcContainerURLWithSAS := scenarioHelper{}.getRawContainerURLWithSAS(a, srcContainerName)
	dstShareURLWithSAS := scenarioHelper{}.getRawShareURLWithSAS(a, dstShareName)
	raw := getDefaultRawCopyInput(srcContainerURLWithSAS.String(), dstShareURLWithSAS.String())
	raw.exclude = excludeString
	raw.recursive = true

	runCopyAndVerify(a, raw, func(err error) {
		a.Nil(err)
		validateS2STransfersAreScheduled(a, "/", "/", loneExcludeList, mockedRPC)
	})

	mockedRPC.reset()
	raw.excludePath = excludePathString
	runCopyAndVerify(a, raw, func(err error) {
		a.Nil(err)
		validateS2STransfersAreScheduled(a, "/", "/", excludePathAndExcludeList, mockedRPC)
	})
}

func TestBlobToFileCopyS2SIncludeExcludeMix(t *testing.T) {
	a := assert.New(t)
	bsc := getBlobServiceClient()
	fsc := getFileServiceClient()

	// generate source container and destination fileshare
	srcContainerClient, srcContainerName := createNewContainer(a, bsc)
	dstShareClient, dstShareName := createNewShare(a, fsc)
	defer deleteContainer(a, srcContainerClient)
	defer deleteShare(a, dstShareClient)

	// create file list to include against
	fileList := []string{
		"includeme.pdf",
		"includeme.jpeg",
		"ohnodontincludeme.pdf",
		"whywouldyouincludeme.jpeg",
		"exactName",

		"subdir/includeme.pdf",
		"subdir/includeme.jpeg",
		"subdir/ohnodontincludeme.pdf",
		"subdir/whywouldyouincludeme.jpeg",
		"subdir/exactName",
	}
	toInclude := []string{
		fileList[0],
		fileList[1],
		fileList[5],
		fileList[6],
	}

	// set up filters and generate blobs
	includeString := "*.pdf;*.jpeg;exactName"
	excludeString := "ohno*;why*;exactName"
	scenarioHelper{}.generateBlobsFromList(a, srcContainerClient, fileList, blockBlobDefaultData)

	// set up the interceptor
	mockedRPC := interceptor{}
	Rpc = mockedRPC.intercept
	mockedRPC.init()

	// construct the raw input
	srcContainerURLWithSAS := scenarioHelper{}.getRawContainerURLWithSAS(a, srcContainerName)
	dstShareURLWithSAS := scenarioHelper{}.getRawShareURLWithSAS(a, dstShareName)
	raw := getDefaultRawCopyInput(srcContainerURLWithSAS.String(), dstShareURLWithSAS.String())
	raw.include = includeString
	raw.exclude = excludeString
	raw.recursive = true

	runCopyAndVerify(a, raw, func(err error) {
		a.Nil(err)
		validateS2STransfersAreScheduled(a, "/", "/", toInclude, mockedRPC)
	})
}

func TestBlobToFileCopyS2SWithDirectory(t *testing.T) {
	a := assert.New(t)
	bsc := getBlobServiceClient()
	fsc := getFileServiceClient()

	// create container and share
	srcContainerClient, srcContainerName := createNewContainer(a, bsc)
	dstShareClient, dstShareName := createNewShare(a, fsc)
	defer deleteContainer(a, srcContainerClient)
	defer deleteShare(a, dstShareClient)

	// create source scenario
	dirName := "copyme"
	fileList := scenarioHelper{}.generateCommonRemoteScenarioForBlob(a, srcContainerClient, dirName+"/")
	a.NotZero(len(fileList))

	// initialize mocked RPC
	mockedRPC := interceptor{}
	Rpc = mockedRPC.intercept
	mockedRPC.init()

	// generate raw copy command
	srcContainerURLWithSAS := scenarioHelper{}.getRawContainerURLWithSAS(a, srcContainerName)
	dstShareURLWithSAS := scenarioHelper{}.getRawShareURLWithSAS(a, dstShareName)
	srcContainerURLWithSAS.Path += "/copyme/"
	raw := getDefaultRawCopyInput(srcContainerURLWithSAS.String(), dstShareURLWithSAS.String())
	raw.recursive = true

	// test folder copies
	expectedList := scenarioHelper{}.shaveOffPrefix(fileList, dirName+"/")
	runCopyAndVerify(a, raw, func(err error) {
		a.Nil(err)
		validateS2STransfersAreScheduled(a, "/", "/"+dirName+"/", expectedList, mockedRPC)
	})
}
