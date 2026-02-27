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
	"github.com/Azure/azure-storage-azcopy/v10/common"
	"github.com/Azure/azure-storage-azcopy/v10/jobsAdmin"
	"github.com/stretchr/testify/assert"
	"strings"
	"testing"
)

// regular file->file copy
func TestFileCopyS2SWithSingleFile(t *testing.T) {
	a := assert.New(t)
	fsc := getFileServiceClient()
	srcShareClient, srcShareName := createNewShare(a, fsc)
	dstShareClient, dstShareName := createNewShare(a, fsc)
	defer deleteShare(a, srcShareClient)
	defer deleteShare(a, dstShareClient)

	for _, fileName := range []string{"singlefileisbest", "打麻将.txt", "%4509%4254$85140&"} {
		// set up the source share with a single file
		fileList := []string{fileName}
		scenarioHelper{}.generateShareFilesFromList(a, srcShareClient, fsc, fileList)

		// set up interceptor
		mockedRPC := interceptor{}
		jobsAdmin.ExecuteNewCopyJobPartOrder = func(order common.CopyJobPartOrderRequest) common.CopyJobPartOrderResponse {
			return mockedRPC.intercept(order)
		}
		mockedRPC.init()

		// construct the raw input to simulate user input
		srcFileURLWithSAS := scenarioHelper{}.getRawFileURLWithSAS(a, srcShareName, fileList[0])
		dstFileURLWithSAS := scenarioHelper{}.getRawFileURLWithSAS(a, dstShareName, fileList[0])
		raw := getDefaultCopyRawInput(srcFileURLWithSAS.String(), dstFileURLWithSAS.String())

		runCopyAndVerify(a, raw, func(err error) {
			a.Nil(err)
			validateS2STransfersAreScheduled(a, "", "", []string{""}, mockedRPC)
		})
	}

	for _, fileName := range []string{"singlefileisbest", "打麻将.txt", "%4509%4254$85140&"} {
		// No need to generate files since we already have them

		// set up interceptor
		mockedRPC := interceptor{}
		jobsAdmin.ExecuteNewCopyJobPartOrder = func(order common.CopyJobPartOrderRequest) common.CopyJobPartOrderResponse {
			return mockedRPC.intercept(order)
		}
		mockedRPC.init()

		// construct the raw input to simulate user input
		srcFileURLWithSAS := scenarioHelper{}.getRawFileURLWithSAS(a, srcShareName, fileName)
		dstShareURLWithSAS := scenarioHelper{}.getRawShareURLWithSAS(a, dstShareName)
		raw := getDefaultCopyRawInput(srcFileURLWithSAS.String(), dstShareURLWithSAS.String())

		runCopyAndVerify(a, raw, func(err error) {
			a.Nil(err)

			// put the filename in the destination dir name
			// this is because validateS2STransfersAreScheduled dislikes when the relative paths differ
			// In this case, the relative path should absolutely differ. (explicit file path -> implicit)
			validateS2STransfersAreScheduled(a, "", "/"+strings.ReplaceAll(fileName, "%", "%25"), []string{""}, mockedRPC)
		})
	}
}

// regular share->share copy
func TestFileCopyS2SWithShares(t *testing.T) {
	a := assert.New(t)
	fsc := getFileServiceClient()
	srcShareClient, srcShareName := createNewShare(a, fsc)
	dstShareClient, dstShareName := createNewShare(a, fsc)
	defer deleteShare(a, srcShareClient)
	defer deleteShare(a, dstShareClient)

	// set up the source share with numerous files
	fileList := scenarioHelper{}.generateCommonRemoteScenarioForAzureFile(a, srcShareClient, fsc, "")
	a.NotZero(len(fileList))

	// set up interceptor
	mockedRPC := interceptor{}
	jobsAdmin.ExecuteNewCopyJobPartOrder = func(order common.CopyJobPartOrderRequest) common.CopyJobPartOrderResponse {
		return mockedRPC.intercept(order)
	}
	mockedRPC.init()

	// construct the raw input to simulate user input
	srcShareURLWithSAS := scenarioHelper{}.getRawShareURLWithSAS(a, srcShareName)
	dstShareURLWithSAS := scenarioHelper{}.getRawShareURLWithSAS(a, dstShareName)
	raw := getDefaultCopyRawInput(srcShareURLWithSAS.String(), dstShareURLWithSAS.String())
	raw.recursive = true

	// all files at source should be copied to destination
	expectedList := scenarioHelper{}.addFoldersToList(fileList, false) // since this is files-to-files and so folder aware
	runCopyAndVerify(a, raw, func(err error) {
		a.Nil(err)

		// validate that the right number of transfers were scheduled
		a.Equal(len(expectedList), len(mockedRPC.transfers))

		// validate that the right transfers were sent
		validateS2STransfersAreScheduled(a, "/", "/", expectedList, mockedRPC)
	})

	// turn off recursive, we should be getting an error
	raw.recursive = false
	mockedRPC.reset()
	runCopyAndVerify(a, raw, func(err error) {
		a.NotNil(err)
		// make sure the failure was due to the recursive flag
		a.Contains(err.Error(), "recursive")
	})
}

// include flag limits the scope of source/destination comparison
func TestFileCopyS2SWithIncludeFlag(t *testing.T) {
	a := assert.New(t)
	fsc := getFileServiceClient()
	srcShareClient, srcShareName := createNewShare(a, fsc)
	dstShareClient, dstShareName := createNewShare(a, fsc)
	defer deleteShare(a, srcShareClient)
	defer deleteShare(a, dstShareClient)

	// set up the source share with numerous files
	fileList := scenarioHelper{}.generateCommonRemoteScenarioForAzureFile(a, srcShareClient, fsc, "")
	a.NotZero(len(fileList))

	// add special files that we wish to include
	filesToInclude := []string{"important.pdf", "includeSub/amazing.jpeg", "exactName"}
	scenarioHelper{}.generateShareFilesFromList(a, srcShareClient, fsc, filesToInclude)
	includeString := "*.pdf;*.jpeg;exactName"

	// set up interceptor
	mockedRPC := interceptor{}
	jobsAdmin.ExecuteNewCopyJobPartOrder = func(order common.CopyJobPartOrderRequest) common.CopyJobPartOrderResponse {
		return mockedRPC.intercept(order)
	}
	mockedRPC.init()

	// construct the raw input to simulate user input
	srcShareURLWithSAS := scenarioHelper{}.getRawShareURLWithSAS(a, srcShareName)
	dstShareURLWithSAS := scenarioHelper{}.getRawShareURLWithSAS(a, dstShareName)
	raw := getDefaultCopyRawInput(srcShareURLWithSAS.String(), dstShareURLWithSAS.String())
	raw.include = includeString
	raw.recursive = true

	// verify that only the files specified by the include flag are copied
	runCopyAndVerify(a, raw, func(err error) {
		a.Nil(err)
		validateS2STransfersAreScheduled(a, "/", "/", filesToInclude, mockedRPC)
	})
}

// exclude flag limits the scope of source/destination comparison
func TestFileCopyS2SWithExcludeFlag(t *testing.T) {
	a := assert.New(t)
	fsc := getFileServiceClient()
	srcShareClient, srcShareName := createNewShare(a, fsc)
	dstShareClient, dstShareName := createNewShare(a, fsc)
	defer deleteShare(a, srcShareClient)
	defer deleteShare(a, dstShareClient)

	// set up the source share with numerous files
	fileList := scenarioHelper{}.generateCommonRemoteScenarioForAzureFile(a, srcShareClient, fsc, "")
	a.NotZero(len(fileList))

	// add special files that we wish to exclude
	filesToExclude := []string{"notGood.pdf", "excludeSub/lame.jpeg", "exactName"}
	scenarioHelper{}.generateShareFilesFromList(a, srcShareClient, fsc, filesToExclude)
	excludeString := "*.pdf;*.jpeg;exactName"

	// set up interceptor
	mockedRPC := interceptor{}
	jobsAdmin.ExecuteNewCopyJobPartOrder = func(order common.CopyJobPartOrderRequest) common.CopyJobPartOrderResponse {
		return mockedRPC.intercept(order)
	}
	mockedRPC.init()

	// construct the raw input to simulate user input
	srcShareURLWithSAS := scenarioHelper{}.getRawShareURLWithSAS(a, srcShareName)
	dstShareURLWithSAS := scenarioHelper{}.getRawShareURLWithSAS(a, dstShareName)
	raw := getDefaultCopyRawInput(srcShareURLWithSAS.String(), dstShareURLWithSAS.String())
	raw.exclude = excludeString
	raw.recursive = true

	// make sure the list doesn't include the files specified by the exclude flag
	runCopyAndVerify(a, raw, func(err error) {
		a.Nil(err)
		validateS2STransfersAreScheduled(a, "/", "/", fileList, mockedRPC)
	})
}

// include and exclude flag can work together to limit the scope of source/destination comparison
func TestFileCopyS2SWithIncludeAndExcludeFlag(t *testing.T) {
	a := assert.New(t)
	fsc := getFileServiceClient()
	srcShareClient, srcShareName := createNewShare(a, fsc)
	dstShareClient, dstShareName := createNewShare(a, fsc)
	defer deleteShare(a, srcShareClient)
	defer deleteShare(a, dstShareClient)

	// set up the source share with numerous files
	fileList := scenarioHelper{}.generateCommonRemoteScenarioForAzureFile(a, srcShareClient, fsc, "")
	a.NotZero(len(fileList))

	// add special files that we wish to include
	filesToInclude := []string{"important.pdf", "includeSub/amazing.jpeg"}
	scenarioHelper{}.generateShareFilesFromList(a, srcShareClient, fsc, filesToInclude)
	includeString := "*.pdf;*.jpeg;exactName"

	// add special files that we wish to exclude
	// note that the excluded files also match the include string
	filesToExclude := []string{"sorry.pdf", "exclude/notGood.jpeg", "exactName", "sub/exactName"}
	scenarioHelper{}.generateShareFilesFromList(a, srcShareClient, fsc, filesToExclude)
	excludeString := "so*;not*;exactName"

	// set up interceptor
	mockedRPC := interceptor{}
	jobsAdmin.ExecuteNewCopyJobPartOrder = func(order common.CopyJobPartOrderRequest) common.CopyJobPartOrderResponse {
		return mockedRPC.intercept(order)
	}
	mockedRPC.init()

	// construct the raw input to simulate user input
	srcShareURLWithSAS := scenarioHelper{}.getRawShareURLWithSAS(a, srcShareName)
	dstShareURLWithSAS := scenarioHelper{}.getRawShareURLWithSAS(a, dstShareName)
	raw := getDefaultCopyRawInput(srcShareURLWithSAS.String(), dstShareURLWithSAS.String())
	raw.include = includeString
	raw.exclude = excludeString
	raw.recursive = true

	// verify that only the files specified by the include flag are copied
	runCopyAndVerify(a, raw, func(err error) {
		a.Nil(err)
		validateS2STransfersAreScheduled(a, "/", "/", filesToInclude, mockedRPC)
	})
}

// regular dir -> dir copy
func TestFileCopyS2SWithDirectory(t *testing.T) {
	a := assert.New(t)
	fsc := getFileServiceClient()
	srcShareClient, srcShareName := createNewShare(a, fsc)
	dstShareClient, dstShareName := createNewShare(a, fsc)
	defer deleteShare(a, srcShareClient)
	defer deleteShare(a, dstShareClient)

	// set up the source share with numerous files
	dirName := "dir"
	fileList := scenarioHelper{}.generateCommonRemoteScenarioForAzureFile(a, srcShareClient, fsc, dirName+"/")
	a.NotZero(len(fileList))

	// set up the destination with the exact same files
	scenarioHelper{}.generateShareFilesFromList(a, dstShareClient, fsc, fileList)

	// set up interceptor
	mockedRPC := interceptor{}
	jobsAdmin.ExecuteNewCopyJobPartOrder = func(order common.CopyJobPartOrderRequest) common.CopyJobPartOrderResponse {
		return mockedRPC.intercept(order)
	}
	mockedRPC.init()

	// construct the raw input to simulate user input
	srcShareURLWithSAS := scenarioHelper{}.getRawShareURLWithSAS(a, srcShareName)
	dstShareURLWithSAS := scenarioHelper{}.getRawShareURLWithSAS(a, dstShareName)
	srcShareURLWithSAS.Path += "/" + dirName
	raw := getDefaultCopyRawInput(srcShareURLWithSAS.String(), dstShareURLWithSAS.String())
	raw.recursive = true

	expectedList := scenarioHelper{}.shaveOffPrefix(fileList, dirName+"/")
	expectedList = scenarioHelper{}.addFoldersToList(expectedList, true) // since this is files-to-files and so folder aware
	runCopyAndVerify(a, raw, func(err error) {
		a.Nil(err)
		validateS2STransfersAreScheduled(a, "/", "/"+dirName+"/", expectedList, mockedRPC)
	})
}
