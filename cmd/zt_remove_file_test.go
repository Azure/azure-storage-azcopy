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
	"github.com/Azure/azure-storage-azcopy/v10/common"
	"github.com/Azure/azure-storage-azcopy/v10/jobsAdmin"
	"github.com/stretchr/testify/assert"
	"net/url"
	"strings"
	"testing"
)

func TestRemoveSingleFile(t *testing.T) {
	a := assert.New(t)
	fsc := getFileServiceClient()
	shareClient, shareName := createNewShare(a, fsc)
	defer deleteShare(a, shareClient)

	for _, fileName := range []string{"top/mid/low/singlefileisbest", "打麻将.txt", "%4509%4254$85140&"} {
		// set up the share with a single file
		fileList := []string{fileName}
		scenarioHelper{}.generateShareFilesFromList(a, shareClient, fsc, fileList)
		a.NotNil(shareClient)

		// set up interceptor
		mockedRPC := interceptor{}
		jobsAdmin.ExecuteNewCopyJobPartOrder = func(order common.CopyJobPartOrderRequest) common.CopyJobPartOrderResponse {
			return mockedRPC.intercept(order)
		}
		mockedRPC.init()

		// construct the raw input to simulate user input
		rawFileURLWithSAS := scenarioHelper{}.getRawFileURLWithSAS(a, shareName, fileList[0])
		raw := getDefaultRemoveRawInput(rawFileURLWithSAS.String())

		runOldCopyAndVerify(a, raw, func(err error) {
			a.Nil(err)

			// note that when we are targeting single files, the relative path is empty ("") since the root path already points to the file
			validateRemoveTransfersAreScheduled(a, true, []string{""}, mockedRPC)
		})
	}
}

func TestRemoveFilesUnderShare(t *testing.T) {
	a := assert.New(t)
	fsc := getFileServiceClient()

	// set up the share with numerous files
	shareClient, shareName := createNewShare(a, fsc)
	defer deleteShare(a, shareClient)
	fileList := scenarioHelper{}.generateCommonRemoteScenarioForAzureFile(a, shareClient, fsc, "")
	a.NotNil(shareClient)
	a.NotZero(len(fileList))

	// set up interceptor
	mockedRPC := interceptor{}
	jobsAdmin.ExecuteNewCopyJobPartOrder = func(order common.CopyJobPartOrderRequest) common.CopyJobPartOrderResponse {
		return mockedRPC.intercept(order)
	}
	mockedRPC.init()

	// construct the raw input to simulate user input
	rawShareURLWithSAS := scenarioHelper{}.getRawShareURLWithSAS(a, shareName)
	raw := getDefaultRemoveRawInput(rawShareURLWithSAS.String())
	raw.recursive = true

	// this is our current behaviour (schedule it, but STE does nothing for
	// any attempt to remove the share root. It will remove roots that are _directories_,
	// i.e. not the file share itself).
	includeRootInTransfers := true

	expectedRemovals := scenarioHelper{}.addFoldersToList(fileList, includeRootInTransfers)

	runOldCopyAndVerify(a, raw, func(err error) {
		a.Nil(err)

		// validate that the right number of transfers were scheduled
		a.Equal(len(expectedRemovals), len(mockedRPC.transfers))

		// validate that the right transfers were sent
		validateRemoveTransfersAreScheduled(a, true, expectedRemovals, mockedRPC)
	})

	// turn off recursive, this time only top files should be deleted
	raw.recursive = false
	mockedRPC.reset()

	runOldCopyAndVerify(a, raw, func(err error) {
		a.Nil(err)
		a.NotEqual(len(expectedRemovals), len(mockedRPC.transfers))

		for _, transfer := range mockedRPC.transfers {
			source := strings.TrimPrefix(transfer.Source, "/")
			a.False(strings.Contains(source, common.AZCOPY_PATH_SEPARATOR_STRING))
		}
	})
}

func TestRemoveFilesUnderDirectory(t *testing.T) {
	a := assert.New(t)
	fsc := getFileServiceClient()
	dirName := "dir1/dir2/dir3/"

	// set up the share with numerous files
	shareClient, shareName := createNewShare(a, fsc)
	defer deleteShare(a, shareClient)
	fileList := scenarioHelper{}.generateCommonRemoteScenarioForAzureFile(a, shareClient, fsc, dirName)
	a.NotNil(shareClient)
	a.NotZero(len(fileList))

	// set up interceptor
	mockedRPC := interceptor{}
	jobsAdmin.ExecuteNewCopyJobPartOrder = func(order common.CopyJobPartOrderRequest) common.CopyJobPartOrderResponse {
		return mockedRPC.intercept(order)
	}
	mockedRPC.init()

	// construct the raw input to simulate user input
	rawDirectoryURLWithSAS := scenarioHelper{}.getRawFileURLWithSAS(a, shareName, dirName)
	raw := getDefaultRemoveRawInput(rawDirectoryURLWithSAS.String())
	raw.recursive = true

	expectedDeletionMap := scenarioHelper{}.convertListToMap(
		scenarioHelper{}.addFoldersToList(fileList, false),
	)
	delete(expectedDeletionMap, "dir1")
	delete(expectedDeletionMap, "dir1/dir2")
	delete(expectedDeletionMap, "dir1/dir2/dir3")
	expectedDeletionMap[""] = 0 // add this one, because that's how dir1/dir2/dir3 appears, relative to the root (which itself)
	expectedDeletions := scenarioHelper{}.convertMapKeysToList(expectedDeletionMap)

	runOldCopyAndVerify(a, raw, func(err error) {
		a.Nil(err)

		// validate that the right number of transfers were scheduled
		a.Equal(len(expectedDeletions), len(mockedRPC.transfers))

		// validate that the right transfers were sent
		expectedTransfers := scenarioHelper{}.shaveOffPrefix(expectedDeletions, dirName)
		validateRemoveTransfersAreScheduled(a, true, expectedTransfers, mockedRPC)
	})

	// turn off recursive, this time only top files should be deleted
	raw.recursive = false
	mockedRPC.reset()

	runOldCopyAndVerify(a, raw, func(err error) {
		a.Nil(err)
		a.NotEqual(len(expectedDeletions), len(mockedRPC.transfers))

		for _, transfer := range mockedRPC.transfers {
			source := strings.TrimPrefix(transfer.Source, "/")
			a.False(strings.Contains(source, common.AZCOPY_PATH_SEPARATOR_STRING))
		}
	})
}

// include flag limits the scope of the delete
func TestRemoveFilesWithIncludeFlag(t *testing.T) {
	a := assert.New(t)
	fsc := getFileServiceClient()

	// set up the share with numerous files
	shareClient, shareName := createNewShare(a, fsc)
	fileList := scenarioHelper{}.generateCommonRemoteScenarioForAzureFile(a, shareClient, fsc, "")
	defer deleteShare(a, shareClient)
	a.NotNil(shareClient)
	a.NotZero(len(fileList))

	// add special files that we wish to include
	filesToInclude := []string{"important.pdf", "includeSub/amazing.jpeg", "exactName"}
	scenarioHelper{}.generateShareFilesFromList(a, shareClient, fsc, filesToInclude)
	includeString := "*.pdf;*.jpeg;exactName"

	// set up interceptor
	mockedRPC := interceptor{}
	jobsAdmin.ExecuteNewCopyJobPartOrder = func(order common.CopyJobPartOrderRequest) common.CopyJobPartOrderResponse {
		return mockedRPC.intercept(order)
	}
	mockedRPC.init()

	// construct the raw input to simulate user input
	rawShareURLWithSAS := scenarioHelper{}.getRawShareURLWithSAS(a, shareName)
	raw := getDefaultRemoveRawInput(rawShareURLWithSAS.String())
	raw.include = includeString
	raw.recursive = true

	runOldCopyAndVerify(a, raw, func(err error) {
		a.Nil(err)
		validateRemoveTransfersAreScheduled(a, true, filesToInclude, mockedRPC)
	})
}

// exclude flag limits the scope of the delete
func TestRemoveFilesWithExcludeFlag(t *testing.T) {
	a := assert.New(t)
	fsc := getFileServiceClient()

	// set up the share with numerous files
	shareClient, shareName := createNewShare(a, fsc)
	fileList := scenarioHelper{}.generateCommonRemoteScenarioForAzureFile(a, shareClient, fsc, "")
	defer deleteShare(a, shareClient)
	a.NotNil(shareClient)
	a.NotZero(len(fileList))

	// add special files that we wish to exclude
	filesToExclude := []string{"notGood.pdf", "excludeSub/lame.jpeg", "exactName"}
	scenarioHelper{}.generateShareFilesFromList(a, shareClient, fsc, filesToExclude)
	excludeString := "*.pdf;*.jpeg;exactName"

	// set up interceptor
	mockedRPC := interceptor{}
	jobsAdmin.ExecuteNewCopyJobPartOrder = func(order common.CopyJobPartOrderRequest) common.CopyJobPartOrderResponse {
		return mockedRPC.intercept(order)
	}
	mockedRPC.init()

	// construct the raw input to simulate user input
	rawShareURLWithSAS := scenarioHelper{}.getRawShareURLWithSAS(a, shareName)
	raw := getDefaultRemoveRawInput(rawShareURLWithSAS.String())
	raw.exclude = excludeString
	raw.recursive = true

	runOldCopyAndVerify(a, raw, func(err error) {
		a.Nil(err)
		validateRemoveTransfersAreScheduled(a, true, fileList, mockedRPC)
	})
}

// include and exclude flag can work together to limit the scope of the delete
func TestRemoveFilesWithIncludeAndExcludeFlag(t *testing.T) {
	a := assert.New(t)
	fsc := getFileServiceClient()

	// set up the share with numerous files
	shareClient, shareName := createNewShare(a, fsc)
	fileList := scenarioHelper{}.generateCommonRemoteScenarioForAzureFile(a, shareClient, fsc, "")
	defer deleteShare(a, shareClient)
	a.NotNil(shareClient)
	a.NotZero(len(fileList))

	// add special files that we wish to include
	filesToInclude := []string{"important.pdf", "includeSub/amazing.jpeg"}
	scenarioHelper{}.generateShareFilesFromList(a, shareClient, fsc, filesToInclude)
	includeString := "*.pdf;*.jpeg;exactName"

	// add special files that we wish to exclude
	// note that the excluded files also match the include string
	filesToExclude := []string{"sorry.pdf", "exclude/notGood.jpeg", "exactName", "sub/exactName"}
	scenarioHelper{}.generateShareFilesFromList(a, shareClient, fsc, filesToExclude)
	excludeString := "so*;not*;exactName"

	// set up interceptor
	mockedRPC := interceptor{}
	jobsAdmin.ExecuteNewCopyJobPartOrder = func(order common.CopyJobPartOrderRequest) common.CopyJobPartOrderResponse {
		return mockedRPC.intercept(order)
	}
	mockedRPC.init()

	// construct the raw input to simulate user input
	rawShareURLWithSAS := scenarioHelper{}.getRawShareURLWithSAS(a, shareName)
	raw := getDefaultRemoveRawInput(rawShareURLWithSAS.String())
	raw.include = includeString
	raw.exclude = excludeString
	raw.recursive = true

	runOldCopyAndVerify(a, raw, func(err error) {
		a.Nil(err)
		validateRemoveTransfersAreScheduled(a, true, filesToInclude, mockedRPC)
	})
}

// note: list-of-files flag is used
func TestRemoveListOfFilesAndDirectories(t *testing.T) {
	a := assert.New(t)
	fsc := getFileServiceClient()
	dirName := "megadir"

	// set up the share with numerous files
	shareClient, shareName := createNewShare(a, fsc)
	a.NotNil(shareClient)
	defer deleteShare(a, shareClient)
	individualFilesList := scenarioHelper{}.generateCommonRemoteScenarioForAzureFile(a, shareClient, fsc, "")
	filesUnderTopDir := scenarioHelper{}.generateCommonRemoteScenarioForAzureFile(a, shareClient, fsc, dirName+"/")
	combined := append(individualFilesList, filesUnderTopDir...)
	a.NotZero(len(combined))

	// set up interceptor
	mockedRPC := interceptor{}
	jobsAdmin.ExecuteNewCopyJobPartOrder = func(order common.CopyJobPartOrderRequest) common.CopyJobPartOrderResponse {
		return mockedRPC.intercept(order)
	}
	mockedRPC.init()

	// construct the raw input to simulate user input
	rawShareURLWithSAS := scenarioHelper{}.getRawShareURLWithSAS(a, shareName)
	raw := getDefaultRemoveRawInput(rawShareURLWithSAS.String())
	raw.recursive = true

	// make the input for list-of-files
	listOfFiles := append(individualFilesList, dirName)

	// add some random files that don't actually exist
	listOfFiles = append(listOfFiles, "WUTAMIDOING")
	listOfFiles = append(listOfFiles, "DONTKNOW")
	raw.listOfFilesToCopy = scenarioHelper{}.generateListOfFiles(a, listOfFiles)

	expectedDeletions := append(
		scenarioHelper{}.addFoldersToList(filesUnderTopDir, false), // this is a directory in the list of files list, so it will be recursively processed. Don't include root of megadir itself
		individualFilesList..., // these are individual files in the files list (so not recursively processed)
	)
	runOldCopyAndVerify(a, raw, func(err error) {
		a.Nil(err)

		// validate that the right number of transfers were scheduled
		a.Equal(len(expectedDeletions), len(mockedRPC.transfers))

		// validate that the right transfers were sent
		validateRemoveTransfersAreScheduled(a, true, expectedDeletions, mockedRPC)
	})

	// turn off recursive, this time only top files should be deleted
	raw.recursive = false
	mockedRPC.reset()

	runOldCopyAndVerify(a, raw, func(err error) {
		a.Nil(err)
		a.NotEqual(len(expectedDeletions), len(mockedRPC.transfers))

		for _, transfer := range mockedRPC.transfers {
			source, err := url.PathUnescape(transfer.Source)
			a.Nil(err)

			// if the transfer is under the given dir, make sure only the top level files were scheduled
			if strings.HasPrefix(source, dirName) {
				trimmedSource := strings.TrimPrefix(source, dirName+"/")
				a.False(strings.Contains(trimmedSource, common.AZCOPY_PATH_SEPARATOR_STRING))
			}
		}
	})
}

// include and exclude flag can work together to limit the scope of the delete
func TestRemoveListOfFilesWithIncludeAndExclude(t *testing.T) {
	a := assert.New(t)
	fsc := getFileServiceClient()
	dirName := "megadir"

	// set up the share with numerous files
	shareClient, shareName := createNewShare(a, fsc)
	a.NotNil(shareClient)
	defer deleteShare(a, shareClient)
	individualFilesList := scenarioHelper{}.generateCommonRemoteScenarioForAzureFile(a, shareClient, fsc, "")
	scenarioHelper{}.generateCommonRemoteScenarioForAzureFile(a, shareClient, fsc, dirName+"/")

	// add special files that we wish to include
	filesToInclude := []string{"important.pdf", "includeSub/amazing.jpeg"}
	scenarioHelper{}.generateShareFilesFromList(a, shareClient, fsc, filesToInclude)
	includeString := "*.pdf;*.jpeg;exactName"

	// add special files that we wish to exclude
	// note that the excluded files also match the include string
	filesToExclude := []string{"sorry.pdf", "exclude/notGood.jpeg", "exactName", "sub/exactName"}
	scenarioHelper{}.generateShareFilesFromList(a, shareClient, fsc, filesToExclude)
	excludeString := "so*;not*;exactName"

	// set up interceptor
	mockedRPC := interceptor{}
	jobsAdmin.ExecuteNewCopyJobPartOrder = func(order common.CopyJobPartOrderRequest) common.CopyJobPartOrderResponse {
		return mockedRPC.intercept(order)
	}
	mockedRPC.init()

	// construct the raw input to simulate user input
	rawShareURLWithSAS := scenarioHelper{}.getRawShareURLWithSAS(a, shareName)
	raw := getDefaultRemoveRawInput(rawShareURLWithSAS.String())
	raw.recursive = true
	raw.include = includeString
	raw.exclude = excludeString

	// make the input for list-of-files
	listOfFiles := append(individualFilesList, dirName)

	// add some random files that don't actually exist
	listOfFiles = append(listOfFiles, "WUTAMIDOING")
	listOfFiles = append(listOfFiles, "DONTKNOW")

	// add files to both include and exclude
	listOfFiles = append(listOfFiles, filesToInclude...)
	listOfFiles = append(listOfFiles, filesToExclude...)
	raw.listOfFilesToCopy = scenarioHelper{}.generateListOfFiles(a, listOfFiles)

	runOldCopyAndVerify(a, raw, func(err error) {
		a.Nil(err)

		// validate that the right number of transfers were scheduled
		a.Equal(len(filesToInclude), len(mockedRPC.transfers))

		// validate that the right transfers were sent
		validateRemoveTransfersAreScheduled(a, true, filesToInclude, mockedRPC)
	})
}

func TestRemoveSingleFileWithFromTo(t *testing.T) {
	a := assert.New(t)
	fsc := getFileServiceClient()
	shareClient, shareName := createNewShare(a, fsc)
	defer deleteShare(a, shareClient)

	for _, fileName := range []string{"top/mid/low/singlefileisbest", "打麻将.txt", "%4509%4254$85140&"} {
		// set up the share with a single file
		fileList := []string{fileName}
		scenarioHelper{}.generateShareFilesFromList(a, shareClient, fsc, fileList)
		a.NotNil(shareClient)

		// set up interceptor
		mockedRPC := interceptor{}
		jobsAdmin.ExecuteNewCopyJobPartOrder = func(order common.CopyJobPartOrderRequest) common.CopyJobPartOrderResponse {
			return mockedRPC.intercept(order)
		}
		mockedRPC.init()

		// construct the raw input to simulate user input
		rawFileURLWithSAS := scenarioHelper{}.getRawFileURLWithSAS(a, shareName, fileList[0])
		raw := getDefaultRemoveRawInput(rawFileURLWithSAS.String())
		raw.fromTo = "FileTrash"

		runOldCopyAndVerify(a, raw, func(err error) {
			a.Nil(err)

			// note that when we are targeting single files, the relative path is empty ("") since the root path already points to the file
			validateRemoveTransfersAreScheduled(a, true, []string{""}, mockedRPC)
		})
	}
}

func TestRemoveFilesUnderShareWithFromTo(t *testing.T) {
	a := assert.New(t)
	fsc := getFileServiceClient()

	// set up the share with numerous files
	shareClient, shareName := createNewShare(a, fsc)
	defer deleteShare(a, shareClient)
	fileList := scenarioHelper{}.generateCommonRemoteScenarioForAzureFile(a, shareClient, fsc, "")
	a.NotNil(shareClient)
	a.NotZero(len(fileList))

	// set up interceptor
	mockedRPC := interceptor{}
	jobsAdmin.ExecuteNewCopyJobPartOrder = func(order common.CopyJobPartOrderRequest) common.CopyJobPartOrderResponse {
		return mockedRPC.intercept(order)
	}
	mockedRPC.init()

	// construct the raw input to simulate user input
	rawShareURLWithSAS := scenarioHelper{}.getRawShareURLWithSAS(a, shareName)
	raw := getDefaultRemoveRawInput(rawShareURLWithSAS.String())
	raw.recursive = true
	raw.fromTo = "FileTrash"

	// this is our current behaviour (schedule it, but STE does nothing for
	// any attempt to remove the share root. It will remove roots that are _directories_,
	// i.e. not the file share itself).
	includeRootInTransfers := true

	expectedRemovals := scenarioHelper{}.addFoldersToList(fileList, includeRootInTransfers)

	runOldCopyAndVerify(a, raw, func(err error) {
		a.Nil(err)

		// validate that the right number of transfers were scheduled
		a.Equal(len(expectedRemovals), len(mockedRPC.transfers))

		// validate that the right transfers were sent
		validateRemoveTransfersAreScheduled(a, true, expectedRemovals, mockedRPC)
	})

	// turn off recursive, this time only top files should be deleted
	raw.recursive = false
	mockedRPC.reset()

	runOldCopyAndVerify(a, raw, func(err error) {
		a.Nil(err)
		a.NotEqual(len(expectedRemovals), len(mockedRPC.transfers))

		for _, transfer := range mockedRPC.transfers {
			source := strings.TrimPrefix(transfer.Source, "/")
			a.False(strings.Contains(source, common.AZCOPY_PATH_SEPARATOR_STRING))
		}
	})
}

func TestRemoveFilesUnderDirectoryWithFromTo(t *testing.T) {
	a := assert.New(t)
	fsc := getFileServiceClient()
	dirName := "dir1/dir2/dir3/"

	// set up the share with numerous files
	shareClient, shareName := createNewShare(a, fsc)
	defer deleteShare(a, shareClient)
	fileList := scenarioHelper{}.generateCommonRemoteScenarioForAzureFile(a, shareClient, fsc, dirName)
	a.NotNil(shareClient)
	a.NotZero(len(fileList))

	// set up interceptor
	mockedRPC := interceptor{}
	jobsAdmin.ExecuteNewCopyJobPartOrder = func(order common.CopyJobPartOrderRequest) common.CopyJobPartOrderResponse {
		return mockedRPC.intercept(order)
	}
	mockedRPC.init()

	// construct the raw input to simulate user input
	rawDirectoryURLWithSAS := scenarioHelper{}.getRawFileURLWithSAS(a, shareName, dirName)
	raw := getDefaultRemoveRawInput(rawDirectoryURLWithSAS.String())
	raw.recursive = true
	raw.fromTo = "FileTrash"

	expectedDeletionMap := scenarioHelper{}.convertListToMap(
		scenarioHelper{}.addFoldersToList(fileList, false),
	)
	delete(expectedDeletionMap, "dir1")
	delete(expectedDeletionMap, "dir1/dir2")
	delete(expectedDeletionMap, "dir1/dir2/dir3")
	expectedDeletionMap[""] = 0 // add this one, because that's how dir1/dir2/dir3 appears, relative to the root (which itself)
	expectedDeletions := scenarioHelper{}.convertMapKeysToList(expectedDeletionMap)

	runOldCopyAndVerify(a, raw, func(err error) {
		a.Nil(err)

		// validate that the right number of transfers were scheduled
		a.Equal(len(expectedDeletions), len(mockedRPC.transfers))

		// validate that the right transfers were sent
		expectedTransfers := scenarioHelper{}.shaveOffPrefix(expectedDeletions, dirName)
		validateRemoveTransfersAreScheduled(a, true, expectedTransfers, mockedRPC)
	})

	// turn off recursive, this time only top files should be deleted
	raw.recursive = false
	mockedRPC.reset()

	runOldCopyAndVerify(a, raw, func(err error) {
		a.Nil(err)
		a.NotEqual(len(expectedDeletions), len(mockedRPC.transfers))

		for _, transfer := range mockedRPC.transfers {
			source := strings.TrimPrefix(transfer.Source, "/")
			a.False(strings.Contains(source, common.AZCOPY_PATH_SEPARATOR_STRING))
		}
	})
}
