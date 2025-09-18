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
	"context"
	"os"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/Azure/azure-storage-azcopy/v10/azcopy"
	"github.com/Azure/azure-storage-azcopy/v10/common"
	"github.com/stretchr/testify/assert"
)

// regular file->file sync
func TestFileSyncS2SWithSingleFile(t *testing.T) {
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

		// set up the destination share with the same single file
		scenarioHelper{}.generateShareFilesFromList(a, dstShareClient, fsc, fileList)

		// set up interceptor
		mockedRPC := interceptor{}
		mockedRPC.init()

		// construct the raw input to simulate user input
		srcFileURLWithSAS := scenarioHelper{}.getRawFileURLWithSAS(a, srcShareName, fileList[0])
		dstFileURLWithSAS := scenarioHelper{}.getRawFileURLWithSAS(a, dstShareName, fileList[0])
		raw := getDefaultSyncRawInput(srcFileURLWithSAS.String(), dstFileURLWithSAS.String())

		// the destination was created after the source, so no sync should happen
		runSyncAndVerify(a, raw, mockedRPC.intercept, mockedRPC.delete, func(err error) {
			a.Nil(err)

			// validate that the right number of transfers were scheduled
			a.Zero(len(mockedRPC.transfers))
		})

		// recreate the source file to have a later last modified time
		scenarioHelper{}.generateShareFilesFromList(a, srcShareClient, fsc, fileList)
		mockedRPC.reset()

		runSyncAndVerify(a, raw, mockedRPC.intercept, mockedRPC.delete, func(err error) {
			a.Nil(err)
			validateS2SSyncTransfersAreScheduled(a, []string{""}, mockedRPC)
		})
	}
}

// regular share->share sync but destination is empty, so everything has to be transferred
func TestFileSyncS2SWithEmptyDestination(t *testing.T) {
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
	mockedRPC.init()

	// construct the raw input to simulate user input
	srcShareURLWithSAS := scenarioHelper{}.getRawShareURLWithSAS(a, srcShareName)
	dstShareURLWithSAS := scenarioHelper{}.getRawShareURLWithSAS(a, dstShareName)
	raw := getDefaultSyncRawInput(srcShareURLWithSAS.String(), dstShareURLWithSAS.String())

	// all files at source should be synced to destination
	expectedList := scenarioHelper{}.addFoldersToList(fileList, false)
	runSyncAndVerify(a, raw, mockedRPC.intercept, mockedRPC.delete, func(err error) {
		a.Nil(err)

		// validate that the right number of transfers were scheduled
		a.Equal(len(expectedList), len(mockedRPC.transfers))

		// validate that the right transfers were sent
		validateS2SSyncTransfersAreScheduled(a, expectedList, mockedRPC)
	})

	// turn off recursive, this time only top files should be transferred
	raw.recursive = false
	mockedRPC.reset()
	runSyncAndVerify(a, raw, mockedRPC.intercept, mockedRPC.delete, func(err error) {
		a.Nil(err)
		a.NotEqual(len(fileList), len(mockedRPC.transfers))

		for _, transfer := range mockedRPC.transfers {
			source := strings.TrimPrefix(transfer.Source, "/")
			a.False(strings.Contains(source, common.AZCOPY_PATH_SEPARATOR_STRING))
		}
	})
}

// regular share->share sync but destination is identical to the source, transfers are scheduled based on lmt
func TestFileSyncS2SWithIdenticalDestination(t *testing.T) {
	a := assert.New(t)
	fsc := getFileServiceClient()
	srcShareClient, srcShareName := createNewShare(a, fsc)
	dstShareClient, dstShareName := createNewShare(a, fsc)
	defer deleteShare(a, srcShareClient)
	defer deleteShare(a, dstShareClient)

	// set up the source share with numerous files
	fileList := scenarioHelper{}.generateCommonRemoteScenarioForAzureFile(a, srcShareClient, fsc, "")
	a.NotZero(len(fileList))

	// set up the destination with the exact same files
	scenarioHelper{}.generateShareFilesFromList(a, dstShareClient, fsc, fileList)

	// set up interceptor
	mockedRPC := interceptor{}
	mockedRPC.init()

	// construct the raw input to simulate user input
	srcShareURLWithSAS := scenarioHelper{}.getRawShareURLWithSAS(a, srcShareName)
	dstShareURLWithSAS := scenarioHelper{}.getRawShareURLWithSAS(a, dstShareName)
	raw := getDefaultSyncRawInput(srcShareURLWithSAS.String(), dstShareURLWithSAS.String())

	// nothing should be sync since the source is older
	runSyncAndVerify(a, raw, mockedRPC.intercept, mockedRPC.delete, func(err error) {
		a.Nil(err)

		// validate that the right number of transfers were scheduled
		a.Zero(len(mockedRPC.transfers))
	})

	// refresh the source files' last modified time so that they get synced
	scenarioHelper{}.generateShareFilesFromList(a, srcShareClient, fsc, fileList)
	mockedRPC.reset()
	runSyncAndVerify(a, raw, mockedRPC.intercept, mockedRPC.delete, func(err error) {
		a.Nil(err)
		validateS2SSyncTransfersAreScheduled(a, fileList, mockedRPC)
	})
}

// regular share->share sync where destination is missing some files from source, and also has some extra files
func TestFileSyncS2SWithMismatchedDestination(t *testing.T) {
	a := assert.New(t)
	fsc := getFileServiceClient()
	srcShareClient, srcShareName := createNewShare(a, fsc)
	dstShareClient, dstShareName := createNewShare(a, fsc)
	defer deleteShare(a, srcShareClient)
	defer deleteShare(a, dstShareClient)

	// set up the share with numerous files
	fileList := scenarioHelper{}.generateCommonRemoteScenarioForAzureFile(a, srcShareClient, fsc, "")
	a.NotZero(len(fileList))

	// set up the destination with half of the files from source
	filesAlreadyAtDestination := fileList[0 : len(fileList)/2]
	scenarioHelper{}.generateShareFilesFromList(a, dstShareClient, fsc, filesAlreadyAtDestination)
	expectedOutput := fileList[len(fileList)/2:] // the missing half of source files should be transferred

	// add some extra files that shouldn't be included
	extras := scenarioHelper{}.generateCommonRemoteScenarioForAzureFile(a, dstShareClient, fsc, "extra")

	// set up interceptor
	mockedRPC := interceptor{}
	mockedRPC.init()

	// construct the raw input to simulate user input
	srcShareURLWithSAS := scenarioHelper{}.getRawShareURLWithSAS(a, srcShareName)
	dstShareURLWithSAS := scenarioHelper{}.getRawShareURLWithSAS(a, dstShareName)
	raw := getDefaultSyncRawInput(srcShareURLWithSAS.String(), dstShareURLWithSAS.String())

	expectedOutputMap := scenarioHelper{}.convertListToMap(
		scenarioHelper{}.addFoldersToList(expectedOutput, false))
	everythingAlreadyAtDestination := scenarioHelper{}.convertListToMap(
		scenarioHelper{}.addFoldersToList(filesAlreadyAtDestination, false))
	for exists := range everythingAlreadyAtDestination {
		delete(expectedOutputMap, exists) // remove directories that actually exist at destination
	}
	expectedOutput = scenarioHelper{}.convertMapKeysToList(expectedOutputMap)

	runSyncAndVerify(a, raw, mockedRPC.intercept, mockedRPC.delete, func(err error) {
		a.Nil(err)
		validateS2SSyncTransfersAreScheduled(a, expectedOutput, mockedRPC)

		validateDeleteTransfersAreScheduled(a, extras, mockedRPC)
	})
}

// include flag limits the scope of source/destination comparison
func TestFileSyncS2SWithIncludeFlag(t *testing.T) {
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
	mockedRPC.init()

	// construct the raw input to simulate user input
	srcShareURLWithSAS := scenarioHelper{}.getRawShareURLWithSAS(a, srcShareName)
	dstShareURLWithSAS := scenarioHelper{}.getRawShareURLWithSAS(a, dstShareName)
	raw := getDefaultSyncRawInput(srcShareURLWithSAS.String(), dstShareURLWithSAS.String())
	raw.include = includeString

	// verify that only the files specified by the include flag are synced
	runSyncAndVerify(a, raw, mockedRPC.intercept, mockedRPC.delete, func(err error) {
		a.Nil(err)
		validateS2SSyncTransfersAreScheduled(a, filesToInclude, mockedRPC)
	})
}

// exclude flag limits the scope of source/destination comparison
func TestFileSyncS2SWithExcludeFlag(t *testing.T) {
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
	mockedRPC.init()

	// construct the raw input to simulate user input
	srcShareURLWithSAS := scenarioHelper{}.getRawShareURLWithSAS(a, srcShareName)
	dstShareURLWithSAS := scenarioHelper{}.getRawShareURLWithSAS(a, dstShareName)
	raw := getDefaultSyncRawInput(srcShareURLWithSAS.String(), dstShareURLWithSAS.String())
	raw.exclude = excludeString

	// make sure the list doesn't include the files specified by the exclude flag
	runSyncAndVerify(a, raw, mockedRPC.intercept, mockedRPC.delete, func(err error) {
		a.Nil(err)
		validateS2SSyncTransfersAreScheduled(a, fileList, mockedRPC)
	})
}

// include and exclude flag can work together to limit the scope of source/destination comparison
func TestFileSyncS2SWithIncludeAndExcludeFlag(t *testing.T) {
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
	mockedRPC.init()

	// construct the raw input to simulate user input
	srcShareURLWithSAS := scenarioHelper{}.getRawShareURLWithSAS(a, srcShareName)
	dstShareURLWithSAS := scenarioHelper{}.getRawShareURLWithSAS(a, dstShareName)
	raw := getDefaultSyncRawInput(srcShareURLWithSAS.String(), dstShareURLWithSAS.String())
	raw.include = includeString
	raw.exclude = excludeString

	// verify that only the files specified by the include flag are synced
	runSyncAndVerify(a, raw, mockedRPC.intercept, mockedRPC.delete, func(err error) {
		a.Nil(err)
		validateS2SSyncTransfersAreScheduled(a, filesToInclude, mockedRPC)
	})
}

// TODO: Fix me, passes locally (Windows and WSL2), but not on CI
// // validate the bug fix for this scenario
// func TestFileSyncS2SWithMissingDestination(t *testing.T) {
//	a := assert.New(t)
// 	fsc := getFileServiceClient()
// 	srcShareURL, srcShareName := createNewAzureShare(a, fsu)
// 	dstShareURL, dstShareName := createNewAzureShare(a, fsu)
// 	defer deleteShareV1(a, srcShareURL)
//
// 	// delete the destination share to simulate non-existing destination, or recently removed destination
// 	deleteShareV1(a, dstShareURL)
//
// 	// set up the share with numerous files
// 	fileList := scenarioHelper{}.generateCommonRemoteScenarioForAzureFile(a, srcShareURL, "")
// 	a.NotZero(len(fileList))
//
// 	// set up interceptor
// 	mockedRPC := interceptor{}
// 		jobsAdmin.ExecuteNewCopyJobPartOrder = func(order common.CopyJobPartOrderRequest) common.CopyJobPartOrderResponse {
//return mockedRPC.intercept(order)
//}
// 	mockedRPC.init()
//
// 	// construct the raw input to simulate user input
// 	srcShareURLWithSAS := scenarioHelper{}.getRawShareURLWithSAS(a, srcShareName)
// 	dstShareURLWithSAS := scenarioHelper{}.getRawShareURLWithSAS(a, dstShareName)
// 	raw := getDefaultSyncRawInput(srcShareURLWithSAS.String(), dstShareURLWithSAS.String())
//
// 	// verify error is thrown
// 	runSyncAndVerify(a, raw, func(err error) {
// 		// error should not be nil, but the app should not crash either
// 		a.NotNil(err)
//
// 		// validate that the right number of transfers were scheduled
// 		a.Zero(len(mockedRPC.transfers))
// 	})
// }

// share <-> dir sync
func TestFileSyncS2SShareAndEmptyDir(t *testing.T) {
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
	mockedRPC.init()

	// construct the raw input to simulate user input
	srcShareURLWithSAS := scenarioHelper{}.getRawShareURLWithSAS(a, srcShareName)
	dirName := "emptydir"
	_, err := dstShareClient.NewDirectoryClient(dirName).Create(context.Background(), nil)
	a.Nil(err)
	dstDirURLWithSAS := scenarioHelper{}.getRawFileURLWithSAS(a, dstShareName, dirName)
	raw := getDefaultSyncRawInput(srcShareURLWithSAS.String(), dstDirURLWithSAS.String())

	// verify that targeting a directory works fine
	expectedList := scenarioHelper{}.addFoldersToList(fileList, false)
	runSyncAndVerify(a, raw, mockedRPC.intercept, mockedRPC.delete, func(err error) {
		a.Nil(err)

		// validate that the right number of transfers were scheduled
		a.Equal(len(expectedList), len(mockedRPC.transfers))

		// validate that the right transfers were sent
		validateS2SSyncTransfersAreScheduled(a, expectedList, mockedRPC)
	})

	// turn off recursive, this time only top files should be transferred
	raw.recursive = false
	mockedRPC.reset()

	runSyncAndVerify(a, raw, mockedRPC.intercept, mockedRPC.delete, func(err error) {
		a.Nil(err)
		a.NotEqual(len(fileList), len(mockedRPC.transfers))

		for _, transfer := range mockedRPC.transfers {
			source := strings.TrimPrefix(transfer.Source, "/")
			a.False(strings.Contains(source, common.AZCOPY_PATH_SEPARATOR_STRING))
		}
	})
}

// regular dir -> dir sync
func TestFileSyncS2SBetweenDirs(t *testing.T) {
	a := assert.New(t)
	fsc := getFileServiceClient()
	srcShareClient, srcShareName := createNewShare(a, fsc)
	dstShareClient, dstShareName := createNewShare(a, fsc)
	defer deleteShare(a, srcShareClient)
	defer deleteShare(a, dstShareClient)

	// set up the source share with numerous files
	dirName := "dir"
	fileList := scenarioHelper{}.generateCommonRemoteScenarioForAzureFile(a, srcShareClient, fsc, dirName+common.AZCOPY_PATH_SEPARATOR_STRING)
	a.NotZero(len(fileList))

	// set up the destination with the exact same files
	scenarioHelper{}.generateShareFilesFromList(a, dstShareClient, fsc, fileList)

	// set up interceptor
	mockedRPC := interceptor{}
	mockedRPC.init()

	// construct the raw input to simulate user input
	srcShareURLWithSAS := scenarioHelper{}.getRawShareURLWithSAS(a, srcShareName)
	dstShareURLWithSAS := scenarioHelper{}.getRawShareURLWithSAS(a, dstShareName)
	srcShareURLWithSAS.Path += common.AZCOPY_PATH_SEPARATOR_STRING + dirName
	dstShareURLWithSAS.Path += common.AZCOPY_PATH_SEPARATOR_STRING + dirName
	raw := getDefaultSyncRawInput(srcShareURLWithSAS.String(), dstShareURLWithSAS.String())

	// nothing should be synced since the source is older
	runSyncAndVerify(a, raw, mockedRPC.intercept, mockedRPC.delete, func(err error) {
		a.Nil(err)

		// validate that the right number of transfers were scheduled
		a.Zero(len(mockedRPC.transfers))
	})

	// refresh the files' last modified time so that they are newer
	scenarioHelper{}.generateShareFilesFromList(a, srcShareClient, fsc, fileList)
	mockedRPC.reset()
	expectedList := scenarioHelper{}.shaveOffPrefix(fileList, dirName+common.AZCOPY_PATH_SEPARATOR_STRING)
	runSyncAndVerify(a, raw, mockedRPC.intercept, mockedRPC.delete, func(err error) {
		a.Nil(err)
		validateS2SSyncTransfersAreScheduled(a, expectedList, mockedRPC)
	})
}

func TestDryrunSyncFiletoFile(t *testing.T) {
	a := assert.New(t)
	fsc := getFileServiceClient()

	//set up src share
	filesToInclude := []string{"AzURE2.jpeg", "TestOne.txt"}
	srcShareClient, srcShareName := createNewShare(a, fsc)
	defer deleteShare(a, srcShareClient)
	scenarioHelper{}.generateShareFilesFromList(a, srcShareClient, fsc, filesToInclude)

	//set up dst share
	dstShareClient, dstShareName := createNewShare(a, fsc)
	defer deleteShare(a, dstShareClient)
	fileToDelete := []string{"testThree.jpeg"}
	scenarioHelper{}.generateShareFilesFromList(a, dstShareClient, fsc, fileToDelete)

	// set up interceptor
	mockedLcm := mockedLifecycleManager{dryrunLog: make(chan string, 50)}
	mockedLcm.SetOutputFormat(common.EOutputFormat.Text())
	glcm = &mockedLcm

	// construct the raw input to simulate user input
	srcShareURLWithSAS := scenarioHelper{}.getRawShareURLWithSAS(a, srcShareName)
	dstShareURLWithSAS := scenarioHelper{}.getRawShareURLWithSAS(a, dstShareName)
	raw := getDefaultSyncRawInput(srcShareURLWithSAS.String(), dstShareURLWithSAS.String())
	raw.dryrun = true
	raw.deleteDestination = "true"

	runSyncAndVerify(a, raw, dryrunNewCopyJobPartOrder, dryrunDelete, func(err error) {

		msg := mockedLcm.GatherAllLogs(mockedLcm.dryrunLog)
		sort.Strings(msg)
		for i := 0; i < len(msg); i++ {
			if strings.Contains(msg[i], "DRYRUN: remove") {
				a.True(strings.Contains(msg[i], dstShareClient.URL()))
			} else {
				a.True(strings.Contains(msg[i], "DRYRUN: copy"))
				a.True(strings.Contains(msg[i], srcShareName))
				a.True(strings.Contains(msg[i], dstShareClient.URL()))
			}
		}

		a.True(testDryrunStatements(fileToDelete, msg))
		a.True(testDryrunStatements(filesToInclude, msg))
	})
}

func TestDryrunSyncLocaltoFile(t *testing.T) {
	a := assert.New(t)
	fsc := getFileServiceClient()

	//set up local src
	blobsToInclude := []string{"AzURE2.jpeg"}
	srcDirName := scenarioHelper{}.generateLocalDirectory(a)
	defer os.RemoveAll(srcDirName)
	scenarioHelper{}.generateLocalFilesFromList(a, srcDirName, blobsToInclude)

	//set up dst share
	dstShareClient, dstShareName := createNewShare(a, fsc)
	defer deleteShare(a, dstShareClient)
	fileToDelete := []string{"testThree.jpeg"}
	scenarioHelper{}.generateShareFilesFromList(a, dstShareClient, fsc, fileToDelete)

	// set up interceptor
	mockedLcm := mockedLifecycleManager{dryrunLog: make(chan string, 50)}
	mockedLcm.SetOutputFormat(common.EOutputFormat.Text())
	glcm = &mockedLcm

	// construct the raw input to simulate user input
	dstShareURLWithSAS := scenarioHelper{}.getRawShareURLWithSAS(a, dstShareName)
	raw := getDefaultSyncRawInput(srcDirName, dstShareURLWithSAS.String())
	raw.dryrun = true
	raw.deleteDestination = "true"

	runSyncAndVerify(a, raw, dryrunNewCopyJobPartOrder, dryrunDelete, func(err error) {

		msg := mockedLcm.GatherAllLogs(mockedLcm.dryrunLog)
		sort.Strings(msg)
		for i := 0; i < len(msg); i++ {
			if strings.Contains(msg[i], "DRYRUN: remove") {
				a.True(strings.Contains(msg[i], dstShareClient.URL()))
			} else if strings.Contains(msg[i], "DRYRUN: copy") {
				a.True(strings.Contains(msg[i], "DRYRUN: copy"))
				a.True(strings.Contains(msg[i], srcDirName))
				a.True(strings.Contains(msg[i], dstShareClient.URL()))
			} else {
				a.True(strings.Contains(msg[i], "DRYRUN: warn"))
				a.True(strings.Contains(msg[i], azcopy.LocalToFileShareWarnMsg))
			}
		}

		a.True(testDryrunStatements(blobsToInclude, msg))
		a.True(testDryrunStatements(fileToDelete, msg))
	})
}

// regular share->share sync but destination is identical to the source, transfers are scheduled based on lmt
func TestFileSyncS2SWithIdenticalDestinationTemp(t *testing.T) {
	a := assert.New(t)
	fsc := getFileServiceClient()
	srcShareClient, srcShareName := createNewShare(a, fsc)
	dstShareClient, dstShareName := createNewShare(a, fsc)
	defer deleteShare(a, srcShareClient)
	defer deleteShare(a, dstShareClient)

	// set up the source share with numerous files
	fileList := scenarioHelper{}.generateCommonRemoteScenarioForAzureFile(a, srcShareClient, fsc, "")
	a.NotZero(len(fileList))

	// set up the destination with the exact same files
	scenarioHelper{}.generateShareFilesFromList(a, dstShareClient, fsc, fileList)

	// set up interceptor
	mockedRPC := interceptor{}
	mockedRPC.init()

	// construct the raw input to simulate user input
	srcShareURLWithSAS := scenarioHelper{}.getRawShareURLWithSAS(a, srcShareName)
	dstShareURLWithSAS := scenarioHelper{}.getRawShareURLWithSAS(a, dstShareName)
	raw := getDefaultSyncRawInput(srcShareURLWithSAS.String(), dstShareURLWithSAS.String())
	raw.preserveInfo = false

	// nothing should be sync since the source is older
	runSyncAndVerify(a, raw, mockedRPC.intercept, mockedRPC.delete, func(err error) {
		a.Nil(err)

		// validate that the right number of transfers were scheduled
		a.Zero(len(mockedRPC.transfers))
	})

	// refresh the source files' last modified time so that they get synced
	scenarioHelper{}.generateShareFilesFromList(a, srcShareClient, fsc, fileList)
	mockedRPC.reset()
	currentTime := time.Now().UTC()
	newTime := currentTime.Add(-time.Hour).UTC() // give extra hour
	runSyncAndVerify(a, raw, mockedRPC.intercept, mockedRPC.delete, func(err error) {
		a.Nil(err)
		validateS2SSyncTransfersAreScheduled(a, fileList, mockedRPC)

		for _, transfer := range mockedRPC.transfers {
			if !(transfer.LastModifiedTime.Before(currentTime) && transfer.LastModifiedTime.After(newTime)) {
				t.Fail()
			}
		}
	})
}
