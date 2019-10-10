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
	"strings"

	"github.com/Azure/azure-storage-azcopy/common"
	"github.com/Azure/azure-storage-file-go/azfile"
	chk "gopkg.in/check.v1"
)

// regular file->file sync
func (s *cmdIntegrationSuite) TestFileSyncS2SWithSingleFile(c *chk.C) {
	fsu := getFSU()
	srcShareURL, srcShareName := createNewAzureShare(c, fsu)
	dstShareURL, dstShareName := createNewAzureShare(c, fsu)
	defer deleteShare(c, srcShareURL)
	defer deleteShare(c, dstShareURL)

	for _, fileName := range []string{"singlefileisbest", "打麻将.txt", "%4509%4254$85140&"} {
		// set up the source share with a single file
		fileList := []string{fileName}
		scenarioHelper{}.generateAzureFilesFromList(c, srcShareURL, fileList)

		// set up the destination share with the same single file
		scenarioHelper{}.generateAzureFilesFromList(c, dstShareURL, fileList)

		// set up interceptor
		mockedRPC := interceptor{}
		Rpc = mockedRPC.intercept
		mockedRPC.init()

		// construct the raw input to simulate user input
		srcFileURLWithSAS := scenarioHelper{}.getRawFileURLWithSAS(c, srcShareName, fileList[0])
		dstFileURLWithSAS := scenarioHelper{}.getRawFileURLWithSAS(c, dstShareName, fileList[0])
		raw := getDefaultSyncRawInput(srcFileURLWithSAS.String(), dstFileURLWithSAS.String())

		// the destination was created after the source, so no sync should happen
		runSyncAndVerify(c, raw, func(err error) {
			c.Assert(err, chk.IsNil)

			// validate that the right number of transfers were scheduled
			c.Assert(len(mockedRPC.transfers), chk.Equals, 0)
		})

		// recreate the source file to have a later last modified time
		scenarioHelper{}.generateAzureFilesFromList(c, srcShareURL, fileList)
		mockedRPC.reset()

		runSyncAndVerify(c, raw, func(err error) {
			c.Assert(err, chk.IsNil)
			validateS2SSyncTransfersAreScheduled(c, "", "", []string{""}, mockedRPC)
		})
	}
}

// regular share->share sync but destination is empty, so everything has to be transferred
func (s *cmdIntegrationSuite) TestFileSyncS2SWithEmptyDestination(c *chk.C) {
	fsu := getFSU()
	srcShareURL, srcShareName := createNewAzureShare(c, fsu)
	dstShareURL, dstShareName := createNewAzureShare(c, fsu)
	defer deleteShare(c, srcShareURL)
	defer deleteShare(c, dstShareURL)

	// set up the source share with numerous files
	fileList := scenarioHelper{}.generateCommonRemoteScenarioForAzureFile(c, srcShareURL, "")
	c.Assert(len(fileList), chk.Not(chk.Equals), 0)

	// set up interceptor
	mockedRPC := interceptor{}
	Rpc = mockedRPC.intercept
	mockedRPC.init()

	// construct the raw input to simulate user input
	srcShareURLWithSAS := scenarioHelper{}.getRawShareURLWithSAS(c, srcShareName)
	dstShareURLWithSAS := scenarioHelper{}.getRawShareURLWithSAS(c, dstShareName)
	raw := getDefaultSyncRawInput(srcShareURLWithSAS.String(), dstShareURLWithSAS.String())

	// all files at source should be synced to destination
	runSyncAndVerify(c, raw, func(err error) {
		c.Assert(err, chk.IsNil)

		// validate that the right number of transfers were scheduled
		c.Assert(len(mockedRPC.transfers), chk.Equals, len(fileList))

		// validate that the right transfers were sent
		validateS2SSyncTransfersAreScheduled(c, "", "", fileList, mockedRPC)
	})

	// turn off recursive, this time only top files should be transferred
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

// regular share->share sync but destination is identical to the source, transfers are scheduled based on lmt
func (s *cmdIntegrationSuite) TestFileSyncS2SWithIdenticalDestination(c *chk.C) {
	fsu := getFSU()
	srcShareURL, srcShareName := createNewAzureShare(c, fsu)
	dstShareURL, dstShareName := createNewAzureShare(c, fsu)
	defer deleteShare(c, srcShareURL)
	defer deleteShare(c, dstShareURL)

	// set up the source share with numerous files
	fileList := scenarioHelper{}.generateCommonRemoteScenarioForAzureFile(c, srcShareURL, "")
	c.Assert(len(fileList), chk.Not(chk.Equals), 0)

	// set up the destination with the exact same files
	scenarioHelper{}.generateAzureFilesFromList(c, dstShareURL, fileList)

	// set up interceptor
	mockedRPC := interceptor{}
	Rpc = mockedRPC.intercept
	mockedRPC.init()

	// construct the raw input to simulate user input
	srcShareURLWithSAS := scenarioHelper{}.getRawShareURLWithSAS(c, srcShareName)
	dstShareURLWithSAS := scenarioHelper{}.getRawShareURLWithSAS(c, dstShareName)
	raw := getDefaultSyncRawInput(srcShareURLWithSAS.String(), dstShareURLWithSAS.String())

	// nothing should be sync since the source is older
	runSyncAndVerify(c, raw, func(err error) {
		c.Assert(err, chk.IsNil)

		// validate that the right number of transfers were scheduled
		c.Assert(len(mockedRPC.transfers), chk.Equals, 0)
	})

	// refresh the source files' last modified time so that they get synced
	scenarioHelper{}.generateAzureFilesFromList(c, srcShareURL, fileList)
	mockedRPC.reset()
	runSyncAndVerify(c, raw, func(err error) {
		c.Assert(err, chk.IsNil)
		validateS2SSyncTransfersAreScheduled(c, "", "", fileList, mockedRPC)
	})
}

// regular share->share sync where destination is missing some files from source, and also has some extra files
func (s *cmdIntegrationSuite) TestFileSyncS2SWithMismatchedDestination(c *chk.C) {
	fsu := getFSU()
	srcShareURL, srcShareName := createNewAzureShare(c, fsu)
	dstShareURL, dstShareName := createNewAzureShare(c, fsu)
	defer deleteShare(c, srcShareURL)
	defer deleteShare(c, dstShareURL)

	// set up the share with numerous files
	fileList := scenarioHelper{}.generateCommonRemoteScenarioForAzureFile(c, srcShareURL, "")
	c.Assert(len(fileList), chk.Not(chk.Equals), 0)

	// set up the destination with half of the files from source
	scenarioHelper{}.generateAzureFilesFromList(c, dstShareURL, fileList[0:len(fileList)/2])
	expectedOutput := fileList[len(fileList)/2:] // the missing half of source files should be transferred

	// add some extra files that shouldn't be included
	scenarioHelper{}.generateCommonRemoteScenarioForAzureFile(c, dstShareURL, "extra")

	// set up interceptor
	mockedRPC := interceptor{}
	Rpc = mockedRPC.intercept
	mockedRPC.init()

	// construct the raw input to simulate user input
	srcShareURLWithSAS := scenarioHelper{}.getRawShareURLWithSAS(c, srcShareName)
	dstShareURLWithSAS := scenarioHelper{}.getRawShareURLWithSAS(c, dstShareName)
	raw := getDefaultSyncRawInput(srcShareURLWithSAS.String(), dstShareURLWithSAS.String())

	runSyncAndVerify(c, raw, func(err error) {
		c.Assert(err, chk.IsNil)
		validateS2SSyncTransfersAreScheduled(c, "", "", expectedOutput, mockedRPC)

		// make sure the extra files were deleted
		extraFilesFound := false
		for marker := (azfile.Marker{}); marker.NotDone(); {
			listResponse, err := dstShareURL.NewRootDirectoryURL().ListFilesAndDirectoriesSegment(ctx, marker, azfile.ListFilesAndDirectoriesOptions{})
			c.Assert(err, chk.IsNil)
			marker = listResponse.NextMarker

			// if ever the extra files are found, note it down
			for _, file := range listResponse.FileItems {
				if strings.Contains(file.Name, "extra") {
					extraFilesFound = true
				}
			}
		}

		c.Assert(extraFilesFound, chk.Equals, false)
	})
}

// include flag limits the scope of source/destination comparison
func (s *cmdIntegrationSuite) TestFileSyncS2SWithIncludeFlag(c *chk.C) {
	fsu := getFSU()
	srcShareURL, srcShareName := createNewAzureShare(c, fsu)
	dstShareURL, dstShareName := createNewAzureShare(c, fsu)
	defer deleteShare(c, srcShareURL)
	defer deleteShare(c, dstShareURL)

	// set up the source share with numerous files
	fileList := scenarioHelper{}.generateCommonRemoteScenarioForAzureFile(c, srcShareURL, "")
	c.Assert(len(fileList), chk.Not(chk.Equals), 0)

	// add special files that we wish to include
	filesToInclude := []string{"important.pdf", "includeSub/amazing.jpeg", "exactName"}
	scenarioHelper{}.generateAzureFilesFromList(c, srcShareURL, filesToInclude)
	includeString := "*.pdf;*.jpeg;exactName"

	// set up interceptor
	mockedRPC := interceptor{}
	Rpc = mockedRPC.intercept
	mockedRPC.init()

	// construct the raw input to simulate user input
	srcShareURLWithSAS := scenarioHelper{}.getRawShareURLWithSAS(c, srcShareName)
	dstShareURLWithSAS := scenarioHelper{}.getRawShareURLWithSAS(c, dstShareName)
	raw := getDefaultSyncRawInput(srcShareURLWithSAS.String(), dstShareURLWithSAS.String())
	raw.include = includeString

	// verify that only the files specified by the include flag are synced
	runSyncAndVerify(c, raw, func(err error) {
		c.Assert(err, chk.IsNil)
		validateS2SSyncTransfersAreScheduled(c, "", "", filesToInclude, mockedRPC)
	})
}

// exclude flag limits the scope of source/destination comparison
func (s *cmdIntegrationSuite) TestFileSyncS2SWithExcludeFlag(c *chk.C) {
	fsu := getFSU()
	srcShareURL, srcShareName := createNewAzureShare(c, fsu)
	dstShareURL, dstShareName := createNewAzureShare(c, fsu)
	defer deleteShare(c, srcShareURL)
	defer deleteShare(c, dstShareURL)

	// set up the source share with numerous files
	fileList := scenarioHelper{}.generateCommonRemoteScenarioForAzureFile(c, srcShareURL, "")
	c.Assert(len(fileList), chk.Not(chk.Equals), 0)

	// add special files that we wish to exclude
	filesToExclude := []string{"notGood.pdf", "excludeSub/lame.jpeg", "exactName"}
	scenarioHelper{}.generateAzureFilesFromList(c, srcShareURL, filesToExclude)
	excludeString := "*.pdf;*.jpeg;exactName"

	// set up interceptor
	mockedRPC := interceptor{}
	Rpc = mockedRPC.intercept
	mockedRPC.init()

	// construct the raw input to simulate user input
	srcShareURLWithSAS := scenarioHelper{}.getRawShareURLWithSAS(c, srcShareName)
	dstShareURLWithSAS := scenarioHelper{}.getRawShareURLWithSAS(c, dstShareName)
	raw := getDefaultSyncRawInput(srcShareURLWithSAS.String(), dstShareURLWithSAS.String())
	raw.exclude = excludeString

	// make sure the list doesn't include the files specified by the exclude flag
	runSyncAndVerify(c, raw, func(err error) {
		c.Assert(err, chk.IsNil)
		validateS2SSyncTransfersAreScheduled(c, "", "", fileList, mockedRPC)
	})
}

// include and exclude flag can work together to limit the scope of source/destination comparison
func (s *cmdIntegrationSuite) TestFileSyncS2SWithIncludeAndExcludeFlag(c *chk.C) {
	fsu := getFSU()
	srcShareURL, srcShareName := createNewAzureShare(c, fsu)
	dstShareURL, dstShareName := createNewAzureShare(c, fsu)
	defer deleteShare(c, srcShareURL)
	defer deleteShare(c, dstShareURL)

	// set up the source share with numerous files
	fileList := scenarioHelper{}.generateCommonRemoteScenarioForAzureFile(c, srcShareURL, "")
	c.Assert(len(fileList), chk.Not(chk.Equals), 0)

	// add special files that we wish to include
	filesToInclude := []string{"important.pdf", "includeSub/amazing.jpeg"}
	scenarioHelper{}.generateAzureFilesFromList(c, srcShareURL, filesToInclude)
	includeString := "*.pdf;*.jpeg;exactName"

	// add special files that we wish to exclude
	// note that the excluded files also match the include string
	filesToExclude := []string{"sorry.pdf", "exclude/notGood.jpeg", "exactName", "sub/exactName"}
	scenarioHelper{}.generateAzureFilesFromList(c, srcShareURL, filesToExclude)
	excludeString := "so*;not*;exactName"

	// set up interceptor
	mockedRPC := interceptor{}
	Rpc = mockedRPC.intercept
	mockedRPC.init()

	// construct the raw input to simulate user input
	srcShareURLWithSAS := scenarioHelper{}.getRawShareURLWithSAS(c, srcShareName)
	dstShareURLWithSAS := scenarioHelper{}.getRawShareURLWithSAS(c, dstShareName)
	raw := getDefaultSyncRawInput(srcShareURLWithSAS.String(), dstShareURLWithSAS.String())
	raw.include = includeString
	raw.exclude = excludeString

	// verify that only the files specified by the include flag are synced
	runSyncAndVerify(c, raw, func(err error) {
		c.Assert(err, chk.IsNil)
		validateS2SSyncTransfersAreScheduled(c, "", "", filesToInclude, mockedRPC)
	})
}

// validate the bug fix for this scenario
func (s *cmdIntegrationSuite) TestFileSyncS2SWithMissingDestination(c *chk.C) {
	fsu := getFSU()
	srcShareURL, srcShareName := createNewAzureShare(c, fsu)
	dstShareURL, dstShareName := createNewAzureShare(c, fsu)
	defer deleteShare(c, srcShareURL)

	// delete the destination share to simulate non-existing destination, or recently removed destination
	deleteShare(c, dstShareURL)

	// set up the share with numerous files
	fileList := scenarioHelper{}.generateCommonRemoteScenarioForAzureFile(c, srcShareURL, "")
	c.Assert(len(fileList), chk.Not(chk.Equals), 0)

	// set up interceptor
	mockedRPC := interceptor{}
	Rpc = mockedRPC.intercept
	mockedRPC.init()

	// construct the raw input to simulate user input
	srcShareURLWithSAS := scenarioHelper{}.getRawShareURLWithSAS(c, srcShareName)
	dstShareURLWithSAS := scenarioHelper{}.getRawShareURLWithSAS(c, dstShareName)
	raw := getDefaultSyncRawInput(srcShareURLWithSAS.String(), dstShareURLWithSAS.String())

	// verify error is thrown
	runSyncAndVerify(c, raw, func(err error) {
		// error should not be nil, but the app should not crash either
		c.Assert(err, chk.NotNil)

		// validate that the right number of transfers were scheduled
		c.Assert(len(mockedRPC.transfers), chk.Equals, 0)
	})
}

// there is a type mismatch between the source and destination
func (s *cmdIntegrationSuite) TestFileSyncS2SMismatchShareAndFile(c *chk.C) {
	fsu := getFSU()
	srcShareURL, srcShareName := createNewAzureShare(c, fsu)
	dstShareURL, dstShareName := createNewAzureShare(c, fsu)
	defer deleteShare(c, srcShareURL)
	defer deleteShare(c, dstShareURL)

	// set up the source share with numerous files
	fileList := scenarioHelper{}.generateCommonRemoteScenarioForAzureFile(c, srcShareURL, "")
	c.Assert(len(fileList), chk.Not(chk.Equals), 0)

	// set up the destination share with a single file
	singleFileName := "single"
	scenarioHelper{}.generateAzureFilesFromList(c, dstShareURL, []string{singleFileName})

	// set up interceptor
	mockedRPC := interceptor{}
	Rpc = mockedRPC.intercept
	mockedRPC.init()

	// construct the raw input to simulate user input
	srcShareURLWithSAS := scenarioHelper{}.getRawShareURLWithSAS(c, srcShareName)
	dstFileURLWithSAS := scenarioHelper{}.getRawFileURLWithSAS(c, dstShareName, singleFileName)
	raw := getDefaultSyncRawInput(srcShareURLWithSAS.String(), dstFileURLWithSAS.String())

	// type mismatch, we should get an error
	runSyncAndVerify(c, raw, func(err error) {
		c.Assert(err, chk.NotNil)

		// validate that the right number of transfers were scheduled
		c.Assert(len(mockedRPC.transfers), chk.Equals, 0)
	})

	// reverse the source and destination
	raw = getDefaultSyncRawInput(dstFileURLWithSAS.String(), srcShareURLWithSAS.String())

	// type mismatch again, we should also get an error
	runSyncAndVerify(c, raw, func(err error) {
		c.Assert(err, chk.NotNil)

		// validate that the right number of transfers were scheduled
		c.Assert(len(mockedRPC.transfers), chk.Equals, 0)
	})
}

// share <-> dir sync
func (s *cmdIntegrationSuite) TestFileSyncS2SShareAndEmptyDir(c *chk.C) {
	fsu := getFSU()
	srcShareURL, srcShareName := createNewAzureShare(c, fsu)
	dstShareURL, dstShareName := createNewAzureShare(c, fsu)
	defer deleteShare(c, srcShareURL)
	defer deleteShare(c, dstShareURL)

	// set up the source share with numerous files
	fileList := scenarioHelper{}.generateCommonRemoteScenarioForAzureFile(c, srcShareURL, "")
	c.Assert(len(fileList), chk.Not(chk.Equals), 0)

	// set up interceptor
	mockedRPC := interceptor{}
	Rpc = mockedRPC.intercept
	mockedRPC.init()

	// construct the raw input to simulate user input
	srcShareURLWithSAS := scenarioHelper{}.getRawShareURLWithSAS(c, srcShareName)
	dirName := "emptydir"
	_, err := dstShareURL.NewDirectoryURL(dirName).Create(context.Background(), azfile.Metadata{})
	c.Assert(err, chk.IsNil)
	dstDirURLWithSAS := scenarioHelper{}.getRawFileURLWithSAS(c, dstShareName, dirName)
	raw := getDefaultSyncRawInput(srcShareURLWithSAS.String(), dstDirURLWithSAS.String())

	// verify that targeting a directory works fine
	runSyncAndVerify(c, raw, func(err error) {
		c.Assert(err, chk.IsNil)

		// validate that the right number of transfers were scheduled
		c.Assert(len(mockedRPC.transfers), chk.Equals, len(fileList))

		// validate that the right transfers were sent
		validateS2SSyncTransfersAreScheduled(c, "", "", fileList, mockedRPC)
	})

	// turn off recursive, this time only top files should be transferred
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

// regular dir -> dir sync
func (s *cmdIntegrationSuite) TestFileSyncS2SBetweenDirs(c *chk.C) {
	fsu := getFSU()
	srcShareURL, srcShareName := createNewAzureShare(c, fsu)
	dstShareURL, dstShareName := createNewAzureShare(c, fsu)
	defer deleteShare(c, srcShareURL)
	defer deleteShare(c, dstShareURL)

	// set up the source share with numerous files
	dirName := "dir"
	fileList := scenarioHelper{}.generateCommonRemoteScenarioForAzureFile(c, srcShareURL, dirName+common.AZCOPY_PATH_SEPARATOR_STRING)
	c.Assert(len(fileList), chk.Not(chk.Equals), 0)

	// set up the destination with the exact same files
	scenarioHelper{}.generateAzureFilesFromList(c, dstShareURL, fileList)

	// set up interceptor
	mockedRPC := interceptor{}
	Rpc = mockedRPC.intercept
	mockedRPC.init()

	// construct the raw input to simulate user input
	srcShareURLWithSAS := scenarioHelper{}.getRawShareURLWithSAS(c, srcShareName)
	dstShareURLWithSAS := scenarioHelper{}.getRawShareURLWithSAS(c, dstShareName)
	srcShareURLWithSAS.Path += common.AZCOPY_PATH_SEPARATOR_STRING + dirName
	dstShareURLWithSAS.Path += common.AZCOPY_PATH_SEPARATOR_STRING + dirName
	raw := getDefaultSyncRawInput(srcShareURLWithSAS.String(), dstShareURLWithSAS.String())

	// nothing should be synced since the source is older
	runSyncAndVerify(c, raw, func(err error) {
		c.Assert(err, chk.IsNil)

		// validate that the right number of transfers were scheduled
		c.Assert(len(mockedRPC.transfers), chk.Equals, 0)
	})

	// refresh the files' last modified time so that they are newer
	scenarioHelper{}.generateAzureFilesFromList(c, srcShareURL, fileList)
	mockedRPC.reset()
	expectedList := scenarioHelper{}.shaveOffPrefix(fileList, dirName+common.AZCOPY_PATH_SEPARATOR_STRING)
	runSyncAndVerify(c, raw, func(err error) {
		c.Assert(err, chk.IsNil)
		validateS2SSyncTransfersAreScheduled(c, "", "", expectedList, mockedRPC)
	})
}
