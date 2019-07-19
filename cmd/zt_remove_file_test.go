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
	"strings"
)

func (s *cmdIntegrationSuite) TestRemoveSingleFile(c *chk.C) {
	fsu := getFSU()
	shareURL, shareName := createNewAzureShare(c, fsu)
	defer deleteShare(c, shareURL)

	for _, fileName := range []string{"top/mid/low/singlefileisbest", "打麻将.txt", "%4509%4254$85140&"} {
		// set up the share with a single file
		fileList := []string{fileName}
		scenarioHelper{}.generateAzureFilesFromList(c, shareURL, fileList)
		c.Assert(shareURL, chk.NotNil)

		// set up interceptor
		mockedRPC := interceptor{}
		Rpc = mockedRPC.intercept
		mockedRPC.init()

		// construct the raw input to simulate user input
		rawFileURLWithSAS := scenarioHelper{}.getRawFileURLWithSAS(c, shareName, fileList[0])
		raw := getDefaultRemoveRawInput(rawFileURLWithSAS.String())

		runCopyAndVerify(c, raw, func(err error) {
			c.Assert(err, chk.IsNil)

			// note that when we are targeting single files, the relative path is empty ("") since the root path already points to the file
			validateRemoveTransfersAreScheduled(c, true, []string{""}, mockedRPC)
		})
	}
}

func (s *cmdIntegrationSuite) TestRemoveFilesUnderShare(c *chk.C) {
	fsu := getFSU()

	// set up the share with numerous files
	shareURL, shareName := createNewAzureShare(c, fsu)
	defer deleteShare(c, shareURL)
	fileList := scenarioHelper{}.generateCommonRemoteScenarioForAzureFile(c, shareURL, "")
	c.Assert(shareURL, chk.NotNil)
	c.Assert(len(fileList), chk.Not(chk.Equals), 0)

	// set up interceptor
	mockedRPC := interceptor{}
	Rpc = mockedRPC.intercept
	mockedRPC.init()

	// construct the raw input to simulate user input
	rawShareURLWithSAS := scenarioHelper{}.getRawShareURLWithSAS(c, shareName)
	raw := getDefaultRemoveRawInput(rawShareURLWithSAS.String())
	raw.recursive = true

	runCopyAndVerify(c, raw, func(err error) {
		c.Assert(err, chk.IsNil)

		// validate that the right number of transfers were scheduled
		c.Assert(len(mockedRPC.transfers), chk.Equals, len(fileList))

		// validate that the right transfers were sent
		validateRemoveTransfersAreScheduled(c, true, fileList, mockedRPC)
	})

	// turn off recursive, this time only top files should be deleted
	raw.recursive = false
	mockedRPC.reset()

	runCopyAndVerify(c, raw, func(err error) {
		c.Assert(err, chk.IsNil)
		c.Assert(len(mockedRPC.transfers), chk.Not(chk.Equals), len(fileList))

		for _, transfer := range mockedRPC.transfers {
			c.Assert(strings.Contains(transfer.Source, common.AZCOPY_PATH_SEPARATOR_STRING), chk.Equals, false)
		}
	})
}

func (s *cmdIntegrationSuite) TestRemoveFilesUnderDirectory(c *chk.C) {
	fsu := getFSU()
	dirName := "dir1/dir2/dir3/"

	// set up the share with numerous files
	shareURL, shareName := createNewAzureShare(c, fsu)
	defer deleteShare(c, shareURL)
	fileList := scenarioHelper{}.generateCommonRemoteScenarioForAzureFile(c, shareURL, dirName)
	c.Assert(shareURL, chk.NotNil)
	c.Assert(len(fileList), chk.Not(chk.Equals), 0)

	// set up interceptor
	mockedRPC := interceptor{}
	Rpc = mockedRPC.intercept
	mockedRPC.init()

	// construct the raw input to simulate user input
	rawDirectoryURLWithSAS := scenarioHelper{}.getRawFileURLWithSAS(c, shareName, dirName)
	raw := getDefaultRemoveRawInput(rawDirectoryURLWithSAS.String())
	raw.recursive = true

	runCopyAndVerify(c, raw, func(err error) {
		c.Assert(err, chk.IsNil)

		// validate that the right number of transfers were scheduled
		c.Assert(len(mockedRPC.transfers), chk.Equals, len(fileList))

		// validate that the right transfers were sent
		expectedTransfers := scenarioHelper{}.shaveOffPrefix(fileList, dirName)
		validateRemoveTransfersAreScheduled(c, true, expectedTransfers, mockedRPC)
	})

	// turn off recursive, this time only top files should be deleted
	raw.recursive = false
	mockedRPC.reset()

	runCopyAndVerify(c, raw, func(err error) {
		c.Assert(err, chk.IsNil)
		c.Assert(len(mockedRPC.transfers), chk.Not(chk.Equals), len(fileList))

		for _, transfer := range mockedRPC.transfers {
			c.Assert(strings.Contains(transfer.Source, common.AZCOPY_PATH_SEPARATOR_STRING), chk.Equals, false)
		}
	})
}

// include flag limits the scope of the delete
func (s *cmdIntegrationSuite) TestRemoveFilesWithIncludeFlag(c *chk.C) {
	fsu := getFSU()

	// set up the share with numerous files
	shareURL, shareName := createNewAzureShare(c, fsu)
	fileList := scenarioHelper{}.generateCommonRemoteScenarioForAzureFile(c, shareURL, "")
	defer deleteShare(c, shareURL)
	c.Assert(shareURL, chk.NotNil)
	c.Assert(len(fileList), chk.Not(chk.Equals), 0)

	// add special files that we wish to include
	filesToInclude := []string{"important.pdf", "includeSub/amazing.jpeg", "exactName"}
	scenarioHelper{}.generateAzureFilesFromList(c, shareURL, filesToInclude)
	includeString := "*.pdf;*.jpeg;exactName"

	// set up interceptor
	mockedRPC := interceptor{}
	Rpc = mockedRPC.intercept
	mockedRPC.init()

	// construct the raw input to simulate user input
	rawShareURLWithSAS := scenarioHelper{}.getRawShareURLWithSAS(c, shareName)
	raw := getDefaultRemoveRawInput(rawShareURLWithSAS.String())
	raw.include = includeString
	raw.recursive = true

	runCopyAndVerify(c, raw, func(err error) {
		c.Assert(err, chk.IsNil)
		validateDownloadTransfersAreScheduled(c, "", "", filesToInclude, mockedRPC)
	})
}

// exclude flag limits the scope of the delete
func (s *cmdIntegrationSuite) TestRemoveFilesWithExcludeFlag(c *chk.C) {
	fsu := getFSU()

	// set up the share with numerous files
	shareURL, shareName := createNewAzureShare(c, fsu)
	fileList := scenarioHelper{}.generateCommonRemoteScenarioForAzureFile(c, shareURL, "")
	defer deleteShare(c, shareURL)
	c.Assert(shareURL, chk.NotNil)
	c.Assert(len(fileList), chk.Not(chk.Equals), 0)

	// add special files that we wish to exclude
	filesToExclude := []string{"notGood.pdf", "excludeSub/lame.jpeg", "exactName"}
	scenarioHelper{}.generateAzureFilesFromList(c, shareURL, filesToExclude)
	excludeString := "*.pdf;*.jpeg;exactName"

	// set up interceptor
	mockedRPC := interceptor{}
	Rpc = mockedRPC.intercept
	mockedRPC.init()

	// construct the raw input to simulate user input
	rawShareURLWithSAS := scenarioHelper{}.getRawShareURLWithSAS(c, shareName)
	raw := getDefaultRemoveRawInput(rawShareURLWithSAS.String())
	raw.exclude = excludeString
	raw.recursive = true

	runCopyAndVerify(c, raw, func(err error) {
		c.Assert(err, chk.IsNil)
		validateDownloadTransfersAreScheduled(c, "", "", fileList, mockedRPC)
	})
}

// include and exclude flag can work together to limit the scope of the delete
func (s *cmdIntegrationSuite) TestRemoveFilesWithIncludeAndExcludeFlag(c *chk.C) {
	fsu := getFSU()

	// set up the share with numerous files
	shareURL, shareName := createNewAzureShare(c, fsu)
	fileList := scenarioHelper{}.generateCommonRemoteScenarioForAzureFile(c, shareURL, "")
	defer deleteShare(c, shareURL)
	c.Assert(shareURL, chk.NotNil)
	c.Assert(len(fileList), chk.Not(chk.Equals), 0)

	// add special files that we wish to include
	filesToInclude := []string{"important.pdf", "includeSub/amazing.jpeg"}
	scenarioHelper{}.generateAzureFilesFromList(c, shareURL, filesToInclude)
	includeString := "*.pdf;*.jpeg;exactName"

	// add special files that we wish to exclude
	// note that the excluded files also match the include string
	filesToExclude := []string{"sorry.pdf", "exclude/notGood.jpeg", "exactName", "sub/exactName"}
	scenarioHelper{}.generateAzureFilesFromList(c, shareURL, filesToExclude)
	excludeString := "so*;not*;exactName"

	// set up interceptor
	mockedRPC := interceptor{}
	Rpc = mockedRPC.intercept
	mockedRPC.init()

	// construct the raw input to simulate user input
	rawShareURLWithSAS := scenarioHelper{}.getRawShareURLWithSAS(c, shareName)
	raw := getDefaultRemoveRawInput(rawShareURLWithSAS.String())
	raw.include = includeString
	raw.exclude = excludeString
	raw.recursive = true

	runCopyAndVerify(c, raw, func(err error) {
		c.Assert(err, chk.IsNil)
		validateDownloadTransfersAreScheduled(c, "", "", filesToInclude, mockedRPC)
	})
}

// note: list-of-files flag is used
func (s *cmdIntegrationSuite) TestRemoveListOfFilesAndDirectories(c *chk.C) {
	fsu := getFSU()
	dirName := "megadir"

	// set up the share with numerous files
	shareURL, shareName := createNewAzureShare(c, fsu)
	c.Assert(shareURL, chk.NotNil)
	defer deleteShare(c, shareURL)
	individualFilesList := scenarioHelper{}.generateCommonRemoteScenarioForAzureFile(c, shareURL, "")
	filesUnderTopDir := scenarioHelper{}.generateCommonRemoteScenarioForAzureFile(c, shareURL, dirName+"/")
	fileList := append(individualFilesList, filesUnderTopDir...)
	c.Assert(len(fileList), chk.Not(chk.Equals), 0)

	// set up interceptor
	mockedRPC := interceptor{}
	Rpc = mockedRPC.intercept
	mockedRPC.init()

	// construct the raw input to simulate user input
	rawShareURLWithSAS := scenarioHelper{}.getRawShareURLWithSAS(c, shareName)
	raw := getDefaultRemoveRawInput(rawShareURLWithSAS.String())
	raw.recursive = true

	// make the input for list-of-files
	listOfFiles := append(individualFilesList, dirName)

	// add some random files that don't actually exist
	listOfFiles = append(listOfFiles, "WUTAMIDOING")
	listOfFiles = append(listOfFiles, "DONTKNOW")
	raw.listOfFilesToCopy = scenarioHelper{}.generateListOfFiles(c, listOfFiles)

	runCopyAndVerify(c, raw, func(err error) {
		c.Assert(err, chk.IsNil)

		// validate that the right number of transfers were scheduled
		c.Assert(len(mockedRPC.transfers), chk.Equals, len(fileList))

		// validate that the right transfers were sent
		validateRemoveTransfersAreScheduled(c, true, fileList, mockedRPC)
	})

	// turn off recursive, this time only top files should be deleted
	raw.recursive = false
	mockedRPC.reset()

	runCopyAndVerify(c, raw, func(err error) {
		c.Assert(err, chk.IsNil)
		c.Assert(len(mockedRPC.transfers), chk.Not(chk.Equals), len(fileList))

		for _, transfer := range mockedRPC.transfers {
			source, err := url.PathUnescape(transfer.Source)
			c.Assert(err, chk.IsNil)

			// if the transfer is under the given dir, make sure only the top level files were scheduled
			if strings.HasPrefix(source, dirName) {
				trimmedSource := strings.TrimPrefix(source, dirName+"/")
				c.Assert(strings.Contains(trimmedSource, common.AZCOPY_PATH_SEPARATOR_STRING), chk.Equals, false)
			}
		}
	})
}

// include and exclude flag can work together to limit the scope of the delete
func (s *cmdIntegrationSuite) TestRemoveListOfFilesWithIncludeAndExclude(c *chk.C) {
	fsu := getFSU()
	dirName := "megadir"

	// set up the share with numerous files
	shareURL, shareName := createNewAzureShare(c, fsu)
	c.Assert(shareURL, chk.NotNil)
	defer deleteShare(c, shareURL)
	individualFilesList := scenarioHelper{}.generateCommonRemoteScenarioForAzureFile(c, shareURL, "")
	scenarioHelper{}.generateCommonRemoteScenarioForAzureFile(c, shareURL, dirName+"/")

	// add special files that we wish to include
	filesToInclude := []string{"important.pdf", "includeSub/amazing.jpeg"}
	scenarioHelper{}.generateAzureFilesFromList(c, shareURL, filesToInclude)
	includeString := "*.pdf;*.jpeg;exactName"

	// add special files that we wish to exclude
	// note that the excluded files also match the include string
	filesToExclude := []string{"sorry.pdf", "exclude/notGood.jpeg", "exactName", "sub/exactName"}
	scenarioHelper{}.generateAzureFilesFromList(c, shareURL, filesToExclude)
	excludeString := "so*;not*;exactName"

	// set up interceptor
	mockedRPC := interceptor{}
	Rpc = mockedRPC.intercept
	mockedRPC.init()

	// construct the raw input to simulate user input
	rawShareURLWithSAS := scenarioHelper{}.getRawShareURLWithSAS(c, shareName)
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
	raw.listOfFilesToCopy = scenarioHelper{}.generateListOfFiles(c, listOfFiles)

	runCopyAndVerify(c, raw, func(err error) {
		c.Assert(err, chk.IsNil)

		// validate that the right number of transfers were scheduled
		c.Assert(len(mockedRPC.transfers), chk.Equals, len(filesToInclude))

		// validate that the right transfers were sent
		validateRemoveTransfersAreScheduled(c, true, filesToInclude, mockedRPC)
	})
}
