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
	"strings"

	chk "gopkg.in/check.v1"
)

// regular file->file copy
func (s *cmdIntegrationSuite) TestFileCopyS2SWithSingleFile(c *chk.C) {
	fsu := getFSU()
	srcShareURL, srcShareName := createNewAzureShare(c, fsu)
	dstShareURL, dstShareName := createNewAzureShare(c, fsu)
	defer deleteShare(c, srcShareURL)
	defer deleteShare(c, dstShareURL)

	for _, fileName := range []string{"singlefileisbest", "打麻将.txt", "%4509%4254$85140&"} {
		// set up the source share with a single file
		fileList := []string{fileName}
		scenarioHelper{}.generateAzureFilesFromList(c, srcShareURL, fileList)

		// set up interceptor
		mockedRPC := interceptor{}
		Rpc = mockedRPC.intercept
		mockedRPC.init()

		// construct the raw input to simulate user input
		srcFileURLWithSAS := scenarioHelper{}.getRawFileURLWithSAS(c, srcShareName, fileList[0])
		dstFileURLWithSAS := scenarioHelper{}.getRawFileURLWithSAS(c, dstShareName, fileList[0])
		raw := getDefaultCopyRawInput(srcFileURLWithSAS.String(), dstFileURLWithSAS.String())

		runCopyAndVerify(c, raw, func(err error) {
			c.Assert(err, chk.IsNil)
			validateS2STransfersAreScheduled(c, "", "", []string{""}, mockedRPC)
		})
	}

	for _, fileName := range []string{"singlefileisbest", "打麻将.txt", "%4509%4254$85140&"} {
		// No need to generate files since we already have them

		// set up interceptor
		mockedRPC := interceptor{}
		Rpc = mockedRPC.intercept
		mockedRPC.init()

		// construct the raw input to simulate user input
		srcFileURLWithSAS := scenarioHelper{}.getRawFileURLWithSAS(c, srcShareName, fileName)
		dstShareURLWithSAS := scenarioHelper{}.getRawShareURLWithSAS(c, dstShareName)
		raw := getDefaultCopyRawInput(srcFileURLWithSAS.String(), dstShareURLWithSAS.String())

		runCopyAndVerify(c, raw, func(err error) {
			c.Assert(err, chk.IsNil)

			// put the filename in the destination dir name
			// this is because validateS2STransfersAreScheduled dislikes when the relative paths differ
			// In this case, the relative path should absolutely differ. (explicit file path -> implicit)
			validateS2STransfersAreScheduled(c, "", "/" + strings.ReplaceAll(fileName, "%", "%25"), []string{""}, mockedRPC)
		})
	}
}

// regular share->share copy
func (s *cmdIntegrationSuite) TestFileCopyS2SWithShares(c *chk.C) {
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
	raw := getDefaultCopyRawInput(srcShareURLWithSAS.String(), dstShareURLWithSAS.String())
	raw.recursive = true

	// all files at source should be copied to destination
	runCopyAndVerify(c, raw, func(err error) {
		c.Assert(err, chk.IsNil)

		// validate that the right number of transfers were scheduled
		c.Assert(len(mockedRPC.transfers), chk.Equals, len(fileList))

		// validate that the right transfers were sent
		validateS2STransfersAreScheduled(c, "/", "/", fileList, mockedRPC)
	})

	// turn off recursive, we should be getting an error
	raw.recursive = false
	mockedRPC.reset()
	runCopyAndVerify(c, raw, func(err error) {
		c.Assert(err, chk.NotNil)
		// make sure the failure was due to the recursive flag
		c.Assert(err.Error(), StringContains, "recursive")
	})
}

// include flag limits the scope of source/destination comparison
func (s *cmdIntegrationSuite) TestFileCopyS2SWithIncludeFlag(c *chk.C) {
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
	raw := getDefaultCopyRawInput(srcShareURLWithSAS.String(), dstShareURLWithSAS.String())
	raw.include = includeString
	raw.recursive = true

	// verify that only the files specified by the include flag are copyed
	runCopyAndVerify(c, raw, func(err error) {
		c.Assert(err, chk.IsNil)
		validateS2STransfersAreScheduled(c, "/", "/", filesToInclude, mockedRPC)
	})
}

// exclude flag limits the scope of source/destination comparison
func (s *cmdIntegrationSuite) TestFileCopyS2SWithExcludeFlag(c *chk.C) {
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
	raw := getDefaultCopyRawInput(srcShareURLWithSAS.String(), dstShareURLWithSAS.String())
	raw.exclude = excludeString
	raw.recursive = true

	// make sure the list doesn't include the files specified by the exclude flag
	runCopyAndVerify(c, raw, func(err error) {
		c.Assert(err, chk.IsNil)
		validateS2STransfersAreScheduled(c, "/", "/", fileList, mockedRPC)
	})
}

// include and exclude flag can work together to limit the scope of source/destination comparison
func (s *cmdIntegrationSuite) TestFileCopyS2SWithIncludeAndExcludeFlag(c *chk.C) {
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
	raw := getDefaultCopyRawInput(srcShareURLWithSAS.String(), dstShareURLWithSAS.String())
	raw.include = includeString
	raw.exclude = excludeString
	raw.recursive = true

	// verify that only the files specified by the include flag are copyed
	runCopyAndVerify(c, raw, func(err error) {
		c.Assert(err, chk.IsNil)
		validateS2STransfersAreScheduled(c, "/", "/", filesToInclude, mockedRPC)
	})
}

// regular dir -> dir copy
func (s *cmdIntegrationSuite) TestFileCopyS2SWithDirectory(c *chk.C) {
	fsu := getFSU()
	srcShareURL, srcShareName := createNewAzureShare(c, fsu)
	dstShareURL, dstShareName := createNewAzureShare(c, fsu)
	defer deleteShare(c, srcShareURL)
	defer deleteShare(c, dstShareURL)

	// set up the source share with numerous files
	dirName := "dir"
	fileList := scenarioHelper{}.generateCommonRemoteScenarioForAzureFile(c, srcShareURL, dirName+"/")
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
	srcShareURLWithSAS.Path += "/" + dirName
	raw := getDefaultCopyRawInput(srcShareURLWithSAS.String(), dstShareURLWithSAS.String())
	raw.recursive = true

	expectedList := scenarioHelper{}.shaveOffPrefix(fileList, dirName+"/")
	runCopyAndVerify(c, raw, func(err error) {
		c.Assert(err, chk.IsNil)
		validateS2STransfersAreScheduled(c, "/", "/"+dirName+"/", expectedList, mockedRPC)
	})
}
