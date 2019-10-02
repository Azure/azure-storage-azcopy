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

	"github.com/Azure/azure-storage-blob-go/azblob"
	chk "gopkg.in/check.v1"
)

// TestBlobAccountCopyToFileShareS2S actually ends up testing the entire account->container scenario as that is not dependent on destination or source.
func (s *cmdIntegrationSuite) TestBlobAccountCopyToFileShareS2S(c *chk.C) {
	bsu := getBSU()
	fsu := getFSU()

	// Ensure no containers with similar naming schemes exist
	cleanBlobAccount(c, bsu)

	containerSources := map[string]azblob.ContainerURL{}
	expectedTransfers := make([]string, 0)

	for k := range make([]bool, 5) {
		name := generateName(fmt.Sprintf("blobacc-file%dcontainer", k), 63)

		// create the container
		containerSources[name] = bsu.NewContainerURL(name)
		_, err := containerSources[name].Create(ctx, azblob.Metadata{}, azblob.PublicAccessNone)
		c.Assert(err, chk.IsNil)

		// Generate the remote scenario
		fileNames := scenarioHelper{}.generateCommonRemoteScenarioForBlob(c, containerSources[name], "")
		fileNames = scenarioHelper{}.addPrefix(fileNames, name+"/")
		expectedTransfers = append(expectedTransfers, fileNames...)

		// Prepare to delete all 5 containers
		//noinspection GoDeferInLoop
		defer deleteContainer(c, containerSources[name])
	}

	// generate destination share
	dstShareURL, dstShareName := createNewAzureShare(c, fsu)
	defer deleteShare(c, dstShareURL)

	// initialize mocked RPC
	mockedRPC := interceptor{}
	Rpc = mockedRPC.intercept
	mockedRPC.init()

	// generate raw input
	blobServiceURLWithSAS := scenarioHelper{}.getRawBlobServiceURLWithSAS(c)
	blobServiceURLWithSAS.Path = "/blobacc-file*container*" // wildcard the container name
	dstShareURLWithSAS := scenarioHelper{}.getRawShareURLWithSAS(c, dstShareName)
	raw := getDefaultRawCopyInput(blobServiceURLWithSAS.String(), dstShareURLWithSAS.String())

	runCopyAndVerify(c, raw, func(err error) {
		c.Assert(err, chk.IsNil)

		c.Assert(len(mockedRPC.transfers), chk.Equals, len(expectedTransfers))

		validateS2STransfersAreScheduled(c, "/", "/", expectedTransfers, mockedRPC)
	})
}

// TestBlobCopyToFileS2SImplicitDstShare uses a service-level URL on the destination to implicitly create the destination share.
func (s *cmdIntegrationSuite) TestBlobCopyToFileS2SImplicitDstShare(c *chk.C) {
	bsu := getBSU()
	fsu := getFSU()

	// create source container
	srcContainerURL, srcContainerName := createNewContainer(c, bsu)
	defer deleteContainer(c, srcContainerURL)

	// prepare a destination container URL to be deleted.
	dstShareURL := fsu.NewShareURL(srcContainerName)
	defer deleteShare(c, dstShareURL)

	// create a scenario on the source container
	fileList := scenarioHelper{}.generateCommonRemoteScenarioForBlob(c, srcContainerURL, "blobFileImplicitDest")
	c.Assert(len(fileList), chk.Not(chk.Equals), 0) // Ensure that at least one blob is present

	// initialize the mocked RPC
	mockedRPC := interceptor{}
	Rpc = mockedRPC.intercept
	mockedRPC.init()

	// Create raw arguments
	srcContainerURLWithSAS := scenarioHelper{}.getRawContainerURLWithSAS(c, srcContainerName)
	dstServiceURLWithSAS := scenarioHelper{}.getRawFileServiceURLWithSAS(c)
	raw := getDefaultRawCopyInput(srcContainerURLWithSAS.String(), dstServiceURLWithSAS.String())
	// recursive is enabled by default

	// run the copy, check the container, and check the transfer success.
	runCopyAndVerify(c, raw, func(err error) {
		c.Assert(err, chk.IsNil) // Check there was no error

		_, err = dstShareURL.GetProperties(ctx)
		c.Assert(err, chk.IsNil) // Ensure the destination share exists

		// Ensure the transfers were scheduled
		validateS2STransfersAreScheduled(c, "/", "/"+srcContainerName+"/", fileList, mockedRPC)
	})
}

func (s *cmdIntegrationSuite) TestBlobCopyToFileS2SWithSingleFile(c *chk.C) {
	bsu := getBSU()
	fsu := getFSU()

	srcContainerURL, srcContainerName := createNewContainer(c, bsu)
	dstShareURL, dstShareName := createNewAzureShare(c, fsu)
	defer deleteContainer(c, srcContainerURL)
	defer deleteShare(c, dstShareURL)

	// copy to explicit destination
	for _, fileName := range []string{"singlefileisbest", "打麻将.txt", "%4509%4254$85140&"} {
		// set up the source container with a single file
		scenarioHelper{}.generateBlobsFromList(c, srcContainerURL, []string{fileName}, blockBlobDefaultData)

		// set up the interceptor
		mockedRPC := interceptor{}
		Rpc = mockedRPC.intercept
		mockedRPC.init()

		// construct the raw input for explicit destination
		srcBlobURLWithSAS := scenarioHelper{}.getRawBlobURLWithSAS(c, srcContainerName, fileName)
		dstFileURLWithSAS := scenarioHelper{}.getRawFileURLWithSAS(c, dstShareName, fileName)
		raw := getDefaultRawCopyInput(srcBlobURLWithSAS.String(), dstFileURLWithSAS.String())

		runCopyAndVerify(c, raw, func(err error) {
			c.Assert(err, chk.IsNil)

			validateS2STransfersAreScheduled(c, "", "", []string{""}, mockedRPC)
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
		srcBlobURLWithSAS := scenarioHelper{}.getRawBlobURLWithSAS(c, srcContainerName, fileName)
		dstShareURLWithSAS := scenarioHelper{}.getRawShareURLWithSAS(c, dstShareName)
		raw := getDefaultRawCopyInput(srcBlobURLWithSAS.String(), dstShareURLWithSAS.String())

		runCopyAndVerify(c, raw, func(err error) {
			c.Assert(err, chk.IsNil)

			// put the filename in the destination dir name
			// this is because validateS2STransfersAreScheduled dislikes when the relative paths differ
			// In this case, the relative path should absolutely differ. (explicit file path -> implicit)
			validateS2STransfersAreScheduled(c, "", "/"+strings.ReplaceAll(fileName, "%", "%25"), []string{""}, mockedRPC)
		})
	}
}

func (s *cmdIntegrationSuite) TestContainerToShareCopyS2S(c *chk.C) {
	bsu := getBSU()
	fsu := getFSU()

	// Create source container and destination share, schedule their deletion
	srcContainerURL, srcContainerName := createNewContainer(c, bsu)
	dstShareURL, dstShareName := createNewAzureShare(c, fsu)
	defer deleteContainer(c, srcContainerURL)
	defer deleteShare(c, dstShareURL)

	// set up the source container with numerous files
	fileList := scenarioHelper{}.generateCommonRemoteScenarioForBlob(c, srcContainerURL, "")
	c.Assert(len(fileList), chk.Not(chk.Equals), 0)

	// set up the interceptor
	mockedRPC := interceptor{}
	Rpc = mockedRPC.intercept
	mockedRPC.init()

	// set up the raw input with recursive = true to copy
	srcContainerURLWithSAS := scenarioHelper{}.getRawContainerURLWithSAS(c, srcContainerName)
	dstShareURLWithSAS := scenarioHelper{}.getRawShareURLWithSAS(c, dstShareName)
	raw := getDefaultRawCopyInput(srcContainerURLWithSAS.String(), dstShareURLWithSAS.String())

	runCopyAndVerify(c, raw, func(err error) {
		c.Assert(err, chk.IsNil)

		// validate the transfer count is correct
		c.Assert(len(mockedRPC.transfers), chk.Equals, len(fileList))

		validateS2STransfersAreScheduled(c, "/", "/", fileList, mockedRPC)
	})

	// turn off recursive and set recursive to false
	raw.recursive = false
	mockedRPC.reset()

	runCopyAndVerify(c, raw, func(err error) {
		c.Assert(err, chk.NotNil)
		// make sure that the failure was due to the recursive flag
		c.Assert(err.Error(), StringContains, "recursive")
	})
}

func (s *cmdIntegrationSuite) TestBlobFileCopyS2SWithIncludeAndIncludeDirFlag(c *chk.C) {
	bsu := getBSU()
	fsu := getFSU()

	// generate source container and destination fileshare
	srcContainerURL, srcContainerName := createNewContainer(c, bsu)
	dstShareURL, dstShareName := createNewAzureShare(c, fsu)
	defer deleteContainer(c, srcContainerURL)
	defer deleteShare(c, dstShareURL)

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
	scenarioHelper{}.generateBlobsFromList(c, srcContainerURL, fileList, blockBlobDefaultData)

	// set up the interceptor
	mockedRPC := interceptor{}
	Rpc = mockedRPC.intercept
	mockedRPC.init()

	// construct the raw input
	srcContainerURLWithSAS := scenarioHelper{}.getRawContainerURLWithSAS(c, srcContainerName)
	dstShareURLWithSAS := scenarioHelper{}.getRawShareURLWithSAS(c, dstShareName)
	raw := getDefaultRawCopyInput(srcContainerURLWithSAS.String(), dstShareURLWithSAS.String())
	raw.include = includeString
	raw.recursive = true

	runCopyAndVerify(c, raw, func(err error) {
		c.Assert(err, chk.IsNil)
		validateS2STransfersAreScheduled(c, "/", "/", loneIncludeList, mockedRPC)
	})

	mockedRPC.reset()
	raw.includePath = includePathString
	runCopyAndVerify(c, raw, func(err error) {
		c.Assert(err, chk.IsNil)
		validateS2STransfersAreScheduled(c, "/", "/", includePathAndIncludeList, mockedRPC)
	})
}

func (s *cmdIntegrationSuite) TestBlobToFileCopyS2SWithExcludeAndExcludeDirFlag(c *chk.C) {
	bsu := getBSU()
	fsu := getFSU()

	// generate source container and destination fileshare
	srcContainerURL, srcContainerName := createNewContainer(c, bsu)
	dstShareURL, dstShareName := createNewAzureShare(c, fsu)
	defer deleteContainer(c, srcContainerURL)
	defer deleteShare(c, dstShareURL)

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
	scenarioHelper{}.generateBlobsFromList(c, srcContainerURL, fileList, blockBlobDefaultData)

	// set up the interceptor
	mockedRPC := interceptor{}
	Rpc = mockedRPC.intercept
	mockedRPC.init()

	// construct the raw input
	srcContainerURLWithSAS := scenarioHelper{}.getRawContainerURLWithSAS(c, srcContainerName)
	dstShareURLWithSAS := scenarioHelper{}.getRawShareURLWithSAS(c, dstShareName)
	raw := getDefaultRawCopyInput(srcContainerURLWithSAS.String(), dstShareURLWithSAS.String())
	raw.exclude = excludeString
	raw.recursive = true

	runCopyAndVerify(c, raw, func(err error) {
		c.Assert(err, chk.IsNil)
		validateS2STransfersAreScheduled(c, "/", "/", loneExcludeList, mockedRPC)
	})

	mockedRPC.reset()
	raw.excludePath = excludePathString
	runCopyAndVerify(c, raw, func(err error) {
		c.Assert(err, chk.IsNil)
		validateS2STransfersAreScheduled(c, "/", "/", excludePathAndExcludeList, mockedRPC)
	})
}

func (s *cmdIntegrationSuite) TestBlobToFileCopyS2SIncludeExcludeMix(c *chk.C) {
	bsu := getBSU()
	fsu := getFSU()

	// generate source container and destination fileshare
	srcContainerURL, srcContainerName := createNewContainer(c, bsu)
	dstShareURL, dstShareName := createNewAzureShare(c, fsu)
	defer deleteContainer(c, srcContainerURL)
	defer deleteShare(c, dstShareURL)

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
	scenarioHelper{}.generateBlobsFromList(c, srcContainerURL, fileList, blockBlobDefaultData)

	// set up the interceptor
	mockedRPC := interceptor{}
	Rpc = mockedRPC.intercept
	mockedRPC.init()

	// construct the raw input
	srcContainerURLWithSAS := scenarioHelper{}.getRawContainerURLWithSAS(c, srcContainerName)
	dstShareURLWithSAS := scenarioHelper{}.getRawShareURLWithSAS(c, dstShareName)
	raw := getDefaultRawCopyInput(srcContainerURLWithSAS.String(), dstShareURLWithSAS.String())
	raw.include = includeString
	raw.exclude = excludeString
	raw.recursive = true

	runCopyAndVerify(c, raw, func(err error) {
		c.Assert(err, chk.IsNil)
		validateS2STransfersAreScheduled(c, "/", "/", toInclude, mockedRPC)
	})
}

func (s *cmdIntegrationSuite) TestBlobToFileCopyS2SWithDirectory(c *chk.C) {
	bsu := getBSU()
	fsu := getFSU()

	// create container and share
	srcContainerURL, srcContainerName := createNewContainer(c, bsu)
	dstShareURL, dstShareName := createNewAzureShare(c, fsu)
	defer deleteContainer(c, srcContainerURL)
	defer deleteShare(c, dstShareURL)

	// create source scenario
	dirName := "copyme"
	fileList := scenarioHelper{}.generateCommonRemoteScenarioForBlob(c, srcContainerURL, dirName+"/")
	c.Assert(len(fileList), chk.Not(chk.Equals), 0)

	// initialize mocked RPC
	mockedRPC := interceptor{}
	Rpc = mockedRPC.intercept
	mockedRPC.init()

	// generate raw copy command
	srcContainerURLWithSAS := scenarioHelper{}.getRawContainerURLWithSAS(c, srcContainerName)
	dstShareURLWithSAS := scenarioHelper{}.getRawShareURLWithSAS(c, dstShareName)
	srcContainerURLWithSAS.Path += "/copyme/"
	raw := getDefaultRawCopyInput(srcContainerURLWithSAS.String(), dstShareURLWithSAS.String())
	raw.recursive = true

	// test folder copies
	expectedList := scenarioHelper{}.shaveOffPrefix(fileList, dirName+"/")
	runCopyAndVerify(c, raw, func(err error) {
		c.Assert(err, chk.IsNil)
		validateS2STransfersAreScheduled(c, "/", "/"+dirName+"/", expectedList, mockedRPC)
	})
}
