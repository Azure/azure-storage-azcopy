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
	"os"

	chk "gopkg.in/check.v1"
)

func (s *cmdIntegrationSuite) TestSyncUploadWithExcludeAttrFlag(c *chk.C) {
	bsu := getBSU()

	srcDirName := scenarioHelper{}.generateLocalDirectory(c)
	defer os.RemoveAll(srcDirName)
	fileList := scenarioHelper{}.generateCommonRemoteScenarioForLocal(c, srcDirName, "")

	// add special files with attributes that we wish to exclude
	filesToExclude := []string{"file1.pdf", "file2.txt", "file3"}
	scenarioHelper{}.generateLocalFilesFromList(c, srcDirName, filesToExclude)
	attrList := []string{"H", "I", "C"}
	excludeAttrsStr := "H;I;S"
	scenarioHelper{}.setAttributesForLocalFiles(c, srcDirName, filesToExclude, attrList)

	// set up the destination as an empty container
	containerURL, containerName := createNewContainer(c, bsu)
	defer deleteContainer(c, containerURL)

	// set up interceptor
	mockedRPC := interceptor{}
	Rpc = mockedRPC.intercept
	mockedRPC.init()

	rawContainerURLWithSAS := scenarioHelper{}.getRawContainerURLWithSAS(c, containerName)
	raw := getDefaultSyncRawInput(srcDirName, rawContainerURLWithSAS.String())
	raw.excludeFileAttributes = excludeAttrsStr

	runSyncAndVerify(c, raw, func(err error) {
		c.Assert(err, chk.IsNil)
		validateUploadTransfersAreScheduled(c, "", "", fileList, mockedRPC)
	})
}

func (s *cmdIntegrationSuite) TestSyncUploadWithIncludeAttrFlag(c *chk.C) {
	bsu := getBSU()

	srcDirName := scenarioHelper{}.generateLocalDirectory(c)
	defer os.RemoveAll(srcDirName)
	scenarioHelper{}.generateCommonRemoteScenarioForLocal(c, srcDirName, "")

	// add special files with attributes that we wish to include
	filesToInclude := []string{"file1.txt", "file2.pdf", "file3.pdf"}
	scenarioHelper{}.generateLocalFilesFromList(c, srcDirName, filesToInclude)
	attrList := []string{"H", "I", "C"}
	includeAttrsStr := "H;I;S"
	scenarioHelper{}.setAttributesForLocalFiles(c, srcDirName, filesToInclude, attrList)

	// set up the destination as an empty container
	containerURL, containerName := createNewContainer(c, bsu)
	defer deleteContainer(c, containerURL)

	// set up interceptor
	mockedRPC := interceptor{}
	Rpc = mockedRPC.intercept
	mockedRPC.init()

	rawContainerURLWithSAS := scenarioHelper{}.getRawContainerURLWithSAS(c, containerName)
	raw := getDefaultSyncRawInput(srcDirName, rawContainerURLWithSAS.String())
	raw.includeFileAttributes = includeAttrsStr

	runSyncAndVerify(c, raw, func(err error) {
		c.Assert(err, chk.IsNil)
		validateUploadTransfersAreScheduled(c, "", "", filesToInclude, mockedRPC)
	})
}

// Asserting that name filter and attribute filter are ANDed
// Create one file that matches only the name filter
// Create one file that matches only the attribute filter
// Create one file that matches both
// Only the last file should be transferred
func (s *cmdIntegrationSuite) TestSyncUploadWithIncludeAndIncludeAttrFlags(c *chk.C) {
	bsu := getBSU()

	srcDirName := scenarioHelper{}.generateLocalDirectory(c)
	defer os.RemoveAll(srcDirName)
	scenarioHelper{}.generateCommonRemoteScenarioForLocal(c, srcDirName, "")

	fileList := []string{"file1.txt", "file2.png", "file3.txt"}
	scenarioHelper{}.generateLocalFilesFromList(c, srcDirName, fileList)
	includeString := "*.txt"
	includeAttrsStr := "H;I;S"
	attrList := []string{"H", "I", "C"}
	scenarioHelper{}.setAttributesForLocalFiles(c, srcDirName, fileList[1:], attrList)

	containerURL, containerName := createNewContainer(c, bsu)
	defer deleteContainer(c, containerURL)

	mockedRPC := interceptor{}
	Rpc = mockedRPC.intercept
	mockedRPC.init()

	rawContainerURLWithSAS := scenarioHelper{}.getRawContainerURLWithSAS(c, containerName)
	raw := getDefaultSyncRawInput(srcDirName, rawContainerURLWithSAS.String())
	raw.includeFileAttributes = includeAttrsStr
	raw.include = includeString

	runSyncAndVerify(c, raw, func(err error) {
		c.Assert(err, chk.IsNil)
		validateUploadTransfersAreScheduled(c, "", "", fileList[2:], mockedRPC)
	})
}

// Asserting that name filter and attribute filter are ANDed
// Create one file that matches only the name filter
// Create one file that matches only the attribute filter
// Create one file that matches both
// None of them should be transferred
func (s *cmdIntegrationSuite) TestSyncUploadWithExcludeAndExcludeAttrFlags(c *chk.C) {
	bsu := getBSU()

	srcDirName := scenarioHelper{}.generateLocalDirectory(c)
	defer os.RemoveAll(srcDirName)
	commonFileList := scenarioHelper{}.generateCommonRemoteScenarioForLocal(c, srcDirName, "")

	fileList := []string{"file1.bin", "file2.png", "file3.bin"}
	scenarioHelper{}.generateLocalFilesFromList(c, srcDirName, fileList)
	excludeString := "*.bin"
	excludeAttrsStr := "H;I;S"
	attrList := []string{"H", "I", "C"}
	scenarioHelper{}.setAttributesForLocalFiles(c, srcDirName, fileList[1:], attrList)

	containerURL, containerName := createNewContainer(c, bsu)
	defer deleteContainer(c, containerURL)

	mockedRPC := interceptor{}
	Rpc = mockedRPC.intercept
	mockedRPC.init()

	rawContainerURLWithSAS := scenarioHelper{}.getRawContainerURLWithSAS(c, containerName)
	raw := getDefaultSyncRawInput(srcDirName, rawContainerURLWithSAS.String())
	raw.excludeFileAttributes = excludeAttrsStr
	raw.exclude = excludeString

	runSyncAndVerify(c, raw, func(err error) {
		c.Assert(err, chk.IsNil)
		validateUploadTransfersAreScheduled(c, "", "", commonFileList, mockedRPC)
	})
}
