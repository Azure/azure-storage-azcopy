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
	chk "gopkg.in/check.v1"
)

func (s *cmdIntegrationSuite) TestSyncUploadWithExcludeAttrFlag(c *chk.C) {
	bsu := getBSU()

	srcDirName := scenarioHelper{}.generateLocalDirectory(c)
	fileList := scenarioHelper{}.generateCommonRemoteScenarioForLocal(c, srcDirName, "")

	// add special files with attributes that we wish to exclude
	filesToExclude := []string{"file1.pdf", "file2.txt", "file3"}
	scenarioHelper{}.generateLocalFilesFromList(c, srcDirName, filesToExclude)
	attrList := []string{"H", "I", "C"}
	// exclude files with H, I and S attributes
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
	scenarioHelper{}.generateCommonRemoteScenarioForLocal(c, srcDirName, "")

	// add special files with attributes that we wish to include
	filesToInclude := []string{"file1.txt", "file2.pdf", "file3.pdf"}
	scenarioHelper{}.generateLocalFilesFromList(c, srcDirName, filesToInclude)
	includeAttrsStr := "H;I"
	attrList := []string{"H", "I"}
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
