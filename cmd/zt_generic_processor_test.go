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
	"os"
	"path/filepath"
	"time"
)

type genericProcessorSuite struct{}

var _ = chk.Suite(&genericProcessorSuite{})

type processorTestSuiteHelper struct{}

// return a list of sample entities
func (processorTestSuiteHelper) getSampleObjectList() []storedObject {
	return []storedObject{
		{name: "file1", relativePath: "file1", lastModifiedTime: time.Now()},
		{name: "file2", relativePath: "file2", lastModifiedTime: time.Now()},
		{name: "file3", relativePath: "sub1/file3", lastModifiedTime: time.Now()},
		{name: "file4", relativePath: "sub1/file4", lastModifiedTime: time.Now()},
		{name: "file5", relativePath: "sub1/sub2/file5", lastModifiedTime: time.Now()},
		{name: "file6", relativePath: "sub1/sub2/file6", lastModifiedTime: time.Now()},
	}
}

// given a list of entities, return the relative paths in a list, to help with validations
func (processorTestSuiteHelper) getExpectedTransferFromStoredObjectList(storedObjectList []storedObject) []string {
	expectedTransfers := make([]string, 0)
	for _, storedObject := range storedObjectList {
		expectedTransfers = append(expectedTransfers, storedObject.relativePath)
	}

	return expectedTransfers
}

func (processorTestSuiteHelper) getCopyJobTemplate() *common.CopyJobPartOrderRequest {
	return &common.CopyJobPartOrderRequest{Fpo: common.EFolderPropertiesOption.NoFolders()}
}

func (s *genericProcessorSuite) TestCopyTransferProcessorMultipleFiles(c *chk.C) {
	bsu := getBSU()

	// set up source and destination
	containerURL, _ := getContainerURL(c, bsu)
	dstDirName := scenarioHelper{}.generateLocalDirectory(c)
	defer os.RemoveAll(dstDirName)

	// set up interceptor
	mockedRPC := interceptor{}
	Rpc = mockedRPC.intercept
	mockedRPC.init()

	// exercise the processor
	sampleObjects := processorTestSuiteHelper{}.getSampleObjectList()
	for _, numOfParts := range []int{1, 3} {
		numOfTransfersPerPart := len(sampleObjects) / numOfParts
		copyProcessor := newCopyTransferProcessor(processorTestSuiteHelper{}.getCopyJobTemplate(), numOfTransfersPerPart,
			newRemoteRes(containerURL.String()), newLocalRes(dstDirName), nil, nil, false)

		// go through the objects and make sure they are processed without error
		for _, storedObject := range sampleObjects {
			err := copyProcessor.scheduleCopyTransfer(storedObject)
			c.Assert(err, chk.IsNil)
		}

		// make sure everything has been dispatched apart from the final one
		c.Assert(copyProcessor.copyJobTemplate.PartNum, chk.Equals, common.PartNumber(numOfParts-1))

		// dispatch final part
		jobInitiated, err := copyProcessor.dispatchFinalPart()
		c.Assert(jobInitiated, chk.Equals, true)
		c.Assert(err, chk.IsNil)

		// assert the right transfers were scheduled
		validateCopyTransfersAreScheduled(c, false, false, "", "", processorTestSuiteHelper{}.getExpectedTransferFromStoredObjectList(sampleObjects), mockedRPC)

		mockedRPC.reset()
	}
}

func (s *genericProcessorSuite) TestCopyTransferProcessorSingleFile(c *chk.C) {
	bsu := getBSU()
	containerURL, _ := createNewContainer(c, bsu)
	defer deleteContainer(c, containerURL)

	// set up the container with a single blob
	blobList := []string{"singlefile101"}
	scenarioHelper{}.generateBlobsFromList(c, containerURL, blobList, blockBlobDefaultData)
	c.Assert(containerURL, chk.NotNil)

	// set up the directory with a single file
	dstDirName := scenarioHelper{}.generateLocalDirectory(c)
	defer os.RemoveAll(dstDirName)
	dstFileName := blobList[0]
	scenarioHelper{}.generateLocalFilesFromList(c, dstDirName, blobList)

	// set up interceptor
	mockedRPC := interceptor{}
	Rpc = mockedRPC.intercept
	mockedRPC.init()

	// set up the processor
	blobURL := containerURL.NewBlockBlobURL(blobList[0]).String()
	copyProcessor := newCopyTransferProcessor(processorTestSuiteHelper{}.getCopyJobTemplate(), 2,
		newRemoteRes(blobURL), newLocalRes(filepath.Join(dstDirName, dstFileName)), nil, nil, false)

	// exercise the copy transfer processor
	storedObject := newStoredObject(noPreProccessor, blobList[0], "", common.EEntityType.File(), time.Now(), 0, noContentProps, noBlobProps, noMetdata, "")
	err := copyProcessor.scheduleCopyTransfer(storedObject)
	c.Assert(err, chk.IsNil)

	// no part should have been dispatched
	c.Assert(copyProcessor.copyJobTemplate.PartNum, chk.Equals, common.PartNumber(0))

	// dispatch final part
	jobInitiated, err := copyProcessor.dispatchFinalPart()
	c.Assert(jobInitiated, chk.Equals, true)

	// In cases of syncing file to file, the source and destination are empty because this info is already in the root
	// path.
	c.Assert(mockedRPC.transfers[0].Source, chk.Equals, "")
	c.Assert(mockedRPC.transfers[0].Destination, chk.Equals, "")

	// assert the right transfers were scheduled
	validateCopyTransfersAreScheduled(c, false, false, "", "", []string{""}, mockedRPC)
}
