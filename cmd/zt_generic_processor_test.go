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
	chk "gopkg.in/check.v1"
	"os"
	"path/filepath"
	"testing"
	"time"
)

type genericProcessorSuite struct{}

var _ = chk.Suite(&genericProcessorSuite{})

type processorTestSuiteHelper struct{}

// return a list of sample entities
func (processorTestSuiteHelper) getSampleObjectList() []StoredObject {
	return []StoredObject{
		{name: "file1", relativePath: "file1", lastModifiedTime: time.Now()},
		{name: "file2", relativePath: "file2", lastModifiedTime: time.Now()},
		{name: "file3", relativePath: "sub1/file3", lastModifiedTime: time.Now()},
		{name: "file4", relativePath: "sub1/file4", lastModifiedTime: time.Now()},
		{name: "file5", relativePath: "sub1/sub2/file5", lastModifiedTime: time.Now()},
		{name: "file6", relativePath: "sub1/sub2/file6", lastModifiedTime: time.Now()},
	}
}

// given a list of entities, return the relative paths in a list, to help with validations
func (processorTestSuiteHelper) getExpectedTransferFromStoredObjectList(storedObjectList []StoredObject) []string {
	expectedTransfers := make([]string, 0)
	for _, storedObject := range storedObjectList {
		expectedTransfers = append(expectedTransfers, "/"+storedObject.relativePath)
	}

	return expectedTransfers
}

func (processorTestSuiteHelper) getCopyJobTemplate() *common.CopyJobPartOrderRequest {
	return &common.CopyJobPartOrderRequest{Fpo: common.EFolderPropertiesOption.NoFolders(), SymlinkHandlingType: common.ESymlinkHandlingType.Skip()}
}

func TestCopyTransferProcessorMultipleFiles(t *testing.T) {
	a := assert.New(t)
	bsc := getBlobServiceClient()

	// set up source and destination
	cc, _ := getContainerClient(a, bsc)
	dstDirName := scenarioHelper{}.generateLocalDirectory(a)
	defer os.RemoveAll(dstDirName)

	// set up interceptor
	mockedRPC := interceptor{}
	jobsAdmin.ExecuteNewCopyJobPartOrder = func(order common.CopyJobPartOrderRequest) common.CopyJobPartOrderResponse {
		return mockedRPC.intercept(order)
	}
	mockedRPC.init()

	// exercise the processor
	sampleObjects := processorTestSuiteHelper{}.getSampleObjectList()
	for _, numOfParts := range []int{1, 3} {
		numOfTransfersPerPart := len(sampleObjects) / numOfParts
		copyProcessor := newCopyTransferProcessor(processorTestSuiteHelper{}.getCopyJobTemplate(), numOfTransfersPerPart, newRemoteRes(cc.URL()), newLocalRes(dstDirName), nil, nil, false, false)

		// go through the objects and make sure they are processed without error
		for _, storedObject := range sampleObjects {
			err := copyProcessor.scheduleCopyTransfer(storedObject)
			a.Nil(err)
		}

		// make sure everything has been dispatched apart from the final one
		a.Equal(common.PartNumber(numOfParts-1), copyProcessor.copyJobTemplate.PartNum)

		// dispatch final part
		jobInitiated, err := copyProcessor.dispatchFinalPart()
		a.True(jobInitiated)
		a.Nil(err)

		// assert the right transfers were scheduled
		validateCopyTransfersAreScheduled(a, false, false, "", "", processorTestSuiteHelper{}.getExpectedTransferFromStoredObjectList(sampleObjects), mockedRPC)

		mockedRPC.reset()
	}
}

func TestCopyTransferProcessorSingleFile(t *testing.T) {
	a := assert.New(t)
	bsc := getBlobServiceClient()
	cc, _ := createNewContainer(a, bsc)
	defer deleteContainer(a, cc)

	// set up the container with a single blob
	blobList := []string{"singlefile101"}
	scenarioHelper{}.generateBlobsFromList(a, cc, blobList, blockBlobDefaultData)
	a.NotNil(cc)

	// set up the directory with a single file
	dstDirName := scenarioHelper{}.generateLocalDirectory(a)
	defer os.RemoveAll(dstDirName)
	dstFileName := blobList[0]
	scenarioHelper{}.generateLocalFilesFromList(a, dstDirName, blobList)

	// set up interceptor
	mockedRPC := interceptor{}
	jobsAdmin.ExecuteNewCopyJobPartOrder = func(order common.CopyJobPartOrderRequest) common.CopyJobPartOrderResponse {
		return mockedRPC.intercept(order)
	}
	mockedRPC.init()

	// set up the processor
	blobURL := cc.NewBlobClient(blobList[0]).URL()
	copyProcessor := newCopyTransferProcessor(processorTestSuiteHelper{}.getCopyJobTemplate(), 2, newRemoteRes(blobURL), newLocalRes(filepath.Join(dstDirName, dstFileName)), nil, nil, false, false)

	// exercise the copy transfer processor
	storedObject := newStoredObject(noPreProccessor, blobList[0], "", common.EEntityType.File(), time.Now(), 0, noContentProps, noBlobProps, noMetadata, "")
	err := copyProcessor.scheduleCopyTransfer(storedObject)
	a.Nil(err)

	// no part should have been dispatched
	a.Equal(common.PartNumber(0), copyProcessor.copyJobTemplate.PartNum)

	// dispatch final part
	jobInitiated, err := copyProcessor.dispatchFinalPart()
	a.True(jobInitiated)

	// In cases of syncing file to file, the source and destination are empty because this info is already in the root
	// path.
	a.Equal("", mockedRPC.transfers[0].Source)
	a.Equal("", mockedRPC.transfers[0].Destination)

	// assert the right transfers were scheduled
	validateCopyTransfersAreScheduled(a, false, false, "", "", []string{""}, mockedRPC)
}
