package cmd

import (
	"github.com/Azure/azure-storage-azcopy/common"
	chk "gopkg.in/check.v1"
	"path/filepath"
	"time"
)

type syncProcessorSuite struct{}

var _ = chk.Suite(&syncProcessorSuite{})

type syncProcessorSuiteHelper struct{}

// return a list of sample entities
func (syncProcessorSuiteHelper) getSampleEntityList() []genericEntity {
	return []genericEntity{
		{name: "file1", relativePath: "file1", lastModifiedTime: time.Now()},
		{name: "file2", relativePath: "file2", lastModifiedTime: time.Now()},
		{name: "file3", relativePath: "sub1/file3", lastModifiedTime: time.Now()},
		{name: "file4", relativePath: "sub1/file4", lastModifiedTime: time.Now()},
		{name: "file5", relativePath: "sub1/sub2/file5", lastModifiedTime: time.Now()},
		{name: "file6", relativePath: "sub1/sub2/file6", lastModifiedTime: time.Now()},
	}
}

// given a list of entities, return the relative paths in a list, to help with validations
func (syncProcessorSuiteHelper) getExpectedTransferFromEntityList(entityList []genericEntity) []string {
	expectedTransfers := make([]string, 0)
	for _, entity := range entityList {
		expectedTransfers = append(expectedTransfers, entity.relativePath)
	}

	return expectedTransfers
}

func (s *syncProcessorSuite) TestSyncProcessorMultipleFiles(c *chk.C) {
	bsu := getBSU()

	// set up source and destination
	containerURL, containerName := getContainerURL(c, bsu)
	dstDirName := scenarioHelper{}.generateLocalDirectory(c)

	// set up interceptor
	mockedRPC := interceptor{}
	Rpc = mockedRPC.intercept
	mockedRPC.init()

	// construct the raw input to simulate user input
	rawContainerURLWithSAS := scenarioHelper{}.getRawContainerURLWithSAS(c, containerName)
	raw := getDefaultRawInput(rawContainerURLWithSAS.String(), dstDirName)
	cooked, err := raw.cook()
	c.Assert(err, chk.IsNil)

	// exercise the sync processor
	sampleEntities := syncProcessorSuiteHelper{}.getSampleEntityList()

	for _, numOfParts := range []int{1, 3} {
		// note we set the numOfTransfersPerPart here
		syncProcessor := newSyncTransferProcessor(&cooked, len(sampleEntities)/numOfParts)

		// go through the entities and make sure they are processed without error
		for _, entity := range sampleEntities {
			err := syncProcessor.process(entity)
			c.Assert(err, chk.IsNil)
		}

		// make sure everything has been dispatched apart from the final one
		c.Assert(syncProcessor.copyJobTemplate.PartNum, chk.Equals, common.PartNumber(numOfParts-1))

		// dispatch final part
		jobInitiated, err := syncProcessor.dispatchFinalPart()
		c.Assert(jobInitiated, chk.Equals, true)
		c.Assert(err, chk.IsNil)

		// assert the right transfers were scheduled
		validateTransfersAreScheduled(c, containerURL.String(), dstDirName,
			syncProcessorSuiteHelper{}.getExpectedTransferFromEntityList(sampleEntities), mockedRPC)

		mockedRPC.reset()
	}
}

func (s *syncProcessorSuite) TestSyncProcessorSingleFile(c *chk.C) {
	bsu := getBSU()
	containerURL, containerName := createNewContainer(c, bsu)
	defer deleteContainer(c, containerURL)

	// set up the container with a single blob
	blobList := []string{"singlefile101"}
	scenarioHelper{}.generateBlobs(c, containerURL, blobList)
	c.Assert(containerURL, chk.NotNil)

	// set up the directory with a single file
	dstDirName := scenarioHelper{}.generateLocalDirectory(c)
	dstFileName := blobList[0]
	scenarioHelper{}.generateFilesFromList(c, dstDirName, blobList)

	// set up interceptor
	mockedRPC := interceptor{}
	Rpc = mockedRPC.intercept
	mockedRPC.init()

	// construct the raw input to simulate user input
	rawBlobURLWithSAS := scenarioHelper{}.getRawBlobURLWithSAS(c, containerName, blobList[0])
	raw := getDefaultRawInput(rawBlobURLWithSAS.String(), filepath.Join(dstDirName, dstFileName))
	cooked, err := raw.cook()
	c.Assert(err, chk.IsNil)

	// exercise the sync processor
	syncProcessor := newSyncTransferProcessor(&cooked, 2)
	entity := genericEntity{
		name:             blobList[0],
		relativePath:     "",
		lastModifiedTime: time.Now(),
	}
	err = syncProcessor.process(entity)
	c.Assert(err, chk.IsNil)

	// no part should have been dispatched
	c.Assert(syncProcessor.copyJobTemplate.PartNum, chk.Equals, common.PartNumber(0))

	// dispatch final part
	jobInitiated, err := syncProcessor.dispatchFinalPart()
	c.Assert(jobInitiated, chk.Equals, true)

	// assert the right transfers were scheduled
	validateTransfersAreScheduled(c, containerURL.String(), dstDirName,
		blobList, mockedRPC)
}
