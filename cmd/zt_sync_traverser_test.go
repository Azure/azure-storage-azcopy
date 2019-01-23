package cmd

import (
	"github.com/Azure/azure-storage-azcopy/common"
	chk "gopkg.in/check.v1"
	"path"
	"path/filepath"
	"strings"
)

type syncTraverserSuite struct{}

var _ = chk.Suite(&syncTraverserSuite{})

type dummyProcessor struct {
	record []genericEntity
}

func (d *dummyProcessor) process(entity genericEntity) (err error) {
	d.record = append(d.record, entity)
	return
}

func (s *syncTraverserSuite) TestSyncTraverserSingleEntity(c *chk.C) {
	bsu := getBSU()
	containerURL, containerName := createNewContainer(c, bsu)
	defer deleteContainer(c, containerURL)

	// test two scenarios, either blob is at the root virtual dir, or inside sub virtual dirs
	for _, blobName := range []string{"sub1/sub2/singleblobisbest", "nosubsingleblob"} {
		// set up the container with a single blob
		blobList := []string{blobName}
		scenarioHelper{}.generateBlobs(c, containerURL, blobList)
		c.Assert(containerURL, chk.NotNil)

		// set up the directory as a single file
		dstDirName := scenarioHelper{}.generateLocalDirectory(c)
		dstFileName := blobName
		scenarioHelper{}.generateFilesFromList(c, dstDirName, blobList)

		// simulate cca with typical values
		rawBlobURLWithSAS := scenarioHelper{}.getRawBlobURLWithSAS(c, containerName, blobList[0])
		raw := getDefaultRawInput(rawBlobURLWithSAS.String(), filepath.Join(dstDirName, dstFileName))
		cca, err := raw.cook()
		c.Assert(err, chk.IsNil)

		// construct a local traverser
		localTraverser := newLocalTraverser(&cca, false)

		// invoke the local traversal with a dummy processor
		localDummyProcessor := dummyProcessor{}
		err = localTraverser.traverse(&localDummyProcessor, nil)
		c.Assert(err, chk.IsNil)
		c.Assert(len(localDummyProcessor.record), chk.Equals, 1)

		// construct a blob traverser
		blobTraverser, err := newBlobTraverser(&cca, true)
		c.Assert(err, chk.IsNil)

		// invoke the local traversal with a dummy processor
		blobDummyProcessor := dummyProcessor{}
		err = blobTraverser.traverse(&blobDummyProcessor, nil)
		c.Assert(err, chk.IsNil)
		c.Assert(len(blobDummyProcessor.record), chk.Equals, 1)

		// assert the important info are correct
		c.Assert(localDummyProcessor.record[0].name, chk.Equals, blobDummyProcessor.record[0].name)
		c.Assert(localDummyProcessor.record[0].relativePath, chk.Equals, blobDummyProcessor.record[0].relativePath)
	}
}

func (s *syncTraverserSuite) TestSyncTraverserContainerAndLocalDirectory(c *chk.C) {
	bsu := getBSU()
	containerURL, containerName := createNewContainer(c, bsu)
	defer deleteContainer(c, containerURL)

	// set up the container with numerous blobs
	fileList := scenarioHelper{}.generateCommonRemoteScenario(c, containerURL, "")
	c.Assert(containerURL, chk.NotNil)

	// set up the destination with a folder that have the exact same files
	dstDirName := scenarioHelper{}.generateLocalDirectory(c)
	scenarioHelper{}.generateFilesFromList(c, dstDirName, fileList)

	// test two scenarios, either recursive or not
	for _, isRecursiveOn := range []bool{true, false} {
		// simulate cca with typical values
		rawContainerURLWithSAS := scenarioHelper{}.getRawContainerURLWithSAS(c, containerName)
		raw := getDefaultRawInput(rawContainerURLWithSAS.String(), dstDirName)
		raw.recursive = isRecursiveOn
		cca, err := raw.cook()
		c.Assert(err, chk.IsNil)

		// construct a local traverser
		localTraverser := newLocalTraverser(&cca, false)

		// invoke the local traversal with an indexer
		localIndexer := newDestinationIndexer()
		err = localTraverser.traverse(localIndexer, nil)
		c.Assert(err, chk.IsNil)

		// construct a blob traverser
		blobTraverser, err := newBlobTraverser(&cca, true)
		c.Assert(err, chk.IsNil)

		// invoke the local traversal with a dummy processor
		blobDummyProcessor := dummyProcessor{}
		err = blobTraverser.traverse(&blobDummyProcessor, nil)
		c.Assert(err, chk.IsNil)

		// make sure the results are the same
		c.Assert(len(blobDummyProcessor.record), chk.Equals, len(localIndexer.indexMap))
		for _, entity := range blobDummyProcessor.record {
			correspondingLocalFile, present := localIndexer.indexMap[entity.relativePath]

			c.Assert(present, chk.Equals, true)
			c.Assert(correspondingLocalFile.name, chk.Equals, entity.name)
			c.Assert(correspondingLocalFile.isMoreRecentThan(entity), chk.Equals, true)

			if !isRecursiveOn {
				c.Assert(strings.Contains(entity.relativePath, common.AZCOPY_PATH_SEPARATOR_STRING), chk.Equals, false)
			}
		}
	}
}

func (s *syncTraverserSuite) TestSyncTraverserVirtualAndLocalDirectory(c *chk.C) {
	bsu := getBSU()
	containerURL, containerName := createNewContainer(c, bsu)
	defer deleteContainer(c, containerURL)

	// set up the container with numerous blobs
	virDirName := "virdir"
	fileList := scenarioHelper{}.generateCommonRemoteScenario(c, containerURL, virDirName+"/")
	c.Assert(containerURL, chk.NotNil)

	// set up the destination with a folder that have the exact same files
	dstDirName := scenarioHelper{}.generateLocalDirectory(c)
	scenarioHelper{}.generateFilesFromList(c, dstDirName, fileList)

	// test two scenarios, either recursive or not
	for _, isRecursiveOn := range []bool{true, false} {
		// simulate cca with typical values
		rawVirDirURLWithSAS := scenarioHelper{}.getRawBlobURLWithSAS(c, containerName, virDirName)
		raw := getDefaultRawInput(rawVirDirURLWithSAS.String(), path.Join(dstDirName, virDirName))
		raw.recursive = isRecursiveOn
		cca, err := raw.cook()
		c.Assert(err, chk.IsNil)

		// construct a local traverser
		localTraverser := newLocalTraverser(&cca, false)

		// invoke the local traversal with an indexer
		localIndexer := newDestinationIndexer()
		err = localTraverser.traverse(localIndexer, nil)
		c.Assert(err, chk.IsNil)

		// construct a blob traverser
		blobTraverser, err := newBlobTraverser(&cca, true)
		c.Assert(err, chk.IsNil)

		// invoke the local traversal with a dummy processor
		blobDummyProcessor := dummyProcessor{}
		err = blobTraverser.traverse(&blobDummyProcessor, nil)
		c.Assert(err, chk.IsNil)

		// make sure the results are the same
		c.Assert(len(blobDummyProcessor.record), chk.Equals, len(localIndexer.indexMap))
		for _, entity := range blobDummyProcessor.record {
			correspondingLocalFile, present := localIndexer.indexMap[entity.relativePath]

			c.Assert(present, chk.Equals, true)
			c.Assert(correspondingLocalFile.name, chk.Equals, entity.name)
			c.Assert(correspondingLocalFile.isMoreRecentThan(entity), chk.Equals, true)

			if !isRecursiveOn {
				c.Assert(strings.Contains(entity.relativePath, common.AZCOPY_PATH_SEPARATOR_STRING), chk.Equals, false)
			}
		}
	}
}
