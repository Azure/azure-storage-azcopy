package cmd

import (
	"context"
	"github.com/Azure/azure-storage-azcopy/common"
	"github.com/Azure/azure-storage-azcopy/ste"
	"github.com/Azure/azure-storage-blob-go/azblob"
	chk "gopkg.in/check.v1"
	"path/filepath"
	"strings"
	"time"
)

type genericTraverserSuite struct{}

var _ = chk.Suite(&genericTraverserSuite{})

// validate traversing a single blob and a single file
// compare that blob and local traversers get consistent results
func (s *genericTraverserSuite) TestTraverserWithSingleObject(c *chk.C) {
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

		// construct a local traverser
		localTraverser := newLocalTraverser(filepath.Join(dstDirName, dstFileName), false, func() {})

		// invoke the local traversal with a dummy processor
		localDummyProcessor := dummyProcessor{}
		err := localTraverser.traverse(localDummyProcessor.process, nil)
		c.Assert(err, chk.IsNil)
		c.Assert(len(localDummyProcessor.record), chk.Equals, 1)

		// construct a blob traverser
		ctx := context.WithValue(context.TODO(), ste.ServiceAPIVersionOverride, ste.DefaultServiceApiVersion)
		p := azblob.NewPipeline(azblob.NewAnonymousCredential(), azblob.PipelineOptions{})
		rawBlobURLWithSAS := scenarioHelper{}.getRawBlobURLWithSAS(c, containerName, blobList[0])
		blobTraverser := newBlobTraverser(&rawBlobURLWithSAS, p, ctx, false, func() {})

		// invoke the blob traversal with a dummy processor
		blobDummyProcessor := dummyProcessor{}
		err = blobTraverser.traverse(blobDummyProcessor.process, nil)
		c.Assert(err, chk.IsNil)
		c.Assert(len(blobDummyProcessor.record), chk.Equals, 1)

		// assert the important info are correct
		c.Assert(localDummyProcessor.record[0].name, chk.Equals, blobDummyProcessor.record[0].name)
		c.Assert(localDummyProcessor.record[0].relativePath, chk.Equals, blobDummyProcessor.record[0].relativePath)
	}
}

// validate traversing a container and a local directory containing the same objects
// compare that blob and local traversers get consistent results
func (s *genericTraverserSuite) TestTraverserContainerAndLocalDirectory(c *chk.C) {
	bsu := getBSU()
	containerURL, containerName := createNewContainer(c, bsu)
	defer deleteContainer(c, containerURL)

	// set up the container with numerous blobs
	fileList := scenarioHelper{}.generateCommonRemoteScenario(c, containerURL, "")
	c.Assert(containerURL, chk.NotNil)

	// set up the destination with a folder that have the exact same files
	time.Sleep(2 * time.Second) // make the lmts of local files newer
	dstDirName := scenarioHelper{}.generateLocalDirectory(c)
	scenarioHelper{}.generateFilesFromList(c, dstDirName, fileList)

	// test two scenarios, either recursive or not
	for _, isRecursiveOn := range []bool{true, false} {
		// construct a local traverser
		localTraverser := newLocalTraverser(dstDirName, isRecursiveOn, func() {})

		// invoke the local traversal with an indexer
		// so that the results are indexed for easy validation
		localIndexer := newObjectIndexer()
		err := localTraverser.traverse(localIndexer.store, nil)
		c.Assert(err, chk.IsNil)

		// construct a blob traverser
		ctx := context.WithValue(context.TODO(), ste.ServiceAPIVersionOverride, ste.DefaultServiceApiVersion)
		p := azblob.NewPipeline(azblob.NewAnonymousCredential(), azblob.PipelineOptions{})
		rawContainerURLWithSAS := scenarioHelper{}.getRawContainerURLWithSAS(c, containerName)
		blobTraverser := newBlobTraverser(&rawContainerURLWithSAS, p, ctx, isRecursiveOn, func() {})

		// invoke the local traversal with a dummy processor
		blobDummyProcessor := dummyProcessor{}
		err = blobTraverser.traverse(blobDummyProcessor.process, nil)
		c.Assert(err, chk.IsNil)

		// make sure the results are the same
		c.Assert(len(blobDummyProcessor.record), chk.Equals, len(localIndexer.indexMap))
		for _, storedObject := range blobDummyProcessor.record {
			correspondingLocalFile, present := localIndexer.indexMap[storedObject.relativePath]

			c.Assert(present, chk.Equals, true)
			c.Assert(correspondingLocalFile.name, chk.Equals, storedObject.name)
			c.Assert(correspondingLocalFile.isMoreRecentThan(storedObject), chk.Equals, true)

			if !isRecursiveOn {
				c.Assert(strings.Contains(storedObject.relativePath, common.AZCOPY_PATH_SEPARATOR_STRING), chk.Equals, false)
			}
		}
	}
}

// validate traversing a virtual and a local directory containing the same objects
// compare that blob and local traversers get consistent results
func (s *genericTraverserSuite) TestTraverserWithVirtualAndLocalDirectory(c *chk.C) {
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
		// construct a local traverser
		localTraverser := newLocalTraverser(filepath.Join(dstDirName, virDirName), isRecursiveOn, func() {})

		// invoke the local traversal with an indexer
		// so that the results are indexed for easy validation
		localIndexer := newObjectIndexer()
		err := localTraverser.traverse(localIndexer.store, nil)
		c.Assert(err, chk.IsNil)

		// construct a blob traverser
		ctx := context.WithValue(context.TODO(), ste.ServiceAPIVersionOverride, ste.DefaultServiceApiVersion)
		p := azblob.NewPipeline(azblob.NewAnonymousCredential(), azblob.PipelineOptions{})
		rawVirDirURLWithSAS := scenarioHelper{}.getRawBlobURLWithSAS(c, containerName, virDirName)
		blobTraverser := newBlobTraverser(&rawVirDirURLWithSAS, p, ctx, isRecursiveOn, func() {})

		// invoke the local traversal with a dummy processor
		blobDummyProcessor := dummyProcessor{}
		err = blobTraverser.traverse(blobDummyProcessor.process, nil)
		c.Assert(err, chk.IsNil)

		// make sure the results are the same
		c.Assert(len(blobDummyProcessor.record), chk.Equals, len(localIndexer.indexMap))
		for _, storedObject := range blobDummyProcessor.record {
			correspondingLocalFile, present := localIndexer.indexMap[storedObject.relativePath]

			c.Assert(present, chk.Equals, true)
			c.Assert(correspondingLocalFile.name, chk.Equals, storedObject.name)
			c.Assert(correspondingLocalFile.isMoreRecentThan(storedObject), chk.Equals, true)

			if !isRecursiveOn {
				c.Assert(strings.Contains(storedObject.relativePath, common.AZCOPY_PATH_SEPARATOR_STRING), chk.Equals, false)
			}
		}
	}
}
