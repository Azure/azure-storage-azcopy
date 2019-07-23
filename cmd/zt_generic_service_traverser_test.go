package cmd

import (
	"github.com/Azure/azure-storage-blob-go/azblob"
	"github.com/Azure/azure-storage-file-go/azfile"
	chk "gopkg.in/check.v1"
)

func (s *genericTraverserSuite) TestServiceTraverserWithManyObjects(c *chk.C) {
	bsu := getBSU()
	fsu := getFSU()
	s3Client, err := createS3ClientWithMinio(createS3ResOptions{})
	c.Assert(err, chk.IsNil)

	// Clean the accounts to ensure that only the containers we create exist
	cleanS3Account(c, s3Client)
	cleanBlobAccount(c, bsu)
	cleanFileAccount(c, fsu)

	containerList := []string{
		generateName("suchcontainermanystorage", 63),
		generateName("containertwoelectricboogaloo", 63),
		generateName("funnymemereference", 63),
		generateName("gettingmeta", 63),
	}

	// convert containerList into a map for easy validation
	cnames := map[string]bool{}
	for _, v := range containerList {
		cnames[v] = true
	}

	objectList := []string{
		generateName("basedir", 63),
		"allyourbase/" + generateName("arebelongtous", 63),
		"sub1/sub2/" + generateName("", 63),
		generateName("someobject", 63),
	}

	objectData := "Hello world!"

	// Generate remote scenarios
	scenarioHelper{}.generateBlobContainersAndBlobsFromLists(c, bsu, containerList, objectList, objectData)
	scenarioHelper{}.generateFileSharesAndFilesFromLists(c, fsu, containerList, objectList, objectData)
	scenarioHelper{}.generateS3BucketsAndObjectsFromLists(c, s3Client, containerList, objectList, objectData)

	// Generate local files to ensure behavior conforms to other traversers
	dstDirName := scenarioHelper{}.generateLocalDirectory(c)
	scenarioHelper{}.generateLocalFilesFromList(c, dstDirName, objectList)

	// Create a local traversal
	localTraverser := newLocalTraverser(dstDirName, true, func() {})

	// Invoke the traversal with an indexer so the results are indexed for easy validation
	localIndexer := newObjectIndexer()
	err = localTraverser.traverse(localIndexer.store, nil)
	c.Assert(err, chk.IsNil)

	// construct a blob account traverser
	blobPipeline := azblob.NewPipeline(azblob.NewAnonymousCredential(), azblob.PipelineOptions{})
	rawBSU := scenarioHelper{}.getRawBlobServiceURLWithSAS(c)
	blobAccountTraverser := newBlobAccountTraverser(&rawBSU, blobPipeline, ctx, func() {})

	// invoke the blob account traversal with a dummy processor
	blobDummyProcessor := dummyProcessor{}
	err = blobAccountTraverser.traverse(blobDummyProcessor.process, nil)
	c.Assert(err, chk.IsNil)

	// construct a file account traverser
	filePipeline := azfile.NewPipeline(azfile.NewAnonymousCredential(), azfile.PipelineOptions{})
	rawFSU := scenarioHelper{}.getRawFileServiceURLWithSAS(c)
	fileAccountTraverser := newFileAccountTraverser(&rawFSU, filePipeline, ctx, func() {})

	// invoke the file account traversal with a dummy processor
	fileDummyProcessor := dummyProcessor{}
	err = fileAccountTraverser.traverse(fileDummyProcessor.process, nil)
	c.Assert(err, chk.IsNil)

	// construct a s3 service traverser
	accountURL := scenarioHelper{}.getRawS3AccountURL(c, "")
	s3ServiceTraverser, err := newS3ServiceTraverser(&accountURL, ctx, func() {})
	c.Assert(err, chk.IsNil)

	// invoke the s3 service traversal with a dummy processor
	s3DummyProcessor := dummyProcessor{}
	err = s3ServiceTraverser.traverse(s3DummyProcessor.process, nil)
	c.Assert(err, chk.IsNil)

	c.Assert(len(blobDummyProcessor.record), chk.Equals, len(localIndexer.indexMap)*len(containerList))
	c.Assert(len(fileDummyProcessor.record), chk.Equals, len(localIndexer.indexMap)*len(containerList))
	c.Assert(len(s3DummyProcessor.record), chk.Equals, len(localIndexer.indexMap)*len(containerList))
	for _, storedObject := range append(append(blobDummyProcessor.record, fileDummyProcessor.record...), s3DummyProcessor.record...) {
		correspondingLocalFile, present := localIndexer.indexMap[storedObject.relativePath]
		_, cnamePresent := cnames[storedObject.containerName]

		c.Assert(present, chk.Equals, true)
		c.Assert(cnamePresent, chk.Equals, true)
		c.Assert(correspondingLocalFile.name, chk.Equals, storedObject.name)
	}
}
