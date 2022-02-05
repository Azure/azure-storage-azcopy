package cmd

import (
	"github.com/Azure/azure-storage-blob-go/azblob"
	"github.com/Azure/azure-storage-file-go/azfile"
	chk "gopkg.in/check.v1"

	"github.com/nitin-deamon/azure-storage-azcopy/v10/azbfs"
	"github.com/nitin-deamon/azure-storage-azcopy/v10/common"
)

// Separated the ADLS tests from others as ADLS can't safely be tested on the same storage account
func (s *genericTraverserSuite) TestBlobFSServiceTraverserWithManyObjects(c *chk.C) {
	bfssu := GetBFSSU()
	bsu := getBSU() // Only used to clean up

	// BlobFS is tested on the same account, therefore this is safe to clean up this way
	cleanBlobAccount(c, bsu)

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
	scenarioHelper{}.generateFilesystemsAndFilesFromLists(c, bfssu, containerList, objectList, objectData)

	// deferred container cleanup
	defer func() {
		for _, v := range containerList {
			// create container URLs
			blobContainer := bsu.NewContainerURL(v)
			_, _ = blobContainer.Delete(ctx, azblob.ContainerAccessConditions{})
		}
	}()

	// Generate local files to ensure behavior conforms to other traversers
	dstDirName := scenarioHelper{}.generateLocalDirectory(c)
	scenarioHelper{}.generateLocalFilesFromList(c, dstDirName, objectList)

	// Create a local traversal
	localTraverser := newLocalTraverser(nil, dstDirName, true, true, func(common.EntityType) {})

	// Invoke the traversal with an indexer so the results are indexed for easy validation
	localIndexer := newObjectIndexer()
	err := localTraverser.Traverse(noPreProccessor, localIndexer.store, nil)
	c.Assert(err, chk.IsNil)

	// construct a blob account traverser
	blobFSPipeline := azbfs.NewPipeline(azbfs.NewAnonymousCredential(), azbfs.PipelineOptions{})
	rawBSU := scenarioHelper{}.getRawAdlsServiceURLWithSAS(c).URL()
	blobAccountTraverser := newBlobFSAccountTraverser(&rawBSU, blobFSPipeline, ctx, func(common.EntityType) {})

	// invoke the blob account traversal with a dummy processor
	blobDummyProcessor := dummyProcessor{}
	err = blobAccountTraverser.Traverse(noPreProccessor, blobDummyProcessor.process, nil)
	c.Assert(err, chk.IsNil)

	c.Assert(len(blobDummyProcessor.record), chk.Equals, len(localIndexer.indexMap)*len(containerList))

	for _, storedObject := range blobDummyProcessor.record {
		correspondingLocalFile, present := localIndexer.indexMap[storedObject.relativePath]
		_, cnamePresent := cnames[storedObject.ContainerName]

		c.Assert(present, chk.Equals, true)
		c.Assert(cnamePresent, chk.Equals, true)
		c.Assert(correspondingLocalFile.name, chk.Equals, storedObject.name)
	}
}

func (s *genericTraverserSuite) TestServiceTraverserWithManyObjects(c *chk.C) {
	bsu := getBSU()
	fsu := getFSU()
	testS3 := false // Only test S3 if credentials are present.
	testGCP := false
	s3Client, err := createS3ClientWithMinio(createS3ResOptions{})
	// disable S3 testing
	if err == nil && !isS3Disabled() {
		testS3 = true
	} else {
		c.Log("WARNING: Service level traverser is NOT testing S3")
	}

	gcpClient, err := createGCPClientWithGCSSDK()
	if err == nil && !gcpTestsDisabled() {
		testGCP = true
	} else {
		c.Log("WARNING: Service level traverser is NOT testing GCP")
	}

	// Clean the accounts to ensure that only the containers we create exist
	if testS3 {
		cleanS3Account(c, s3Client)
	}
	if testGCP {
		cleanGCPAccount(c, gcpClient)
	}
	// BlobFS is tested on the same account, therefore this is safe to clean up this way
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
	if testS3 {
		scenarioHelper{}.generateS3BucketsAndObjectsFromLists(c, s3Client, containerList, objectList, objectData)
	}
	if testGCP {
		scenarioHelper{}.generateGCPBucketsAndObjectsFromLists(c, gcpClient, containerList, objectList)
	}

	// deferred container cleanup
	defer func() {
		for _, v := range containerList {
			// create container URLs
			blobContainer := bsu.NewContainerURL(v)
			fileShare := fsu.NewShareURL(v)

			// Ignore errors from cleanup.
			if testS3 {
				_ = s3Client.RemoveBucket(v)
			}
			if testGCP {
				deleteGCPBucket(c, gcpClient, v, true)
			}
			_, _ = blobContainer.Delete(ctx, azblob.ContainerAccessConditions{})
			_, _ = fileShare.Delete(ctx, azfile.DeleteSnapshotsOptionNone)
		}
	}()

	// Generate local files to ensure behavior conforms to other traversers
	dstDirName := scenarioHelper{}.generateLocalDirectory(c)
	scenarioHelper{}.generateLocalFilesFromList(c, dstDirName, objectList)

	// Create a local traversal
	localTraverser := newLocalTraverser(nil, dstDirName, true, true, func(common.EntityType) {})

	// Invoke the traversal with an indexer so the results are indexed for easy validation
	localIndexer := newObjectIndexer()
	err = localTraverser.Traverse(noPreProccessor, localIndexer.store, nil)
	c.Assert(err, chk.IsNil)

	// construct a blob account traverser
	blobPipeline := azblob.NewPipeline(azblob.NewAnonymousCredential(), azblob.PipelineOptions{})
	rawBSU := scenarioHelper{}.getRawBlobServiceURLWithSAS(c)
	blobAccountTraverser := newBlobAccountTraverser(&rawBSU, blobPipeline, ctx, false,
		func(common.EntityType) {}, false, common.CpkOptions{})

	// invoke the blob account traversal with a dummy processor
	blobDummyProcessor := dummyProcessor{}
	err = blobAccountTraverser.Traverse(noPreProccessor, blobDummyProcessor.process, nil)
	c.Assert(err, chk.IsNil)

	// construct a file account traverser
	filePipeline := azfile.NewPipeline(azfile.NewAnonymousCredential(), azfile.PipelineOptions{})
	rawFSU := scenarioHelper{}.getRawFileServiceURLWithSAS(c)
	fileAccountTraverser := newFileAccountTraverser(&rawFSU, filePipeline, ctx, false, func(common.EntityType) {})

	// invoke the file account traversal with a dummy processor
	fileDummyProcessor := dummyProcessor{}
	err = fileAccountTraverser.Traverse(noPreProccessor, fileDummyProcessor.process, nil)
	c.Assert(err, chk.IsNil)

	var s3DummyProcessor dummyProcessor
	var gcpDummyProcessor dummyProcessor
	if testS3 {
		// construct a s3 service traverser
		accountURL := scenarioHelper{}.getRawS3AccountURL(c, "")
		s3ServiceTraverser, err := newS3ServiceTraverser(&accountURL, ctx, false, func(common.EntityType) {})
		c.Assert(err, chk.IsNil)

		// invoke the s3 service traversal with a dummy processor
		s3DummyProcessor = dummyProcessor{}
		err = s3ServiceTraverser.Traverse(noPreProccessor, s3DummyProcessor.process, nil)
		c.Assert(err, chk.IsNil)
	}

	if testGCP {

		gcpAccountURL := scenarioHelper{}.getRawGCPAccountURL(c)
		gcpServiceTraverser, err := newGCPServiceTraverser(&gcpAccountURL, ctx, false, func(entityType common.EntityType) {})
		c.Assert(err, chk.IsNil)

		gcpDummyProcessor = dummyProcessor{}
		err = gcpServiceTraverser.Traverse(noPreProccessor, gcpDummyProcessor.process, nil)
		c.Assert(err, chk.IsNil)
	}

	records := append(blobDummyProcessor.record, fileDummyProcessor.record...)

	localTotalCount := len(localIndexer.indexMap)
	localFileOnlyCount := 0
	for _, x := range localIndexer.indexMap {
		if x.entityType == common.EEntityType.File() {
			localFileOnlyCount++
		}
	}
	c.Assert(len(blobDummyProcessor.record), chk.Equals, localFileOnlyCount*len(containerList))
	c.Assert(len(fileDummyProcessor.record), chk.Equals, localTotalCount*len(containerList))
	if testS3 {
		c.Assert(len(s3DummyProcessor.record), chk.Equals, localFileOnlyCount*len(containerList))
		records = append(records, s3DummyProcessor.record...)
	}
	if testGCP {
		c.Assert(len(gcpDummyProcessor.record), chk.Equals, localFileOnlyCount*len(containerList))
		records = append(records, gcpDummyProcessor.record...)
	}

	for _, storedObject := range records {
		correspondingLocalFile, present := localIndexer.indexMap[storedObject.relativePath]
		_, cnamePresent := cnames[storedObject.ContainerName]

		c.Assert(present, chk.Equals, true)
		c.Assert(cnamePresent, chk.Equals, true)
		c.Assert(correspondingLocalFile.name, chk.Equals, storedObject.name)
	}
}

func (s *genericTraverserSuite) TestServiceTraverserWithWildcards(c *chk.C) {
	bsu := getBSU()
	fsu := getFSU()
	bfssu := GetBFSSU()
	testS3 := false // Only test S3 if credentials are present.
	testGCP := false

	s3Client, err := createS3ClientWithMinio(createS3ResOptions{})
	if !isS3Disabled() && err == nil {
		testS3 = true
	} else {
		c.Log("WARNING: Service level traverser is NOT testing S3")
	}

	gcpClient, err := createGCPClientWithGCSSDK()
	if !gcpTestsDisabled() && err == nil {
		testGCP = true
	} else {
		c.Log("WARNING: Service level traverser is NOT testing GCP")
	}

	// Clean the accounts to ensure that only the containers we create exist
	if testS3 {
		cleanS3Account(c, s3Client)
	}
	if testGCP {
		cleanGCPAccount(c, gcpClient)
	}
	cleanBlobAccount(c, bsu)
	cleanFileAccount(c, fsu)

	containerList := []string{
		generateName("objectmatchone", 63),
		generateName("objectnomatchone", 63),
		generateName("objectnomatchtwo", 63),
		generateName("objectmatchtwo", 63),
	}

	bfsContainerList := []string{
		generateName("bfsmatchobjectmatchone", 63),
		generateName("bfsmatchobjectnomatchone", 63),
		generateName("bfsmatchobjectnomatchtwo", 63),
		generateName("bfsmatchobjectmatchtwo", 63),
	}

	// load only matching container names in
	cnames := map[string]bool{
		containerList[0]: true,
		containerList[3]: true,
	}

	// load matching bfs container names in
	bfscnames := map[string]bool{
		bfsContainerList[0]: true,
		bfsContainerList[3]: true,
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
	// Subject ADLS tests to a different container name prefix to avoid conflicts with blob
	scenarioHelper{}.generateFilesystemsAndFilesFromLists(c, bfssu, bfsContainerList, objectList, objectData)
	if testS3 {
		scenarioHelper{}.generateS3BucketsAndObjectsFromLists(c, s3Client, containerList, objectList, objectData)
	}
	if testGCP {
		scenarioHelper{}.generateGCPBucketsAndObjectsFromLists(c, gcpClient, containerList, objectList)
	}

	// deferred container cleanup
	defer func() {
		for _, v := range containerList {
			// create container URLs
			blobContainer := bsu.NewContainerURL(v)
			fileShare := fsu.NewShareURL(v)

			// Ignore errors from cleanup.
			if testS3 {
				_ = s3Client.RemoveBucket(v)
			}
			if testGCP {
				deleteGCPBucket(c, gcpClient, v, true)
			}
			_, _ = blobContainer.Delete(ctx, azblob.ContainerAccessConditions{})
			_, _ = fileShare.Delete(ctx, azfile.DeleteSnapshotsOptionNone)
		}
	}()

	// Generate local files to ensure behavior conforms to other traversers
	dstDirName := scenarioHelper{}.generateLocalDirectory(c)
	scenarioHelper{}.generateLocalFilesFromList(c, dstDirName, objectList)

	// Create a local traversal
	localTraverser := newLocalTraverser(nil, dstDirName, true, true, func(common.EntityType) {})

	// Invoke the traversal with an indexer so the results are indexed for easy validation
	localIndexer := newObjectIndexer()
	err = localTraverser.Traverse(noPreProccessor, localIndexer.store, nil)
	c.Assert(err, chk.IsNil)

	// construct a blob account traverser
	blobPipeline := azblob.NewPipeline(azblob.NewAnonymousCredential(), azblob.PipelineOptions{})
	rawBSU := scenarioHelper{}.getRawBlobServiceURLWithSAS(c)
	rawBSU.Path = "/objectmatch*" // set the container name to contain a wildcard
	blobAccountTraverser := newBlobAccountTraverser(&rawBSU, blobPipeline, ctx, false,
		func(common.EntityType) {}, false, common.CpkOptions{})

	// invoke the blob account traversal with a dummy processor
	blobDummyProcessor := dummyProcessor{}
	err = blobAccountTraverser.Traverse(noPreProccessor, blobDummyProcessor.process, nil)
	c.Assert(err, chk.IsNil)

	// construct a file account traverser
	filePipeline := azfile.NewPipeline(azfile.NewAnonymousCredential(), azfile.PipelineOptions{})
	rawFSU := scenarioHelper{}.getRawFileServiceURLWithSAS(c)
	rawFSU.Path = "/objectmatch*" // set the container name to contain a wildcard
	fileAccountTraverser := newFileAccountTraverser(&rawFSU, filePipeline, ctx, false, func(common.EntityType) {})

	// invoke the file account traversal with a dummy processor
	fileDummyProcessor := dummyProcessor{}
	err = fileAccountTraverser.Traverse(noPreProccessor, fileDummyProcessor.process, nil)
	c.Assert(err, chk.IsNil)

	// construct a ADLS account traverser
	blobFSPipeline := azbfs.NewPipeline(azbfs.NewAnonymousCredential(), azbfs.PipelineOptions{})
	rawBFSSU := scenarioHelper{}.getRawAdlsServiceURLWithSAS(c).URL()
	rawBFSSU.Path = "/bfsmatchobjectmatch*" // set the container name to contain a wildcard and not conflict with blob
	bfsAccountTraverser := newBlobFSAccountTraverser(&rawBFSSU, blobFSPipeline, ctx, func(common.EntityType) {})

	// invoke the blobFS account traversal with a dummy processor
	bfsDummyProcessor := dummyProcessor{}
	err = bfsAccountTraverser.Traverse(noPreProccessor, bfsDummyProcessor.process, nil)

	var s3DummyProcessor dummyProcessor
	var gcpDummyProcessor dummyProcessor
	if testS3 {
		// construct a s3 service traverser
		accountURL, err := common.NewS3URLParts(scenarioHelper{}.getRawS3AccountURL(c, ""))
		c.Assert(err, chk.IsNil)
		accountURL.BucketName = "objectmatch*" // set the container name to contain a wildcard

		urlOut := accountURL.URL()
		s3ServiceTraverser, err := newS3ServiceTraverser(&urlOut, ctx, false, func(common.EntityType) {})
		c.Assert(err, chk.IsNil)

		// invoke the s3 service traversal with a dummy processor
		s3DummyProcessor = dummyProcessor{}
		err = s3ServiceTraverser.Traverse(noPreProccessor, s3DummyProcessor.process, nil)
		c.Assert(err, chk.IsNil)
	}
	if testGCP {
		gcpAccountURL, err := common.NewGCPURLParts(scenarioHelper{}.getRawGCPAccountURL(c))
		c.Assert(err, chk.IsNil)
		gcpAccountURL.BucketName = "objectmatch*"
		urlStr := gcpAccountURL.URL()
		gcpServiceTraverser, err := newGCPServiceTraverser(&urlStr, ctx, false, func(entityType common.EntityType) {})
		c.Assert(err, chk.IsNil)

		gcpDummyProcessor = dummyProcessor{}
		err = gcpServiceTraverser.Traverse(noPreProccessor, gcpDummyProcessor.process, nil)
		c.Assert(err, chk.IsNil)
	}

	records := append(blobDummyProcessor.record, fileDummyProcessor.record...)

	localTotalCount := len(localIndexer.indexMap)
	localFileOnlyCount := 0
	for _, x := range localIndexer.indexMap {
		if x.entityType == common.EEntityType.File() {
			localFileOnlyCount++
		}
	}

	// Only two containers should match.
	c.Assert(len(blobDummyProcessor.record), chk.Equals, localFileOnlyCount*2)
	c.Assert(len(fileDummyProcessor.record), chk.Equals, localTotalCount*2)
	if testS3 {
		c.Assert(len(s3DummyProcessor.record), chk.Equals, localFileOnlyCount*2)
		records = append(records, s3DummyProcessor.record...)
	}
	if testGCP {
		c.Assert(len(gcpDummyProcessor.record), chk.Equals, localFileOnlyCount*2)
		records = append(records, gcpDummyProcessor.record...)
	}

	for _, storedObject := range records {
		correspondingLocalFile, present := localIndexer.indexMap[storedObject.relativePath]
		_, cnamePresent := cnames[storedObject.ContainerName]

		c.Assert(present, chk.Equals, true)
		c.Assert(cnamePresent, chk.Equals, true)
		c.Assert(correspondingLocalFile.name, chk.Equals, storedObject.name)
	}

	// Test ADLSG2 separately due to different container naming
	c.Assert(len(bfsDummyProcessor.record), chk.Equals, len(localIndexer.indexMap)*2)
	for _, storedObject := range bfsDummyProcessor.record {
		correspondingLocalFile, present := localIndexer.indexMap[storedObject.relativePath]
		_, cnamePresent := bfscnames[storedObject.ContainerName]

		c.Assert(present, chk.Equals, true)
		c.Assert(cnamePresent, chk.Equals, true)
		c.Assert(correspondingLocalFile.name, chk.Equals, storedObject.name)
	}
}
