package cmd

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/Azure/azure-storage-azcopy/v10/common"
)

func TestServiceTraverserWithManyObjects(t *testing.T) {
	a := assert.New(t)
	bsc := getBlobServiceClient()
	fsc := getFileServiceClient()
	testS3 := false // Only test S3 if credentials are present.
	testGCP := false
	s3Client, err := createS3ClientWithMinio(createS3ResOptions{})
	// disable S3 testing
	if err == nil && !isS3Disabled() {
		testS3 = true
	} else {
		t.Log("WARNING: Service level traverser is NOT testing S3")
	}

	gcpClient, err := createGCPClientWithGCSSDK()
	if err == nil && !gcpTestsDisabled() {
		testGCP = true
	} else {
		t.Log("WARNING: Service level traverser is NOT testing GCP")
	}

	// Clean the accounts to ensure that only the containers we create exist
	if testS3 {
		cleanS3Account(s3Client)
	}
	if testGCP {
		cleanGCPAccount(gcpClient)
	}
	// BlobFS is tested on the same account, therefore this is safe to clean up this way
	cleanBlobAccount(a, bsc)
	cleanFileAccount(a, fsc)

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
	scenarioHelper{}.generateBlobContainersAndBlobsFromLists(a, bsc, containerList, objectList, objectData)
	scenarioHelper{}.generateFileSharesAndFilesFromLists(a, fsc, containerList, objectList, objectData)
	if testS3 {
		scenarioHelper{}.generateS3BucketsAndObjectsFromLists(a, s3Client, containerList, objectList, objectData)
	}
	if testGCP {
		scenarioHelper{}.generateGCPBucketsAndObjectsFromLists(a, gcpClient, containerList, objectList)
	}

	// deferred container cleanup
	defer func() {
		for _, v := range containerList {
			// create container URLs
			cc := bsc.NewContainerClient(v)
			sc := fsc.NewShareClient(v)

			// Ignore errors from cleanup.
			if testS3 {
				_ = s3Client.RemoveBucket(v)
			}
			if testGCP {
				deleteGCPBucket(gcpClient, v, true)
			}
			_, _ = cc.Delete(ctx, nil)
			_, _ = sc.Delete(ctx, nil)
		}
	}()

	// Generate local files to ensure behavior conforms to other traversers
	dstDirName := scenarioHelper{}.generateLocalDirectory(a)
	scenarioHelper{}.generateLocalFilesFromList(a, dstDirName, objectList)

	// Create a local traversal
	localTraverser, _ := newLocalTraverser(context.TODO(), dstDirName, true, false, common.ESymlinkHandlingType.Follow(), common.ESyncHashType.None(), func(common.EntityType) {}, nil, common.EPreserveHardlinksOption.Follow())

	// Invoke the traversal with an indexer so the results are indexed for easy validation
	localIndexer := newObjectIndexer()
	err = localTraverser.Traverse(noPreProccessor, localIndexer.store, nil)
	a.Nil(err)

	// construct a blob account traverser
	rawBSU := scenarioHelper{}.getBlobServiceClientWithSAS(a)
	blobAccountTraverser := newBlobAccountTraverser(rawBSU, "", ctx, false, func(common.EntityType) {}, false, common.CpkOptions{}, common.EPreservePermissionsOption.None(), false, nil)

	// invoke the blob account traversal with a dummy processor
	blobDummyProcessor := dummyProcessor{}
	err = blobAccountTraverser.Traverse(noPreProccessor, blobDummyProcessor.process, nil)
	a.Nil(err)

	// construct a file account traverser
	rawFSU := scenarioHelper{}.getFileServiceClientWithSAS(a)
	fileAccountTraverser := newFileAccountTraverser(rawFSU, "", ctx, false, func(common.EntityType) {}, common.ETrailingDotOption.Enable(), nil, common.EPreserveHardlinksOption.Follow())

	// invoke the file account traversal with a dummy processor
	fileDummyProcessor := dummyProcessor{}
	err = fileAccountTraverser.Traverse(noPreProccessor, fileDummyProcessor.process, nil)
	a.Nil(err)

	var s3DummyProcessor dummyProcessor
	var gcpDummyProcessor dummyProcessor
	if testS3 {
		// construct a s3 service traverser
		accountURL := scenarioHelper{}.getRawS3AccountURL(a, "")
		s3ServiceTraverser, err := newS3ServiceTraverser(&accountURL, ctx, false, func(common.EntityType) {})
		a.Nil(err)

		// invoke the s3 service traversal with a dummy processor
		s3DummyProcessor = dummyProcessor{}
		err = s3ServiceTraverser.Traverse(noPreProccessor, s3DummyProcessor.process, nil)
		a.Nil(err)
	}

	if testGCP {

		gcpAccountURL := scenarioHelper{}.getRawGCPAccountURL(a)
		gcpServiceTraverser, err := newGCPServiceTraverser(&gcpAccountURL, ctx, false, func(entityType common.EntityType) {})
		a.Nil(err)

		gcpDummyProcessor = dummyProcessor{}
		err = gcpServiceTraverser.Traverse(noPreProccessor, gcpDummyProcessor.process, nil)
		a.Nil(err)
	}

	records := append(blobDummyProcessor.record, fileDummyProcessor.record...)

	localTotalCount := len(localIndexer.indexMap)
	localFileOnlyCount := 0
	for _, x := range localIndexer.indexMap {
		if x.entityType == common.EEntityType.File() {
			localFileOnlyCount++
		}
	}
	a.Equal(localFileOnlyCount*len(containerList), len(blobDummyProcessor.record))
	a.Equal(localTotalCount*len(containerList), len(fileDummyProcessor.record))
	if testS3 {
		a.Equal(localFileOnlyCount*len(containerList), len(s3DummyProcessor.record))
		records = append(records, s3DummyProcessor.record...)
	}
	if testGCP {
		a.Equal(localFileOnlyCount*len(containerList), len(gcpDummyProcessor.record))
		records = append(records, gcpDummyProcessor.record...)
	}

	for _, storedObject := range records {
		correspondingLocalFile, present := localIndexer.indexMap[storedObject.relativePath]
		_, cnamePresent := cnames[storedObject.ContainerName]

		a.True(present)
		a.True(cnamePresent)
		a.Equal(storedObject.name, correspondingLocalFile.name)
	}
}

func TestServiceTraverserWithWildcards(t *testing.T) {
	a := assert.New(t)
	bsc := getBlobServiceClient()
	fsc := getFileServiceClient()
	testS3 := false // Only test S3 if credentials are present.
	testGCP := false

	s3Client, err := createS3ClientWithMinio(createS3ResOptions{})
	if !isS3Disabled() && err == nil {
		testS3 = true
	} else {
		t.Log("WARNING: Service level traverser is NOT testing S3")
	}

	gcpClient, err := createGCPClientWithGCSSDK()
	if !gcpTestsDisabled() && err == nil {
		testGCP = true
	} else {
		t.Log("WARNING: Service level traverser is NOT testing GCP")
	}

	// Clean the accounts to ensure that only the containers we create exist
	if testS3 {
		cleanS3Account(s3Client)
	}
	if testGCP {
		cleanGCPAccount(gcpClient)
	}
	cleanBlobAccount(a, bsc)
	cleanFileAccount(a, fsc)

	containerList := []string{
		generateName("objectmatchone", 63),
		generateName("objectnomatchone", 63),
		generateName("objectnomatchtwo", 63),
		generateName("objectmatchtwo", 63),
	}

	// load only matching container names in
	cnames := map[string]bool{
		containerList[0]: true,
		containerList[3]: true,
	}

	objectList := []string{
		generateName("basedir", 63),
		"allyourbase/" + generateName("arebelongtous", 63),
		"sub1/sub2/" + generateName("", 63),
		generateName("someobject", 63),
	}

	objectData := "Hello world!"

	// Generate remote scenarios
	scenarioHelper{}.generateBlobContainersAndBlobsFromLists(a, bsc, containerList, objectList, objectData)
	scenarioHelper{}.generateFileSharesAndFilesFromLists(a, fsc, containerList, objectList, objectData)
	if testS3 {
		scenarioHelper{}.generateS3BucketsAndObjectsFromLists(a, s3Client, containerList, objectList, objectData)
	}
	if testGCP {
		scenarioHelper{}.generateGCPBucketsAndObjectsFromLists(a, gcpClient, containerList, objectList)
	}

	// deferred container cleanup
	defer func() {
		for _, v := range containerList {
			// create container URLs
			cc := bsc.NewContainerClient(v)
			sc := fsc.NewShareClient(v)

			// Ignore errors from cleanup.
			if testS3 {
				_ = s3Client.RemoveBucket(v)
			}
			if testGCP {
				deleteGCPBucket(gcpClient, v, true)
			}
			_, _ = cc.Delete(ctx, nil)
			_, _ = sc.Delete(ctx, nil)
		}
	}()

	// Generate local files to ensure behavior conforms to other traversers
	dstDirName := scenarioHelper{}.generateLocalDirectory(a)
	scenarioHelper{}.generateLocalFilesFromList(a, dstDirName, objectList)

	// Create a local traversal
	localTraverser, _ := newLocalTraverser(context.TODO(), dstDirName, true, false, common.ESymlinkHandlingType.Follow(), common.ESyncHashType.None(), func(common.EntityType) {}, nil, common.EPreserveHardlinksOption.Follow())

	// Invoke the traversal with an indexer so the results are indexed for easy validation
	localIndexer := newObjectIndexer()
	err = localTraverser.Traverse(noPreProccessor, localIndexer.store, nil)
	a.Nil(err)

	// construct a blob account traverser
	rawBSU := scenarioHelper{}.getBlobServiceClientWithSAS(a)
	container := "objectmatch*" // set the container name to contain a wildcard
	blobAccountTraverser := newBlobAccountTraverser(rawBSU, container, ctx, false, func(common.EntityType) {}, false, common.CpkOptions{}, common.EPreservePermissionsOption.None(), false, nil)

	// invoke the blob account traversal with a dummy processor
	blobDummyProcessor := dummyProcessor{}
	err = blobAccountTraverser.Traverse(noPreProccessor, blobDummyProcessor.process, nil)
	a.Nil(err)

	// construct a file account traverser
	rawFSU := scenarioHelper{}.getFileServiceClientWithSAS(a)
	share := "objectmatch*" // set the container name to contain a wildcard
	fileAccountTraverser := newFileAccountTraverser(rawFSU, share, ctx, false, func(common.EntityType) {}, common.ETrailingDotOption.Enable(), nil, common.EPreserveHardlinksOption.Follow())

	// invoke the file account traversal with a dummy processor
	fileDummyProcessor := dummyProcessor{}
	err = fileAccountTraverser.Traverse(noPreProccessor, fileDummyProcessor.process, nil)
	a.Nil(err)

	var s3DummyProcessor dummyProcessor
	var gcpDummyProcessor dummyProcessor
	if testS3 {
		// construct a s3 service traverser
		accountURL, err := common.NewS3URLParts(scenarioHelper{}.getRawS3AccountURL(a, ""))
		a.Nil(err)
		accountURL.BucketName = "objectmatch*" // set the container name to contain a wildcard

		urlOut := accountURL.URL()
		s3ServiceTraverser, err := newS3ServiceTraverser(&urlOut, ctx, false, func(common.EntityType) {})
		a.Nil(err)

		// invoke the s3 service traversal with a dummy processor
		s3DummyProcessor = dummyProcessor{}
		err = s3ServiceTraverser.Traverse(noPreProccessor, s3DummyProcessor.process, nil)
		a.Nil(err)
	}
	if testGCP {
		gcpAccountURL, err := common.NewGCPURLParts(scenarioHelper{}.getRawGCPAccountURL(a))
		a.Nil(err)
		gcpAccountURL.BucketName = "objectmatch*"
		urlStr := gcpAccountURL.URL()
		gcpServiceTraverser, err := newGCPServiceTraverser(&urlStr, ctx, false, func(entityType common.EntityType) {})
		a.Nil(err)

		gcpDummyProcessor = dummyProcessor{}
		err = gcpServiceTraverser.Traverse(noPreProccessor, gcpDummyProcessor.process, nil)
		a.Nil(err)
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
	a.Equal(localFileOnlyCount*2, len(blobDummyProcessor.record))
	a.Equal(localTotalCount*2, len(fileDummyProcessor.record))
	if testS3 {
		a.Equal(localFileOnlyCount*2, len(s3DummyProcessor.record))
		records = append(records, s3DummyProcessor.record...)
	}
	if testGCP {
		a.Equal(localFileOnlyCount*2, len(gcpDummyProcessor.record))
		records = append(records, gcpDummyProcessor.record...)
	}

	for _, storedObject := range records {
		correspondingLocalFile, present := localIndexer.indexMap[storedObject.relativePath]
		_, cnamePresent := cnames[storedObject.ContainerName]

		a.True(present)
		a.True(cnamePresent)
		a.Equal(storedObject.name, correspondingLocalFile.name)
	}
}
