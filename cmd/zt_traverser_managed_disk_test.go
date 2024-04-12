package cmd

import "testing"

func TestLargeManagedDiskSnapshot(t *testing.T) {
	//a := assert.New(t)
	//
	//// Set up for this test.
	//// In Azure Portal create a managed disk of size 7.9 TB
	//// Select 'Create Snapshot'
	//// Under Snapshot Export
	//// Click on Generate URL and paste that URL below as largeMDSnapshot
	// largeMDSnapshot := "https://md-XXXXX.blob.storage.azure.net/XXXX/XXXX?snapshot=XXXXXXXXsv=2018-03-28&sr=b&si=XXXXXXX&sig=XXXXXXXXXX"
	//serviceClientWithSAS := scenarioHelper{}.getBlobServiceClientWithSASFromURL(a, largeMDSnapshot)
	//
	//// Create a page blob client with OAuth
	//blobClient, err := blob.NewClientWithNoCredential(largeMDSnapshot, nil)
	//a.NoError(err)
	//
	//blobProps, err := blobClient.GetProperties(context.TODO(), nil)
	//a.NoError(err)
	//a.Nil(blobProps.LastModified)
	//
	//blobTraverser := newBlobTraverser(largeMDSnapshot, serviceClientWithSAS, ctx, true, false, func(common.EntityType) {}, false, common.CpkOptions{}, false, false, false, common.EPreservePermissionsOption.None(), false)
	//blobDummyProcessor := dummyProcessor{}
	//err = blobTraverser.Traverse(noPreProccessor, blobDummyProcessor.process, nil)
	//a.NoError(err)
}

func TestLargeManagedDisk(t *testing.T) {
	//a := assert.New(t)
	//
	//// Set up for this test.
	//// In Azure Portal create a managed disk of size 7.9 TB
	//// Under Disk Export
	//// Click on Generate URL and paste that URL below as largeMD
	//// largeMD := "https://md-XXXXX.blob.storage.azure.net/XXXX/XXXX?sv=2018-03-28&sr=b&si=XXXXXXX&sig=XXXXXXXXXX"
	//serviceClientWithSAS := scenarioHelper{}.getBlobServiceClientWithSASFromURL(a, largeMD)
	//
	//// Create a page blob client with OAuth
	//blobClient, err := blob.NewClientWithNoCredential(largeMD, nil)
	//a.NoError(err)
	//
	//blobProps, err := blobClient.GetProperties(context.TODO(), nil)
	//a.NoError(err)
	//a.Nil(blobProps.LastModified)
	//
	//blobTraverser := newBlobTraverser(largeMD, serviceClientWithSAS, ctx, true, false, func(common.EntityType) {}, false, common.CpkOptions{}, false, false, false, common.EPreservePermissionsOption.None(), false)
	//blobDummyProcessor := dummyProcessor{}
	//err = blobTraverser.Traverse(noPreProccessor, blobDummyProcessor.process, nil)
	//a.NoError(err)
}
