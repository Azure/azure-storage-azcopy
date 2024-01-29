package ste

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/Azure/azure-sdk-for-go/sdk/azcore"
	"github.com/Azure/azure-sdk-for-go/sdk/azcore/to"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/blob"
	blobsas "github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/sas"
	blobservice "github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/service"
	"github.com/Azure/azure-storage-azcopy/v10/common"
	"github.com/stretchr/testify/assert"
)

// This function tests the scenario where we return a transfer success even when we receive a 404 response, indicating a resource not found error.
// In this test, we create a container on an HNS enabled account but do not create any blob. This is done to simulate the 404 scenario when attempting to delete a non-existent blob.
// The deletion operation won't find the blob to delete, resulting in a 404 error, and thus returning a transfer success.
func Test404DeleteSuccessLogic(t *testing.T) {
	a := assert.New(t)

	// Setup source and destination
	accountName, accountKey := getAccountAndKey()
	rawURL := fmt.Sprintf("https://%s.dfs.core.windows.net/", accountName)

	credential, err := blob.NewSharedKeyCredential(accountName, accountKey)
	a.Nil(err)
	client, err := blobservice.NewClientWithSharedKeyCredential(rawURL, credential, &blobservice.ClientOptions{
		ClientOptions: azcore.ClientOptions{
			Transport: NewAzcopyHTTPClient(0),
		}})
	a.Nil(err)

	cName := generateContainerName()
	cc := client.NewContainerClient(cName)
	_, err = cc.Create(context.Background(), nil)
	a.Nil(err)
	defer cc.Delete(context.Background(), nil)

	// Generating the name for a blob without actually creating it.
	sourceName := generateBlobName()

	sasURL, err := cc.NewBlobClient(sourceName).GetSASURL(
		blobsas.BlobPermissions{Read: true},
		time.Now().Add(1*time.Hour),
		nil)
	a.Nil(err)

	jptm := &testJobPartTransferManager{
		info: to.Ptr(TransferInfo{
			Source:       sasURL,
			SrcContainer: cName,
			SrcFilePath:  sourceName,
		}),
		fromTo: common.EFromTo.BlobFSTrash(),
	}
	jptm.SetStatus(common.ETransferStatus.Started())
	doDeleteHNSResource(jptm)

	a.Nil(err)
	a.Equal(jptm.status, common.ETransferStatus.Success())
}
