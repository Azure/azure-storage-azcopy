package ste

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/Azure/azure-sdk-for-go/sdk/azcore"
	"github.com/Azure/azure-sdk-for-go/sdk/azcore/to"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azdatalake"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azdatalake/sas"
	datalakeservice "github.com/Azure/azure-sdk-for-go/sdk/storage/azdatalake/service"
	"github.com/Azure/azure-storage-azcopy/v10/common"
	"github.com/stretchr/testify/assert"
)

// This function tests the scenario where we return a transfer success even when we receive a 404 response, indicating a resource not found error.
// In this test, we create a container on an HNS enabled account but do not create any file. This is done to simulate the 404 scenario when attempting to delete a non-existent file/directory.
// The deletion operation won't find the file to delete, resulting in a 404 error, and thus returning a transfer success.
func Test404DeleteSuccessLogic(t *testing.T) {
	a := assert.New(t)

	// Setup source and destination
	accountName, accountKey := getAccountAndKey()
	rawURL := fmt.Sprintf("https://%s.dfs.core.windows.net/", accountName)

	credential, err := azdatalake.NewSharedKeyCredential(accountName, accountKey)
	a.NoError(err)

	client, err := datalakeservice.NewClientWithSharedKeyCredential(rawURL, credential, &datalakeservice.ClientOptions{
		ClientOptions: azcore.ClientOptions{
			Transport: NewAzcopyHTTPClient(0),
		}})
	a.NoError(err)

	cName := generateContainerName()
	cc := client.NewFileSystemClient(cName)
	_, err = cc.Create(context.Background(), nil)
	a.NoError(err)
	defer cc.Delete(context.Background(), nil)

	// Generating the name for a file without actually creating it.
	sourceName := generateBlobName()
	sasURL, err := client.GetSASURL(sas.AccountResourceTypes{Container: true}, sas.AccountPermissions{Read: true, Add: true, Create: true, Delete: true, Write: true, List: true},
		time.Now().Add(1*time.Hour), nil)
	a.NoError(err)

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

	a.NoError(err)
	a.Equal(jptm.status, common.ETransferStatus.Success())
}
