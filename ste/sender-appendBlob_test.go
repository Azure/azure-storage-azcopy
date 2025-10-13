package ste

import (
	"context"
	"fmt"
	"net/http"
	"testing"
	"time"

	"github.com/Azure/azure-sdk-for-go/sdk/azcore"
	"github.com/Azure/azure-sdk-for-go/sdk/azcore/policy"
	"github.com/Azure/azure-sdk-for-go/sdk/azcore/streaming"
	"github.com/Azure/azure-sdk-for-go/sdk/azcore/to"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/appendblob"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/blob"
	blobsas "github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/sas"
	blobservice "github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/service"
	"github.com/Azure/azure-storage-azcopy/v10/common"
	"github.com/stretchr/testify/assert"
)

type appendErrorInjectionPolicy struct {
	timedOut bool
}

func (r *appendErrorInjectionPolicy) Do(req *policy.Request) (*http.Response, error) {
	if req.Raw().URL.Query().Get("comp") == "appendblock" && !r.timedOut {
		req.Next()
		var headers http.Header = make(map[string][]string)
		headers.Add("x-ms-error-code", "OperationTimedOut")
		r.timedOut = true
		return &http.Response{StatusCode: 500, Header: headers}, nil
	}
	return req.Next()
}

func Test500FollowedBy412Logic(t *testing.T) {
	a := assert.New(t)

	// Setup source and destination
	accountName, accountKey := getAccountAndKey()
	rawURL := fmt.Sprintf("https://%s.blob.core.windows.net/", accountName)

	credential, err := blob.NewSharedKeyCredential(accountName, accountKey)
	a.NoError(err)

	client, err := blobservice.NewClientWithSharedKeyCredential(rawURL, credential, &blobservice.ClientOptions{
		ClientOptions: azcore.ClientOptions{
			Transport: NewAzcopyHTTPClient(0),
		}})
	a.NoError(err)

	cName := generateContainerName()
	cc := client.NewContainerClient(cName)
	_, err = cc.Create(context.Background(), nil)
	a.NoError(err)
	defer cc.Delete(context.Background(), nil)

	sourceName := generateBlobName()
	sourceClient := cc.NewBlockBlobClient(sourceName)
	size := 1024 * 1024 * 10
	dataReader, _ := getDataAndReader(t.Name(), size)
	_, err = sourceClient.Upload(context.Background(), streaming.NopCloser(dataReader), nil)
	a.NoError(err)

	sasURL, err := cc.NewBlobClient(sourceName).GetSASURL(
		blobsas.BlobPermissions{Read: true},
		time.Now().Add(1*time.Hour),
		nil)
	a.NoError(err)

	destName := generateBlobName()
	destClient := cc.NewAppendBlobClient(destName)
	destClient.Create(context.Background(), nil)
	a.NoError(err)

	jptm := testJobPartTransferManager{
		info: to.Ptr(TransferInfo{
			Source:       sasURL,
			SrcContainer: cName,
			SrcFilePath:  sourceName,
		}),
		fromTo: common.EFromTo.BlobBlob(),
	}
	blobSIP, err := newBlobSourceInfoProvider(&jptm)
	a.NoError(err)

	injectionPolicy := &appendErrorInjectionPolicy{timedOut: false}
	destClient, err = appendblob.NewClientWithSharedKeyCredential(destClient.URL(), credential, &appendblob.ClientOptions{
		ClientOptions: azcore.ClientOptions{
			PerRetryPolicies: []policy.Policy{newRetryNotificationPolicy(), injectionPolicy},
			Transport:        NewAzcopyHTTPClient(0),
		},
	})
	a.NoError(err)
	base := appendBlobSenderBase{jptm: &jptm, destAppendBlobClient: destClient, sip: blobSIP}

	// Get MD5 range within service calculation
	offset := int64(0)
	count := int64(common.MaxRangeGetSize)
	var timeoutFromCtx bool
	ctx := withTimeoutNotification(context.Background(), &timeoutFromCtx)
	_, err = base.destAppendBlobClient.AppendBlockFromURL(ctx, sasURL,
		&appendblob.AppendBlockFromURLOptions{
			Range:                          blob.HTTPRange{Offset: offset, Count: count},
			AppendPositionAccessConditions: &appendblob.AppendPositionAccessConditions{AppendPosition: &offset},
		})
	errString, err := base.transformAppendConditionMismatchError(timeoutFromCtx, offset, count, err)
	a.NoError(err)
	a.Empty(errString)
}
