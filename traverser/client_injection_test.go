package traverser

import (
	"context"
	"net/url"
	"testing"

	"github.com/Azure/azure-sdk-for-go/sdk/azcore"
	"github.com/Azure/azure-storage-azcopy/v10/common"
	"github.com/Azure/azure-storage-azcopy/v10/common/cred"
	"github.com/Azure/azure-storage-azcopy/v10/common/enum"
	"github.com/stretchr/testify/require"
)

func TestM3TraverserReusesProvidedServiceClient(t *testing.T) {
	client, err := common.GetServiceClientForLocation(common.ELocation.Blob(),
		common.ResourceString{Value: "https://example.blob.core.windows.net/container"},
		enum.ECredentialType.Anonymous(), nil, &azcore.ClientOptions{}, nil)
	require.NoError(t, err)
	service, err := client.BlobServiceClient()
	require.NoError(t, err)
	for _, prefix := range []string{"a/", "b/"} {
		resource := common.ResourceString{Value: "https://example.blob.core.windows.net/container/" + prefix}
		traversal, err := InitResourceTraverser(resource, common.ELocation.Blob(), context.Background(),
			InitResourceTraverserOptions{Client: client})
		require.NoError(t, err)
		blobTraversal, ok := traversal.(*BlobTraverser)
		require.True(t, ok)
		require.Same(t, service, blobTraversal.ServiceClient)
	}
}

func TestM3S3TraversalClientsAreReusedOnlyWithinJob(t *testing.T) {
	ctx := context.Background()
	resource, err := url.Parse("https://s3.amazonaws.com/example-bucket/prefix/")
	require.NoError(t, err)
	info := cred.CredentialInfo{CredentialType: enum.ECredentialType.S3PublicBucket()}
	firstJob := InitResourceTraverserOptions{Credential: &info, S3ClientManager: &S3ClientManager{}}
	secondJob := InitResourceTraverserOptions{Credential: &info, S3ClientManager: &S3ClientManager{}}
	first, err := NewS3Traverser(resource, ctx, firstJob)
	require.NoError(t, err)
	nextDirectory, err := NewS3Traverser(resource, ctx, firstJob)
	require.NoError(t, err)
	otherJob, err := NewS3Traverser(resource, ctx, secondJob)
	require.NoError(t, err)
	require.Same(t, first.s3Client, nextDirectory.s3Client)
	require.NotSame(t, first.s3Client, otherJob.s3Client)
}
