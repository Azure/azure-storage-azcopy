package traverser

import (
	"context"
	"testing"

	"github.com/Azure/azure-storage-azcopy/v10/common"
	"github.com/Azure/azure-storage-azcopy/v10/common/cred"
	"github.com/Azure/azure-storage-azcopy/v10/common/enum"
	"github.com/minio/minio-go/v7/pkg/credentials"
	"github.com/stretchr/testify/require"
)

func TestM6S3ManagerSeparatesProvidersEndpointsAndBuckets(t *testing.T) {
	manager := &S3ClientManager{}
	firstProvider := &credentials.Static{}
	secondProvider := &credentials.Static{}
	info := cred.CredentialInfo{CredentialType: enum.ECredentialType.S3AccessKey(),
		S3CredentialInfo: cred.S3CredentialInfo{Provider: firstProvider}}
	parts := common.S3URLParts{Endpoint: "one.invalid", Region: "us-east-1", BucketName: "first-bucket"}
	first, err := manager.GetS3Client(context.Background(), parts, info)
	require.NoError(t, err)
	same, err := manager.GetS3Client(context.Background(), parts, info)
	require.NoError(t, err)
	require.Same(t, first, same)
	info.S3CredentialInfo.Provider = secondProvider
	second, err := manager.GetS3Client(context.Background(), parts, info)
	require.NoError(t, err)
	require.NotSame(t, first, second)
	parts.Endpoint = "two.invalid"
	otherEndpoint, err := manager.GetS3Client(context.Background(), parts, info)
	require.NoError(t, err)
	require.Equal(t, "two.invalid", otherEndpoint.EndpointURL().Host)
	parts.BucketName = "second-bucket"
	otherBucket, err := manager.GetS3Client(context.Background(), parts, info)
	require.NoError(t, err)
	require.NotSame(t, otherEndpoint, otherBucket)
	parts.Region = "us-west-2"
	otherRegion, err := manager.GetS3Client(context.Background(), parts, info)
	require.NoError(t, err)
	require.NotSame(t, otherBucket, otherRegion)
}

func TestM6S3ManagerRejectedContextProviderDoesNotPoisonManager(t *testing.T) {
	manager := &S3ClientManager{}
	info := cred.CredentialInfo{CredentialType: enum.ECredentialType.S3PublicBucket()}
	parts := common.S3URLParts{Endpoint: "unit-test.invalid", Region: "us-east-1"}
	ctx := context.WithValue(context.Background(), customCreds, "not-a-provider")
	_, err := manager.GetS3Client(ctx, parts, info)
	require.ErrorContains(t, err, "credentials.Provider")
	cancelled, cancel := context.WithCancel(context.Background())
	cancel()
	_, err = manager.GetS3Client(cancelled, parts, info)
	require.ErrorIs(t, err, context.Canceled)
	client, err := manager.GetS3Client(context.Background(), parts, info)
	require.NoError(t, err)
	require.NotNil(t, client)
}
