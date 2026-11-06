package azcopy

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/Azure/azure-storage-azcopy/v10/common"
	"github.com/Azure/azure-storage-azcopy/v10/common/buildmode"
	"github.com/Azure/azure-storage-azcopy/v10/common/cred"
	"github.com/Azure/azure-storage-azcopy/v10/common/enum"
	"github.com/Azure/azure-storage-azcopy/v10/traverser"
	"github.com/minio/minio-go/v7/pkg/credentials"
	"github.com/stretchr/testify/require"
)

// refreshingProvider always reports itself as expired, so every Get through credentials.New retrieves again.
type refreshingProvider struct {
	retrieves atomic.Int32
}

func (p *refreshingProvider) Retrieve() (credentials.Value, error) {
	n := p.retrieves.Add(1)
	return credentials.Value{AccessKeyID: fmt.Sprintf("id-%d", n), SecretAccessKey: "secret"}, nil
}

func (p *refreshingProvider) RetrieveWithCredContext(*credentials.CredContext) (credentials.Value, error) {
	return p.Retrieve()
}

func (p *refreshingProvider) IsExpired() bool { return true }

// recordingFactory hands out a new refreshingProvider per call, in call order.
type recordingFactory struct {
	mu        sync.Mutex
	providers []*refreshingProvider
}

func (f *recordingFactory) create() credentials.Provider {
	f.mu.Lock()
	defer f.mu.Unlock()
	p := &refreshingProvider{}
	f.providers = append(f.providers, p)
	return p
}

// requireIndependentRefresh checks that the enumeration and transfer engine providers are distinct and refresh separately.
func requireIndependentRefresh(t *testing.T, factory *recordingFactory, traversal, ste credentials.Provider) {
	t.Helper()
	require.Len(t, factory.providers, 2, "one provider for enumeration and one for the transfer engine")
	require.Same(t, factory.providers[0], traversal)
	require.Same(t, factory.providers[1], ste)

	traversalCreds := credentials.New(traversal)
	for i := 1; i <= 3; i++ {
		value, err := traversalCreds.Get()
		require.NoError(t, err)
		require.Equal(t, fmt.Sprintf("id-%d", i), value.AccessKeyID, "an expired provider is retrieved again on every Get")
	}
	require.EqualValues(t, 3, factory.providers[0].retrieves.Load())
	require.Zero(t, factory.providers[1].retrieves.Load(), "refreshing enumeration credentials must not touch the transfer engine's")

	_, err := credentials.New(ste).Get()
	require.NoError(t, err)
	require.EqualValues(t, 3, factory.providers[0].retrieves.Load())
	require.EqualValues(t, 1, factory.providers[1].retrieves.Load())
}

func TestCopyS3CredentialsRefreshIndependently(t *testing.T) {
	t.Setenv("AWS_ACCESS_KEY_ID", "id")
	t.Setenv("AWS_SECRET_ACCESS_KEY", "secret")
	factory := &recordingFactory{}
	executor, err := newCopyExecutor(context.Background(), common.NewJobID(), factoryS3Source, factoryBlobTarget, CopyOptions{
		Recursive:                   true,
		S3CredentialProviderFactory: factory.create,
		ErrorChannel:                make(chan traverser.TraverserErrorItemInfo),
	}, nil)
	require.NoError(t, err)

	require.Equal(t, enum.ECredentialType.S3AccessKey(), executor.trp.srcCredType)
	requireIndependentRefresh(t, factory, executor.trp.srcCredInfo.S3CredentialInfo.Provider, executor.trp.s3CredentialProvider)
}

func TestSyncS3CredentialsRefreshIndependently(t *testing.T) {
	if !buildmode.IsMover {
		t.Skip("S3 to Blob sync requires a Mover build")
	}
	t.Setenv("AWS_ACCESS_KEY_ID", "id")
	t.Setenv("AWS_SECRET_ACCESS_KEY", "secret")
	factory := &recordingFactory{}
	executor, err := newSyncExecutor(context.Background(), common.NewJobID(), factoryS3Source, factoryBlobTarget, SyncOptions{
		S3CredentialProviderFactory: factory.create,
		ErrorChannel:                make(chan traverser.TraverserErrorItemInfo),
	}, nil)
	require.NoError(t, err)

	require.Equal(t, enum.ECredentialType.S3AccessKey(), executor.srp.srcCredType)
	requireIndependentRefresh(t, factory, executor.srp.srcCredInfo.S3CredentialInfo.Provider, executor.srp.s3CredentialProvider)
}

func TestResumedS3CredentialsUseTheSuppliedProviderForRefresh(t *testing.T) {
	factory := &recordingFactory{}
	info := cred.CredentialInfo{CredentialType: enum.ECredentialType.S3PublicBucket()}

	applyS3CredentialProvider(&info, factory.create())

	require.Equal(t, enum.ECredentialType.S3AccessKey(), info.CredentialType)
	require.Same(t, factory.providers[0], info.S3CredentialInfo.Provider)

	_, err := credentials.New(info.S3CredentialInfo.Provider).Get()
	require.NoError(t, err)
	require.EqualValues(t, 1, factory.providers[0].retrieves.Load())
}

func TestNilS3ProviderKeepsEnvironmentCredentials(t *testing.T) {
	original := cred.CredentialInfo{CredentialType: enum.ECredentialType.S3PublicBucket()}
	info := original

	applyS3CredentialProvider(&info, nil)

	require.Equal(t, original, info)
}
