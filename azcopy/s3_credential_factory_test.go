package azcopy

import (
	"context"
	"sync/atomic"
	"testing"

	"github.com/Azure/azure-storage-azcopy/v10/common"
	"github.com/Azure/azure-storage-azcopy/v10/common/buildmode"
	"github.com/Azure/azure-storage-azcopy/v10/traverser"
	"github.com/minio/minio-go/v7/pkg/credentials"
	"github.com/stretchr/testify/require"
)

const (
	factoryS3Source   = "https://s3.amazonaws.com/bucket/prefix"
	factoryBlobSource = "https://source.blob.core.windows.net/container?sv=2020-08-04&sig=source"
	factoryBlobTarget = "https://target.blob.core.windows.net/container?sv=2020-08-04&sig=target"
)

func countingS3Factory(calls *atomic.Int32) S3CredentialProviderFactory {
	return func() credentials.Provider {
		calls.Add(1)
		return &credentials.Static{Value: credentials.Value{AccessKeyID: "id", SecretAccessKey: "secret"}}
	}
}

func TestCopyCallsS3CredentialFactoryOncePerConsumer(t *testing.T) {
	t.Setenv("AWS_ACCESS_KEY_ID", "id")
	t.Setenv("AWS_SECRET_ACCESS_KEY", "secret")
	for _, test := range []struct {
		name, source string
		expected     int32
	}{
		// One provider for enumeration and an independent one for the transfer engine.
		{"s3 source", factoryS3Source, 2},
		{"blob source", factoryBlobSource, 0},
	} {
		t.Run(test.name, func(t *testing.T) {
			var calls atomic.Int32
			_, err := newCopyExecutor(context.Background(), common.NewJobID(), test.source, factoryBlobTarget, CopyOptions{
				Recursive:                   true,
				S3CredentialProviderFactory: countingS3Factory(&calls),
				ErrorChannel:                make(chan traverser.TraverserErrorItemInfo),
			}, nil)
			require.NoError(t, err)
			require.Equal(t, test.expected, calls.Load())
		})
	}
}

func TestSyncCallsS3CredentialFactoryOncePerConsumer(t *testing.T) {
	t.Setenv("AWS_ACCESS_KEY_ID", "id")
	t.Setenv("AWS_SECRET_ACCESS_KEY", "secret")
	for _, test := range []struct {
		name, source string
		expected     int32
		needsMover   bool
	}{
		{"s3 source", factoryS3Source, 2, true},
		{"blob source", factoryBlobSource, 0, false},
	} {
		t.Run(test.name, func(t *testing.T) {
			if test.needsMover && !buildmode.IsMover {
				t.Skip("S3 to Blob sync requires a Mover build")
			}
			var calls atomic.Int32
			_, err := newSyncExecutor(context.Background(), common.NewJobID(), test.source, factoryBlobTarget, SyncOptions{
				S3CredentialProviderFactory: countingS3Factory(&calls),
				ErrorChannel:                make(chan traverser.TraverserErrorItemInfo),
			}, nil)
			require.NoError(t, err)
			require.Equal(t, test.expected, calls.Load())
		})
	}
}
