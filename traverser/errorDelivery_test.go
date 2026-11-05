package traverser

import (
	"context"
	"net/url"
	"testing"
	"time"

	blobservice "github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/service"
	fileservice "github.com/Azure/azure-sdk-for-go/sdk/storage/azfile/service"
	"github.com/Azure/azure-storage-azcopy/v10/common"
	"github.com/Azure/azure-storage-azcopy/v10/common/cred"
	"github.com/Azure/azure-storage-azcopy/v10/common/enum"
	"github.com/stretchr/testify/require"
)

func TestSendTraverserErrorWaitsForConsumer(t *testing.T) {
	ch := make(chan TraverserErrorItemInfo, 1)
	ch <- ErrorBlobInfo{BlobName: "first"}

	result := make(chan error, 1)
	go func() {
		result <- sendToErrorChannel(context.Background(), ch, ErrorBlobInfo{BlobName: "second"})
	}()

	select {
	case err := <-result:
		t.Fatalf("send returned while the channel was full: %v", err)
	case <-time.After(50 * time.Millisecond):
	}

	require.Equal(t, "first", (<-ch).Name())
	require.NoError(t, <-result)
	require.Equal(t, "second", (<-ch).Name())
}

func TestSendTraverserErrorCancellationUnblocksSender(t *testing.T) {
	ch := make(chan TraverserErrorItemInfo)
	ctx, cancel := context.WithCancel(context.Background())

	result := make(chan error, 1)
	go func() { result <- sendToErrorChannel(ctx, ch, ErrorBlobInfo{}) }()
	cancel()

	select {
	case err := <-result:
		require.ErrorIs(t, err, context.Canceled)
	case <-time.After(5 * time.Second):
		t.Fatal("cancellation did not unblock the sender")
	}
}

func TestSendTraverserErrorToClosedChannel(t *testing.T) {
	ch := make(chan TraverserErrorItemInfo, 1)
	close(ch)
	require.ErrorIs(t, sendToErrorChannel(context.Background(), ch, ErrorBlobInfo{}), errErrorChannelClosed)
}

func TestSendTraverserErrorWithoutChannelOrContext(t *testing.T) {
	require.NoError(t, sendToErrorChannel(context.Background(), nil, ErrorBlobInfo{}))

	ch := make(chan TraverserErrorItemInfo, 1)
	require.NoError(t, sendToErrorChannel(nil, ch, ErrorBlobInfo{BlobName: "x"})) //nolint:staticcheck // nil context is supported
	require.Equal(t, "x", (<-ch).Name())
}

func TestCountScanErrorAndSendToErrorChannelCountsEveryError(t *testing.T) {
	items := map[string]func(dir bool) TraverserErrorItemInfo{
		"blob": func(dir bool) TraverserErrorItemInfo { return ErrorBlobInfo{Dir: dir} },
		"file": func(dir bool) TraverserErrorItemInfo { return ErrorAzFileInfo{Dir: dir} },
		"s3":   func(dir bool) TraverserErrorItemInfo { return ErrorS3Info{Dir: dir} },
	}
	for name, item := range items {
		t.Run(name, func(t *testing.T) {
			var files, folders int
			count := func(entityType common.EntityType) {
				if entityType == common.EEntityType.Folder() {
					folders++
				} else {
					files++
				}
			}
			ch := make(chan TraverserErrorItemInfo, 2)

			countScanErrorAndSendToErrorChannel(context.Background(), ch, count, item(false))
			countScanErrorAndSendToErrorChannel(context.Background(), ch, count, item(true))
			require.Equal(t, 1, files)
			require.Equal(t, 1, folders)
			require.Len(t, ch, 2)

			// The count does not depend on a consumer being attached.
			countScanErrorAndSendToErrorChannel(context.Background(), nil, count, item(false))
			require.Equal(t, 2, files)

			// Delivery does not depend on a counter being attached.
			<-ch
			<-ch
			countScanErrorAndSendToErrorChannel(context.Background(), ch, nil, item(false))
			require.Len(t, ch, 1)
		})
	}
}

func TestS3TraverserReceivesErrorChannelAndCounter(t *testing.T) {
	resource, err := url.Parse("https://s3.amazonaws.com/example-bucket/prefix/")
	require.NoError(t, err)
	info := cred.CredentialInfo{CredentialType: enum.ECredentialType.S3PublicBucket()}
	ch := make(chan TraverserErrorItemInfo, 1)
	counted := 0
	traversal, err := NewS3Traverser(resource, context.Background(), InitResourceTraverserOptions{
		Credential: &info, S3ClientManager: &S3ClientManager{}, ErrorChannel: ch,
		IncrementScanError: func(common.EntityType) { counted++ },
	})
	require.NoError(t, err)

	countScanErrorAndSendToErrorChannel(traversal.ctx, traversal.errorChannel, traversal.incrementScanError, ErrorS3Info{S3Name: "bad"})
	require.Len(t, ch, 1)
	require.Equal(t, 1, counted)
}

func TestBlobAndFileTraverserConstructorsWireErrorChannelAndCounter(t *testing.T) {
	blobClient, err := blobservice.NewClientWithNoCredential("https://account.blob.core.windows.net", nil)
	require.NoError(t, err)
	fileClient, err := fileservice.NewClientWithNoCredential("https://account.file.core.windows.net", nil)
	require.NoError(t, err)

	type wiring struct {
		ctx   context.Context
		ch    chan<- TraverserErrorItemInfo
		count func(common.EntityType)
	}
	constructors := map[string]func(opts InitResourceTraverserOptions) wiring{
		"blob": func(opts InitResourceTraverserOptions) wiring {
			tr := NewBlobTraverser("https://account.blob.core.windows.net/container", blobClient, context.Background(), opts)
			return wiring{tr.ctx, tr.errorChannel, tr.incrementScanError}
		},
		"file": func(opts InitResourceTraverserOptions) wiring {
			tr := NewFileTraverser("https://account.file.core.windows.net/share", fileClient, context.Background(), opts)
			return wiring{tr.ctx, tr.errorChannel, tr.incrementScanError}
		},
	}
	for name, construct := range constructors {
		t.Run(name, func(t *testing.T) {
			ch := make(chan TraverserErrorItemInfo, 1)
			counted := 0
			w := construct(InitResourceTraverserOptions{
				ErrorChannel:       ch,
				IncrementScanError: func(common.EntityType) { counted++ },
			})

			countScanErrorAndSendToErrorChannel(w.ctx, w.ch, w.count, ErrorBlobInfo{BlobName: "bad"})
			require.Len(t, ch, 1)
			require.Equal(t, 1, counted)
		})
	}
}

func TestLocalTraverserReceivesErrorChannel(t *testing.T) {
	ch := make(chan TraverserErrorItemInfo, 1)
	traversal, err := NewLocalTraverser(t.TempDir(), context.Background(), InitResourceTraverserOptions{ErrorChannel: ch})
	require.NoError(t, err)

	traversal.errorChannel <- ErrorFileInfo{}
	require.Len(t, ch, 1)
}

func TestListTraverserForwardsErrorChannelToChildren(t *testing.T) {
	ch := make(chan TraverserErrorItemInfo, 1)
	list := newListTraverser(common.ResourceString{Value: t.TempDir()}, common.ELocation.Local(), context.Background(),
		InitResourceTraverserOptions{ListOfFiles: make(chan string), ErrorChannel: ch}).(*listTraverser)

	child, err := list.childTraverserGenerator("child")
	require.NoError(t, err)

	child.(*localTraverser).errorChannel <- ErrorFileInfo{}
	require.Len(t, ch, 1)
}
