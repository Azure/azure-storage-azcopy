package traverser

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/Azure/azure-sdk-for-go/sdk/azcore"
	"github.com/Azure/azure-sdk-for-go/sdk/azcore/policy"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azfile/service"
	"github.com/Azure/azure-storage-azcopy/v10/common"
	"github.com/Azure/azure-storage-azcopy/v10/common/parallel"
	"github.com/stretchr/testify/require"
)

func TestM4FilePrefilterPreservesOrchestrationAndExtendedInfo(t *testing.T) {
	previousParallelism := EnumerationParallelism
	EnumerationParallelism = 2
	t.Cleanup(func() { EnumerationParallelism = previousParallelism })
	for _, scenario := range []struct {
		name      string
		recursive bool
		skipRoot  bool
		extended  bool
		prefix    string
	}{
		{name: "nonrecursive", prefix: "match"},
		{name: "nonrecursive-extended", extended: true, prefix: "match"},
		{name: "recursive", recursive: true},
		{name: "directory-orchestration", skipRoot: true, extended: true},
	} {
		t.Run(scenario.name, func(t *testing.T) {
			requests := make(chan *http.Request, 1)
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				w.Header().Set("Last-Modified", time.Unix(1700000000, 0).UTC().Format(http.TimeFormat))
				if r.URL.Query().Get("comp") != "list" {
					w.WriteHeader(http.StatusOK)
					return
				}
				requests <- r.Clone(context.Background())
				w.Header().Set("Content-Type", "application/xml")
				_, _ = fmt.Fprint(w, `<?xml version="1.0" encoding="utf-8"?><EnumerationResults><Entries/><NextMarker/></EnumerationResults>`)
			}))
			defer server.Close()
			client, err := service.NewClientWithNoCredential(server.URL, &service.ClientOptions{
				ClientOptions: azcore.ClientOptions{Retry: policy.RetryOptions{MaxRetries: -1}},
			})
			require.NoError(t, err)
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			traverser := &fileTraverser{
				rawURL: server.URL + "/share", serviceClient: client, ctx: ctx,
				recursive: scenario.recursive, skipRootProperties: scenario.skipRoot,
				includeExtendedInfo: scenario.extended,
				trailingDot:         common.ETrailingDotOption.Enable(), fromTo: common.EFromTo.FileFile(),
			}
			require.NoError(t, traverser.Traverse(nil, func(StoredObject) error { return nil }, BuildIncludeFilters([]string{"match*"})))
			select {
			case request := <-requests:
				require.Equal(t, "list", request.URL.Query().Get("comp"))
				require.Equal(t, scenario.prefix, request.URL.Query().Get("prefix"))
				if scenario.extended {
					require.Equal(t, "true", request.Header.Get("x-ms-file-extended-info"))
					require.Contains(t, request.URL.Query().Get("include"), "Timestamps")
				}
			default:
				t.Fatal("directory listing was not requested")
			}
		})
	}
}

func TestM4LegacyErrorAndMetadataCompatibility(t *testing.T) {
	require.ErrorIs(t, ErrIgnored, IgnoredError)
	ignored, err := getProcessingError(fmt.Errorf("wrapped: %w", IgnoredError))
	require.True(t, ignored)
	require.NoError(t, err)
	require.ErrorIs(t, common.ErrProxyLookupTimeout, common.ProxyLookupTimeoutError)
	require.ErrorIs(t, common.ErrChunkWriterAlreadyFailed, common.ChunkWriterAlreadyFailed)
	require.ErrorIs(t, parallel.ErrReaddirTimeout, parallel.ReaddirTimeoutError)

	metadata := make(common.Metadata)
	common.TryAddMetadata(metadata, "Hdi_isfolder", "true")
	common.TryAddMetadata(metadata, "hdi_isfolder", "false")
	value, ok := common.TryReadMetadata(metadata, "hdi_isfolder")
	require.True(t, ok)
	require.Equal(t, "true", *value)
	require.True(t, DoesBlobRepresentAFolder(metadata))
	require.Equal(t, common.EEntityType.Folder(), GetEntityType(metadata))
}
