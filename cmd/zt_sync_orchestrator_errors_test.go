//go:build smslidingwindow

package cmd

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"runtime"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Azure/azure-storage-azcopy/v10/common"
	"github.com/Azure/azure-storage-azcopy/v10/common/enum"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestSyncOrchestratorContinuesAfterDirectoryErrors(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("the sync orchestrator normalizes source URLs as filesystem paths on Windows")
	}
	for _, mergeJoin := range []bool{false, true} {
		t.Run(fmt.Sprintf("mergeJoin=%t", mergeJoin), func(t *testing.T) {
			const failedDirs = 1100
			var listing strings.Builder
			for i := 0; i < failedDirs; i++ {
				fmt.Fprintf(&listing, "<BlobPrefix><Name>failed%04d/</Name></BlobPrefix>", i)
			}
			listing.WriteString("<BlobPrefix><Name>healthy/</Name></BlobPrefix>")
			var healthyListed atomic.Bool
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.Method == http.MethodHead {
					if strings.HasPrefix(r.URL.Path, "/source/failed") {
						w.Header().Set("x-ms-error-code", "AuthorizationPermissionMismatch")
						w.WriteHeader(http.StatusForbidden)
					} else {
						w.Header().Set("x-ms-error-code", "BlobNotFound")
						w.WriteHeader(http.StatusNotFound)
					}
					return
				}
				if r.Method != http.MethodGet || r.URL.Query().Get("comp") != "list" {
					t.Errorf("unexpected request: %s %s", r.Method, r.URL)
					w.WriteHeader(http.StatusBadRequest)
					return
				}
				var blobs string
				if strings.TrimSuffix(r.URL.Path, "/") == "/source" {
					switch r.URL.Query().Get("prefix") {
					case "":
						blobs = listing.String()
					case "healthy/":
						healthyListed.Store(true)
					default:
						t.Errorf("unexpected source prefix: %s", r.URL.Query().Get("prefix"))
					}
				}
				w.Header().Set("Content-Type", "application/xml")
				fmt.Fprintf(w, `<?xml version="1.0" encoding="utf-8"?><EnumerationResults><Blobs>%s</Blobs><NextMarker /></EnumerationResults>`, blobs)
			}))
			defer server.Close()

			oldOptions, oldParallelism, oldThrottling, oldUseOrchestrator := orchestratorOptions, crawlParallelism, enableThrottling, UseSyncOrchestrator
			t.Cleanup(func() {
				orchestratorOptions, crawlParallelism, enableThrottling, UseSyncOrchestrator = oldOptions, oldParallelism, oldThrottling, oldUseOrchestrator
			})
			orchestratorOptions = &SyncOrchestratorOptions{fromTo: common.EFromTo.BlobBlob()}
			crawlParallelism, enableThrottling, UseSyncOrchestrator = 64, false, true
			baseURL := strings.Replace(server.URL, "127.0.0.1", "localhost", 1)
			cca := &cookedSyncCmdArgs{
				fromTo:                common.EFromTo.BlobBlob(),
				source:                common.ResourceString{Value: baseURL + "/source"},
				destination:           common.ResourceString{Value: baseURL + "/target"},
				useStreamingMergeJoin: mergeJoin,
			}
			errorChannel := make(chan TraverserErrorItemInfo, 3*failedDirs)
			options := InitResourceTraverserOptions{
				Credential:   &common.CredentialInfo{CredentialType: enum.ECredentialType.Anonymous()},
				ErrorChannel: errorChannel,
			}
			ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
			defer cancel()
			primaryTraverser, err := InitResourceTraverser(cca.source, cca.fromTo.From(), ctx, options)
			require.NoError(t, err)
			finalized := false
			enumerator := &syncEnumerator{
				primaryTraverser: primaryTraverser,
				primaryTraverserTemplate: ResourceTraverserTemplate{
					location: common.ELocation.Blob(),
					options:  options,
				},
				secondaryTraverserTemplate: ResourceTraverserTemplate{
					location: common.ELocation.Blob(),
					options:  options,
				},
				objectIndexer:       newObjectIndexer(),
				objectComparator:    func(StoredObject) error { return nil },
				orchestratorOptions: orchestratorOptions,
				ctp: &copyTransferProcessor{
					copyJobTemplate:        &common.CopyJobPartOrderRequest{FromTo: cca.fromTo},
					folderPropertiesOption: common.EFolderPropertiesOption.NoFolders(),
				},
				finalize: func() error {
					finalized = true
					return nil
				},
			}
			require.NoError(t, cca.runSyncOrchestrator(enumerator, ctx))
			require.NoError(t, ctx.Err(), "enumeration must finish without waiting for cancellation")
			assert.True(t, finalized)
			assert.True(t, healthyListed.Load(), "healthy directories must still be enumerated")
			close(errorChannel)
			failedPaths := make(map[string]bool)
			for err := range errorChannel {
				if err.IsDir() {
					assert.Contains(t, err.ErrorMessage().Error(), "AuthorizationPermissionMismatch")
					assert.False(t, failedPaths[err.FullPath()], "directory failure must only be reported once")
					failedPaths[err.FullPath()] = true
				}
			}
			assert.Len(t, failedPaths, failedDirs)
		})
	}
}
