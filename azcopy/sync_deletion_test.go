package azcopy

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/Azure/azure-sdk-for-go/sdk/azcore"
	"github.com/Azure/azure-sdk-for-go/sdk/azcore/policy"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/container"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azfile/share"
	"github.com/Azure/azure-storage-azcopy/v10/common"
	"github.com/stretchr/testify/require"
)

func TestM3RecursiveDeletionReportsListingFailure(t *testing.T) {
	for _, cancelled := range []bool{false, true} {
		for _, protocol := range []string{"blob", "file"} {
			t.Run(protocol+map[bool]string{false: "/service-error", true: "/cancelled"}[cancelled], func(t *testing.T) {
				server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
					w.Header().Set("x-ms-error-code", "InternalError")
					w.WriteHeader(http.StatusInternalServerError)
				}))
				defer server.Close()
				ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
				defer cancel()
				if cancelled {
					cancel()
				}
				manager := common.NewFolderDeletionManager(ctx, common.EFolderPropertiesOption.NoFolders(), nil)
				deleter := &remoteResourceDeleter{ctx: ctx, folderManager: manager}
				options := azcore.ClientOptions{Retry: policy.RetryOptions{MaxRetries: -1}}
				var err error
				if protocol == "blob" {
					client, createErr := container.NewClientWithNoCredential(server.URL+"/container", &container.ClientOptions{ClientOptions: options})
					require.NoError(t, createErr)
					err = deleter.enumerateAndRegisterBlobs(ctx, client, "prefix/", manager, nil, "container")
				} else {
					client, createErr := share.NewClientWithNoCredential(server.URL+"/share", &share.ClientOptions{ClientOptions: options})
					require.NoError(t, createErr)
					err = deleter.enumerateAndRegisterFiles(ctx, client, "prefix", manager, nil, "share")
				}
				if cancelled {
					require.ErrorIs(t, err, context.Canceled)
				} else {
					require.ErrorContains(t, err, "recursive deletion")
				}
			})
		}
	}
}
