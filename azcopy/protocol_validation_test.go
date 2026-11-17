package azcopy

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/Azure/azure-sdk-for-go/sdk/azcore"
	"github.com/Azure/azure-sdk-for-go/sdk/azcore/policy"
	"github.com/Azure/azure-storage-azcopy/v10/common"
	"github.com/Azure/azure-storage-azcopy/v10/common/enum"
	"github.com/stretchr/testify/require"
)

// newShareProtocolClient serves a fake Azure Files share that reports the given enabled protocols.
func newShareProtocolClient(t *testing.T, enabledProtocols string) (*common.ServiceClient, common.ResourceString) {
	t.Helper()
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Last-Modified", "Sun, 13 Sep 2020 12:26:40 GMT")
		w.Header().Set("ETag", `"0x1"`)
		w.Header().Set("x-ms-share-quota", "5")
		if enabledProtocols != "" {
			w.Header().Set("x-ms-enabled-protocols", enabledProtocols)
		}
	}))
	t.Cleanup(server.Close)
	share := common.ResourceString{Value: strings.Replace(server.URL, "127.0.0.1", "localhost", 1) + "/share"}
	client, err := common.GetServiceClientForLocation(common.ELocation.File(), share, enum.ECredentialType.Anonymous(), nil,
		&azcore.ClientOptions{Retry: policy.RetryOptions{MaxRetries: -1}}, &common.FileClientOptions{})
	require.NoError(t, err)
	return client, share
}

// TestValidateProtocolCompatibilityChecksTheSharesProtocol shows the check compares the share's reported protocol
// with the transfer type once a client is available.
func TestValidateProtocolCompatibilityChecksTheSharesProtocol(t *testing.T) {
	local := common.ResourceString{Value: t.TempDir()}
	for _, test := range []struct {
		name, shareProtocols string
		fromTo               common.FromTo
		expectedError        string
	}{
		{"SMB share with an SMB transfer", "SMB", common.EFromTo.LocalFile(), ""},
		{"unreported protocols default to SMB", "", common.EFromTo.LocalFile(), ""},
		{"NFS share with an NFS transfer", "NFS", common.EFromTo.LocalFileNFS(), ""},
		{"NFS share with an SMB transfer", "NFS", common.EFromTo.LocalFile(), "destination share has NFS protocol enabled"},
		{"SMB share with an NFS transfer", "SMB", common.EFromTo.LocalFileNFS(), "destination share has SMB protocol enabled"},
	} {
		t.Run(test.name, func(t *testing.T) {
			client, share := newShareProtocolClient(t, test.shareProtocols)
			err := ValidateProtocolCompatibility(context.Background(), test.fromTo, local, share, nil, client)
			if test.expectedError == "" {
				require.NoError(t, err)
				return
			}
			require.ErrorContains(t, err, test.expectedError)
		})
	}
}

// TestValidateProtocolCompatibilityAllowsOnlySMBSharesWithBlob covers File to/from Blob and BlobFS,
// where no NFS form of --from-to exists.
func TestValidateProtocolCompatibilityAllowsOnlySMBSharesWithBlob(t *testing.T) {
	blob := common.ResourceString{Value: "https://account.blob.core.windows.net/container"}
	for _, test := range []struct {
		name, shareProtocols string
		fromTo               common.FromTo
		shareIsSource        bool
		expectedError        string
	}{
		{"SMB share to Blob", "SMB", common.EFromTo.FileBlob(), true, ""},
		{"NFS share to Blob", "NFS", common.EFromTo.FileBlob(), true, "NFS share to Blob storage is not supported"},
		{"SMB share to BlobFS", "", common.EFromTo.FileBlobFS(), true, ""},
		{"NFS share to BlobFS", "NFS", common.EFromTo.FileBlobFS(), true, "NFS share to Blob storage is not supported"},
		{"Blob to SMB share", "SMB", common.EFromTo.BlobFile(), false, ""},
		{"Blob to NFS share", "NFS", common.EFromTo.BlobFile(), false, "Blob storage to an NFS share is not supported"},
		{"BlobFS to SMB share", "", common.EFromTo.BlobFSFile(), false, ""},
		{"BlobFS to NFS share", "NFS", common.EFromTo.BlobFSFile(), false, "Blob storage to an NFS share is not supported"},
	} {
		t.Run(test.name, func(t *testing.T) {
			client, share := newShareProtocolClient(t, test.shareProtocols)
			var err error
			if test.shareIsSource {
				err = ValidateProtocolCompatibility(context.Background(), test.fromTo, share, blob, client, nil)
			} else {
				err = ValidateProtocolCompatibility(context.Background(), test.fromTo, blob, share, nil, client)
			}
			if test.expectedError == "" {
				require.NoError(t, err)
				return
			}
			require.ErrorContains(t, err, test.expectedError)
		})
	}
}
