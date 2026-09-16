package azcopy

import (
	"testing"

	"github.com/Azure/azure-storage-azcopy/v10/common"
	"github.com/Azure/azure-storage-azcopy/v10/telemetry"
	"github.com/stretchr/testify/assert"
)

func TestSourceEndpointKindAcrossJobDimensions(t *testing.T) {
	for _, test := range []struct {
		name        string
		fromTo      common.FromTo
		source      string
		destination string
		wantSource  string
		wantDest    string
	}{
		{"public download", common.EFromTo.BlobLocal(), "https://account.blob.core.windows.net/c", "local", "public", ""},
		{"private download", common.EFromTo.BlobLocal(), "https://account.privatelink.blob.core.windows.net/c", "local", "private-endpoint", ""},
		{"independent endpoints", common.EFromTo.BlobBlob(), "https://account.privatelink.blob.core.windows.net/c", "https://target.blob.core.windows.net/c", "private-endpoint", "public"},
		{"non Azure source", common.EFromTo.LocalBlob(), "local", "https://target.privatelink.blob.core.windows.net/c", "", "private-endpoint"},
		{"S3 source", common.EFromTo.S3Blob(), "https://s3.amazonaws.com/bucket", "https://target.blob.core.windows.net/c", "", "public"},
		{"GCP source", common.EFromTo.GCPBlob(), "https://storage.cloud.google.com/bucket", "https://target.blob.core.windows.net/c", "", "public"},
		{"NFS source", common.EFromTo.FileNFSLocal(), "https://account.privatelink.file.core.windows.net/share", "local", "private-endpoint", ""},
		{"BlobFS source", common.EFromTo.BlobFSLocal(), "https://account.privatelink.dfs.core.windows.net/c", "local", "private-endpoint", ""},
		{"malformed source", common.EFromTo.BlobLocal(), "https://%invalid", "local", "", ""},
	} {
		t.Run(test.name, func(t *testing.T) {
			source := common.ResourceString{Value: test.source}
			destination := common.ResourceString{Value: test.destination}
			credential := common.ECredentialType.Anonymous()
			dimensions := []telemetry.JobDimensions{
				copyJobDimensions(&CookedTransferOptions{fromTo: test.fromTo, source: source, destination: destination}, credential, credential),
				syncJobDimensions(&cookedSyncOptions{fromTo: test.fromTo, source: source, destination: destination}, credential, credential),
				resumeJobDimensions(common.GetJobDetailsResponse{FromTo: test.fromTo}, source, destination, credential, credential, telemetry.OptionAttributes{}),
			}
			for _, dimension := range dimensions {
				assert.Equal(t, test.wantSource, dimension.SourceEndpointKind, dimension.Command)
				assert.Equal(t, test.wantDest, dimension.DestEndpointKind, dimension.Command)
			}
		})
	}
}
