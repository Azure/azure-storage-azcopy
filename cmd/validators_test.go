package cmd

import (
	"testing"

	"github.com/Azure/azure-storage-azcopy/v10/common"
	"github.com/stretchr/testify/assert"
)

func TestInferArgumentLocation(t *testing.T) {
	a := assert.New(t)

	test := []struct {
		src              string
		expectedLocation common.Location
	}{
		{"https://test.blob.core.windows.net/container8", common.ELocation.Blob()},
		{"https://test.file.core.windows.net/container23", common.ELocation.File()},
		{"https://test.dfs.core.windows.net/container45", common.ELocation.BlobFS()},
		{"https://s3.amazonaws.com/bucket", common.ELocation.S3()},
		{"https://storage.cloud.google.com/bucket", common.ELocation.GCP()},
		{"https://privateendpoint.com/container1", common.ELocation.Unknown()},
		{"http://127.0.0.1:10000/devstoreaccount1/container1", common.ELocation.Unknown()},
		{"https://isd-storage.obs.ae-ad-1.g42cloud.com", common.ELocation.Unknown()},
	}

	for _, v := range test {
		loc := InferArgumentLocation(v.src)
		a.Equal(v.expectedLocation, loc)
	}
}
