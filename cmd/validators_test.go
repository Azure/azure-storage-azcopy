package cmd

import (
	"github.com/Azure/azure-storage-azcopy/v10/common"
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestValidateArgumentLocation(t *testing.T) {
	a := assert.New(t)

	test := []struct {
		src                   string
		userSpecifiedLocation string

		expectedLocation common.Location
		expectedError    string
	}{
		// User does not specify location
		{"https://test.blob.core.windows.net/container1", "", common.ELocation.Blob(), ""},
		{"https://test.file.core.windows.net/container1", "", common.ELocation.File(), ""},
		{"https://test.dfs.core.windows.net/container1", "", common.ELocation.BlobFS(), ""},
		{"https://s3.amazonaws.com/bucket", "", common.ELocation.S3(), ""},
		{"https://storage.cloud.google.com/bucket", "", common.ELocation.GCP(), ""},
		{"https://privateendpoint.com/container1", "", common.ELocation.Unknown(), "the inferred location could not be identified, or is currently not supported"},
		{"http://127.0.0.1:10000/devstoreaccount1/container1", "", common.ELocation.Unknown(), "the inferred location could not be identified, or is currently not supported"},

		// User specifies location
		{"https://privateendpoint.com/container1", "FILE", common.ELocation.File(), ""},
		{"http://127.0.0.1:10000/devstoreaccount1/container1", "BloB", common.ELocation.Blob(), ""},
		{"https://test.file.core.windows.net/container1", "blobfs", common.ELocation.BlobFS(), ""}, // Tests that the endpoint doesnt really matter
		{"https://privateendpoint.com/container1", "random", common.ELocation.Unknown(), "invalid --location value specified"},
	}

	for _, v := range test {
		loc, err := ValidateArgumentLocation(v.src, v.userSpecifiedLocation)
		a.Equal(v.expectedLocation, loc)
		a.Equal(err == nil, v.expectedError == "")
		if err != nil {
			a.Contains(err.Error(), v.expectedError)
		}
	}
}
