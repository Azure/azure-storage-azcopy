package cmd

import (
	"testing"

	"github.com/Azure/azure-storage-azcopy/v10/common"
	"github.com/stretchr/testify/assert"
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
		{"https://test.file.core.windows.net/container1", "blobfs", common.ELocation.BlobFS(), ""}, // Tests that the endpoint does not really matter
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

// TestValidateSymlinkHandlingMode locks in the fromTo matrix for
// SymlinkHandlingType.Preserve(). This is the same validation sync and copy
// cook paths both call to reject unsupported combinations (e.g. LocalFile)
// before enumeration starts.
func TestValidateSymlinkHandlingMode(t *testing.T) {
	a := assert.New(t)

	preserve := common.ESymlinkHandlingType.Preserve()
	follow := common.ESymlinkHandlingType.Follow()
	skip := common.ESymlinkHandlingType.Skip()

	cases := []struct {
		name      string
		handling  common.SymlinkHandlingType
		fromTo    common.FromTo
		expectErr bool
	}{
		// Supported Preserve combinations: Local<->Blob and Blob<->Blob families.
		{"Preserve+LocalBlob", preserve, common.EFromTo.LocalBlob(), false},
		{"Preserve+BlobLocal", preserve, common.EFromTo.BlobLocal(), false},
		{"Preserve+LocalBlobFS", preserve, common.EFromTo.LocalBlobFS(), false},
		{"Preserve+BlobFSLocal", preserve, common.EFromTo.BlobFSLocal(), false},
		{"Preserve+BlobBlob", preserve, common.EFromTo.BlobBlob(), false},
		{"Preserve+BlobBlobFS", preserve, common.EFromTo.BlobBlobFS(), false},
		{"Preserve+BlobFSBlob", preserve, common.EFromTo.BlobFSBlob(), false},
		{"Preserve+BlobFSBlobFS", preserve, common.EFromTo.BlobFSBlobFS(), false},

		// Unsupported Preserve combinations must be rejected.
		{"Preserve+LocalFile", preserve, common.EFromTo.LocalFile(), true},
		{"Preserve+FileLocal", preserve, common.EFromTo.FileLocal(), true},
		{"Preserve+FileFile", preserve, common.EFromTo.FileFile(), true},
		{"Preserve+FileBlob", preserve, common.EFromTo.FileBlob(), true},
		{"Preserve+BlobFile", preserve, common.EFromTo.BlobFile(), true},

		// Non-Preserve modes must pass regardless of fromTo.
		{"Follow+LocalFile", follow, common.EFromTo.LocalFile(), false},
		{"Follow+LocalBlob", follow, common.EFromTo.LocalBlob(), false},
		{"Skip+LocalFile", skip, common.EFromTo.LocalFile(), false},
		{"Skip+BlobBlob", skip, common.EFromTo.BlobBlob(), false},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			err := validateSymlinkHandlingMode(tc.handling, tc.fromTo)
			if tc.expectErr {
				a.Error(err, "expected error for %s", tc.name)
				if err != nil {
					a.Contains(err.Error(), common.PreserveSymlinkFlagName)
				}
			} else {
				a.NoError(err, "did not expect error for %s", tc.name)
			}
		})
	}
}
