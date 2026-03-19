package cmd

import (
	"testing"

	"github.com/Azure/azure-storage-azcopy/v10/azcopy"
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
		loc := azcopy.InferArgumentLocation(v.src)
		a.Equal(v.expectedLocation, loc)
	}
}

func TestValidateFromTo(t *testing.T) {
	a := assert.New(t)

	test := []struct {
		userSpecifiedLocation string

		expectedFromTo common.FromTo
		expectedError  string
	}{
		{"LocalFileSMB", common.EFromTo.LocalFile(), ""},
		{"FileSMBLocal", common.EFromTo.FileLocal(), ""},
		{"LocalFile", common.EFromTo.LocalFile(), ""},
		{"FileLocal", common.EFromTo.FileLocal(), ""},

		{"BlobFileSMB", common.EFromTo.BlobFile(), ""},
		{"FileSMBBlob", common.EFromTo.FileBlob(), ""},
		{"BlobFile", common.EFromTo.BlobFile(), ""},
		{"FileBlob", common.EFromTo.FileBlob(), ""},

		{"FileSMBPipe", common.EFromTo.FilePipe(), ""},
		{"PipeFileSMB", common.EFromTo.PipeFile(), ""},
		{"PipeFile", common.EFromTo.PipeFile(), ""},
		{"FilePipe", common.EFromTo.FilePipe(), ""},

		{"BlobTrash", common.EFromTo.BlobTrash(), ""},
		{"BlobFSTrash", common.EFromTo.BlobFSTrash(), ""},
		{"FileTrash", common.EFromTo.FileTrash(), ""},
		{"FileSMBTrash", common.EFromTo.FileTrash(), ""},

		{"LocalBlobFS", common.EFromTo.LocalBlobFS(), ""},
		{"BlobFSLocal", common.EFromTo.BlobFSLocal(), ""},

		{"BlobFSBlobFS", common.EFromTo.BlobFSBlobFS(), ""},
		{"BlobFSBlob", common.EFromTo.BlobFSBlob(), ""},
		{"BlobFSFileSMB", common.EFromTo.BlobFSFile(), ""},
		{"BlobFSFile", common.EFromTo.BlobFSFile(), ""},

		{"BlobBlobFS", common.EFromTo.BlobBlobFS(), ""},
		{"FileSMBBlobFS", common.EFromTo.FileBlobFS(), ""},
		{"FileBlobFS", common.EFromTo.FileBlobFS(), ""},
		{"BlobBlob", common.EFromTo.BlobBlob(), ""},
		{"FileSMBBlob", common.EFromTo.FileBlob(), ""},
		{"FileBlob", common.EFromTo.FileBlob(), ""},
		{"BlobFileSMB", common.EFromTo.BlobFile(), ""},
		{"BlobFile", common.EFromTo.BlobFile(), ""},
		{"FileFile", common.EFromTo.FileFile(), ""},
		{"FileSMBFileSMB", common.EFromTo.FileFile(), ""},

		{"FileNFSFileSMB", common.EFromTo.FileNFSFileSMB(), ""},
		{"FileSMBFileNFS", common.EFromTo.FileSMBFileNFS(), ""},
		{"FileNFSFileNFS", common.EFromTo.FileNFSFileNFS(), ""},
		{"FileNFSLocal", common.EFromTo.FileNFSLocal(), ""},
		{"LocalFileNFS", common.EFromTo.LocalFileNFS(), ""},

		{"S3Blob", common.EFromTo.S3Blob(), ""},
		{"GCPBlob", common.EFromTo.GCPBlob(), ""},

		// Invalid cases
		{"Random", common.EFromTo.Unknown(), "invalid --from-to value specified"},
	}

	for _, v := range test {
		fromTo, err := azcopy.InferAndValidateFromTo("", "", v.userSpecifiedLocation)
		a.Equal(v.expectedFromTo, fromTo)
		a.Equal(err == nil, v.expectedError == "")
		if err != nil {
			a.Contains(err.Error(), v.expectedError)
		}
	}
}
