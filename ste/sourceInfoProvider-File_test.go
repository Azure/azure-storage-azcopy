package ste

import (
	"github.com/Azure/azure-sdk-for-go/sdk/azcore/to"
	"github.com/Azure/azure-storage-azcopy/v10/common"
	"github.com/Azure/azure-storage-azcopy/v10/mock_server"
	"github.com/stretchr/testify/assert"
	"testing"
	"time"
)

// This test calls GetFreshFileLastModifiedTime on the FileSourceInfoProvider and successfully returns the last modified properties time
func TestGetFreshFileLastModified(t *testing.T) {
	a := assert.New(t)
	srv, close := mock_server.NewServer(mock_server.WithTransformAllRequestsToTestServerUrl())
	defer close()
	_, rawURL, cName, fName := SetUpVariables()

	jptm := testJobPartTransferManager{
		info: to.Ptr(TransferInfo{
			Source:       rawURL,
			SrcContainer: cName,
			SrcFilePath:  fName,
		}),
		fromTo:    common.EFromTo.FileBlob(),
		transport: srv,
	}
	fileSIP, err := newFileSourceInfoProvider(&jptm)
	a.Nil(err)

	srv.AppendResponse(mock_server.WithStatusCode(200), mock_server.WithHeader("Last-Modified", "Tue, 25 Jun 2024 21:28:05 GMT"))

	LastModified, err := fileSIP.GetFreshFileLastModifiedTime()
	a.Nil(err)
	a.NotNil(LastModified)
}

// This test calls GetFreshFileLastModifiedTime on the FileSourceInfoProvider and returns an error and the default last modified time value
func TestGetFreshFileLastModifiedError(t *testing.T) {
	a := assert.New(t)

	srv, close := mock_server.NewServer(mock_server.WithTransformAllRequestsToTestServerUrl())
	defer close()
	_, rawURL, cName, fName := SetUpVariables()

	jptm := testJobPartTransferManager{
		info: to.Ptr(TransferInfo{
			Source:       rawURL,
			SrcContainer: cName,
			SrcFilePath:  fName,
		}),
		fromTo:    common.EFromTo.FileBlob(),
		transport: srv,
	}
	fileSIP, err := newFileSourceInfoProvider(&jptm)
	a.Nil(err)

	srv.AppendResponse(mock_server.WithStatusCode(404), mock_server.WithHeader("x-ms-error-code", "BlobNotFound"), mock_server.WithBody([]byte(MockErrorBody("BlobNotFound"))))

	LastModified, err := fileSIP.GetFreshFileLastModifiedTime()
	a.NotNil(err)
	// check that last modified time is set to default time.Time due to error
	a.Equal(LastModified, time.Time{})
}

// This test calls getFreshProperties for a file and returns the set properties
func TestGetFreshPropFile(t *testing.T) {
	a := assert.New(t)

	srv, close := mock_server.NewServer(mock_server.WithTransformAllRequestsToTestServerUrl())
	defer close()
	_, rawURL, cName, fName := SetUpVariables()

	jptm := testJobPartTransferManager{
		info: to.Ptr(TransferInfo{
			Source:       rawURL,
			SrcContainer: cName,
			SrcFilePath:  fName,
		}),
		fromTo:    common.EFromTo.FileBlob(),
		transport: srv,
	}
	fileSIP, err := newFileSourceInfoProvider(&jptm)
	a.Nil(err)

	srv.AppendResponse(mock_server.WithStatusCode(200), mock_server.WithHeader("x-ms-meta-m1", "v1"))

	properties, err := fileSIP.(*fileSourceInfoProvider).getFreshProperties()
	a.Nil(err)
	// check that the correct metadata is returned from the call
	a.Equal(*properties.Metadata()["M1"], "v1")
}

// This test calls getFreshProperties for a file and returns an error and no properties
func TestGetFreshPropFileError(t *testing.T) {
	a := assert.New(t)

	srv, close := mock_server.NewServer(mock_server.WithTransformAllRequestsToTestServerUrl())
	defer close()
	_, rawURL, cName, fName := SetUpVariables()

	jptm := testJobPartTransferManager{
		info: to.Ptr(TransferInfo{
			Source:       rawURL,
			SrcContainer: cName,
			SrcFilePath:  fName,
		}),
		fromTo:    common.EFromTo.FileBlob(),
		transport: srv,
	}
	fileSIP, err := newFileSourceInfoProvider(&jptm)
	a.Nil(err)

	srv.AppendResponse(mock_server.WithStatusCode(400), mock_server.WithHeader("x-ms-error-code", "FileInvalidPermission"), mock_server.WithBody([]byte(MockErrorBody("FileInvalidPermission"))))

	properties, err := fileSIP.(*fileSourceInfoProvider).getFreshProperties()
	a.NotNil(err)
	a.Nil(properties.Metadata())
}

// This test calls getFreshProperties for a folder and returns the set properties
func TestGetFreshPropFolder(t *testing.T) {
	a := assert.New(t)

	srv, close := mock_server.NewServer(mock_server.WithTransformAllRequestsToTestServerUrl())
	defer close()
	_, rawURL, cName, fName := SetUpVariables()

	jptm := testJobPartTransferManager{
		info: to.Ptr(TransferInfo{
			Source:       rawURL,
			SrcContainer: cName,
			SrcFilePath:  fName,
			// set the entity type as a folder
			EntityType: common.EEntityType.Folder(),
		}),
		fromTo:    common.EFromTo.FileBlob(),
		transport: srv,
	}
	fileSIP, err := newFileSourceInfoProvider(&jptm)
	a.Nil(err)

	srv.AppendResponse(mock_server.WithStatusCode(200), mock_server.WithHeader("x-ms-meta-m1", "v1"))

	properties, err := fileSIP.(*fileSourceInfoProvider).getFreshProperties()
	a.Nil(err)
	a.Equal(*properties.Metadata()["M1"], "v1")
}

// This test calls getFreshProperties for a folder and returns an error and no properties
func TestGetFreshPropFolderError(t *testing.T) {
	a := assert.New(t)

	srv, close := mock_server.NewServer(mock_server.WithTransformAllRequestsToTestServerUrl())
	defer close()
	_, rawURL, cName, fName := SetUpVariables()

	jptm := testJobPartTransferManager{
		info: to.Ptr(TransferInfo{
			Source:       rawURL,
			SrcContainer: cName,
			SrcFilePath:  fName,
			EntityType:   common.EEntityType.Folder(),
		}),
		fromTo:    common.EFromTo.FileBlobFS(),
		transport: srv,
	}
	fileSIP, err := newFileSourceInfoProvider(&jptm)
	a.Nil(err)

	srv.AppendResponse(mock_server.WithStatusCode(400), mock_server.WithHeader("x-ms-error-code", "FileInvalidPermission"), mock_server.WithBody([]byte(MockErrorBody("FileInvalidPermission"))))

	properties, err := fileSIP.(*fileSourceInfoProvider).getFreshProperties()
	a.NotNil(err)
	a.Nil(properties.Metadata())
}

// This test calls getCachedProperties for a file and returns several set properties
func TestGetCachedPropFile(t *testing.T) {
	a := assert.New(t)

	srv, close := mock_server.NewServer(mock_server.WithTransformAllRequestsToTestServerUrl())
	defer close()
	_, rawURL, cName, fName := SetUpVariables()

	jptm := testJobPartTransferManager{
		info: to.Ptr(TransferInfo{
			Source:       rawURL,
			SrcContainer: cName,
			SrcFilePath:  fName,
		}),
		fromTo:    common.EFromTo.FileBlob(),
		transport: srv,
	}
	fileSIP, err := newFileSourceInfoProvider(&jptm)
	a.Nil(err)
	srv.AppendResponse(mock_server.WithStatusCode(200), mock_server.WithHeader("x-ms-meta-m1", "v1"), mock_server.WithHeader("Content-Type", "text/plain"), mock_server.WithHeader("x-ms-file-creation-time", "2024-01-01T15:04:05.0000000Z"))
	properties, err := fileSIP.(*fileSourceInfoProvider).getCachedProperties()
	a.Nil(err)
	//check metadata, content type, and file creation time
	a.Equal(*properties.Metadata()["M1"], "v1")
	a.Equal(properties.ContentType(), "text/plain")
	a.NotNil(properties.FileCreationTime())
	//ensure that the properties are cached and another call does not require mocked response but matches original
	properties2, err := fileSIP.(*fileSourceInfoProvider).getCachedProperties()
	a.Nil(err)
	a.Equal(properties, properties2)
}

// This test calls getCachedProperties for a file and returns an error and no properties
func TestGetCachedPropFileError(t *testing.T) {
	a := assert.New(t)

	srv, close := mock_server.NewServer(mock_server.WithTransformAllRequestsToTestServerUrl())
	defer close()
	_, rawURL, cName, fName := SetUpVariables()

	jptm := testJobPartTransferManager{
		info: to.Ptr(TransferInfo{
			Source:       rawURL,
			SrcContainer: cName,
			SrcFilePath:  fName,
		}),
		fromTo:    common.EFromTo.FileBlob(),
		transport: srv,
	}
	fileSIP, err := newFileSourceInfoProvider(&jptm)
	a.Nil(err)

	srv.AppendResponse(mock_server.WithStatusCode(400), mock_server.WithHeader("x-ms-error-code", "FileInvalidPermission"), mock_server.WithBody([]byte(MockErrorBody("FileInvalidPermission"))))

	properties, err := fileSIP.(*fileSourceInfoProvider).getCachedProperties()
	a.NotNil(err)
	a.Nil(properties.Metadata())
}

// This test calls getCachedProperties for a folder and returns several set properties
func TestGetCachedPropFolder(t *testing.T) {
	a := assert.New(t)

	srv, close := mock_server.NewServer(mock_server.WithTransformAllRequestsToTestServerUrl())
	defer close()
	_, rawURL, cName, fName := SetUpVariables()

	jptm := testJobPartTransferManager{
		info: to.Ptr(TransferInfo{
			Source:       rawURL,
			SrcContainer: cName,
			SrcFilePath:  fName,
			// make sure that entity type is a folder
			EntityType: common.EEntityType.Folder(),
		}),
		fromTo:    common.EFromTo.FileBlob(),
		transport: srv,
	}
	fileSIP, err := newFileSourceInfoProvider(&jptm)
	a.Nil(err)

	srv.AppendResponse(mock_server.WithStatusCode(200), mock_server.WithHeader("x-ms-meta-m1", "v1"), mock_server.WithHeader("x-ms-file-permission-key", "TestPermissionValue"), mock_server.WithHeader("x-ms-file-creation-time", "2024-01-01T15:04:05.0000000Z"))

	properties, err := fileSIP.(*fileSourceInfoProvider).getCachedProperties()
	a.Nil(err)
	// check metadata, creation time, and permission key are stored correctly
	a.Equal(*properties.Metadata()["M1"], "v1")
	a.NotNil(properties.FileCreationTime())
	a.Equal(properties.FilePermissionKey(), "TestPermissionValue")
	//ensure that the properties are cached and another call does not require mocked response but matches original
	properties2, err := fileSIP.(*fileSourceInfoProvider).getCachedProperties()
	a.Nil(err)
	a.Equal(properties, properties2)
}

// This test calls getCachedProperties for a folder and returns an error and no properties
func TestGetCachedPropFolderError(t *testing.T) {
	a := assert.New(t)

	srv, close := mock_server.NewServer(mock_server.WithTransformAllRequestsToTestServerUrl())
	defer close()
	_, rawURL, cName, fName := SetUpVariables()

	jptm := testJobPartTransferManager{
		info: to.Ptr(TransferInfo{
			Source:       rawURL,
			SrcContainer: cName,
			SrcFilePath:  fName,
			//set entity type as folder
			EntityType: common.EEntityType.Folder(),
		}),
		fromTo:    common.EFromTo.FileBlobFS(),
		transport: srv,
	}
	fileSIP, err := newFileSourceInfoProvider(&jptm)
	a.Nil(err)

	srv.AppendResponse(mock_server.WithStatusCode(400), mock_server.WithHeader("x-ms-error-code", "FileInvalidPermission"), mock_server.WithBody([]byte(MockErrorBody("FileInvalidPermission"))))

	properties, err := fileSIP.(*fileSourceInfoProvider).getCachedProperties()
	a.NotNil(err)
	a.Nil(properties.Metadata())
}

// This test calls Properties for a file and gets properties in backend, returning src properties
func TestSRCPropertiesBackendFile(t *testing.T) {
	a := assert.New(t)

	srv, close := mock_server.NewServer(mock_server.WithTransformAllRequestsToTestServerUrl())
	defer close()
	_, rawURL, cName, fName := SetUpVariables()

	jptm := testJobPartTransferManager{
		info: to.Ptr(TransferInfo{
			Source:       rawURL,
			SrcContainer: cName,
			SrcFilePath:  fName,
			//set S2SGetPropertiesInBackend to true
			S2SGetPropertiesInBackend: true,
		}),
		fromTo:    common.EFromTo.FileBlob(),
		transport: srv,
	}
	fileSIP, err := newFileSourceInfoProvider(&jptm)
	a.Nil(err)

	srv.AppendResponse(mock_server.WithStatusCode(200), mock_server.WithHeader("x-ms-meta-m1", "v1"))

	properties, err := fileSIP.(*fileSourceInfoProvider).Properties()
	a.Nil(err)
	a.Equal(*properties.SrcMetadata["M1"], "v1")
}

// This test calls Properties for a folder and gets properties in backend, returning src properties
func TestSRCPropertiesBackendFolder(t *testing.T) {
	a := assert.New(t)

	srv, close := mock_server.NewServer(mock_server.WithTransformAllRequestsToTestServerUrl())
	defer close()
	_, rawURL, cName, fName := SetUpVariables()

	jptm := testJobPartTransferManager{
		info: to.Ptr(TransferInfo{
			Source:       rawURL,
			SrcContainer: cName,
			SrcFilePath:  fName,
			// set entity type as folder and S2SGetPropertiesInBackend to true
			EntityType:                common.EEntityType.Folder(),
			S2SGetPropertiesInBackend: true,
		}),
		fromTo:    common.EFromTo.FileBlob(),
		transport: srv,
	}
	fileSIP, err := newFileSourceInfoProvider(&jptm)
	a.Nil(err)

	srv.AppendResponse(mock_server.WithStatusCode(200), mock_server.WithHeader("x-ms-meta-m1", "v1"))

	properties, err := fileSIP.(*fileSourceInfoProvider).Properties()
	a.Nil(err)
	a.Equal(*properties.SrcMetadata["M1"], "v1")
}
