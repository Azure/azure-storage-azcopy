// Copyright © 2017 Microsoft <wastore@microsoft.com>
//
// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files (the "Software"), to deal
// in the Software without restriction, including without limitation the rights
// to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
// copies of the Software, and to permit persons to whom the Software is
// furnished to do so, subject to the following conditions:
//
// The above copyright notice and this permission notice shall be included in
// all copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
// THE SOFTWARE.

package cmd

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/Azure/azure-sdk-for-go/sdk/azcore"
	"github.com/Azure/azure-sdk-for-go/sdk/azcore/to"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/blob"
	blobservice "github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/service"
	datalakedirectory "github.com/Azure/azure-sdk-for-go/sdk/storage/azdatalake/directory"
	"github.com/Azure/azure-storage-azcopy/v10/common"
	"github.com/Azure/azure-storage-azcopy/v10/mock_server"
	"github.com/Azure/azure-storage-azcopy/v10/ste"
	"github.com/Azure/azure-storage-azcopy/v10/traverser"
	"github.com/stretchr/testify/assert"
)

func TestIsSourceDirWithStub(t *testing.T) {
	a := assert.New(t)
	bsc := getBlobServiceClient()

	// Generate source container and blobs
	cc, containerName := createNewContainer(a, bsc)
	defer deleteContainer(a, cc)
	a.NotNil(cc)

	dirName := "source_dir"
	createNewDirectoryStub(a, cc, dirName)

	// set up to create blob traverser
	ctx := context.WithValue(context.TODO(), ste.ServiceAPIVersionOverride, ste.DefaultServiceApiVersion)

	// List
	rawBlobURLWithSAS := scenarioHelper{}.getBlobClientWithSAS(a, containerName, dirName).URL()
	serviceClientWithSAS := scenarioHelper{}.getBlobServiceClientWithSASFromURL(a, rawBlobURLWithSAS)
	blobTraverser := traverser.NewBlobTraverser(rawBlobURLWithSAS, serviceClientWithSAS, ctx, traverser.InitResourceTraverserOptions{
		Recursive:             true,
		IncludeDirectoryStubs: true,
	})

	isDir, err := blobTraverser.IsDirectory(true)
	a.True(isDir)
	a.Nil(err)
}

func TestIsSourceDirWithNoStub(t *testing.T) {
	a := assert.New(t)
	bsc := getBlobServiceClient()

	// Generate source container and blobs
	cc, containerName := createNewContainer(a, bsc)
	defer deleteContainer(a, cc)
	a.NotNil(cc)

	dirName := "source_dir/"
	ctx := context.WithValue(context.TODO(), ste.ServiceAPIVersionOverride, ste.DefaultServiceApiVersion)

	// List
	rawBlobURLWithSAS := scenarioHelper{}.getBlobClientWithSAS(a, containerName, dirName).URL()
	serviceClientWithSAS := scenarioHelper{}.getBlobServiceClientWithSASFromURL(a, rawBlobURLWithSAS)
	blobTraverser := traverser.NewBlobTraverser(rawBlobURLWithSAS, serviceClientWithSAS, ctx, traverser.InitResourceTraverserOptions{
		Recursive:             true,
		IncludeDirectoryStubs: true,
	})

	isDir, err := blobTraverser.IsDirectory(true)
	a.True(isDir)
	a.Nil(err)
}

func TestIsDestDirWithBlobEP(t *testing.T) {
	a := assert.New(t)
	bsc := getBlobServiceClient()

	// Generate source container and blobs
	containerURL, containerName := createNewContainer(a, bsc)
	defer deleteContainer(a, containerURL)
	a.NotNil(containerURL)

	dirName := "dest_dir/"
	ctx := context.WithValue(context.TODO(), ste.ServiceAPIVersionOverride, ste.DefaultServiceApiVersion)

	// List
	rawBlobURLWithSAS := scenarioHelper{}.getBlobClientWithSAS(a, containerName, dirName).URL()
	serviceClientWithSAS := scenarioHelper{}.getBlobServiceClientWithSASFromURL(a, rawBlobURLWithSAS)
	blobTraverser := traverser.NewBlobTraverser(rawBlobURLWithSAS, serviceClientWithSAS, ctx, traverser.InitResourceTraverserOptions{
		Recursive:             true,
		IncludeDirectoryStubs: true,
	})

	isDir, err := blobTraverser.IsDirectory(false)
	a.True(isDir)
	a.Nil(err)

	//===========================================================
	dirName = "dest_file"
	// List
	rawBlobURLWithSAS = scenarioHelper{}.getBlobClientWithSAS(a, containerName, dirName).URL()
	blobTraverser = traverser.NewBlobTraverser(rawBlobURLWithSAS, serviceClientWithSAS, ctx, traverser.InitResourceTraverserOptions{
		Recursive:             true,
		IncludeDirectoryStubs: true,
	})

	isDir, err = blobTraverser.IsDirectory(false)
	a.False(isDir)
	a.Nil(err)
}

func TestIsDestDirWithDFSEP(t *testing.T) {
	a := assert.New(t)
	bfsClient := getDatalakeServiceClient()

	// Generate source container and blobs
	fileSystemURL, fileSystemName := createNewFilesystem(a, bfsClient)
	defer deleteFilesystem(a, fileSystemURL)
	a.NotNil(fileSystemURL)

	parentDirName := "dest_dir"
	parentDirClient := fileSystemURL.NewDirectoryClient(parentDirName)
	_, err := parentDirClient.Create(ctx, &datalakedirectory.CreateOptions{AccessConditions: &datalakedirectory.AccessConditions{ModifiedAccessConditions: &datalakedirectory.ModifiedAccessConditions{IfNoneMatch: to.Ptr(azcore.ETagAny)}}})
	a.Nil(err)

	ctx := context.WithValue(context.TODO(), ste.ServiceAPIVersionOverride, ste.DefaultServiceApiVersion)

	// List
	rawBlobURLWithSAS := scenarioHelper{}.getBlobClientWithSAS(a, fileSystemName, parentDirName).URL()
	serviceClientWithSAS := scenarioHelper{}.getBlobServiceClientWithSASFromURL(a, rawBlobURLWithSAS)
	blobTraverser := traverser.NewBlobTraverser(rawBlobURLWithSAS, serviceClientWithSAS, ctx, traverser.InitResourceTraverserOptions{
		Recursive:             true,
		IncludeDirectoryStubs: true,
	}, traverser.BlobTraverserOptions{IsDFS: to.Ptr(true)})

	// a directory with name parentDirName exists on target. So irrespective of
	// isSource, IsDirectory()  should return true.
	isDir, err := blobTraverser.IsDirectory(true)
	a.True(isDir)
	a.Nil(err)

	isDir, err = blobTraverser.IsDirectory(false)
	a.True(isDir)
	a.Nil(err)

	//===================================================================//

	// With a directory that does not exist, without path separator.
	parentDirName = "dirDoesNotExist"
	rawBlobURLWithSAS = scenarioHelper{}.getBlobClientWithSAS(a, fileSystemName, parentDirName).URL()
	blobTraverser = traverser.NewBlobTraverser(rawBlobURLWithSAS, serviceClientWithSAS, ctx, traverser.InitResourceTraverserOptions{
		Recursive:             true,
		IncludeDirectoryStubs: true,
	}, traverser.BlobTraverserOptions{IsDFS: to.Ptr(true)})

	// The directory does not exist, so IsDirectory()
	// should return false, in all cases
	isDir, err = blobTraverser.IsDirectory(true)
	a.False(isDir)
	a.NotNil(err) // Not nil because we get 404 from service

	isDir, err = blobTraverser.IsDirectory(false)
	a.False(isDir)
	a.NotNil(err) // Not nil because we get 404 from service

	//===================================================================//

	// With a directory that does not exist, with path separator
	parentDirNameWithSeparator := "dirDoesNotExist" + common.OS_PATH_SEPARATOR
	rawBlobURLWithSAS = scenarioHelper{}.getBlobClientWithSAS(a, fileSystemName, parentDirNameWithSeparator).URL()
	blobTraverser = traverser.NewBlobTraverser(rawBlobURLWithSAS, serviceClientWithSAS, ctx, traverser.InitResourceTraverserOptions{
		Recursive:             true,
		IncludeDirectoryStubs: true,
	}, traverser.BlobTraverserOptions{IsDFS: to.Ptr(true)})

	// The directory does not exist, but with a path separator
	// we should identify it as a directory.
	isDir, err = blobTraverser.IsDirectory(true)
	a.True(isDir)
	a.Nil(err)

	isDir, err = blobTraverser.IsDirectory(false)
	a.True(isDir)
	a.Nil(err)

}

func TestIsSourceFileExists(t *testing.T) {
	a := assert.New(t)
	bsc := getBlobServiceClient()

	// Generate source container and blobs
	cc, containerName := createNewContainer(a, bsc)
	defer deleteContainer(a, cc)
	a.NotNil(cc)

	fileName := "source_file"
	_, fileName = createNewBlockBlob(a, cc, fileName)

	ctx := context.WithValue(context.TODO(), ste.ServiceAPIVersionOverride, ste.DefaultServiceApiVersion)

	// List
	rawBlobURLWithSAS := scenarioHelper{}.getBlobClientWithSAS(a, containerName, fileName).URL()
	serviceClientWithSAS := scenarioHelper{}.getBlobServiceClientWithSASFromURL(a, rawBlobURLWithSAS)
	blobTraverser := traverser.NewBlobTraverser(rawBlobURLWithSAS, serviceClientWithSAS, ctx, traverser.InitResourceTraverserOptions{
		Recursive:             true,
		IncludeDirectoryStubs: true,
	})

	isDir, err := blobTraverser.IsDirectory(true)
	a.False(isDir)
	a.Nil(err)
}

func TestIsSourceFileDoesNotExist(t *testing.T) {
	a := assert.New(t)
	bsc := getBlobServiceClient()

	// Generate source container and blobs
	cc, containerName := createNewContainer(a, bsc)
	defer deleteContainer(a, cc)
	a.NotNil(cc)

	fileName := "file_does_not_exist"
	ctx := context.WithValue(context.TODO(), ste.ServiceAPIVersionOverride, ste.DefaultServiceApiVersion)

	// List
	rawBlobURLWithSAS := scenarioHelper{}.getBlobClientWithSAS(a, containerName, fileName).URL()
	serviceClientWithSAS := scenarioHelper{}.getBlobServiceClientWithSASFromURL(a, rawBlobURLWithSAS)
	blobTraverser := traverser.NewBlobTraverser(rawBlobURLWithSAS, serviceClientWithSAS, ctx, traverser.InitResourceTraverserOptions{
		Recursive:             true,
		IncludeDirectoryStubs: true,
	})

	isDir, err := blobTraverser.IsDirectory(true)
	a.False(isDir)
	a.Equal(common.FILE_NOT_FOUND, err.Error())
}

func TestGetEntityType(t *testing.T) {
	a := assert.New(t)

	// Test case 1: metadata is file
	metadata := make(common.Metadata)
	entityType := traverser.GetEntityType(metadata)
	a.Equal(common.EEntityType.File(), entityType)

	metadata = make(common.Metadata)
	metadata["key"] = to.Ptr("value")
	entityType = traverser.GetEntityType(metadata)
	a.Equal(common.EEntityType.File(), entityType)

	metadata = make(common.Metadata)
	metadata["hdi_isfolder"] = to.Ptr("false")
	entityType = traverser.GetEntityType(metadata)
	a.Equal(common.EEntityType.File(), entityType)

	metadata = make(common.Metadata)
	metadata["Hdi_isfolder"] = to.Ptr("false")
	entityType = traverser.GetEntityType(metadata)
	a.Equal(common.EEntityType.File(), entityType)

	metadata = make(common.Metadata)
	metadata["is_symlink"] = to.Ptr("false")
	entityType = traverser.GetEntityType(metadata)
	a.Equal(common.EEntityType.File(), entityType)

	metadata = make(common.Metadata)
	metadata["Is_symlink"] = to.Ptr("false")
	entityType = traverser.GetEntityType(metadata)
	a.Equal(common.EEntityType.File(), entityType)

	// Test case 2: metadata is a folder
	metadata = make(common.Metadata)
	metadata["hdi_isfolder"] = to.Ptr("true")
	entityType = traverser.GetEntityType(metadata)
	a.Equal(common.EEntityType.Folder(), entityType)

	metadata = make(common.Metadata)
	metadata["hdi_isfolder"] = to.Ptr("True")
	entityType = traverser.GetEntityType(metadata)
	a.Equal(common.EEntityType.Folder(), entityType)

	metadata = make(common.Metadata)
	metadata["Hdi_isfolder"] = to.Ptr("true")
	entityType = traverser.GetEntityType(metadata)
	a.Equal(common.EEntityType.Folder(), entityType)

	// Test case 2: metadata is a symlink
	metadata = make(common.Metadata)
	metadata["is_symlink"] = to.Ptr("true")
	entityType = traverser.GetEntityType(metadata)
	a.Equal(common.EEntityType.Symlink(), entityType)

	metadata = make(common.Metadata)
	metadata["is_symlink"] = to.Ptr("True")
	entityType = traverser.GetEntityType(metadata)
	a.Equal(common.EEntityType.Symlink(), entityType)

	metadata = make(common.Metadata)
	metadata["Is_symlink"] = to.Ptr("true")
	entityType = traverser.GetEntityType(metadata)
	a.Equal(common.EEntityType.Symlink(), entityType)

}

func TestManagedDiskProperties(t *testing.T) {
	a := assert.New(t)

	// Setup
	// Mock the server
	srv, close := mock_server.NewServer(mock_server.WithTransformAllRequestsToTestServerUrl())
	defer close()

	pbProp := &blob.GetPropertiesResponse{ContentLength: nil, LastModified: nil}
	srv.AppendResponse(mock_server.WithStatusCode(200), mock_server.WithBody([]byte(getPageBlobProperties(pbProp))))

	// Create a client
	// Note: the key below is not a secret, this is the publicly documented Azurite key
	accountName := "myfakeaccount"
	accountKey := "Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw=="
	rawURL := fmt.Sprintf("https://%s.blob.core.windows.net/", accountName)

	credential, err := blob.NewSharedKeyCredential(accountName, accountKey)
	a.NoError(err)

	client, err := blobservice.NewClientWithSharedKeyCredential(rawURL, credential,
		&blobservice.ClientOptions{
			ClientOptions: azcore.ClientOptions{
				Transport: srv,
			}})
	a.NoError(err)

	containerName := generateContainerName()
	containerClient := client.NewContainerClient(containerName)

	blobName := generateBlobName()
	blobClient := containerClient.NewPageBlobClient(blobName)

	prop, err := blobClient.GetProperties(ctx, nil)
	a.NoError(err)
	a.Nil(prop.LastModified)
	a.NotNil(prop.ContentLength) // note:content length will never be nil as the service calculates the size of the blob and stores it in this header

	propAdapter := traverser.blobPropertiesResponseAdapter{GetPropertiesResponse: &prop}
	a.Equal(propAdapter.LastModified(), time.Time{})
	a.NotNil(prop.ContentLength) // see note from above
}

func getPageBlobProperties(properties *blob.GetPropertiesResponse) string {
	// these properties have been pulled from https://learn.microsoft.com/en-us/rest/api/storageservices/get-blob-properties
	// with modification to date, content length and last modified time
	body := "x-ms-blob-type: PageBlob" +
		"x-ms-lease-status: unlocked" +
		"x-ms-lease-state: available" +
		getContentLength(properties) +
		"Content-Type: text/plain; charset=UTF-8" +
		fmt.Sprintf("Date: %s", time.Now().String()) +
		"ETag: \"0x8CAE97120C1FF22\"" +
		"Accept-Ranges: bytes" +
		"x-ms-blob-committed–block-count: 1" +
		"x-ms-version: 2015-02-21" +
		getLMT(properties) +
		"Server: Windows-Azure-Blob/1.0 Microsoft-HTTPAPI/2.0"
	return body
}

func getLMT(response *blob.GetPropertiesResponse) string {
	if response.LastModified == nil {
		return ""
	} else {
		return fmt.Sprintf("Last-Modified: %s", response.LastModified.String())
	}
}

func getContentLength(response *blob.GetPropertiesResponse) string {
	if response.ContentLength == nil {
		return ""
	} else {
		return fmt.Sprintf("Content-Length: %d", response.ContentLength)
	}
}
