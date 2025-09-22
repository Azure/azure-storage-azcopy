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
	"encoding/xml"
	"fmt"
	"github.com/Azure/azure-sdk-for-go/sdk/azcore"
	"github.com/Azure/azure-sdk-for-go/sdk/azcore/to"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/blob"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/container"
	blobservice "github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/service"
	datalakedirectory "github.com/Azure/azure-sdk-for-go/sdk/storage/azdatalake/directory"
	"github.com/Azure/azure-storage-azcopy/v10/common"
	"github.com/Azure/azure-storage-azcopy/v10/mock_server"
	"github.com/Azure/azure-storage-azcopy/v10/ste"
	"github.com/stretchr/testify/assert"
	"strings"
	"testing"
	"time"
)

// original code that hits server
func TestIsSourceDirWithStub2(t *testing.T) {
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
	blobTraverser := newBlobTraverser(rawBlobURLWithSAS, serviceClientWithSAS, ctx, InitResourceTraverserOptions{
		Recursive:             true,
		IncludeDirectoryStubs: true,
	})

	isDir, err := blobTraverser.IsDirectory(true)
	a.True(isDir)
	a.Nil(err)
}

func SetUpVariables() (accountname string, rawurl string, bloburl string, blobname string, containername string, credential *blob.SharedKeyCredential, err error) {
	// Create a client
	// Note: the key below is not a secret, this is the publicly documented Azurite key
	accountName := "myfakeaccount"
	accountKey := "Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw=="
	rawURL := fmt.Sprintf("https://%s.blob.core.windows.net/", accountName)
	NewCredential, err := blob.NewSharedKeyCredential(accountName, accountKey)

	if err != nil {
		return "", "", "", "", "", nil, err
	}

	//set container and blob name
	cName := generateContainerName()
	bName := generateBlobName()

	// construct a string that points to blob name above
	// https://accountname.blob.core.windows.net/containername/blobname
	blobURL := fmt.Sprintf("https://%s.blob.core.windows.net/%s/%s", accountName, cName, bName)

	return accountName, rawURL, blobURL, bName, cName, NewCredential, nil
}

// this test calls IsDirectory with the header hdi_isfolder to return true
func TestIsSourceDirWithStub(t *testing.T) {
	a := assert.New(t)

	//create mock server
	srv, close := mock_server.NewServer(mock_server.WithTransformAllRequestsToTestServerUrl())
	defer close()

	//initialize variables
	_, rawURL, blobURL, _, _, credential, err := SetUpVariables()
	a.Nil(err)

	client, err := blobservice.NewClientWithSharedKeyCredential(rawURL, credential,
		&blobservice.ClientOptions{
			ClientOptions: azcore.ClientOptions{
				Transport: srv, //passing in mock server
			}})
	a.Nil(err)

	//define what mock server should return as response
	srv.AppendResponse(mock_server.WithStatusCode(200), mock_server.WithHeader("x-ms-meta-"+common.POSIXFolderMeta, "true"))

	blobTraverser := newBlobTraverser(blobURL, client, ctx, true, true, func(common.EntityType) {}, false, common.CpkOptions{}, false, false, false, common.EPreservePermissionsOption.None(), false)

	//test method and validate
	isDir, err := blobTraverser.IsDirectory(true)
	a.True(isDir)
	a.Nil(err)
}

// this test calls isDirectory mocking a file and isDirStub returns false
func TestIsDirFile(t *testing.T) {
	a := assert.New(t)

	//create mock server (do every time)
	srv, close := mock_server.NewServer(mock_server.WithTransformAllRequestsToTestServerUrl())
	defer close()

	//define what mock server should return as response
	srv.AppendResponse(mock_server.WithStatusCode(200))

	//initialize variables
	_, rawURL, blobURL, _, _, credential, err := SetUpVariables()
	a.Nil(err)

	client, err := blobservice.NewClientWithSharedKeyCredential(rawURL, credential,
		&blobservice.ClientOptions{
			ClientOptions: azcore.ClientOptions{
				Transport: srv, //passing in mock server
			}})
	a.Nil(err)

	//create new blob traverser
	blobTraverser := newBlobTraverser(blobURL, client, ctx, true, true, func(common.EntityType) {}, false, common.CpkOptions{}, false, false, false, common.EPreservePermissionsOption.None(), false)

	//test method and validate
	isDir, err := blobTraverser.IsDirectory(true)
	a.False(isDir)
	a.Nil(err)
}

func MockErrorBody(message string) string {
	return "<?xml version=\"1.0\" encoding=\"utf-8\"?><Error><Code>" + message + "</Code><Message> </Message></Error>"
}

// this test sends the error code "BlobUsesCustomerSpecifiedEncryption" and returns false
func TestIsDirFileError(t *testing.T) {
	a := assert.New(t)

	//create mock server (do every time)
	srv, close := mock_server.NewServer(mock_server.WithTransformAllRequestsToTestServerUrl())
	defer close()

	//initialize variables
	_, rawURL, blobURL, _, _, credential, err := SetUpVariables()
	a.Nil(err)

	client, err := blobservice.NewClientWithSharedKeyCredential(rawURL, credential,
		&blobservice.ClientOptions{
			ClientOptions: azcore.ClientOptions{
				Transport: srv, //passing in mock server
			}})
	a.Nil(err)

	//mock response has "BlobUsesCustomerSpecifiedEncryption" error
	srv.AppendResponse(mock_server.WithStatusCode(409), mock_server.WithHeader("x-ms-error-code", "BlobUsesCustomerSpecifiedEncryption"), mock_server.WithBody([]byte(MockErrorBody("BlobUsesCustomerSpecifiedEncryption"))))
	//create new blob traverser
	blobTraverser := newBlobTraverser(blobURL, client, ctx, true, true, func(common.EntityType) {}, false, common.CpkOptions{}, false, false, false, common.EPreservePermissionsOption.None(), false)

	//test method and validate
	isDir, err := blobTraverser.IsDirectory(true)
	a.False(isDir)
	a.Nil(err)
}

// this test calls isDirectory and fails with get properties and also fails with NewListBlobs call by returning no blobs
func TestIsDirNoFile(t *testing.T) {
	a := assert.New(t)

	//create mock server
	srv, close := mock_server.NewServer(mock_server.WithTransformAllRequestsToTestServerUrl())
	defer close()

	//initialize variables
	accountName, rawURL, blobURL, _, cName, credential, err := SetUpVariables()
	a.Nil(err)

	client, err := blobservice.NewClientWithSharedKeyCredential(rawURL, credential,
		&blobservice.ClientOptions{
			ClientOptions: azcore.ClientOptions{
				Transport: srv, //passing in mock server
			}})
	a.Nil(err)

	//get properties returns error
	srv.AppendResponse(mock_server.WithStatusCode(404), mock_server.WithHeader("x-ms-error-code", "BlobNotFound"), mock_server.WithBody([]byte(MockErrorBody("BlobNotFound"))))
	//list returns no blobs
	srv.AppendResponse(mock_server.WithStatusCode(200), mock_server.WithBody([]byte(SetBodyNoBlob(accountName, cName))))

	//create new blob traverser
	blobTraverser := newBlobTraverser(blobURL, client, ctx, true, true, func(common.EntityType) {}, false, common.CpkOptions{}, false, false, false, common.EPreservePermissionsOption.None(), false)

	//test method and validate
	isDir, err := blobTraverser.IsDirectory(true)
	a.False(isDir)
	a.Equal("The specified file was not found.", err.Error())
}

// this test calls isDirectory and fails with get properties but returns blobs and passes with NewListBlobs call
func TestIsDirWithList(t *testing.T) {
	a := assert.New(t)
	//create mock server
	srv, close := mock_server.NewServer(mock_server.WithTransformAllRequestsToTestServerUrl())
	defer close()

	//initialize variables
	accountName, rawURL, blobURL, _, cName, credential, err := SetUpVariables()
	a.Nil(err)

	client, err := blobservice.NewClientWithSharedKeyCredential(rawURL, credential,
		&blobservice.ClientOptions{
			ClientOptions: azcore.ClientOptions{
				Transport: srv, //passing in mock server
			}})
	a.Nil(err)

	//get properties returns error
	srv.AppendResponse(mock_server.WithStatusCode(404), mock_server.WithHeader("x-ms-error-code", "BlobNotFound"), mock_server.WithBody([]byte(MockErrorBody("BlobNotFound"))))
	//list returns blob items
	srv.AppendResponse(mock_server.WithStatusCode(200), mock_server.WithBody([]byte(SetBodyWithBlob(accountName, cName))))

	//create new blob traverser
	blobTraverser := newBlobTraverser(blobURL, client, ctx, true, true, func(common.EntityType) {}, false, common.CpkOptions{}, false, false, false, common.EPreservePermissionsOption.None(), false)

	//test method and validate
	isDir, err := blobTraverser.IsDirectory(true)
	a.True(isDir)
	a.Nil(err)
}

// this test calls Traverse and getPropertiesIfSingleBlob and responds with the BlobUsesCustomerSpecifiedEncryption error
func TestTraverseGetPropErrorCode(t *testing.T) {
	a := assert.New(t)

	//create mock server
	srv, close := mock_server.NewServer(mock_server.WithTransformAllRequestsToTestServerUrl())
	defer close()

	//initialize variables
	_, rawURL, blobURL, _, _, credential, err := SetUpVariables()
	a.Nil(err)

	client, err := blobservice.NewClientWithSharedKeyCredential(rawURL, credential,
		&blobservice.ClientOptions{
			ClientOptions: azcore.ClientOptions{
				Transport: srv, //passing in mock server
			}})
	a.Nil(err)

	//the mock server returns "BlobUsesCustomerSpecifiedEncryption"
	srv.AppendResponse(mock_server.WithStatusCode(409), mock_server.WithHeader("x-ms-error-code", "BlobUsesCustomerSpecifiedEncryption"), mock_server.WithBody([]byte(MockErrorBody("BlobUsesCustomerSpecifiedEncryption"))))

	//create new blob traverser
	blobTraverser := newBlobTraverser(blobURL, client, ctx, true, true, func(common.EntityType) {}, false, common.CpkOptions{}, false, false, false, common.EPreservePermissionsOption.None(), false)

	//test method and validate
	seenFiles := make(map[string]StoredObject)

	err = blobTraverser.Traverse(nil, func(storedObject StoredObject) error {
		seenFiles[storedObject.relativePath] = storedObject
		return nil
	}, []ObjectFilter{})
	a.Equal(map[string]StoredObject{}, seenFiles)
	a.Equal("this blob uses customer provided encryption keys (CPK). At the moment, AzCopy does not support CPK-encrypted blobs. If you wish to make use of this blob, we recommend using one of the Azure Storage SDKs", err.Error())
}

// this tests if the Traverse function getPropertiesIfSingleBlob status code is 403 and returns error
func TestTraverseGetPropStatusCode(t *testing.T) {
	a := assert.New(t)

	//create mock server
	srv, close := mock_server.NewServer(mock_server.WithTransformAllRequestsToTestServerUrl())
	defer close()

	//initialize variables
	_, rawURL, blobURL, _, _, credential, err := SetUpVariables()
	a.Nil(err)

	client, err := blobservice.NewClientWithSharedKeyCredential(rawURL, credential,
		&blobservice.ClientOptions{
			ClientOptions: azcore.ClientOptions{
				Transport: srv, //passing in mock server
			}})
	a.Nil(err)

	//the mock server responds with a status code error
	srv.AppendResponse(mock_server.WithStatusCode(403), mock_server.WithHeader("x-ms-error-code", "BlobNotFound"), mock_server.WithBody([]byte(MockErrorBody("BlobNotFound"))))

	//create new blob traverser
	blobTraverser := newBlobTraverser(blobURL, client, ctx, true, true, func(common.EntityType) {}, false, common.CpkOptions{}, false, false, false, common.EPreservePermissionsOption.None(), false)

	//test method and validate
	seenFiles := make(map[string]StoredObject)

	err = blobTraverser.Traverse(nil, func(storedObject StoredObject) error {
		seenFiles[storedObject.relativePath] = storedObject
		return nil
	}, []ObjectFilter{})
	a.Equal(map[string]StoredObject{}, seenFiles)
	a.True(strings.Contains(err.Error(), "cannot list files due to reason"))
}

func MockTagBody(TagKey string, TagVal string) string {
	tagSet := []*container.BlobTag{to.Ptr(container.BlobTag{Key: to.Ptr(TagKey), Value: to.Ptr(TagVal)})}
	tags := container.BlobTags{BlobTagSet: tagSet}
	out, err := xml.Marshal(tags)
	if err != nil {
		return ""
	}
	body := string(out)
	return body
}

// this test calls Traverse and mocks a response with blob tags that ensures the set tags are in the stored object
func TestTraverseGetBlobTags(t *testing.T) {
	a := assert.New(t)
	//create mock server
	srv, close := mock_server.NewServer(mock_server.WithTransformAllRequestsToTestServerUrl())
	defer close()

	//initialize necessary variables
	_, rawURL, blobURL, _, _, credential, err := SetUpVariables()
	a.Nil(err)

	client, err := blobservice.NewClientWithSharedKeyCredential(rawURL, credential,
		&blobservice.ClientOptions{
			ClientOptions: azcore.ClientOptions{
				Transport: srv, //passing in mock server
			}})
	a.Nil(err)

	//test the get properties call
	srv.AppendResponse(mock_server.WithStatusCode(200), mock_server.WithHeader("x-ms-meta-proptest", "proptestval"))
	//test the blob tags call
	srv.AppendResponse(mock_server.WithStatusCode(200), mock_server.WithBody([]byte(MockTagBody("keytest", "valtest"))))

	//create new blob traverser
	blobTraverser := newBlobTraverser(blobURL, client, ctx, true, true, func(common.EntityType) {}, true, common.CpkOptions{}, false, false, false, common.EPreservePermissionsOption.None(), false)

	//test method and validate
	seenFiles := make(map[string]StoredObject)
	err = blobTraverser.Traverse(nil, func(storedObject StoredObject) error {
		seenFiles["testpath"] = storedObject
		return nil
	}, nil)
	a.Nil(err)
	a.True(seenFiles["testpath"].blobTags["keytest"] == "valtest")
	a.True(seenFiles["testpath"].Metadata != nil)
}

// this test sends a blob response with no blob tags and checks that the stored object blob tags is nil
func TestTraverseNoBlobTags(t *testing.T) {
	a := assert.New(t)
	//create mock server
	srv, close := mock_server.NewServer(mock_server.WithTransformAllRequestsToTestServerUrl())
	defer close()

	//initialize variables
	_, rawURL, blobURL, _, cName, credential, err := SetUpVariables()
	a.Nil(err)

	client, err := blobservice.NewClientWithSharedKeyCredential(rawURL, credential,
		&blobservice.ClientOptions{
			ClientOptions: azcore.ClientOptions{
				Transport: srv, //passing in mock server
			}})
	a.Nil(err)

	//define what mock server should return as response
	srv.AppendResponse(mock_server.WithStatusCode(200))
	//no tags but successful call
	srv.AppendResponse(mock_server.WithStatusCode(200), mock_server.WithBody([]byte(SetBodyWithBlob(rawURL, cName))))

	//create new blob traverser
	blobTraverser := newBlobTraverser(blobURL, client, ctx, true, true, func(common.EntityType) {}, true, common.CpkOptions{}, false, false, false, common.EPreservePermissionsOption.None(), false)

	//test method and validate
	seenFiles := make(map[string]StoredObject)

	err = blobTraverser.Traverse(nil, func(storedObject StoredObject) error {
		seenFiles["testpath"] = storedObject
		return nil
	}, nil)
	a.Nil(err)
	a.Nil(seenFiles["testpath"].blobTags)
}

// this test calls Traverse and calls Serial List, then returns an error when Next Page is called
func TestTraverseSerialListNextPageError(t *testing.T) {
	a := assert.New(t)
	//create mock server
	srv, close := mock_server.NewServer(mock_server.WithTransformAllRequestsToTestServerUrl())
	defer close()

	//initialize variables
	_, rawURL, blobURL, _, _, credential, err := SetUpVariables()
	a.Nil(err)

	client, err := blobservice.NewClientWithSharedKeyCredential(rawURL, credential,
		&blobservice.ClientOptions{
			ClientOptions: azcore.ClientOptions{
				Transport: srv, //passing in mock server
			}})
	a.Nil(err)

	//define what mock server should return as response
	srv.AppendResponse(mock_server.WithStatusCode(200))
	//no tags but successful call
	srv.AppendResponse(mock_server.WithStatusCode(200))
	//next page error
	srv.AppendResponse(mock_server.WithStatusCode(400))

	//changed includeversion to be true to make parallelListing false
	blobTraverser := newBlobTraverser(blobURL, client, ctx, true, true, func(common.EntityType) {}, true, common.CpkOptions{}, true, false, true, common.EPreservePermissionsOption.None(), false)

	//test method and validate
	seenFiles := make(map[string]StoredObject)

	err = blobTraverser.Traverse(nil, func(storedObject StoredObject) error {
		seenFiles["testpath"] = storedObject
		return nil
	}, nil)
	a.True(strings.Contains(err.Error(), "cannot list blobs. Failed with error"))
}

func SetBodyWithBlob(accounturl string, containername string) string {
	testmetadata := map[string]*string{
		"key1": to.Ptr("value1"),
		"key2": to.Ptr("value2"),
	}
	blobProp := container.BlobProperties{ContentLength: to.Ptr(int64(2))}
	blobitem := []*container.BlobItem{to.Ptr(container.BlobItem{Name: to.Ptr("blob-name"), Properties: to.Ptr(blobProp), Metadata: testmetadata})}
	segment := container.BlobFlatListSegment{BlobItems: blobitem}
	resp := container.ListBlobsFlatSegmentResponse{ContainerName: to.Ptr(containername), Segment: to.Ptr(segment), ServiceEndpoint: to.Ptr(accounturl), MaxResults: to.Ptr(int32(1))}
	out, err := xml.Marshal(resp)
	if err != nil {
		fmt.Printf(err.Error())
	}
	body := string(out)
	newbody := strings.Replace(body, "ListBlobsFlatSegmentResponse", "EnumerationResults", -1)
	return newbody
}

// this test calls Traverse with Serial List, then successfully reads a mocked blob body and stores as stored object
func TestTraverseSerialListSuccess(t *testing.T) {
	a := assert.New(t)
	//create mock server
	srv, close := mock_server.NewServer(mock_server.WithTransformAllRequestsToTestServerUrl())
	defer close()

	//initialize necessary variables
	_, rawURL, blobURL, _, cName, credential, err := SetUpVariables()
	a.Nil(err)

	client, err := blobservice.NewClientWithSharedKeyCredential(rawURL, credential,
		&blobservice.ClientOptions{
			ClientOptions: azcore.ClientOptions{
				Transport: srv, //passing in mock server
			}})
	a.Nil(err)

	//define what mock server should return as response
	srv.AppendResponse(mock_server.WithStatusCode(200))
	//no tags but successful call
	srv.AppendResponse(mock_server.WithStatusCode(200))
	//use marshalling to append body
	srv.AppendResponse(mock_server.WithStatusCode(200), mock_server.WithBody([]byte(SetBodyWithBlob(rawURL, cName))))

	//create new blob traverser
	//changed includeversion to be true to make parallelListing false
	blobTraverser := newBlobTraverser(blobURL, client, ctx, true, true, func(common.EntityType) {}, true, common.CpkOptions{}, true, false, true, common.EPreservePermissionsOption.None(), false)

	//test method and validate
	seenFiles := make(map[string]StoredObject)
	err = blobTraverser.Traverse(nil, func(storedObject StoredObject) error {
		seenFiles[storedObject.name] = storedObject
		return nil
	}, nil)
	a.Nil(err)
	a.Equal(seenFiles["blob-name"].name, "blob-name")
	a.EqualValues(seenFiles["blob-name"].size, int64(2))
	a.Equal(*seenFiles["blob-name"].Metadata["key1"], "value1")
}

func SetBodyWithMultipleBlobs(accounturl string, containername string, folderflag bool) string {
	metadatatest := map[string]*string{
		"key1": to.Ptr("val1"),
	}
	metadatafolder := map[string]*string{"hdi_isfolder": to.Ptr("false")}
	if folderflag {
		metadatafolder["hdi_isfolder"] = to.Ptr("true")
	}
	blobProp := container.BlobProperties{ContentLength: to.Ptr(int64(2))}
	blobitem := []*container.BlobItem{to.Ptr(container.BlobItem{Name: to.Ptr("folder1/file1.txt"), Properties: to.Ptr(blobProp), Metadata: metadatatest}), to.Ptr(container.BlobItem{Name: to.Ptr("folder1/folder2/"), Properties: to.Ptr(blobProp), Metadata: metadatafolder}), to.Ptr(container.BlobItem{Name: to.Ptr("folder1/folder2/file2.txt"), Properties: to.Ptr(blobProp), Metadata: metadatatest})}
	segment := container.BlobFlatListSegment{BlobItems: blobitem}
	resp := container.ListBlobsFlatSegmentResponse{ContainerName: to.Ptr(containername), Segment: to.Ptr(segment), ServiceEndpoint: to.Ptr(accounturl), Marker: to.Ptr(""), MaxResults: to.Ptr(int32(10)), Prefix: to.Ptr("folder1/")}
	out, err := xml.Marshal(resp)
	if err != nil {
		fmt.Printf(err.Error())
	}
	body := string(out)
	newbody := strings.Replace(body, "ListBlobsFlatSegmentResponse", "EnumerationResults", -1)
	return newbody
}

// this test calls Traverse with a Serial List and has a Subdirectory w/ hdi_isfolder that is skipped
func TestTraverseSerialListWithFolder(t *testing.T) {
	a := assert.New(t)
	//create mock server
	srv, close := mock_server.NewServer(mock_server.WithTransformAllRequestsToTestServerUrl())
	defer close()

	//initialize variables
	_, rawURL, blobURL, _, cName, credential, err := SetUpVariables()
	a.Nil(err)

	client, err := blobservice.NewClientWithSharedKeyCredential(rawURL, credential,
		&blobservice.ClientOptions{
			ClientOptions: azcore.ClientOptions{
				Transport: srv, //passing in mock server
			}})
	a.Nil(err)

	//first case is not a blob
	srv.AppendResponse(mock_server.WithStatusCode(400))
	//use marshalling to append body
	srv.AppendResponse(mock_server.WithStatusCode(200), mock_server.WithBody([]byte(SetBodyWithMultipleBlobs(rawURL, cName, true))))

	//changed includeversion to be true to make parallelListing false and changed includeDirectoryStubs to false so hdi_isfolder is recognized
	blobTraverser := newBlobTraverser(blobURL, client, ctx, true, false, func(common.EntityType) {}, true, common.CpkOptions{}, true, false, true, common.EPreservePermissionsOption.None(), false)

	//test method and validate
	seenFiles := make(map[string]StoredObject)

	err = blobTraverser.Traverse(nil, func(storedObject StoredObject) error {
		seenFiles[storedObject.relativePath] = storedObject
		return nil
	}, nil)
	a.Nil(err)
	a.Len(seenFiles, 2)
	a.Equal(*seenFiles["folder1/file1.txt"].Metadata["key1"], "val1")
	a.Equal(*seenFiles["folder1/folder2/file2.txt"].Metadata["key1"], "val1")
}

// this test calls Traverse with a Serial List and has a Subdirectory without hdi_isfolder that is stored
func TestTraverseSerialListNoFolder(t *testing.T) {
	a := assert.New(t)
	//create mock server
	srv, close := mock_server.NewServer(mock_server.WithTransformAllRequestsToTestServerUrl())
	defer close()

	//initialize variables
	_, rawURL, blobURL, _, cName, credential, err := SetUpVariables()
	a.Nil(err)

	client, err := blobservice.NewClientWithSharedKeyCredential(rawURL, credential,
		&blobservice.ClientOptions{
			ClientOptions: azcore.ClientOptions{
				Transport: srv, //passing in mock server
			}})
	a.Nil(err)

	//first case is not a blob
	srv.AppendResponse(mock_server.WithStatusCode(400))
	//use marshalling to append body
	srv.AppendResponse(mock_server.WithStatusCode(200), mock_server.WithBody([]byte(SetBodyWithMultipleBlobs(rawURL, cName, false))))

	//changed includeversion to be true to make parallelListing false and includeDirectoryStubs to false so that hdi_isfolder is recognized
	blobTraverser := newBlobTraverser(blobURL, client, ctx, true, false, func(common.EntityType) {}, true, common.CpkOptions{}, true, false, true, common.EPreservePermissionsOption.None(), false)

	//test method and validate
	seenFiles := make(map[string]StoredObject)

	err = blobTraverser.Traverse(nil, func(storedObject StoredObject) error {
		seenFiles[storedObject.relativePath] = storedObject
		return nil
	}, nil)
	a.Nil(err)
	a.Len(seenFiles, 3)
	a.Equal(*seenFiles["folder1/folder2/"].Metadata["hdi_isfolder"], "false")
	a.Equal(*seenFiles["folder1/file1.txt"].Metadata["key1"], "val1")
}

func SetBodyNoSubDir(accounturl string, containername string, flat bool) string {
	metadatatest := map[string]*string{
		"key1": to.Ptr("val1"),
	}
	blobProp := container.BlobProperties{ContentLength: to.Ptr(int64(2))}
	blobitem := []*container.BlobItem{to.Ptr(container.BlobItem{Name: to.Ptr("folder1/"), Properties: to.Ptr(blobProp), Metadata: metadatatest}), to.Ptr(container.BlobItem{Name: to.Ptr("folder1/file1.txt"), Properties: to.Ptr(blobProp), Metadata: metadatatest})}
	if flat {
		segment := container.BlobFlatListSegment{BlobItems: blobitem}
		resp := container.ListBlobsFlatSegmentResponse{ContainerName: to.Ptr(containername), Segment: to.Ptr(segment), ServiceEndpoint: to.Ptr(accounturl), Marker: to.Ptr(""), MaxResults: to.Ptr(int32(10)), Prefix: to.Ptr("")}
		out, err := xml.Marshal(resp)
		if err != nil {
			fmt.Printf(err.Error())
		}
		body := string(out)
		newbody := strings.Replace(body, "ListBlobsFlatSegmentResponse", "EnumerationResults", -1)
		return newbody
	}
	//Hierarchical case
	segment := container.BlobHierarchyListSegment{BlobItems: blobitem}
	resp := container.ListBlobsHierarchySegmentResponse{ContainerName: to.Ptr(containername), Segment: to.Ptr(segment), ServiceEndpoint: to.Ptr(accounturl), Delimiter: to.Ptr("/"), Marker: to.Ptr(""), MaxResults: to.Ptr(int32(10)), Prefix: to.Ptr("folder1/")}
	out, err := xml.Marshal(resp)
	if err != nil {
		fmt.Printf(err.Error())
	}
	body := string(out)
	newbody := strings.Replace(body, "ListBlobsHierarchySegmentResponse", "EnumerationResults", -1)
	return newbody
}

// this test calls Traverse with a Serial List where there is no subdirectory
func TestTraverseSerialListNoSubDir(t *testing.T) {
	a := assert.New(t)
	//create mock server
	srv, close := mock_server.NewServer(mock_server.WithTransformAllRequestsToTestServerUrl())
	defer close()

	//initialize variables
	_, rawURL, blobURL, _, cName, credential, err := SetUpVariables()
	a.Nil(err)

	client, err := blobservice.NewClientWithSharedKeyCredential(rawURL, credential,
		&blobservice.ClientOptions{
			ClientOptions: azcore.ClientOptions{
				Transport: srv, //passing in mock server
			}})
	a.Nil(err)

	//first case is not a blob
	srv.AppendResponse(mock_server.WithStatusCode(400))
	//use marshalling to append body with no subdirectory
	srv.AppendResponse(mock_server.WithStatusCode(200), mock_server.WithBody([]byte(SetBodyNoSubDir(rawURL, cName, true))))

	//changed includeversion to be true to make parallelListing false
	blobTraverser := newBlobTraverser(blobURL, client, ctx, true, false, func(common.EntityType) {}, true, common.CpkOptions{}, true, false, true, common.EPreservePermissionsOption.None(), false)

	//test method and validate
	seenFiles := make(map[string]StoredObject)

	err = blobTraverser.Traverse(nil, func(storedObject StoredObject) error {
		seenFiles[storedObject.relativePath] = storedObject
		return nil
	}, nil)
	a.Nil(err)
	a.Len(seenFiles, 2)
	a.Equal(*seenFiles["folder1/file1.txt"].Metadata["key1"], "val1")
	a.Equal(*seenFiles["folder1/"].Metadata["key1"], "val1")
}

func SetBodyNoBlob(accounturl string, containername string) string {
	segment := container.BlobFlatListSegment{}
	resp := container.ListBlobsFlatSegmentResponse{ContainerName: to.Ptr(containername), Segment: to.Ptr(segment), ServiceEndpoint: to.Ptr(accounturl), Marker: to.Ptr(""), MaxResults: to.Ptr(int32(10)), Prefix: to.Ptr("")}
	out, err := xml.Marshal(resp)
	if err != nil {
		fmt.Printf(err.Error())
	}
	body := string(out)
	newbody := strings.Replace(body, "ListBlobsFlatSegmentResponse", "EnumerationResults", -1)
	return newbody
}

// this test calls Traverse and calls a Serial List and has no blobs listed
func TestTraverseSerialListNoBlob(t *testing.T) {
	a := assert.New(t)
	//create mock server
	srv, close := mock_server.NewServer(mock_server.WithTransformAllRequestsToTestServerUrl())
	defer close()

	//initialize variables
	_, rawURL, blobURL, _, cName, credential, err := SetUpVariables()
	a.Nil(err)

	client, err := blobservice.NewClientWithSharedKeyCredential(rawURL, credential,
		&blobservice.ClientOptions{
			ClientOptions: azcore.ClientOptions{
				Transport: srv, //passing in mock server
			}})
	a.Nil(err)

	//first case is not a blob
	srv.AppendResponse(mock_server.WithStatusCode(400))
	//use marshalling to append body with no blob
	srv.AppendResponse(mock_server.WithStatusCode(200), mock_server.WithBody([]byte(SetBodyNoBlob(rawURL, cName))))

	blobTraverser := newBlobTraverser(blobURL, client, ctx, true, false, func(common.EntityType) {}, true, common.CpkOptions{}, true, false, true, common.EPreservePermissionsOption.None(), false)

	//test method and validate
	seenFiles := make(map[string]StoredObject)

	err = blobTraverser.Traverse(nil, func(storedObject StoredObject) error {
		seenFiles[storedObject.relativePath] = storedObject
		return nil
	}, nil)
	a.Nil(err)
	a.Len(seenFiles, 0)
}

func SetHierarchicalBody(accounturl string, containername string, folderflag bool) string {
	metadatatest := map[string]*string{
		"key1": to.Ptr("val1"),
	}
	metadatafolder := map[string]*string{"hdi_isfolder": to.Ptr("false")}
	if folderflag {
		metadatafolder["hdi_isfolder"] = to.Ptr("true")
	}
	blobProp := container.BlobProperties{ContentLength: to.Ptr(int64(2))}
	blobitem := []*container.BlobItem{to.Ptr(container.BlobItem{Name: to.Ptr("folder1/file1.txt"), Properties: to.Ptr(blobProp), Metadata: metadatatest}), to.Ptr(container.BlobItem{Name: to.Ptr("folder1/folder2/"), Properties: to.Ptr(blobProp), Metadata: metadatafolder}), to.Ptr(container.BlobItem{Name: to.Ptr("folder1/folder2/file2.txt"), Properties: to.Ptr(blobProp), Metadata: metadatatest})}
	blobprefix := []*container.BlobPrefix{to.Ptr(container.BlobPrefix{Name: to.Ptr("folder1/folder2/")})}
	segment := container.BlobHierarchyListSegment{BlobItems: blobitem, BlobPrefixes: blobprefix}
	resp := container.ListBlobsHierarchySegmentResponse{ContainerName: to.Ptr(containername), Segment: to.Ptr(segment), ServiceEndpoint: to.Ptr(accounturl), Delimiter: to.Ptr("/"), Marker: to.Ptr(""), MaxResults: to.Ptr(int32(10)), Prefix: to.Ptr("folder1/")}
	out, err := xml.Marshal(resp)
	if err != nil {
		fmt.Printf(err.Error())
	}
	body := string(out)
	newbody := strings.Replace(body, "ListBlobsHierarchySegmentResponse", "EnumerationResults", -1)
	return newbody
}

// this test calls Traverse and calls a Parallel List and has a Subdirectory w/ hdi_isfolder
func TestTraverseParallelListWithMarker(t *testing.T) {
	a := assert.New(t)
	//create mock server
	srv, close := mock_server.NewServer(mock_server.WithTransformAllRequestsToTestServerUrl())
	defer close()

	//initialize variables
	_, rawURL, blobURL, _, cName, credential, err := SetUpVariables()
	a.Nil(err)

	client, err := blobservice.NewClientWithSharedKeyCredential(rawURL, credential,
		&blobservice.ClientOptions{
			ClientOptions: azcore.ClientOptions{
				Transport: srv, //passing in mock server
			}})
	a.Nil(err)

	//first case is not a blob
	srv.AppendResponse(mock_server.WithStatusCode(400))
	//use marshalling to append hierarchical body with folder flag
	srv.AppendResponse(mock_server.WithStatusCode(200), mock_server.WithBody([]byte(SetHierarchicalBody(rawURL, cName, true))))

	//create new blob traverser
	//changed includeversion to be false to make parallelListing true, changed includeDirectoryStubs to false so that hdi_isfolder is recognized, and made recursive false
	blobTraverser := newBlobTraverser(blobURL, client, ctx, false, false, func(common.EntityType) {}, true, common.CpkOptions{}, true, false, false, common.EPreservePermissionsOption.None(), false)

	//test method and validate
	seenFiles := make(map[string]StoredObject)

	err = blobTraverser.Traverse(nil, func(storedObject StoredObject) error {
		seenFiles[storedObject.relativePath] = storedObject
		return nil
	}, nil)
	a.Nil(err)
	a.Len(seenFiles, 2)
	a.Equal(*seenFiles["folder1/file1.txt"].Metadata["key1"], "val1")
	a.Equal(*seenFiles["folder1/folder2/file2.txt"].Metadata["key1"], "val1")
}

// this test calls Traverse and calls a Parallel List and has a Subdirectory without hdi_isfolder
func TestTraverseParallelListNoMarker(t *testing.T) {
	a := assert.New(t)
	//create mock server
	srv, close := mock_server.NewServer(mock_server.WithTransformAllRequestsToTestServerUrl())
	defer close()

	//initialize variables
	_, rawURL, blobURL, _, cName, credential, err := SetUpVariables()
	a.Nil(err)

	client, err := blobservice.NewClientWithSharedKeyCredential(rawURL, credential,
		&blobservice.ClientOptions{
			ClientOptions: azcore.ClientOptions{
				Transport: srv, //passing in mock server
			}})
	a.Nil(err)

	//define what mock server should return as response
	//first case is not a blob
	srv.AppendResponse(mock_server.WithStatusCode(400))
	//use marshalling to append hierarchical body with no folder flag
	srv.AppendResponse(mock_server.WithStatusCode(200), mock_server.WithBody([]byte(SetHierarchicalBody(rawURL, cName, false))))

	//changed includeversion to be false to make parallelListing true, changed includeDirectoryStubs to false so that hdi_isfolder is recognized, and made recursive false
	blobTraverser := newBlobTraverser(blobURL, client, ctx, false, false, func(common.EntityType) {}, true, common.CpkOptions{}, true, false, false, common.EPreservePermissionsOption.None(), false)

	//test method and validate
	seenFiles := make(map[string]StoredObject)

	err = blobTraverser.Traverse(nil, func(storedObject StoredObject) error {
		seenFiles[storedObject.relativePath] = storedObject
		return nil
	}, nil)
	a.Nil(err)
	a.Len(seenFiles, 3)
	a.Equal(*seenFiles["folder1/file1.txt"].Metadata["key1"], "val1")
	a.Equal(*seenFiles["folder1/folder2/file2.txt"].Metadata["key1"], "val1")
	a.Equal(*seenFiles["folder1/folder2/"].Metadata["hdi_isfolder"], "false")
}

// this test calls Traverse with a Parallel List that has no Subdirectory
func TestTraverseParallelListNoSubDir(t *testing.T) {
	a := assert.New(t)
	//create mock server
	srv, close := mock_server.NewServer(mock_server.WithTransformAllRequestsToTestServerUrl())
	defer close()

	//initialize variables
	_, rawURL, blobURL, _, cName, credential, err := SetUpVariables()
	a.Nil(err)

	client, err := blobservice.NewClientWithSharedKeyCredential(rawURL, credential,
		&blobservice.ClientOptions{
			ClientOptions: azcore.ClientOptions{
				Transport: srv, //passing in mock server
			}})
	a.Nil(err)

	//define what mock server should return as response
	//first case is not a blob
	srv.AppendResponse(mock_server.WithStatusCode(400))
	//use marshalling to append body
	srv.AppendResponse(mock_server.WithStatusCode(200), mock_server.WithBody([]byte(SetBodyNoSubDir(rawURL, cName, false))))

	//create new blob traverser
	//changed includeversion to be false to make parallelListing true and changed includeDirectoryStubs to false*** so that hdi_isfolder is recognized and made recursive false
	blobTraverser := newBlobTraverser(blobURL, client, ctx, false, false, func(common.EntityType) {}, true, common.CpkOptions{}, true, false, false, common.EPreservePermissionsOption.None(), false)

	//test method and validate
	seenFiles := make(map[string]StoredObject)

	err = blobTraverser.Traverse(nil, func(storedObject StoredObject) error {
		seenFiles[storedObject.relativePath] = storedObject
		return nil
	}, nil)
	a.Nil(err)
	a.Len(seenFiles, 2)
	a.Equal(*seenFiles["folder1/file1.txt"].Metadata["key1"], "val1")
	a.Equal(*seenFiles["folder1/"].Metadata["key1"], "val1")
}

// set two bodies where the first blobprefix becomes prefix for second call
func SetHierarchicalBodyRecursive(accounturl string, containername string, folderflag bool, secondcall bool) string {
	metadatatest := map[string]*string{
		"key1": to.Ptr("val1"),
	}
	metadatafolder := map[string]*string{"hdi_isfolder": to.Ptr("false")}
	if folderflag {
		metadatafolder["hdi_isfolder"] = to.Ptr("true")
	}
	if !secondcall {
		blobProp := container.BlobProperties{ContentLength: to.Ptr(int64(2))}
		blobitem := []*container.BlobItem{to.Ptr(container.BlobItem{Name: to.Ptr("folder1/file1.txt"), Properties: to.Ptr(blobProp), Metadata: metadatatest}), to.Ptr(container.BlobItem{Name: to.Ptr("folder1/folder2/"), Properties: to.Ptr(blobProp), Metadata: metadatafolder})}
		blobprefix := []*container.BlobPrefix{to.Ptr(container.BlobPrefix{Name: to.Ptr("folder1/folder2/")})}
		segment := container.BlobHierarchyListSegment{BlobItems: blobitem, BlobPrefixes: blobprefix}
		resp := container.ListBlobsHierarchySegmentResponse{ContainerName: to.Ptr(containername), Segment: to.Ptr(segment), ServiceEndpoint: to.Ptr(accounturl), Delimiter: to.Ptr("/"), Marker: to.Ptr(""), MaxResults: to.Ptr(int32(10)), Prefix: to.Ptr("folder1/")}
		out, err := xml.Marshal(resp)
		if err != nil {
			fmt.Printf(err.Error())
		}
		body := string(out)
		newbody := strings.Replace(body, "ListBlobsHierarchySegmentResponse", "EnumerationResults", -1)
		return newbody
	} else {
		blobProp := container.BlobProperties{ContentLength: to.Ptr(int64(2))}
		blobitem := []*container.BlobItem{to.Ptr(container.BlobItem{Name: to.Ptr("folder1/folder2/file2.txt"), Properties: to.Ptr(blobProp), Metadata: metadatatest})}
		segment := container.BlobHierarchyListSegment{BlobItems: blobitem}
		resp := container.ListBlobsHierarchySegmentResponse{ContainerName: to.Ptr(containername), Segment: to.Ptr(segment), ServiceEndpoint: to.Ptr(accounturl), Delimiter: to.Ptr("/"), Marker: to.Ptr(""), MaxResults: to.Ptr(int32(10)), Prefix: to.Ptr("folder1/folder2/")}
		out, err := xml.Marshal(resp)
		if err != nil {
			fmt.Printf(err.Error())
		}
		body := string(out)
		newbody := strings.Replace(body, "ListBlobsHierarchySegmentResponse", "EnumerationResults", -1)
		return newbody
	}
}

// this test calls Traverse and calls a Parallel List with a folder that is skipped and two responses with the second prefix set as the first blobprefix
func TestTraverseParallelListWithMarkerRecursive(t *testing.T) {
	a := assert.New(t)
	//create mock server
	srv, close := mock_server.NewServer(mock_server.WithTransformAllRequestsToTestServerUrl())
	defer close()

	//initialize variables
	_, rawURL, blobURL, _, cName, credential, err := SetUpVariables()
	a.Nil(err)

	client, err := blobservice.NewClientWithSharedKeyCredential(rawURL, credential,
		&blobservice.ClientOptions{
			ClientOptions: azcore.ClientOptions{
				Transport: srv, //passing in mock server
			}})
	a.Nil(err)

	//define what mock server should return as response
	//first case is not a blob
	srv.AppendResponse(mock_server.WithStatusCode(400))
	//use marshalling to append original body
	srv.AppendResponse(mock_server.WithStatusCode(200), mock_server.WithBody([]byte(SetHierarchicalBodyRecursive(rawURL, cName, true, false))))
	//second body uses first prefix
	srv.AppendResponse(mock_server.WithStatusCode(200), mock_server.WithBody([]byte(SetHierarchicalBodyRecursive(rawURL, cName, false, true))))

	//create new blob traverser
	//changed includeversion to be false to make parallelListing true, changed includeDirectoryStubs to false so that hdi_isfolder is recognized, and made recursive true for second call to occur
	blobTraverser := newBlobTraverser(blobURL, client, ctx, true, false, func(common.EntityType) {}, true, common.CpkOptions{}, true, false, false, common.EPreservePermissionsOption.None(), false)

	//test method and validate
	seenFiles := make(map[string]StoredObject)

	err = blobTraverser.Traverse(nil, func(storedObject StoredObject) error {
		seenFiles[storedObject.relativePath] = storedObject
		return nil
	}, nil)
	a.Nil(err)
	a.Len(seenFiles, 2)
	a.Equal(*seenFiles["folder1/file1.txt"].Metadata["key1"], "val1")
	a.Equal(*seenFiles["folder1/folder2/file2.txt"].Metadata["key1"], "val1")
}

// set tags for hierarchical mock response
func SetHierarchicalBodyBlobTags(accounturl string, containername string) string {
	metadatatest := map[string]*string{
		"key1": to.Ptr("val1"),
	}
	blobProp := container.BlobProperties{ContentLength: to.Ptr(int64(2))}
	blobtags := container.BlobTags{BlobTagSet: []*container.BlobTag{to.Ptr(container.BlobTag{Key: to.Ptr("tagval"), Value: to.Ptr("keyval")})}}
	blobitem := []*container.BlobItem{to.Ptr(container.BlobItem{Name: to.Ptr("folder1/file1.txt"), Properties: to.Ptr(blobProp), BlobTags: to.Ptr(blobtags), Metadata: metadatatest}), to.Ptr(container.BlobItem{Name: to.Ptr("folder1/folder2/"), Properties: to.Ptr(blobProp), Metadata: metadatatest})}
	segment := container.BlobHierarchyListSegment{BlobItems: blobitem}
	resp := container.ListBlobsHierarchySegmentResponse{ContainerName: to.Ptr(containername), Segment: to.Ptr(segment), ServiceEndpoint: to.Ptr(accounturl), Delimiter: to.Ptr("/"), Marker: to.Ptr(""), MaxResults: to.Ptr(int32(10)), Prefix: to.Ptr("folder1/")}
	out, err := xml.Marshal(resp)
	if err != nil {
		fmt.Printf(err.Error())
	}
	body := string(out)
	newbody := strings.Replace(body, "ListBlobsHierarchySegmentResponse", "EnumerationResults", -1)
	return newbody
}

// this test calls Traverse with a Parallel List with blob tags and metadata stored
func TestTraverseParallelListBlobTags(t *testing.T) {
	a := assert.New(t)
	//create mock server
	srv, close := mock_server.NewServer(mock_server.WithTransformAllRequestsToTestServerUrl())
	defer close()

	//initialize variables
	_, rawURL, blobURL, _, cName, credential, err := SetUpVariables()
	a.Nil(err)

	client, err := blobservice.NewClientWithSharedKeyCredential(rawURL, credential,
		&blobservice.ClientOptions{
			ClientOptions: azcore.ClientOptions{
				Transport: srv, //passing in mock server
			}})
	a.Nil(err)

	//first case is not a blob
	srv.AppendResponse(mock_server.WithStatusCode(400))
	//use marshalling to append hierarchical body with tags
	srv.AppendResponse(mock_server.WithStatusCode(200), mock_server.WithBody([]byte(SetHierarchicalBodyBlobTags(rawURL, cName))))

	//create new blob traverser
	//changed includeversion to be false to make parallelListing true, changed includeDirectoryStubs to false so that hdi_isfolder is recognized, and made recursive true
	blobTraverser := newBlobTraverser(blobURL, client, ctx, true, true, func(common.EntityType) {}, true, common.CpkOptions{}, true, false, false, common.EPreservePermissionsOption.None(), false)

	//test method and validate
	seenFiles := make(map[string]StoredObject)

	err = blobTraverser.Traverse(nil, func(storedObject StoredObject) error {
		seenFiles[storedObject.relativePath] = storedObject
		return nil
	}, nil)
	a.Nil(err)
	a.Len(seenFiles, 2)
	a.Equal(*seenFiles["folder1/file1.txt"].Metadata["key1"], "val1")
	a.Equal(*seenFiles["folder1/folder2/"].Metadata["key1"], "val1")
	a.Equal(seenFiles["folder1/file1.txt"].blobTags["tagval"], "keyval")
}

func SetHierarchicalBodyNoBlob(accounturl string, containername string) string {
	segment := container.BlobHierarchyListSegment{}
	resp := container.ListBlobsHierarchySegmentResponse{ContainerName: to.Ptr(containername), Segment: to.Ptr(segment), ServiceEndpoint: to.Ptr(accounturl), Delimiter: to.Ptr("/"), Marker: to.Ptr(""), MaxResults: to.Ptr(int32(10)), Prefix: to.Ptr("")}
	out, err := xml.Marshal(resp)
	if err != nil {
		fmt.Printf(err.Error())
	}
	body := string(out)
	newbody := strings.Replace(body, "ListBlobsHierarchySegmentResponse", "EnumerationResults", -1)
	return newbody
}

// this test calls Traverse and calls a Parallel List with no blob item or blob prefix
func TestTraverseParallelListNoBlob(t *testing.T) {
	a := assert.New(t)
	//create mock server
	srv, close := mock_server.NewServer(mock_server.WithTransformAllRequestsToTestServerUrl())
	defer close()
	//initialize variables
	_, rawURL, blobURL, _, cName, credential, err := SetUpVariables()
	a.Nil(err)

	client, err := blobservice.NewClientWithSharedKeyCredential(rawURL, credential,
		&blobservice.ClientOptions{
			ClientOptions: azcore.ClientOptions{
				Transport: srv, //passing in mock server
			}})
	a.Nil(err)
	//first case is not a blob
	srv.AppendResponse(mock_server.WithStatusCode(400))
	//use marshalling to append body with no blob
	srv.AppendResponse(mock_server.WithStatusCode(200), mock_server.WithBody([]byte(SetHierarchicalBodyNoBlob(rawURL, cName))))

	//create new blob traverser
	blobTraverser := newBlobTraverser(blobURL, client, ctx, false, false, func(common.EntityType) {}, true, common.CpkOptions{}, true, false, false, common.EPreservePermissionsOption.None(), false)

	//test method and validate
	seenFiles := make(map[string]StoredObject)

	err = blobTraverser.Traverse(nil, func(storedObject StoredObject) error {
		seenFiles[storedObject.relativePath] = storedObject
		return nil
	}, nil)
	a.Nil(err)
	a.Len(seenFiles, 0)
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
	blobTraverser := newBlobTraverser(rawBlobURLWithSAS, serviceClientWithSAS, ctx, InitResourceTraverserOptions{
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
	blobTraverser := newBlobTraverser(rawBlobURLWithSAS, serviceClientWithSAS, ctx, InitResourceTraverserOptions{
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
	blobTraverser = newBlobTraverser(rawBlobURLWithSAS, serviceClientWithSAS, ctx, InitResourceTraverserOptions{
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
	blobTraverser := newBlobTraverser(rawBlobURLWithSAS, serviceClientWithSAS, ctx, InitResourceTraverserOptions{
		Recursive:             true,
		IncludeDirectoryStubs: true,
	}, BlobTraverserOptions{isDFS: to.Ptr(true)})

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
	blobTraverser = newBlobTraverser(rawBlobURLWithSAS, serviceClientWithSAS, ctx, InitResourceTraverserOptions{
		Recursive:             true,
		IncludeDirectoryStubs: true,
	}, BlobTraverserOptions{isDFS: to.Ptr(true)})

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
	blobTraverser = newBlobTraverser(rawBlobURLWithSAS, serviceClientWithSAS, ctx, InitResourceTraverserOptions{
		Recursive:             true,
		IncludeDirectoryStubs: true,
	}, BlobTraverserOptions{isDFS: to.Ptr(true)})

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
	blobTraverser := newBlobTraverser(rawBlobURLWithSAS, serviceClientWithSAS, ctx, InitResourceTraverserOptions{
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
	blobTraverser := newBlobTraverser(rawBlobURLWithSAS, serviceClientWithSAS, ctx, InitResourceTraverserOptions{
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
	entityType := getEntityType(metadata)
	a.Equal(common.EEntityType.File(), entityType)

	metadata = make(common.Metadata)
	metadata["key"] = to.Ptr("value")
	entityType = getEntityType(metadata)
	a.Equal(common.EEntityType.File(), entityType)

	metadata = make(common.Metadata)
	metadata["hdi_isfolder"] = to.Ptr("false")
	entityType = getEntityType(metadata)
	a.Equal(common.EEntityType.File(), entityType)

	metadata = make(common.Metadata)
	metadata["Hdi_isfolder"] = to.Ptr("false")
	entityType = getEntityType(metadata)
	a.Equal(common.EEntityType.File(), entityType)

	metadata = make(common.Metadata)
	metadata["is_symlink"] = to.Ptr("false")
	entityType = getEntityType(metadata)
	a.Equal(common.EEntityType.File(), entityType)

	metadata = make(common.Metadata)
	metadata["Is_symlink"] = to.Ptr("false")
	entityType = getEntityType(metadata)
	a.Equal(common.EEntityType.File(), entityType)

	// Test case 2: metadata is a folder
	metadata = make(common.Metadata)
	metadata["hdi_isfolder"] = to.Ptr("true")
	entityType = getEntityType(metadata)
	a.Equal(common.EEntityType.Folder(), entityType)

	metadata = make(common.Metadata)
	metadata["hdi_isfolder"] = to.Ptr("True")
	entityType = getEntityType(metadata)
	a.Equal(common.EEntityType.Folder(), entityType)

	metadata = make(common.Metadata)
	metadata["Hdi_isfolder"] = to.Ptr("true")
	entityType = getEntityType(metadata)
	a.Equal(common.EEntityType.Folder(), entityType)

	// Test case 2: metadata is a symlink
	metadata = make(common.Metadata)
	metadata["is_symlink"] = to.Ptr("true")
	entityType = getEntityType(metadata)
	a.Equal(common.EEntityType.Symlink(), entityType)

	metadata = make(common.Metadata)
	metadata["is_symlink"] = to.Ptr("True")
	entityType = getEntityType(metadata)
	a.Equal(common.EEntityType.Symlink(), entityType)

	metadata = make(common.Metadata)
	metadata["Is_symlink"] = to.Ptr("true")
	entityType = getEntityType(metadata)
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

	propAdapter := blobPropertiesResponseAdapter{GetPropertiesResponse: &prop}
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
