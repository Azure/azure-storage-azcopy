package cmd

import (
	"encoding/xml"
	"fmt"
	"github.com/Azure/azure-sdk-for-go/sdk/azcore"
	"github.com/Azure/azure-sdk-for-go/sdk/azcore/to"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azfile/directory"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azfile/file"
	fileservice "github.com/Azure/azure-sdk-for-go/sdk/storage/azfile/service"
	"github.com/Azure/azure-storage-azcopy/v10/common"
	"github.com/Azure/azure-storage-azcopy/v10/mock_server"
	"github.com/stretchr/testify/assert"
	"testing"
	"time"
)

// function to initialize variables for mock testing
func SetUpVariablesFile() (accountname string, rawurl string, bloburl string, filename string, containername string, credential *file.SharedKeyCredential, err error) {
	// Create a client - Note: the key below is not a secret, this is the publicly documented Azurite key
	accountName := "myfakeaccount"
	accountKey := "Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw=="
	rawURL := fmt.Sprintf("https://%s.file.core.windows.net/", accountName)
	NewCredential, err := file.NewSharedKeyCredential(accountName, accountKey)

	if err != nil {
		return "", "", "", "", "", nil, err
	}

	//set container and blob name
	cName := generateContainerName()
	fName := generateBlobName()

	// construct a string that points to file name above : https://accountname.file.core.windows.net/containername/filename
	blobURL := fmt.Sprintf("https://%s.file.core.windows.net/%s/%s", accountName, cName, fName)

	return accountName, rawURL, blobURL, fName, cName, NewCredential, nil
}

// this test calls IsDirectory and returns true from a successful Get Properties response
func TestIsDir(t *testing.T) {
	a := assert.New(t)

	//create mock server
	srv, close := mock_server.NewServer(mock_server.WithTransformAllRequestsToTestServerUrl())
	defer close()

	//initialize variables
	_, rawURL, fileURL, _, _, credential, err := SetUpVariablesFile()
	a.Nil(err)

	client, err := fileservice.NewClientWithSharedKeyCredential(rawURL, credential,
		&fileservice.ClientOptions{
			ClientOptions: azcore.ClientOptions{
				Transport: srv, //passing in mock server
			}})
	a.Nil(err)

	//define what mock server should return as response from GetProperties
	srv.AppendResponse(mock_server.WithStatusCode(200))

	fileTraverser := newFileTraverser(fileURL, client, ctx, false, true, func(common.EntityType) {}, common.ETrailingDotOption.Enable(), nil)
	//test method and validate
	isDir, err := fileTraverser.IsDirectory(true)
	a.True(isDir)
	a.Nil(err)
}

// this test calls IsDirectory and returns false from a Get Properties call error
func TestIsDirFail(t *testing.T) {
	a := assert.New(t)

	//create mock server
	srv, close := mock_server.NewServer(mock_server.WithTransformAllRequestsToTestServerUrl())
	defer close()

	//initialize variables
	_, rawURL, fileURL, _, _, credential, err := SetUpVariablesFile()
	a.Nil(err)

	client, err := fileservice.NewClientWithSharedKeyCredential(rawURL, credential,
		&fileservice.ClientOptions{
			ClientOptions: azcore.ClientOptions{
				Transport: srv, //passing in mock server
			}})
	a.Nil(err)

	//define what mock server should return as response from GetProperties
	srv.AppendResponse(mock_server.WithStatusCode(404), mock_server.WithHeader("x-ms-error-code", "ResourceNotFound"), mock_server.WithBody([]byte(MockErrorBody("ResourceNotFound"))))

	fileTraverser := newFileTraverser(fileURL, client, ctx, false, true, func(common.EntityType) {}, common.ETrailingDotOption.Enable(), nil)
	//test method and validate
	isDir, err := fileTraverser.IsDirectory(true)
	a.False(isDir)
	a.Nil(err)
}

// this test calls getPropertiesIfSingleFile and successfully returns the properties from the get properties call
func TestGetPropSingleFile(t *testing.T) {
	a := assert.New(t)

	//create mock server
	srv, close := mock_server.NewServer(mock_server.WithTransformAllRequestsToTestServerUrl())
	defer close()

	//initialize variables
	_, rawURL, fileURL, _, _, credential, err := SetUpVariablesFile()
	a.Nil(err)

	client, err := fileservice.NewClientWithSharedKeyCredential(rawURL, credential,
		&fileservice.ClientOptions{
			ClientOptions: azcore.ClientOptions{
				Transport: srv, //passing in mock server
			}})
	a.Nil(err)

	//define what mock server should return as response from get properties call
	srv.AppendResponse(mock_server.WithStatusCode(200), mock_server.WithHeader("x-ms-type", "File"), mock_server.WithHeader("x-ms-meta-m1", "v1"), mock_server.WithHeader("Date", fmt.Sprintf("%s", time.Now().Format("Mon, 02 Jan 2006 15:04:05 MST"))))

	fileTraverser := newFileTraverser(fileURL, client, ctx, false, true, func(common.EntityType) {}, common.ETrailingDotOption.Enable(), nil)
	//check that all properties are properly returned
	FileProps, SingleFile, err := fileTraverser.getPropertiesIfSingleFile()
	a.True(SingleFile)
	a.Nil(err)
	a.NotNil(*FileProps.Date)
	a.Equal(*FileProps.Metadata["M1"], "v1")
	a.Equal(*FileProps.FileType, "File")
}

// this test calls getPropertiesIfSingleFile and returns no properties and false from a get properties call error
func TestGetPropSingleFileError(t *testing.T) {
	a := assert.New(t)

	//create mock server
	srv, close := mock_server.NewServer(mock_server.WithTransformAllRequestsToTestServerUrl())
	defer close()

	//initialize variables
	_, rawURL, fileURL, _, _, credential, err := SetUpVariablesFile()
	a.Nil(err)

	client, err := fileservice.NewClientWithSharedKeyCredential(rawURL, credential,
		&fileservice.ClientOptions{
			ClientOptions: azcore.ClientOptions{
				Transport: srv, //passing in mock server
			}})
	a.Nil(err)

	//define what mock server should return as response from get properties call
	srv.AppendResponse(mock_server.WithStatusCode(404), mock_server.WithHeader("x-ms-error-code", "ResourceNotFound"), mock_server.WithBody([]byte(MockErrorBody("ResourceNotFound"))))

	fileTraverser := newFileTraverser(fileURL, client, ctx, false, true, func(common.EntityType) {}, common.ETrailingDotOption.Enable(), nil)
	// check that no properties were returned and that single file is false
	FileProps, SingleFile, err := fileTraverser.getPropertiesIfSingleFile()
	a.False(SingleFile)
	a.Nil(FileProps)
	a.Nil(err)
}

// this test calls traverse for the case where the url points to a single file --> sets "isFile" to true in traverse
func TestTraverseSingleFile(t *testing.T) {
	a := assert.New(t)

	//create mock server
	srv, close := mock_server.NewServer(mock_server.WithTransformAllRequestsToTestServerUrl())
	defer close()

	//initialize variables
	_, rawURL, fileURL, fname, _, credential, err := SetUpVariablesFile()
	a.Nil(err)

	client, err := fileservice.NewClientWithSharedKeyCredential(rawURL, credential,
		&fileservice.ClientOptions{
			ClientOptions: azcore.ClientOptions{
				Transport: srv, //passing in mock server
			}})
	a.Nil(err)

	//define what mock server should return as response from get properties if single file call
	srv.AppendResponse(mock_server.WithStatusCode(200), mock_server.WithHeader("x-ms-type", "File"), mock_server.WithHeader("x-ms-meta-m1", "v1"), mock_server.WithHeader("Date", fmt.Sprintf("%s", time.Now().Format("Mon, 02 Jan 2006 15:04:05 MST"))), mock_server.WithHeader("Last-Modified", fmt.Sprintf("%s", time.Now().Format("Mon, 02 Jan 2006 15:04:05 MST"))), mock_server.WithHeader("Content-Length", "10"), mock_server.WithHeader("x-ms-file-last-write-time", "2024-07-09T15:04:05.0000000Z"))

	fileTraverser := newFileTraverser(fileURL, client, ctx, false, true, func(common.EntityType) {}, common.ETrailingDotOption.Enable(), nil)

	localDummyProcessor := dummyProcessorMap{}
	localDummyProcessor.MakeMap()
	err = fileTraverser.Traverse(nil, localDummyProcessor.process, []ObjectFilter{})
	a.Nil(err)
	// check that isFile was set to true, resulting in the following properties being set from single file
	a.Len(localDummyProcessor.record, 1)
	a.Equal(*localDummyProcessor.record[fname].Metadata["M1"], "v1")
	a.Equal(localDummyProcessor.record[fname].smbLastModifiedTime.Day(), 9)
}

// this function sets the response body for "NewListFilesAndDirectoriesPager" with two files
func SetBodyTwoFiles(accounturl string) string {
	fileprop := directory.FileProperty{ContentLength: to.Ptr(int64(2))}
	files := []*directory.File{to.Ptr(directory.File{Name: to.Ptr("filename"), Properties: to.Ptr(fileprop), Attributes: to.Ptr("Archive"), ID: to.Ptr("file-id"), PermissionKey: to.Ptr("testperm")}), to.Ptr(directory.File{Name: to.Ptr("filename2"), Properties: to.Ptr(fileprop), Attributes: to.Ptr("Archive"), ID: to.Ptr("file-id"), PermissionKey: to.Ptr("testperm")})}
	entries := directory.FilesAndDirectoriesListSegment{Files: files}
	resp := directory.ListFilesAndDirectoriesSegmentResponse{DirectoryPath: to.Ptr("dirpath"), NextMarker: to.Ptr(""), Prefix: to.Ptr(""), Segment: to.Ptr(entries), ServiceEndpoint: to.Ptr(accounturl), ShareName: to.Ptr("myshare"), DirectoryID: to.Ptr("mydir"), Encoded: to.Ptr(false), Marker: to.Ptr(""), MaxResults: to.Ptr(int32(5)), ShareSnapshot: to.Ptr("")}
	out, err := xml.Marshal(resp)
	if err != nil {
		fmt.Printf(err.Error())
	}
	body := string(out)
	return body
}

// this test calls traverse with the new traverser variable "getProperties" set to false, storing two files
func TestTraverseMultipleFilesNoProp(t *testing.T) {
	a := assert.New(t)

	//create mock server
	srv, close := mock_server.NewServer(mock_server.WithTransformAllRequestsToTestServerUrl())
	defer close()

	//initialize variables
	_, rawURL, fileURL, _, _, credential, err := SetUpVariablesFile()
	a.Nil(err)

	client, err := fileservice.NewClientWithSharedKeyCredential(rawURL, credential,
		&fileservice.ClientOptions{
			ClientOptions: azcore.ClientOptions{
				Transport: srv, //passing in mock server
			}})
	a.Nil(err)

	//define what mock server should return as response
	// isFile is false from getpropifsinglecall since there are two files
	srv.AppendResponse(mock_server.WithStatusCode(404), mock_server.WithHeader("x-ms-error-code", "ResourceNotFound"), mock_server.WithBody([]byte(MockErrorBody("ResourceNotFound"))))
	//directory client get properties call
	srv.AppendResponse(mock_server.WithStatusCode(200), mock_server.WithHeader("x-ms-meta-m1", "v1"), mock_server.WithHeader("Date", fmt.Sprintf("%s", time.Now().Format("Mon, 02 Jan 2006 15:04:05 MST"))), mock_server.WithHeader("Last-Modified", fmt.Sprintf("%s", time.Now().Format("Mon, 02 Jan 2006 15:04:05 MST"))), mock_server.WithHeader("x-ms-file-last-write-time", "2024-07-09T15:04:05.0000000Z"))
	//list files and  dir response
	srv.AppendResponse(mock_server.WithStatusCode(200), mock_server.WithHeader("Content-Type", "application/xml"), mock_server.WithBody([]byte(SetBodyTwoFiles(rawURL))))
	//set getProperties to false
	fileTraverser := newFileTraverser(fileURL, client, ctx, false, false, func(common.EntityType) {}, common.ETrailingDotOption.Enable(), nil)
	localDummyProcessor := dummyProcessorMap{}
	localDummyProcessor.MakeMap()
	err = fileTraverser.Traverse(nil, localDummyProcessor.process, []ObjectFilter{})
	//check that both files are stored with data set from response body
	a.Nil(err)
	a.Len(localDummyProcessor.record, 3)
	a.EqualValues(localDummyProcessor.record["filename"].size, 2)
}

// this test calls traverse with two files and a directory with "getProperties" traverser variable set to true
func TestTraverseMultipleFilesAndDirGetProp(t *testing.T) {
	a := assert.New(t)

	//create mock server
	srv, close := mock_server.NewServer(mock_server.WithTransformAllRequestsToTestServerUrl())
	defer close()

	//initialize variables
	_, rawURL, fileURL, _, _, credential, err := SetUpVariablesFile()
	a.Nil(err)

	client, err := fileservice.NewClientWithSharedKeyCredential(rawURL, credential,
		&fileservice.ClientOptions{
			ClientOptions: azcore.ClientOptions{
				Transport: srv, //passing in mock server
			}})
	a.Nil(err)

	//define what mock server should return as response
	//get prop if single file: false since not a single file
	srv.AppendResponse(mock_server.WithStatusCode(404), mock_server.WithHeader("x-ms-error-code", "ResourceNotFound"), mock_server.WithBody([]byte(MockErrorBody("ResourceNotFound"))))
	//directory get properties call
	srv.AppendResponse(mock_server.WithStatusCode(200))
	//directory get properties with properties sent
	srv.AppendResponse(mock_server.WithStatusCode(200), mock_server.WithHeader("x-ms-meta-m1", "v1"), mock_server.WithHeader("Date", fmt.Sprintf("%s", time.Now().Format("Mon, 02 Jan 2006 15:04:05 MST"))), mock_server.WithHeader("Last-Modified", fmt.Sprintf("%s", time.Now().Format("Mon, 02 Jan 2006 15:04:05 MST"))), mock_server.WithHeader("x-ms-file-last-write-time", "2024-07-09T15:04:05.0000000Z"))
	//new list files and dir pager with response body
	srv.AppendResponse(mock_server.WithStatusCode(200), mock_server.WithHeader("Content-Type", "application/xml"), mock_server.WithHeader("x-ms-file-extended-info", "true"), mock_server.WithBody([]byte(SetBodyWithMultipleFilesandDir(rawURL))))
	//first file client get properties call
	srv.AppendResponse(mock_server.WithStatusCode(200), mock_server.WithHeader("x-ms-type", "File"), mock_server.WithHeader("x-ms-meta-m2", "v2"), mock_server.WithHeader("Date", fmt.Sprintf("%s", time.Now().Format("Mon, 02 Jan 2006 15:04:05 MST"))), mock_server.WithHeader("Last-Modified", fmt.Sprintf("%s", time.Now().Format("Mon, 02 Jan 2006 15:04:05 MST"))), mock_server.WithHeader("Content-Length", "5"), mock_server.WithHeader("x-ms-file-last-write-time", "2024-07-09T15:04:05.0000000Z"))
	//second file client get properties call
	srv.AppendResponse(mock_server.WithStatusCode(200), mock_server.WithHeader("x-ms-type", "File"), mock_server.WithHeader("x-ms-meta-m3", "v3"), mock_server.WithHeader("Date", fmt.Sprintf("%s", time.Now().Format("Mon, 02 Jan 2006 15:04:05 MST"))), mock_server.WithHeader("Last-Modified", fmt.Sprintf("%s", time.Now().Format("Mon, 02 Jan 2006 15:04:05 MST"))), mock_server.WithHeader("Content-Length", "10"), mock_server.WithHeader("x-ms-file-last-write-time", "2024-07-09T15:04:05.0000000Z"))
	//directory from response get properties call
	srv.AppendResponse(mock_server.WithStatusCode(200), mock_server.WithHeader("x-ms-meta-m4", "v4"), mock_server.WithHeader("Date", fmt.Sprintf("%s", time.Now().Format("Mon, 02 Jan 2006 15:04:05 MST"))), mock_server.WithHeader("Last-Modified", fmt.Sprintf("%s", time.Now().Format("Mon, 02 Jan 2006 15:04:05 MST"))), mock_server.WithHeader("x-ms-file-last-write-time", "2024-07-09T15:04:05.0000000Z"))

	//create traverser with getProperties set to true
	fileTraverser := newFileTraverser(fileURL, client, ctx, false, true, func(common.EntityType) {}, common.ETrailingDotOption.Enable(), nil)
	localDummyProcessor := dummyProcessorMap{}
	localDummyProcessor.MakeMap()
	err = fileTraverser.Traverse(nil, localDummyProcessor.process, []ObjectFilter{})
	a.Nil(err)
	a.Len(localDummyProcessor.record, 4)
	a.EqualValues(localDummyProcessor.record["filename2"].size, 10)
	a.EqualValues(*localDummyProcessor.record["dirname"].Metadata["M4"], "v4")
}

// this function sets the response body for "NewListFilesAndDirectoriesPager" with two files and a directory
func SetBodyWithMultipleFilesandDir(accounturl string) string {
	fileprop := directory.FileProperty{ContentLength: to.Ptr(int64(2))}
	dirprop := directory.FileProperty{ContentLength: to.Ptr(int64(2))}
	files := []*directory.File{to.Ptr(directory.File{Name: to.Ptr("filename"), Properties: to.Ptr(fileprop), Attributes: to.Ptr("Archive"), ID: to.Ptr("file-id"), PermissionKey: to.Ptr("testperm")}), to.Ptr(directory.File{Name: to.Ptr("filename2"), Properties: to.Ptr(fileprop), Attributes: to.Ptr("Archive"), ID: to.Ptr("file-id"), PermissionKey: to.Ptr("testperm")})}
	directories := []*directory.Directory{to.Ptr(directory.Directory{Name: to.Ptr("dirname"), Attributes: to.Ptr(""), ID: to.Ptr("testid"), PermissionKey: to.Ptr(""), Properties: to.Ptr(dirprop)})}
	entries := directory.FilesAndDirectoriesListSegment{Directories: directories, Files: files}
	resp := directory.ListFilesAndDirectoriesSegmentResponse{DirectoryPath: to.Ptr("dirpath"), NextMarker: to.Ptr(""), Prefix: to.Ptr(""), Segment: to.Ptr(entries), ServiceEndpoint: to.Ptr(accounturl), ShareName: to.Ptr("myshare"), DirectoryID: to.Ptr("mydir"), Encoded: to.Ptr(false), Marker: to.Ptr(""), MaxResults: to.Ptr(int32(5)), ShareSnapshot: to.Ptr("")}
	out, err := xml.Marshal(resp)
	if err != nil {
		fmt.Printf(err.Error())
	}
	body := string(out)
	return body
}

// this test calls traverse with two files and a directory with "getProperties" traverser variable set to false
func TestTraverseMultipleFilesAndDirNoProp(t *testing.T) {
	a := assert.New(t)

	//create mock server
	srv, close := mock_server.NewServer(mock_server.WithTransformAllRequestsToTestServerUrl())
	defer close()

	//initialize variables
	_, rawURL, fileURL, _, _, credential, err := SetUpVariablesFile()
	a.Nil(err)

	client, err := fileservice.NewClientWithSharedKeyCredential(rawURL, credential,
		&fileservice.ClientOptions{
			ClientOptions: azcore.ClientOptions{
				Transport: srv, //passing in mock server
			}})
	a.Nil(err)

	//single file call error: sets isFile to False
	srv.AppendResponse(mock_server.WithStatusCode(404), mock_server.WithHeader("x-ms-error-code", "ResourceNotFound"), mock_server.WithBody([]byte(MockErrorBody("ResourceNotFound"))))
	//directory get properties call
	srv.AppendResponse(mock_server.WithStatusCode(200), mock_server.WithHeader("x-ms-meta-m1", "v1"), mock_server.WithHeader("Date", fmt.Sprintf("%s", time.Now().Format("Mon, 02 Jan 2006 15:04:05 MST"))), mock_server.WithHeader("Last-Modified", fmt.Sprintf("%s", time.Now().Format("Mon, 02 Jan 2006 15:04:05 MST"))), mock_server.WithHeader("x-ms-file-last-write-time", "2024-07-09T15:04:05.0000000Z"))
	//list files and dir response body
	srv.AppendResponse(mock_server.WithStatusCode(200), mock_server.WithHeader("Content-Type", "application/xml"), mock_server.WithBody([]byte(SetBodyWithMultipleFilesandDir(rawURL))))
	//getProperties is set to false
	fileTraverser := newFileTraverser(fileURL, client, ctx, false, false, func(common.EntityType) {}, common.ETrailingDotOption.Enable(), nil)
	localDummyProcessor := dummyProcessorMap{}
	localDummyProcessor.MakeMap()
	err = fileTraverser.Traverse(nil, localDummyProcessor.process, []ObjectFilter{})

	//check that the files and directories were stored but corresponding properties were not
	a.Nil(err)
	a.Len(localDummyProcessor.record, 4)
	a.EqualValues(localDummyProcessor.record["filename"].entityType, 0)
	a.Len(localDummyProcessor.record["dirname"].Metadata, 0)
}

// this function creates the response body for the first and second call of the recursive traversal case
// first returns a file and directory then returns a file within the first directory with dirpath set to it
func SetBodyRecursiveFilesandDir(accounturl string, firstcall bool) string {
	if firstcall {
		fileprop := directory.FileProperty{ContentLength: to.Ptr(int64(2))}
		dirprop := directory.FileProperty{ContentLength: to.Ptr(int64(2))}
		files := []*directory.File{to.Ptr(directory.File{Name: to.Ptr("filename"), Properties: to.Ptr(fileprop), Attributes: to.Ptr("Archive"), ID: to.Ptr("file-id"), PermissionKey: to.Ptr("testperm")})}
		directories := []*directory.Directory{to.Ptr(directory.Directory{Name: to.Ptr("dirname/subdirname"), Attributes: to.Ptr(""), ID: to.Ptr("testid"), PermissionKey: to.Ptr(""), Properties: to.Ptr(dirprop)})}
		entries := directory.FilesAndDirectoriesListSegment{Directories: directories, Files: files}
		resp := directory.ListFilesAndDirectoriesSegmentResponse{DirectoryPath: to.Ptr(""), NextMarker: to.Ptr(""), Prefix: to.Ptr(""), Segment: to.Ptr(entries), ServiceEndpoint: to.Ptr(accounturl), ShareName: to.Ptr("myshare"), DirectoryID: to.Ptr("mydir"), Encoded: to.Ptr(false), Marker: to.Ptr(""), MaxResults: to.Ptr(int32(5)), ShareSnapshot: to.Ptr("")}
		out, err := xml.Marshal(resp)
		if err != nil {
			fmt.Printf(err.Error())
		}
		body := string(out)
		return body
	}
	fileprop := directory.FileProperty{ContentLength: to.Ptr(int64(1))}
	files := []*directory.File{to.Ptr(directory.File{Name: to.Ptr("filename2"), Attributes: to.Ptr(""), ID: to.Ptr("testid"), PermissionKey: to.Ptr(""), Properties: to.Ptr(fileprop)})}
	entries := directory.FilesAndDirectoriesListSegment{Files: files}
	resp := directory.ListFilesAndDirectoriesSegmentResponse{DirectoryPath: to.Ptr("dirname"), NextMarker: to.Ptr(""), Prefix: to.Ptr(""), Segment: to.Ptr(entries), ServiceEndpoint: to.Ptr(accounturl), ShareName: to.Ptr("myshare"), DirectoryID: to.Ptr("mydir"), Encoded: to.Ptr(false), Marker: to.Ptr(""), MaxResults: to.Ptr(int32(5)), ShareSnapshot: to.Ptr("")}
	out, err := xml.Marshal(resp)
	if err != nil {
		fmt.Printf(err.Error())
	}
	body := string(out)
	return body
}

// this test calls traverse with a file and directory then a file within the directory with recursive set to true
func TestTraverseFileAndDirRecursive(t *testing.T) {
	a := assert.New(t)

	//create mock server
	srv, close := mock_server.NewServer(mock_server.WithTransformAllRequestsToTestServerUrl())
	defer close()

	//initialize variables
	_, rawURL, fileURL, _, _, credential, err := SetUpVariablesFile()
	a.Nil(err)

	client, err := fileservice.NewClientWithSharedKeyCredential(rawURL, credential,
		&fileservice.ClientOptions{
			ClientOptions: azcore.ClientOptions{
				Transport: srv, //passing in mock server
			}})
	a.Nil(err)

	//define what mock server should return as response
	//single file call error: sets isFile to False
	srv.AppendResponse(mock_server.WithStatusCode(404), mock_server.WithHeader("x-ms-error-code", "ResourceNotFound"), mock_server.WithBody([]byte(MockErrorBody("ResourceNotFound"))))
	//directory get properties
	srv.AppendResponse(mock_server.WithStatusCode(200), mock_server.WithHeader("x-ms-meta-m1", "v1"), mock_server.WithHeader("Date", fmt.Sprintf("%s", time.Now().Format("Mon, 02 Jan 2006 15:04:05 MST"))), mock_server.WithHeader("Last-Modified", fmt.Sprintf("%s", time.Now().Format("Mon, 02 Jan 2006 15:04:05 MST"))), mock_server.WithHeader("x-ms-file-last-write-time", "2024-07-09T15:04:05.0000000Z"))
	//list files and dir response body: first call
	srv.AppendResponse(mock_server.WithStatusCode(200), mock_server.WithHeader("Content-Type", "application/xml"), mock_server.WithBody([]byte(SetBodyRecursiveFilesandDir(rawURL, true))))
	//list files and dir recursive response: second call
	srv.AppendResponse(mock_server.WithStatusCode(200), mock_server.WithHeader("Content-Type", "application/xml"), mock_server.WithBody([]byte(SetBodyRecursiveFilesandDir(rawURL, false))))

	//recursive is set to true
	fileTraverser := newFileTraverser(fileURL, client, ctx, true, false, func(common.EntityType) {}, common.ETrailingDotOption.Enable(), nil)
	localDummyProcessor := dummyProcessorMap{}
	localDummyProcessor.MakeMap()
	err = fileTraverser.Traverse(nil, localDummyProcessor.process, []ObjectFilter{})
	a.Nil(err)
	//check that both files and the directory/subdirectory/file within it are stored
	a.Len(localDummyProcessor.record, 4)
	a.EqualValues(localDummyProcessor.record["filename"].entityType, 0)
	a.EqualValues(localDummyProcessor.record["dirname/subdirname/filename2"].entityType, 0)
}
