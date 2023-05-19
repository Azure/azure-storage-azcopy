package azbfs_test

import (
	"context"
	"fmt"
	"github.com/stretchr/testify/assert"
	"net/http"
	"net/url"
	"strings"
	"testing"
	"time"

	"github.com/Azure/azure-storage-azcopy/v10/azbfs"
)

// TestCreateDirectory test the creation of a directory
func TestCreateDeleteDirectory(t *testing.T) {
	a := assert.New(t)
	// Create a file system
	fsu := getBfsServiceURL()
	fsURL, _ := createNewFileSystem(a, fsu)
	defer deleteFileSystem(a, fsURL)

	// Create a directory url from the fileSystem Url
	dirUrl, _ := getDirectoryURLFromFileSystem(a, fsURL)
	cResp, err := dirUrl.Create(context.Background(), true)
	defer deleteDirectory(a, dirUrl)

	// Assert the directory create response header attributes
	a.Nil(err)
	a.Equal(http.StatusCreated, cResp.StatusCode())
	a.NotEqual("", cResp.ETag())
	a.NotEqual("", cResp.LastModified())
	a.NotEqual("", cResp.XMsRequestID())
	a.NotEqual("", cResp.XMsVersion())
	a.NotEqual("", cResp.Date())
}

// TestCreateSubDir tests creating the sub-directory inside a directory
func TestCreateSubDir(t *testing.T) {
	a := assert.New(t)
	// Create the file system
	fsu := getBfsServiceURL()
	fsURL, _ := createNewFileSystem(a, fsu)
	defer deleteFileSystem(a, fsURL)

	// Create the directory Url from fileSystem Url and create directory
	dirUrl, _ := getDirectoryURLFromFileSystem(a, fsURL)
	cResp, err := dirUrl.Create(context.Background(), true)
	defer deleteDirectory(a, dirUrl)

	a.Nil(err)
	a.Equal(http.StatusCreated, cResp.StatusCode())
	a.NotEqual("", cResp.ETag())
	a.NotEqual("", cResp.LastModified())
	a.NotEqual("", cResp.XMsRequestID())
	a.NotEqual("", cResp.XMsVersion())
	a.NotEqual("", cResp.Date())

	// Create the sub-directory url from directory Url and create sub-directory
	subDirUrl, _ := getDirectoryURLFromDirectory(a, dirUrl)
	cResp, err = subDirUrl.Create(context.Background(), true)
	defer deleteDirectory(a, subDirUrl)

	a.Nil(err)
	a.Equal(http.StatusCreated, cResp.StatusCode())
	a.NotEqual("", cResp.ETag())
	a.NotEqual("", cResp.LastModified())
	a.NotEqual("", cResp.XMsRequestID())
	a.NotEqual("", cResp.XMsVersion())
	a.NotEqual("", cResp.Date())

}

// TestDirectoryCreateAndGetProperties tests the create directory and
// get directory properties
func TestDirectoryCreateAndGetProperties(t *testing.T) {
	a := assert.New(t)
	// Create file system
	fsu := getBfsServiceURL()
	fsURL, _ := createNewFileSystem(a, fsu)
	defer deleteFileSystem(a, fsURL)

	// Create directory url from fileSystemUrl and create directory
	dirUrl, _ := getDirectoryURLFromFileSystem(a, fsURL)
	cResp, err := dirUrl.Create(context.Background(), true)
	defer deleteDirectory(a, dirUrl)

	a.Nil(err)
	a.Equal(http.StatusCreated, cResp.StatusCode())
	a.NotEqual("", cResp.ETag())
	a.NotEqual("", cResp.LastModified())
	a.NotEqual("", cResp.XMsRequestID())
	a.NotEqual("", cResp.XMsVersion())
	a.NotEqual("", cResp.Date())

	// Get the directory properties and verify the resource type
	gResp, err := dirUrl.GetProperties(context.Background())
	a.Nil(err)
	a.Equal(http.StatusOK, gResp.StatusCode())
	a.Equal("directory", gResp.XMsResourceType())
}

// TestCreateDirectoryAndFiles tests the create directory and create file inside the directory
func TestCreateDirectoryAndFiles(t *testing.T) {
	a := assert.New(t)
	// Create the file system
	fsu := getBfsServiceURL()
	fsURL, _ := createNewFileSystem(a, fsu)
	defer deleteFileSystem(a, fsURL)

	// Create the directoryUrl from fileSystemUrl
	// and create directory
	dirUrl, _ := getDirectoryURLFromFileSystem(a, fsURL)
	cResp, err := dirUrl.Create(context.Background(), true)
	defer deleteDirectory(a, dirUrl)

	a.Nil(err)
	a.Equal(http.StatusCreated, cResp.StatusCode())
	a.NotEqual("", cResp.ETag())
	a.NotEqual("", cResp.LastModified())
	a.NotEqual("", cResp.XMsRequestID())
	a.NotEqual("", cResp.XMsVersion())
	a.NotEqual("", cResp.Date())

	// Create fileUrl from directoryUrl and create file inside the directory
	fileUrl, _ := getFileURLFromDirectory(a, dirUrl)
	fresp, err := fileUrl.Create(context.Background(), azbfs.BlobFSHTTPHeaders{}, azbfs.BlobFSAccessControl{})
	defer deleteFile(a, fileUrl)

	a.Nil(err)
	a.Equal(http.StatusCreated, fresp.Response().StatusCode)
	a.NotEqual("", fresp.ETag())
	a.NotEqual("", fresp.LastModified())
	a.NotEqual("", fresp.XMsRequestID())
	a.NotEqual("", fresp.XMsVersion())
	a.NotEqual("", fresp.Date())

}

// TestReCreateDirectory tests the creation of directories that already exist
func TestReCreateDirectory(t *testing.T) {
	a := assert.New(t)
	// Create the file system
	fsu := getBfsServiceURL()
	fsURL, _ := createNewFileSystem(a, fsu)
	defer deleteFileSystem(a, fsURL)

	// Create the directoryUrl from fileSystemUrl and create directory
	dirUrl, _ := getDirectoryURLFromFileSystem(a, fsURL)
	cResp, err := dirUrl.Create(context.Background(), true)
	defer deleteDirectory(a, dirUrl)
	a.Nil(err)
	a.Equal(http.StatusCreated, cResp.StatusCode())

	// Re-create it (allowing overwrite)
	// TODO: put some files in it before this, and make assertions about what happens to them after the re-creation
	cResp, err = dirUrl.Create(context.Background(), true)
	a.Nil(err)
	a.Equal(http.StatusCreated, cResp.StatusCode())

	// Attempt to re-create it (but do NOT allow overwrite)
	cResp, err = dirUrl.Create(context.Background(), false) // <- false for re-create
	a.NotNil(err)
	stgErr, ok := err.(azbfs.StorageError)
	a.True(ok)
	a.Equal(http.StatusConflict, stgErr.Response().StatusCode)
	a.Equal(azbfs.ServiceCodePathAlreadyExists, stgErr.ServiceCode())
}

// TestCreateMetadataDeleteDirectory test the creation of a directory with metadata
func TestCreateMetadataDeleteDirectory(t *testing.T) {
	a := assert.New(t)
	// Create a file system
	fsu := getBfsServiceURL()
	fsURL, _ := createNewFileSystem(a, fsu)
	defer deleteFileSystem(a, fsURL)

	// Create metadata
	metadata := make(map[string]string)
	metadata["foo"] = "bar"

	// Create a directory url from the fileSystem Url
	dirUrl, _ := getDirectoryURLFromFileSystem(a, fsURL)
	cResp, err := dirUrl.CreateWithOptions(context.Background(),
		azbfs.CreateDirectoryOptions{RecreateIfExists: true, Metadata: metadata})
	defer deleteDirectory(a, dirUrl)

	// Assert the directory create response header attributes
	a.Nil(err)
	a.Equal(http.StatusCreated, cResp.StatusCode())
	a.NotEqual("", cResp.ETag())
	a.NotEqual("", cResp.LastModified())
	a.NotEqual("", cResp.XMsRequestID())
	a.NotEqual("", cResp.XMsVersion())
	a.NotEqual("", cResp.Date())

	getResp, err := dirUrl.GetProperties(context.Background())
	a.Nil(err)
	a.Equal(http.StatusOK, getResp.StatusCode())
	a.NotEqual("", getResp.XMsProperties()) // Check metadata returned is not null.
}

// TestDirectoryStructure tests creating dir, sub-dir inside dir and files
// inside dirs and sub-dirs. Then verify the count of files / sub-dirs inside directory
func TestDirectoryStructure(t *testing.T) {
	a := assert.New(t)
	// Create file system
	fsu := getBfsServiceURL()
	fsURL, _ := createNewFileSystem(a, fsu)
	defer deleteFileSystem(a, fsURL)

	// Create a directory inside filesystem
	dirUrl, _ := getDirectoryURLFromFileSystem(a, fsURL)
	cResp, err := dirUrl.Create(context.Background(), true)
	defer deleteDirectory(a, dirUrl)

	a.Nil(err)
	a.Equal(http.StatusCreated, cResp.StatusCode())
	a.NotEqual("", cResp.ETag())
	a.NotEqual("", cResp.LastModified())
	a.NotEqual("", cResp.XMsRequestID())
	a.NotEqual("", cResp.XMsVersion())
	a.NotEqual("", cResp.Date())

	// Create a sub-dir inside the above create directory
	subDirUrl, _ := getDirectoryURLFromDirectory(a, dirUrl)
	cResp, err = subDirUrl.Create(context.Background(), true)
	defer deleteDirectory(a, subDirUrl)

	a.Nil(err)
	a.Equal(http.StatusCreated, cResp.StatusCode())
	a.NotEqual("", cResp.ETag())
	a.NotEqual("", cResp.LastModified())
	a.NotEqual("", cResp.XMsRequestID())
	a.NotEqual("", cResp.XMsVersion())
	a.NotEqual("", cResp.Date())

	// Create a file inside directory
	fileUrl, _ := getFileURLFromDirectory(a, dirUrl)
	fresp, err := fileUrl.Create(context.Background(), azbfs.BlobFSHTTPHeaders{}, azbfs.BlobFSAccessControl{})
	defer deleteFile(a, fileUrl)

	a.Nil(err)
	a.Equal(http.StatusCreated, fresp.Response().StatusCode)
	a.NotEqual("", fresp.ETag())
	a.NotEqual("", fresp.LastModified())
	a.NotEqual("", fresp.XMsRequestID())
	a.NotEqual("", fresp.XMsVersion())
	a.NotEqual("", fresp.Date())

	// create a file inside the sub-dir created above
	subDirfileUrl, _ := getFileURLFromDirectory(a, subDirUrl)
	fresp, err = subDirfileUrl.Create(context.Background(), azbfs.BlobFSHTTPHeaders{}, azbfs.BlobFSAccessControl{})
	defer deleteFile(a, subDirfileUrl)

	a.Nil(err)
	a.Equal(http.StatusCreated, fresp.Response().StatusCode)
	a.NotEqual("", fresp.ETag())
	a.NotEqual("", fresp.LastModified())
	a.NotEqual("", fresp.XMsRequestID())
	a.NotEqual("", fresp.XMsVersion())
	a.NotEqual("", fresp.Date())

	// list the directory create above.
	// expected number of file inside the dir is 2 i.e one
	// inside the dir itself and one inside the sub-dir
	// expected number of sub-dir inside the dir is 1
	continuationMarker := ""
	lresp, err := dirUrl.ListDirectorySegment(context.Background(), &continuationMarker, true)

	a.Nil(err)
	a.Equal(http.StatusOK, lresp.Response().StatusCode)
	a.Equal(2, len(lresp.Files()))
	a.Equal(1, len(lresp.Directories()))
	a.Equal("", lresp.ETag())
	a.Equal("", lresp.LastModified())
	a.NotEqual("", lresp.XMsRequestID())
	a.NotEqual("", lresp.XMsVersion())
	a.NotEqual("", lresp.Date())
}

func TestListDirectoryWithSpaces(t *testing.T) {
	a := assert.New(t)
	// Create file system
	fsu := getBfsServiceURL()
	fsURL, _ := createNewFileSystem(a, fsu)
	defer deleteFileSystem(a, fsURL)

	// Create a directory inside filesystem
	dirUrl := fsURL.NewDirectoryURL("New Folder Test 2")
	_, err := dirUrl.Create(context.Background(), true)
	defer deleteDirectory(a, dirUrl)

	// Create a file inside directory
	fileUrl, _ := getFileURLFromDirectory(a, dirUrl)
	_, err = fileUrl.Create(context.Background(), azbfs.BlobFSHTTPHeaders{}, azbfs.BlobFSAccessControl{})
	defer deleteFile(a, fileUrl)

	// list the directory created above.
	// expected number of files inside the dir is 1
	continuationMarker := ""
	lresp, err := dirUrl.ListDirectorySegment(context.Background(), &continuationMarker, true)
	a.Nil(err)
	a.Equal(http.StatusOK, lresp.Response().StatusCode)
	a.Equal(1, len(lresp.Files()))
	a.Equal(0, len(lresp.Directories()))
	a.Equal("", lresp.ETag())
	a.Equal("", lresp.LastModified())
	a.NotEqual("", lresp.XMsRequestID())
	a.NotEqual("", lresp.XMsVersion())
	a.NotEqual("", lresp.Date())
}

func TestRenameDirectory(t *testing.T) {
	a := assert.New(t)
	fsu := getBfsServiceURL()
	fileSystemURL, _ := createNewFileSystem(a, fsu)
	defer deleteFileSystem(a, fileSystemURL)

	dirURL, dirName := createNewDirectoryFromFileSystem(a, fileSystemURL)
	dirRename := dirName + "rename"

	renamedDirURL, err := dirURL.Rename(context.Background(), azbfs.RenameDirectoryOptions{DestinationPath: dirRename})
	a.NotNil(renamedDirURL)
	a.Nil(err)

	// Check that the old directory does not exist
	getPropertiesResp, err := dirURL.GetProperties(context.Background())
	a.NotNil(err) // TODO: I want to check the status code is 404 but not sure how since the resp is nil
	a.Nil(getPropertiesResp)

	// Check that the renamed directory does exist
	getPropertiesResp, err = renamedDirURL.GetProperties(context.Background())
	a.Equal(http.StatusOK, getPropertiesResp.StatusCode())
	a.Nil(err)
}

func TestRenameDirWithFile(t *testing.T) {
	a := assert.New(t)
	fsu := getBfsServiceURL()
	fileSystemURL, _ := createNewFileSystem(a, fsu)
	defer deleteFileSystem(a, fileSystemURL)

	dirURL, dirName := createNewDirectoryFromFileSystem(a, fileSystemURL)
	fileName := "test.txt"
	fileURL := dirURL.NewFileURL(fileName)
	dirRename := dirName + "rename"

	renamedDirURL, err := dirURL.Rename(context.Background(), azbfs.RenameDirectoryOptions{DestinationPath: dirRename})
	a.NotNil(renamedDirURL)
	a.Nil(err)

	// Check that the old directory and file do not exist
	getPropertiesResp, err := dirURL.GetProperties(context.Background())
	a.NotNil(err) // TODO: I want to check the status code is 404 but not sure how since the resp is nil
	a.Nil(getPropertiesResp)
	getPropertiesResp2, err := fileURL.GetProperties(context.Background())
	a.NotNil(err) // TODO: I want to check the status code is 404 but not sure how since the resp is nil
	a.Nil(getPropertiesResp2)

	// Check that the renamed directory and file do exist
	getPropertiesResp, err = renamedDirURL.GetProperties(context.Background())
	a.Equal(http.StatusOK, getPropertiesResp.StatusCode())
	a.Nil(err)
	getPropertiesResp2, err = renamedDirURL.NewFileURL(fileName).GetProperties(context.Background())
	a.NotNil(err) // TODO: I want to check the status code is 404 but not sure how since the resp is nil
	a.Nil(getPropertiesResp2)
}

func TestSetACL(t *testing.T) {
	a := assert.New(t)
	// Create a filesystem
	fsu := getBfsServiceURL()
	fsURL, _ := createNewFileSystem(a, fsu)
	defer deleteFileSystem(a, fsURL)

	// Create a directory inside the filesystem
	dirURL := fsURL.NewDirectoryURL("test")
	_, err := dirURL.Create(ctx, true)
	a.Nil(err)

	// Grab it's default ACLs
	folderAccess, err := dirURL.GetAccessControl(ctx)
	a.Nil(err)

	// Modify it slightly
	folderAccess.ACL = "user::r-x,group::r-x,other::---"
	folderAccess.Permissions = ""
	_, err = dirURL.SetAccessControl(ctx, folderAccess)
	a.Nil(err)

	// Compare them
	folderAccessToValidate, err := dirURL.GetAccessControl(ctx)
	a.Nil(err)
	// We're checking ACLs are the same
	folderAccessToValidate.Permissions = ""
	a.Equal(folderAccess, folderAccessToValidate)

	// Create a file
	fileUrl := dirURL.NewFileURL("foo.bar")
	_, err = fileUrl.Create(ctx, azbfs.BlobFSHTTPHeaders{}, azbfs.BlobFSAccessControl{})
	a.Nil(err)

	// Grab it's default ACLs
	fileAccess, err := fileUrl.GetAccessControl(ctx)
	a.Nil(err)

	// Modify it slightly.
	fileAccess.ACL = "user::r-x,group::r-x,other::---"
	fileAccess.Permissions = ""
	_, err = fileUrl.SetAccessControl(ctx, fileAccess)
	a.Nil(err)

	// Compare them
	fileAccessToValidate, err := fileUrl.GetAccessControl(ctx)
	a.Nil(err)
	// We're checking ACLs are the same
	fileAccessToValidate.Permissions = ""
	a.Equal(fileAccess, fileAccessToValidate)

	// Don't bother testing the root ACLs, since it calls into the directoryclient
}

func TestRenameDirectoryWithSas(t *testing.T) {
	a := assert.New(t)
	name, key := getAccountAndKey()
	credential := azbfs.NewSharedKeyCredential(name, key)
	sasQueryParams, err := azbfs.AccountSASSignatureValues{
		Protocol:      azbfs.SASProtocolHTTPS,
		ExpiryTime:    time.Now().Add(48 * time.Hour),
		Permissions:   azbfs.AccountSASPermissions{Read: true, List: true, Write: true, Delete: true, Add: true, Create: true, Update: true, Process: true}.String(),
		Services:      azbfs.AccountSASServices{File: true, Blob: true, Queue: true}.String(),
		ResourceTypes: azbfs.AccountSASResourceTypes{Service: true, Container: true, Object: true}.String(),
	}.NewSASQueryParameters(credential)
	a.Nil(err)

	qp := sasQueryParams.Encode()
	rawURL := fmt.Sprintf("https://%s.dfs.core.windows.net/?%s",
		credential.AccountName(), qp)
	fullURL, err := url.Parse(rawURL)
	a.Nil(err)

	fsu := azbfs.NewServiceURL(*fullURL, azbfs.NewPipeline(azbfs.NewAnonymousCredential(), azbfs.PipelineOptions{}))

	fileSystemURL, _ := createNewFileSystem(a, fsu)
	defer deleteFileSystem(a, fileSystemURL)

	dirURL, dirName := createNewDirectoryFromFileSystem(a, fileSystemURL)
	dirRename := dirName + "rename"

	renamedDirURL, err := dirURL.Rename(context.Background(), azbfs.RenameDirectoryOptions{DestinationPath: dirRename})
	a.NotNil(renamedDirURL)
	a.Nil(err)

	// Check that the old directory does not exist
	getPropertiesResp, err := dirURL.GetProperties(context.Background())
	a.NotNil(err) // TODO: I want to check the status code is 404 but not sure how since the resp is nil
	a.Nil(getPropertiesResp)

	// Check that the renamed directory does exist
	getPropertiesResp, err = renamedDirURL.GetProperties(context.Background())
	a.Equal(http.StatusOK, getPropertiesResp.StatusCode())
	a.Nil(err)
}

func TestRenameDirectoryWithDestinationSas(t *testing.T) {
	a := assert.New(t)
	name, key := getAccountAndKey()
	credential := azbfs.NewSharedKeyCredential(name, key)
	sourceSasQueryParams, err := azbfs.AccountSASSignatureValues{
		Protocol:      azbfs.SASProtocolHTTPS,
		ExpiryTime:    time.Now().Add(48 * time.Hour),
		Permissions:   azbfs.AccountSASPermissions{Read: true, List: true, Write: true, Delete: true, Add: true, Create: true, Update: true, Process: true}.String(),
		Services:      azbfs.AccountSASServices{File: true, Blob: true, Queue: true}.String(),
		ResourceTypes: azbfs.AccountSASResourceTypes{Service: true, Container: true, Object: true}.String(),
	}.NewSASQueryParameters(credential)
	a.Nil(err)
	destinationSasQueryParams, err := azbfs.AccountSASSignatureValues{
		Protocol:      azbfs.SASProtocolHTTPS,
		ExpiryTime:    time.Now().Add(24 * time.Hour),
		Permissions:   azbfs.AccountSASPermissions{Read: true, Write: true, Delete: true, Add: true, Create: true, Update: true, Process: true}.String(),
		Services:      azbfs.AccountSASServices{File: true, Blob: true}.String(),
		ResourceTypes: azbfs.AccountSASResourceTypes{Service: true, Container: true, Object: true}.String(),
	}.NewSASQueryParameters(credential)
	a.Nil(err)

	sourceQp := sourceSasQueryParams.Encode()
	destQp := destinationSasQueryParams.Encode()
	rawURL := fmt.Sprintf("https://%s.dfs.core.windows.net/?%s",
		credential.AccountName(), sourceQp)
	fullURL, err := url.Parse(rawURL)
	a.Nil(err)

	fsu := azbfs.NewServiceURL(*fullURL, azbfs.NewPipeline(azbfs.NewAnonymousCredential(), azbfs.PipelineOptions{}))

	fileSystemURL, _ := createNewFileSystem(a, fsu)
	defer deleteFileSystem(a, fileSystemURL)

	dirURL, dirName := createNewDirectoryFromFileSystem(a, fileSystemURL)
	dirRename := dirName + "rename"

	renamedDirURL, err := dirURL.Rename(
		context.Background(), azbfs.RenameDirectoryOptions{DestinationPath: dirRename, DestinationSas: &destQp})
	a.NotNil(renamedDirURL)
	a.Nil(err)
	found := strings.Contains(renamedDirURL.String(), destQp)
	// make sure the correct SAS is used
	a.True(found)

	// Check that the old directory does not exist
	getPropertiesResp, err := dirURL.GetProperties(context.Background())
	a.NotNil(err) // TODO: I want to check the status code is 404 but not sure how since the resp is nil
	a.Nil(getPropertiesResp)

	// Check that the renamed directory does exist
	getPropertiesResp, err = renamedDirURL.GetProperties(context.Background())
	a.Equal(http.StatusOK, getPropertiesResp.StatusCode())
	a.Nil(err)
}
