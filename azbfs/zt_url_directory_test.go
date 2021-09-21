package azbfs_test

import (
	"context"
	"net/http"

	"github.com/nitin-deamon/azure-storage-azcopy/v10/azbfs"
	chk "gopkg.in/check.v1"
)

type DirectoryUrlSuite struct{}

var _ = chk.Suite(&DirectoryUrlSuite{})

// deleteDirectory deletes the directory represented by directory Url
func deleteDirectory(c *chk.C, dul azbfs.DirectoryURL) {
	resp, err := dul.Delete(context.Background(), nil, true)
	c.Assert(err, chk.IsNil)
	c.Assert(resp.Response().StatusCode, chk.Equals, http.StatusOK)
}

// TestCreateDirectory test the creation of a directory
func (dus *DirectoryUrlSuite) TestCreateDeleteDirectory(c *chk.C) {
	// Create a file system
	fsu := getBfsServiceURL()
	fsURL, _ := createNewFileSystem(c, fsu)
	defer delFileSystem(c, fsURL)

	// Create a directory url from the fileSystem Url
	dirUrl, _ := getDirectoryURLFromFileSystem(c, fsURL)
	cResp, err := dirUrl.Create(context.Background(), true)
	defer deleteDirectory(c, dirUrl)

	// Assert the directory create response header attributes
	c.Assert(err, chk.IsNil)
	c.Assert(cResp.StatusCode(), chk.Equals, http.StatusCreated)
	c.Assert(cResp.ETag(), chk.Not(chk.Equals), "")
	c.Assert(cResp.LastModified(), chk.Not(chk.Equals), "")
	c.Assert(cResp.XMsRequestID(), chk.Not(chk.Equals), "")
	c.Assert(cResp.XMsVersion(), chk.Not(chk.Equals), "")
	c.Assert(cResp.Date(), chk.Not(chk.Equals), "")
}

// TestCreateSubDir tests creating the sub-directory inside a directory
func (dus *DirectoryUrlSuite) TestCreateSubDir(c *chk.C) {
	// Create the file system
	fsu := getBfsServiceURL()
	fsURL, _ := createNewFileSystem(c, fsu)
	defer delFileSystem(c, fsURL)

	// Create the directory Url from fileSystem Url and create directory
	dirUrl, _ := getDirectoryURLFromFileSystem(c, fsURL)
	cResp, err := dirUrl.Create(context.Background(), true)
	defer deleteDirectory(c, dirUrl)

	c.Assert(err, chk.IsNil)
	c.Assert(cResp.StatusCode(), chk.Equals, http.StatusCreated)
	c.Assert(cResp.ETag(), chk.Not(chk.Equals), "")
	c.Assert(cResp.LastModified(), chk.Not(chk.Equals), "")
	c.Assert(cResp.XMsRequestID(), chk.Not(chk.Equals), "")
	c.Assert(cResp.XMsVersion(), chk.Not(chk.Equals), "")
	c.Assert(cResp.Date(), chk.Not(chk.Equals), "")

	// Create the sub-directory url from directory Url and create sub-directory
	subDirUrl, _ := getDirectoryURLFromDirectory(c, dirUrl)
	cResp, err = subDirUrl.Create(context.Background(), true)
	defer deleteDirectory(c, subDirUrl)

	c.Assert(err, chk.IsNil)
	c.Assert(cResp.StatusCode(), chk.Equals, http.StatusCreated)
	c.Assert(cResp.ETag(), chk.Not(chk.Equals), "")
	c.Assert(cResp.LastModified(), chk.Not(chk.Equals), "")
	c.Assert(cResp.XMsRequestID(), chk.Not(chk.Equals), "")
	c.Assert(cResp.XMsVersion(), chk.Not(chk.Equals), "")
	c.Assert(cResp.Date(), chk.Not(chk.Equals), "")

}

// TestDirectoryCreateAndGetProperties tests the create directory and
// get directory properties
func (dus *DirectoryUrlSuite) TestDirectoryCreateAndGetProperties(c *chk.C) {
	// Create file system
	fsu := getBfsServiceURL()
	fsURL, _ := createNewFileSystem(c, fsu)
	defer delFileSystem(c, fsURL)

	// Create directory url from fileSystemUrl and create directory
	dirUrl, _ := getDirectoryURLFromFileSystem(c, fsURL)
	cResp, err := dirUrl.Create(context.Background(), true)
	defer deleteDirectory(c, dirUrl)

	c.Assert(err, chk.IsNil)
	c.Assert(cResp.StatusCode(), chk.Equals, http.StatusCreated)
	c.Assert(cResp.ETag(), chk.Not(chk.Equals), "")
	c.Assert(cResp.LastModified(), chk.Not(chk.Equals), "")
	c.Assert(cResp.XMsRequestID(), chk.Not(chk.Equals), "")
	c.Assert(cResp.XMsVersion(), chk.Not(chk.Equals), "")
	c.Assert(cResp.Date(), chk.Not(chk.Equals), "")

	// Get the directory properties and verify the resource type
	gResp, err := dirUrl.GetProperties(context.Background())
	c.Assert(err, chk.IsNil)
	c.Assert(gResp.StatusCode(), chk.Equals, http.StatusOK)
	c.Assert(gResp.XMsResourceType(), chk.Equals, "directory")
}

// TestCreateDirectoryAndFiles tests the create directory and create file inside the directory
func (dus *DirectoryUrlSuite) TestCreateDirectoryAndFiles(c *chk.C) {
	// Create the file system
	fsu := getBfsServiceURL()
	fsURL, _ := createNewFileSystem(c, fsu)
	defer delFileSystem(c, fsURL)

	// Create the directoryUrl from fileSystemUrl
	// and create directory
	dirUrl, _ := getDirectoryURLFromFileSystem(c, fsURL)
	cResp, err := dirUrl.Create(context.Background(), true)
	defer deleteDirectory(c, dirUrl)

	c.Assert(err, chk.IsNil)
	c.Assert(cResp.StatusCode(), chk.Equals, http.StatusCreated)
	c.Assert(cResp.ETag(), chk.Not(chk.Equals), "")
	c.Assert(cResp.LastModified(), chk.Not(chk.Equals), "")
	c.Assert(cResp.XMsRequestID(), chk.Not(chk.Equals), "")
	c.Assert(cResp.XMsVersion(), chk.Not(chk.Equals), "")
	c.Assert(cResp.Date(), chk.Not(chk.Equals), "")

	// Create fileUrl from directoryUrl and create file inside the directory
	fileUrl, _ := getFileURLFromDirectory(c, dirUrl)
	fresp, err := fileUrl.Create(context.Background(), azbfs.BlobFSHTTPHeaders{})
	defer delFile(c, fileUrl)

	c.Assert(err, chk.IsNil)
	c.Assert(fresp.Response().StatusCode, chk.Equals, http.StatusCreated)
	c.Assert(fresp.ETag(), chk.Not(chk.Equals), "")
	c.Assert(fresp.LastModified(), chk.Not(chk.Equals), "")
	c.Assert(fresp.XMsRequestID(), chk.Not(chk.Equals), "")
	c.Assert(fresp.XMsVersion(), chk.Not(chk.Equals), "")
	c.Assert(fresp.Date(), chk.Not(chk.Equals), "")

}

// TestReCreateDirectory tests the creation of directories that already exist
func (dus *DirectoryUrlSuite) TestReCreateDirectory(c *chk.C) {
	// Create the file system
	fsu := getBfsServiceURL()
	fsURL, _ := createNewFileSystem(c, fsu)
	defer delFileSystem(c, fsURL)

	// Create the directoryUrl from fileSystemUrl and create directory
	dirUrl, _ := getDirectoryURLFromFileSystem(c, fsURL)
	cResp, err := dirUrl.Create(context.Background(), true)
	defer deleteDirectory(c, dirUrl)
	c.Assert(err, chk.IsNil)
	c.Assert(cResp.StatusCode(), chk.Equals, http.StatusCreated)

	// Re-create it (allowing overwrite)
	// TODO: put some files in it before this, and make assertions about what happens to them after the re-creation
	cResp, err = dirUrl.Create(context.Background(), true)
	c.Assert(err, chk.IsNil)
	c.Assert(cResp.StatusCode(), chk.Equals, http.StatusCreated)

	// Attempt to re-create it (but do NOT allow overwrite)
	cResp, err = dirUrl.Create(context.Background(), false) // <- false for re-create
	c.Assert(err, chk.NotNil)
	stgErr, ok := err.(azbfs.StorageError)
	c.Assert(ok, chk.Equals, true)
	c.Assert(stgErr.Response().StatusCode, chk.Equals, http.StatusConflict)
	c.Assert(stgErr.ServiceCode(), chk.Equals, azbfs.ServiceCodePathAlreadyExists)
}

// TestCreateMetadataDeleteDirectory test the creation of a directory with metadata
func (dus *DirectoryUrlSuite) TestCreateMetadataDeleteDirectory(c *chk.C) {
	// Create a file system
	fsu := getBfsServiceURL()
	fsURL, _ := createNewFileSystem(c, fsu)
	defer delFileSystem(c, fsURL)

	// Create metadata
	metadata := make(map[string]string)
	metadata["foo"] = "bar"

	// Create a directory url from the fileSystem Url
	dirUrl, _ := getDirectoryURLFromFileSystem(c, fsURL)
	cResp, err := dirUrl.CreateWithOptions(context.Background(),
		azbfs.CreateDirectoryOptions{RecreateIfExists: true, Metadata: metadata})
	defer deleteDirectory(c, dirUrl)

	// Assert the directory create response header attributes
	c.Assert(err, chk.IsNil)
	c.Assert(cResp.StatusCode(), chk.Equals, http.StatusCreated)
	c.Assert(cResp.ETag(), chk.Not(chk.Equals), "")
	c.Assert(cResp.LastModified(), chk.Not(chk.Equals), "")
	c.Assert(cResp.XMsRequestID(), chk.Not(chk.Equals), "")
	c.Assert(cResp.XMsVersion(), chk.Not(chk.Equals), "")
	c.Assert(cResp.Date(), chk.Not(chk.Equals), "")

	getResp, err := dirUrl.GetProperties(context.Background())
	c.Assert(err, chk.IsNil)
	c.Assert(getResp.Response().StatusCode, chk.Equals, http.StatusOK)
	c.Assert(getResp.XMsProperties(), chk.Not(chk.Equals), "") // Check metadata returned is not null.
}

// TestDirectoryStructure tests creating dir, sub-dir inside dir and files
// inside dirs and sub-dirs. Then verify the count of files / sub-dirs inside directory
func (dus *DirectoryUrlSuite) TestDirectoryStructure(c *chk.C) {
	// Create file system
	fsu := getBfsServiceURL()
	fsURL, _ := createNewFileSystem(c, fsu)
	defer delFileSystem(c, fsURL)

	// Create a directory inside filesystem
	dirUrl, _ := getDirectoryURLFromFileSystem(c, fsURL)
	cResp, err := dirUrl.Create(context.Background(), true)
	defer deleteDirectory(c, dirUrl)

	c.Assert(err, chk.IsNil)
	c.Assert(cResp.StatusCode(), chk.Equals, http.StatusCreated)
	c.Assert(cResp.ETag(), chk.Not(chk.Equals), "")
	c.Assert(cResp.LastModified(), chk.Not(chk.Equals), "")
	c.Assert(cResp.XMsRequestID(), chk.Not(chk.Equals), "")
	c.Assert(cResp.XMsVersion(), chk.Not(chk.Equals), "")
	c.Assert(cResp.Date(), chk.Not(chk.Equals), "")

	// Create a sub-dir inside the above create directory
	subDirUrl, _ := getDirectoryURLFromDirectory(c, dirUrl)
	cResp, err = subDirUrl.Create(context.Background(), true)
	defer deleteDirectory(c, subDirUrl)

	c.Assert(err, chk.IsNil)
	c.Assert(cResp.StatusCode(), chk.Equals, http.StatusCreated)
	c.Assert(cResp.ETag(), chk.Not(chk.Equals), "")
	c.Assert(cResp.LastModified(), chk.Not(chk.Equals), "")
	c.Assert(cResp.XMsRequestID(), chk.Not(chk.Equals), "")
	c.Assert(cResp.XMsVersion(), chk.Not(chk.Equals), "")
	c.Assert(cResp.Date(), chk.Not(chk.Equals), "")

	// Create a file inside directory
	fileUrl, _ := getFileURLFromDirectory(c, dirUrl)
	fresp, err := fileUrl.Create(context.Background(), azbfs.BlobFSHTTPHeaders{})
	defer delFile(c, fileUrl)

	c.Assert(err, chk.IsNil)
	c.Assert(fresp.Response().StatusCode, chk.Equals, http.StatusCreated)
	c.Assert(fresp.ETag(), chk.Not(chk.Equals), "")
	c.Assert(fresp.LastModified(), chk.Not(chk.Equals), "")
	c.Assert(fresp.XMsRequestID(), chk.Not(chk.Equals), "")
	c.Assert(fresp.XMsVersion(), chk.Not(chk.Equals), "")
	c.Assert(fresp.Date(), chk.Not(chk.Equals), "")

	// create a file inside the sub-dir created above
	subDirfileUrl, _ := getFileURLFromDirectory(c, subDirUrl)
	fresp, err = subDirfileUrl.Create(context.Background(), azbfs.BlobFSHTTPHeaders{})
	defer delFile(c, subDirfileUrl)

	c.Assert(err, chk.IsNil)
	c.Assert(fresp.Response().StatusCode, chk.Equals, http.StatusCreated)
	c.Assert(fresp.ETag(), chk.Not(chk.Equals), "")
	c.Assert(fresp.LastModified(), chk.Not(chk.Equals), "")
	c.Assert(fresp.XMsRequestID(), chk.Not(chk.Equals), "")
	c.Assert(fresp.XMsVersion(), chk.Not(chk.Equals), "")
	c.Assert(fresp.Date(), chk.Not(chk.Equals), "")

	// list the directory create above.
	// expected number of file inside the dir is 2 i.e one
	// inside the dir itself and one inside the sub-dir
	// expected number of sub-dir inside the dir is 1
	continuationMarker := ""
	lresp, err := dirUrl.ListDirectorySegment(context.Background(), &continuationMarker, true)

	c.Assert(err, chk.IsNil)
	c.Assert(lresp.Response().StatusCode, chk.Equals, http.StatusOK)
	c.Assert(len(lresp.Files()), chk.Equals, 2)
	c.Assert(len(lresp.Directories()), chk.Equals, 1)
	c.Assert(lresp.ETag(), chk.Equals, "")
	c.Assert(lresp.LastModified(), chk.Equals, "")
	c.Assert(lresp.XMsRequestID(), chk.Not(chk.Equals), "")
	c.Assert(lresp.XMsVersion(), chk.Not(chk.Equals), "")
	c.Assert(lresp.Date(), chk.Not(chk.Equals), "")
}

func (dus *DirectoryUrlSuite) TestListDirectoryWithSpaces(c *chk.C) {
	// Create file system
	fsu := getBfsServiceURL()
	fsURL, _ := createNewFileSystem(c, fsu)
	defer delFileSystem(c, fsURL)

	// Create a directory inside filesystem
	dirUrl := fsURL.NewDirectoryURL("New Folder Test 2")
	_, err := dirUrl.Create(context.Background(), true)
	defer deleteDirectory(c, dirUrl)

	// Create a file inside directory
	fileUrl, _ := getFileURLFromDirectory(c, dirUrl)
	_, err = fileUrl.Create(context.Background(), azbfs.BlobFSHTTPHeaders{})
	defer delFile(c, fileUrl)

	// list the directory created above.
	// expected number of files inside the dir is 1
	continuationMarker := ""
	lresp, err := dirUrl.ListDirectorySegment(context.Background(), &continuationMarker, true)
	c.Assert(err, chk.IsNil)
	c.Assert(lresp.Response().StatusCode, chk.Equals, http.StatusOK)
	c.Assert(len(lresp.Files()), chk.Equals, 1)
	c.Assert(len(lresp.Directories()), chk.Equals, 0)
	c.Assert(lresp.ETag(), chk.Equals, "")
	c.Assert(lresp.LastModified(), chk.Equals, "")
	c.Assert(lresp.XMsRequestID(), chk.Not(chk.Equals), "")
	c.Assert(lresp.XMsVersion(), chk.Not(chk.Equals), "")
	c.Assert(lresp.Date(), chk.Not(chk.Equals), "")
}

func (s *FileURLSuite) TestRenameDirectory(c *chk.C) {
	fsu := getBfsServiceURL()
	fileSystemURL, _ := createNewFileSystem(c, fsu)
	defer delFileSystem(c, fileSystemURL)

	dirURL, dirName := createNewDirectoryFromFileSystem(c, fileSystemURL)
	dirRename := dirName + "rename"

	renamedDirURL, err := dirURL.Rename(context.Background(), azbfs.RenameDirectoryOptions{DestinationPath: dirRename})
	c.Assert(renamedDirURL, chk.NotNil)
	c.Assert(err, chk.IsNil)

	// Check that the old directory does not exist
	getPropertiesResp, err := dirURL.GetProperties(context.Background())
	c.Assert(err, chk.NotNil) // TODO: I want to check the status code is 404 but not sure how since the resp is nil
	c.Assert(getPropertiesResp, chk.IsNil)

	// Check that the renamed directory does exist
	getPropertiesResp, err = renamedDirURL.GetProperties(context.Background())
	c.Assert(getPropertiesResp.StatusCode(), chk.Equals, http.StatusOK)
	c.Assert(err, chk.IsNil)
}

func (s *FileURLSuite) TestRenameDirWithFile(c *chk.C) {
	fsu := getBfsServiceURL()
	fileSystemURL, _ := createNewFileSystem(c, fsu)
	defer delFileSystem(c, fileSystemURL)

	dirURL, dirName := createNewDirectoryFromFileSystem(c, fileSystemURL)
	fileName := "test.txt"
	fileURL := dirURL.NewFileURL(fileName)
	dirRename := dirName + "rename"

	renamedDirURL, err := dirURL.Rename(context.Background(), azbfs.RenameDirectoryOptions{DestinationPath: dirRename})
	c.Assert(renamedDirURL, chk.NotNil)
	c.Assert(err, chk.IsNil)

	// Check that the old directory and file do not exist
	getPropertiesResp, err := dirURL.GetProperties(context.Background())
	c.Assert(err, chk.NotNil) // TODO: I want to check the status code is 404 but not sure how since the resp is nil
	c.Assert(getPropertiesResp, chk.IsNil)
	getPropertiesResp2, err := fileURL.GetProperties(context.Background())
	c.Assert(err, chk.NotNil) // TODO: I want to check the status code is 404 but not sure how since the resp is nil
	c.Assert(getPropertiesResp2, chk.IsNil)

	// Check that the renamed directory and file do exist
	getPropertiesResp, err = renamedDirURL.GetProperties(context.Background())
	c.Assert(getPropertiesResp.StatusCode(), chk.Equals, http.StatusOK)
	c.Assert(err, chk.IsNil)
	getPropertiesResp2, err = renamedDirURL.NewFileURL(fileName).GetProperties(context.Background())
	c.Assert(err, chk.NotNil) // TODO: I want to check the status code is 404 but not sure how since the resp is nil
	c.Assert(getPropertiesResp2, chk.IsNil)
}

func (dus *DirectoryUrlSuite) TestSetACL(c *chk.C) {
	// Create a filesystem
	fsu := getBfsServiceURL()
	fsURL, _ := createNewFileSystem(c, fsu)
	defer delFileSystem(c, fsURL)

	// Create a directory inside the filesystem
	dirURL := fsURL.NewDirectoryURL("test")
	_, err := dirURL.Create(ctx, true)
	c.Assert(err, chk.IsNil)

	// Grab it's default ACLs
	folderAccess, err := dirURL.GetAccessControl(ctx)
	c.Assert(err, chk.IsNil)

	// Modify it slightly
	folderAccess.ACL = "user::r-x,group::r-x,other::---"
	folderAccess.Permissions = ""
	_, err = dirURL.SetAccessControl(ctx, folderAccess)
	c.Assert(err, chk.IsNil)

	// Compare them
	folderAccessToValidate, err := dirURL.GetAccessControl(ctx)
	c.Assert(err, chk.IsNil)
	// We're checking ACLs are the same
	folderAccessToValidate.Permissions = ""
	c.Assert(folderAccessToValidate, chk.Equals, folderAccess)

	// Create a file
	fileUrl := dirURL.NewFileURL("foo.bar")
	_, err = fileUrl.Create(ctx, azbfs.BlobFSHTTPHeaders{})
	c.Assert(err, chk.IsNil)

	// Grab it's default ACLs
	fileAccess, err := fileUrl.GetAccessControl(ctx)
	c.Assert(err, chk.IsNil)

	// Modify it slightly.
	fileAccess.ACL = "user::r-x,group::r-x,other::---"
	fileAccess.Permissions = ""
	_, err = fileUrl.SetAccessControl(ctx, fileAccess)
	c.Assert(err, chk.IsNil)

	// Compare them
	fileAccessToValidate, err := fileUrl.GetAccessControl(ctx)
	c.Assert(err, chk.IsNil)
	// We're checking ACLs are the same
	fileAccessToValidate.Permissions = ""
	c.Assert(fileAccessToValidate, chk.Equals, fileAccess)

	// Don't bother testing the root ACLs, since it calls into the directoryclient
}
