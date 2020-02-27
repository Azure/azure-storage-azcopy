package azbfs_test

import (
	"context"
	"github.com/Azure/azure-storage-azcopy/azbfs"
	chk "gopkg.in/check.v1"
	"net/http"
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
