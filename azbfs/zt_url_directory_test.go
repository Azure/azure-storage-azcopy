package azbfs_test

import (
	chk "gopkg.in/check.v1" // go get gopkg.in/check.v1
	"context"
	"github.com/Azure/azure-storage-file-go/2017-07-29/azfile"
	"github.com/Azure/azure-storage-azcopy/azbfs"
	"fmt"
)
type DirectoryUrlSuite struct {}

var _ = chk.Suite(&DirectoryUrlSuite{})

// deleteDirectory deletes the directory represented by directory Url
func deleteDirectory(c *chk.C, dul azbfs.DirectoryURL) {
	resp, err := dul.Delete(context.Background(), nil)
	c.Assert(err, chk.IsNil)
	// Check for Status code 200 which means 'Ok'
	c.Assert(resp.Response().StatusCode, chk.Equals, 200)
}

// TestCreateDirectory test the creation of a directory
func (dus *DirectoryUrlSuite) TestCreateDirectory(c *chk.C) {
	// Create a file system
	fsu := getBfsServiceURL()
	fsURL, _ := createNewFileSystem(c, fsu)
	defer delFileSystem(c, fsURL)

	// create a directory url from the fileSystem Url
	dirUrl := fsURL.NewDirectoryURL(generateDirectoryName())
	cResp, err := dirUrl.Create(context.Background())
	defer deleteDirectory(c, dirUrl)

	// Assert the directory create response header attributes
	c.Assert(err, chk.IsNil)
	c.Assert(cResp.StatusCode(), chk.Equals, 201)
	c.Assert(cResp.ETag(), chk.Not(chk.Equals), azfile.ETagNone)
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

	// Create the directory Url from fileSystem Url
	// and create directory
	dirUrl := fsURL.NewDirectoryURL(generateDirectoryName())
	cResp, err := dirUrl.Create(context.Background())
	defer deleteDirectory(c, dirUrl)

	c.Assert(err, chk.IsNil)
	c.Assert(cResp.StatusCode(), chk.Equals, 201)
	c.Assert(cResp.ETag(), chk.Not(chk.Equals), azfile.ETagNone)
	c.Assert(cResp.LastModified(), chk.Not(chk.Equals), "")
	c.Assert(cResp.XMsRequestID(), chk.Not(chk.Equals), "")
	c.Assert(cResp.XMsVersion(), chk.Not(chk.Equals), "")
	c.Assert(cResp.Date(), chk.Not(chk.Equals), "")

	// create the sub-directory url from directory Url
	// and create sub-directory
	subDirUrl := dirUrl.NewSubDirectoryUrl(generateDirectoryName())
	cResp, err = subDirUrl.Create(context.Background())
	defer deleteDirectory(c, subDirUrl)

	c.Assert(err, chk.IsNil)
	c.Assert(cResp.StatusCode(), chk.Equals, 201)
	c.Assert(cResp.ETag(), chk.Not(chk.Equals), azfile.ETagNone)
	c.Assert(cResp.LastModified(), chk.Not(chk.Equals), "")
	c.Assert(cResp.XMsRequestID(), chk.Not(chk.Equals), "")
	c.Assert(cResp.XMsVersion(), chk.Not(chk.Equals), "")
	c.Assert(cResp.Date(), chk.Not(chk.Equals), "")

}

// TestDirectoryCreateAndGetProperties tests the create directory and
// get directory properties
func (dus *DirectoryUrlSuite) TestDirectoryCreateAndGetProperties(c *chk.C) {
	// create file system
	fsu := getBfsServiceURL()
	fsURL, _ := createNewFileSystem(c, fsu)
	defer delFileSystem(c, fsURL)

	// create directory url from fileSystemUrl and create directory
	dirUrl := fsURL.NewDirectoryURL(generateDirectoryName())
	cResp, err := dirUrl.Create(context.Background())
	defer deleteDirectory(c, dirUrl)

	c.Assert(err, chk.IsNil)
	c.Assert(cResp.StatusCode(), chk.Equals, 201)
	c.Assert(cResp.ETag(), chk.Not(chk.Equals), azfile.ETagNone)
	c.Assert(cResp.LastModified(), chk.Not(chk.Equals), "")
	c.Assert(cResp.XMsRequestID(), chk.Not(chk.Equals), "")
	c.Assert(cResp.XMsVersion(), chk.Not(chk.Equals), "")
	c.Assert(cResp.Date(), chk.Not(chk.Equals), "")

	// Get the directory properties and verify the resource type
	gResp, err := dirUrl.GetProperties(context.Background())
	c.Assert(err, chk.IsNil)
	c.Assert(gResp.StatusCode(), chk.Equals, 200)
	c.Assert(gResp.XMsResourceType(), chk.Equals, azbfs.DirectoryResourceType)
}

// TestCreateDirectoryAndFiles tests the create directory and create file inside the directory
func (dus *DirectoryUrlSuite) TestCreateDirectoryAndFiles(c *chk.C) {
	// create the file system
	fsu := getBfsServiceURL()
	fsURL, _ := createNewFileSystem(c, fsu)
	defer delFileSystem(c, fsURL)

	// create the directoryUrl from fileSystemUrl
	// and create directory
	dirUrl := fsURL.NewDirectoryURL(generateDirectoryName())
	cResp, err := dirUrl.Create(context.Background())
	defer deleteDirectory(c, dirUrl)

	c.Assert(err, chk.IsNil)
	c.Assert(cResp.StatusCode(), chk.Equals, 201)
	c.Assert(cResp.ETag(), chk.Not(chk.Equals), azfile.ETagNone)
	c.Assert(cResp.LastModified(), chk.Not(chk.Equals), "")
	c.Assert(cResp.XMsRequestID(), chk.Not(chk.Equals), "")
	c.Assert(cResp.XMsVersion(), chk.Not(chk.Equals), "")
	c.Assert(cResp.Date(), chk.Not(chk.Equals), "")

	// create fileUrl from directoryUrl and create file inside the directory
	fileUrl := dirUrl.NewFileURL(generateFileName())
	fresp, err := fileUrl.Create(context.Background())
	defer delFile(c, fileUrl)

	c.Assert(err, chk.IsNil)
	c.Assert(fresp.Response().StatusCode, chk.Equals, 201)
	c.Assert(fresp.ETag(), chk.Not(chk.Equals), azfile.ETagNone)
	c.Assert(fresp.LastModified(), chk.Not(chk.Equals), "")
	c.Assert(fresp.XMsRequestID(), chk.Not(chk.Equals), "")
	c.Assert(fresp.XMsVersion(), chk.Not(chk.Equals), "")
	c.Assert(fresp.Date(), chk.Not(chk.Equals), "")

}

// TestDirectoryStructure tests creating dir, sub-dir inside dir and files
// inside dirs and sub-dirs. Then verify the count of files / sub-dirs inside directory
func (dus *DirectoryUrlSuite) TestDirectoryStructure(c *chk.C) {
	// Create file system
	fsu := getBfsServiceURL()
	fsURL, _ := createNewFileSystem(c, fsu)
	defer delFileSystem(c, fsURL)

	// Create a directory inside filesystem
	dirUrl := fsURL.NewDirectoryURL(generateDirectoryName())
	cResp, err := dirUrl.Create(context.Background())
	defer deleteDirectory(c, dirUrl)

	c.Assert(err, chk.IsNil)
	c.Assert(cResp.StatusCode(), chk.Equals, 201)
	c.Assert(cResp.ETag(), chk.Not(chk.Equals), azfile.ETagNone)
	c.Assert(cResp.LastModified(), chk.Not(chk.Equals), "")
	c.Assert(cResp.XMsRequestID(), chk.Not(chk.Equals), "")
	c.Assert(cResp.XMsVersion(), chk.Not(chk.Equals), "")
	c.Assert(cResp.Date(), chk.Not(chk.Equals), "")

	// create a sub-dir inside the above create directory
	subDirUrl := dirUrl.NewSubDirectoryUrl(generateDirectoryName())
	cResp, err = subDirUrl.Create(context.Background())
	defer deleteDirectory(c, subDirUrl)

	c.Assert(err, chk.IsNil)
	c.Assert(cResp.StatusCode(), chk.Equals, 201)
	c.Assert(cResp.ETag(), chk.Not(chk.Equals), azfile.ETagNone)
	c.Assert(cResp.LastModified(), chk.Not(chk.Equals), "")
	c.Assert(cResp.XMsRequestID(), chk.Not(chk.Equals), "")
	c.Assert(cResp.XMsVersion(), chk.Not(chk.Equals), "")
	c.Assert(cResp.Date(), chk.Not(chk.Equals), "")

	// create a file inside directory
	fileUrl := dirUrl.NewFileURL(generateFileName())
	fresp, err := fileUrl.Create(context.Background())
	defer delFile(c, fileUrl)

	c.Assert(err, chk.IsNil)
	c.Assert(fresp.Response().StatusCode, chk.Equals, 201)
	c.Assert(fresp.ETag(), chk.Not(chk.Equals), azfile.ETagNone)
	c.Assert(fresp.LastModified(), chk.Not(chk.Equals), "")
	c.Assert(fresp.XMsRequestID(), chk.Not(chk.Equals), "")
	c.Assert(fresp.XMsVersion(), chk.Not(chk.Equals), "")
	c.Assert(fresp.Date(), chk.Not(chk.Equals), "")

	// create a file inside the sub-dir created above
	subDirfileUrl := subDirUrl.NewFileURL(generateFileName())
	fresp, err = subDirfileUrl.Create(context.Background())
	defer delFile(c, subDirfileUrl)

	c.Assert(err, chk.IsNil)
	c.Assert(fresp.Response().StatusCode, chk.Equals, 201)
	c.Assert(fresp.ETag(), chk.Not(chk.Equals), azfile.ETagNone)
	c.Assert(fresp.LastModified(), chk.Not(chk.Equals), "")
	c.Assert(fresp.XMsRequestID(), chk.Not(chk.Equals), "")
	c.Assert(fresp.XMsVersion(), chk.Not(chk.Equals), "")
	c.Assert(fresp.Date(), chk.Not(chk.Equals), "")

	// list the directory create above.
	// expected number of file inside the dir is 2 i.e one
	// inside the dir itself and one inside the sub-dir
	// expected number of sub-dir inside the dir is 1
	lresp, err := dirUrl.ListDirectory(context.Background(), nil, true)
	if st, ok := err.(azbfs.StorageError) ; ok {
		fmt.Println("Error ", st.Response().StatusCode)
		fmt.Println("error ", err.Error())
	}
	c.Assert(err, chk.IsNil)
	c.Assert(lresp.Response().StatusCode, chk.Equals, 200)
	c.Assert(len(lresp.Files()), chk.Equals, 2)
	c.Assert(len(lresp.Directories()), chk.Equals, 1)
	c.Assert(lresp.ETag(), chk.Not(chk.Equals), azfile.ETagNone)
	c.Assert(lresp.LastModified(), chk.Equals, "")
	c.Assert(lresp.XMsRequestID(), chk.Not(chk.Equals), "")
	c.Assert(lresp.XMsVersion(), chk.Not(chk.Equals), "")
	c.Assert(lresp.Date(), chk.Not(chk.Equals), "")

}
