package azbfs_test

import (
	"context"
	"os"

	"github.com/nitin-deamon/azure-storage-azcopy/v10/azbfs"
	chk "gopkg.in/check.v1"
	"net/http"
	"net/url"
)

type FileSystemURLSuite struct{}

var _ = chk.Suite(&FileSystemURLSuite{})

func delFileSystem(c *chk.C, fs azbfs.FileSystemURL) {
	resp, err := fs.Delete(context.Background())
	c.Assert(err, chk.IsNil)
	c.Assert(resp.Response().StatusCode, chk.Equals, http.StatusAccepted)
}

func (s *FileSystemURLSuite) TestFileSystemCreateRootDirectoryURL(c *chk.C) {
	fsu := getBfsServiceURL()
	testURL := fsu.NewFileSystemURL(fileSystemPrefix).NewRootDirectoryURL()

	correctURL := "https://" + os.Getenv("ACCOUNT_NAME") + ".dfs.core.windows.net/" + fileSystemPrefix
	temp := testURL.URL()
	c.Assert(temp.String(), chk.Equals, correctURL)
}

func (s *FileSystemURLSuite) TestFileSystemCreateDirectoryURL(c *chk.C) {
	fsu := getBfsServiceURL()
	testURL := fsu.NewFileSystemURL(fileSystemPrefix).NewDirectoryURL(directoryPrefix)

	correctURL := "https://" + os.Getenv("ACCOUNT_NAME") + ".dfs.core.windows.net/" + fileSystemPrefix + "/" + directoryPrefix
	temp := testURL.URL()
	c.Assert(temp.String(), chk.Equals, correctURL)
	c.Assert(testURL.String(), chk.Equals, correctURL)
}

func (s *FileSystemURLSuite) TestFileSystemNewFileSystemURLNegative(c *chk.C) {
	c.Assert(func() { azbfs.NewFileSystemURL(url.URL{}, nil) }, chk.Panics, "p can't be nil")
}

func (s *FileSystemURLSuite) TestFileSystemCreateDelete(c *chk.C) {
	fsu := getBfsServiceURL()
	fileSystemURL, _ := getFileSystemURL(c, fsu)

	_, err := fileSystemURL.Create(ctx)
	defer delFileSystem(c, fileSystemURL)
	c.Assert(err, chk.IsNil)

	// Test get properties
	resp, err := fileSystemURL.GetProperties(ctx)
	c.Assert(resp.StatusCode(), chk.Equals, http.StatusOK)
	c.Assert(err, chk.IsNil)
}

func (s *FileSystemURLSuite) TestFileSystemList(c *chk.C) {
	fsu := getBfsServiceURL()
	fileSystemURL, _ := getFileSystemURL(c, fsu)

	_, err := fileSystemURL.Create(ctx)
	defer delFileSystem(c, fileSystemURL)
	c.Assert(err, chk.IsNil)

	// List Setup
	dirUrl, dirName := getDirectoryURLFromFileSystem(c, fileSystemURL)
	dirUrl.Create(context.Background(), true)

	fileUrl, fileName := getFileURLFromFileSystem(c, fileSystemURL)
	fileUrl.Create(context.Background(), azbfs.BlobFSHTTPHeaders{})

	// List
	paths, err := fileSystemURL.ListPaths(context.Background(), azbfs.ListPathsFilesystemOptions{Recursive: false})
	c.Assert(err, chk.IsNil)
	c.Assert(paths.Paths, chk.NotNil)
	c.Assert(len(paths.Paths), chk.Equals, 2)
	dirPath := paths.Paths[0]
	c.Assert(*dirPath.Name, chk.Equals, dirName)
	c.Assert(*dirPath.IsDirectory, chk.Equals, true)
	filePath := paths.Paths[1]
	c.Assert(*filePath.Name, chk.Equals, fileName)
	c.Assert(filePath.IsDirectory, chk.IsNil)
}