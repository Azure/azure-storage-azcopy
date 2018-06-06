package azbfs_test

import (
	"context"
	"os"

	chk "gopkg.in/check.v1"
	"github.com/Azure/azure-storage-azcopy/azbfs"
	"net/url"
)

type FileSystemURLSuite struct{}

var _ = chk.Suite(&FileSystemURLSuite{})

type testPipeline struct{}

func delFileSystem(c *chk.C, fs azbfs.FileSystemURL) {
	resp, err := fs.Delete(context.Background())
	c.Assert(err, chk.IsNil)
	c.Assert(resp.Response().StatusCode, chk.Equals, 202)
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

func (s *FileSystemURLSuite) TestFileSystemCreate(c *chk.C) {
	fsu := getBfsServiceURL()
	fileSystemURL, _ := getFileSystemURL(c, fsu)

	_, err := fileSystemURL.Create(ctx)
	defer delFileSystem(c, fileSystemURL)
	c.Assert(err, chk.IsNil)

	_, err = fileSystemURL.GetProperties(ctx)
	c.Assert(err, chk.IsNil)
}