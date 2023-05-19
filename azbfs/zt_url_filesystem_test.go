package azbfs_test

import (
	"context"
	"github.com/stretchr/testify/assert"
	"os"
	"testing"

	"net/http"
	"net/url"

	"github.com/Azure/azure-storage-azcopy/v10/azbfs"
)

func TestFileSystemCreateRootDirectoryURL(t *testing.T) {
	a := assert.New(t)
	fsu := getBfsServiceURL()
	testURL := fsu.NewFileSystemURL(fileSystemPrefix).NewRootDirectoryURL()

	correctURL := "https://" + os.Getenv("ACCOUNT_NAME") + ".dfs.core.windows.net/" + fileSystemPrefix
	temp := testURL.URL()
	a.Equal(correctURL, temp.String())
}

func TestFileSystemCreateDirectoryURL(t *testing.T) {
	a := assert.New(t)
	fsu := getBfsServiceURL()
	testURL := fsu.NewFileSystemURL(fileSystemPrefix).NewDirectoryURL(directoryPrefix)

	correctURL := "https://" + os.Getenv("ACCOUNT_NAME") + ".dfs.core.windows.net/" + fileSystemPrefix + "/" + directoryPrefix
	temp := testURL.URL()
	a.Equal(correctURL, temp.String())
	a.Equal(correctURL, testURL.String())
}

func TestFileSystemNewFileSystemURLNegative(t *testing.T) {
	a := assert.New(t)
	a.Panics(func() { azbfs.NewFileSystemURL(url.URL{}, nil) }, "p can't be nil")
}

func TestFileSystemCreateDelete(t *testing.T) {
	a := assert.New(t)
	fsu := getBfsServiceURL()
	fileSystemURL, _ := getFileSystemURL(a, fsu)

	_, err := fileSystemURL.Create(ctx)
	defer deleteFileSystem(a, fileSystemURL)
	a.Nil(err)

	// Test get properties
	resp, err := fileSystemURL.GetProperties(ctx)
	a.Equal(http.StatusOK, resp.StatusCode())
	a.Nil(err)
}

func TestFileSystemList(t *testing.T) {
	a := assert.New(t)
	fsu := getBfsServiceURL()
	fileSystemURL, _ := getFileSystemURL(a, fsu)

	_, err := fileSystemURL.Create(ctx)
	defer deleteFileSystem(a, fileSystemURL)
	a.Nil(err)

	// List Setup
	dirUrl, dirName := getDirectoryURLFromFileSystem(a, fileSystemURL)
	dirUrl.Create(context.Background(), true)

	fileUrl, fileName := getFileURLFromFileSystem(a, fileSystemURL)
	fileUrl.Create(context.Background(), azbfs.BlobFSHTTPHeaders{}, azbfs.BlobFSAccessControl{})

	// List
	paths, err := fileSystemURL.ListPaths(context.Background(), azbfs.ListPathsFilesystemOptions{Recursive: false})
	a.Nil(err)
	a.NotNil(paths.Paths)
	a.Len(paths.Paths, 2)
	dirPath := paths.Paths[0]
	a.Equal(dirName, *dirPath.Name)
	a.True(*dirPath.IsDirectory)
	filePath := paths.Paths[1]
	a.Equal(fileName, *filePath.Name)
	a.Nil(filePath.IsDirectory)
}
