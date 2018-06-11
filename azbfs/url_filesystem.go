package azbfs

import (
	"context"
	"net/url"

	"github.com/Azure/azure-pipeline-go/pipeline"
)

const FileSystemResourceName = "filesystem"

// A FileSystemURL represents a URL to the Azure Storage Blob File System allowing you to manipulate its directories and files.
type FileSystemURL struct {
	fileSystemClient managementClient
	name             string
}

// NewFileSystemURL creates a FileSystemURL object using the specified URL and request policy pipeline.
func NewFileSystemURL(url url.URL, p pipeline.Pipeline) FileSystemURL {
	if p == nil {
		panic("p can't be nil")
	}
	fileSystemClient := newManagementClient(url, p)

	urlParts := NewFileURLParts(url)
	return FileSystemURL{fileSystemClient: fileSystemClient, name: urlParts.FileSystemName}
}

// URL returns the URL endpoint used by the FileSystemURL object.
func (s FileSystemURL) URL() url.URL {
	return s.fileSystemClient.URL()
}

// String returns the URL as a string.
func (s FileSystemURL) String() string {
	u := s.URL()
	return u.String()
}

// WithPipeline creates a new FileSystemURL object identical to the source but with the specified request policy pipeline.
func (s FileSystemURL) WithPipeline(p pipeline.Pipeline) FileSystemURL {
	return NewFileSystemURL(s.URL(), p)
}

// NewDirectoryURL creates a new DirectoryURL object by concatenating directoryName to the end of
// FileSystemURL's URL. The new DirectoryURL uses the same request policy pipeline as the FileSystemURL.
// To change the pipeline, create the DirectoryURL and then call its WithPipeline method passing in the
// desired pipeline object. Or, call this package's NewDirectoryURL instead of calling this object's
// NewDirectoryURL method.
func (s FileSystemURL) NewDirectoryURL(directoryName string) DirectoryURL {
	directoryURL := appendToURLPath(s.URL(), directoryName)
	return NewDirectoryURL(directoryURL, s.fileSystemClient.Pipeline())
}

// NewRootDirectoryURL creates a new DirectoryURL object using FileSystemURL's URL.
// The new DirectoryURL uses the same request policy pipeline as the
// FileSystemURL. To change the pipeline, create the DirectoryURL and then call its WithPipeline method
// passing in the desired pipeline object. Or, call NewDirectoryURL instead of calling the NewDirectoryURL method.
func (s FileSystemURL) NewRootDirectoryURL() DirectoryURL {
	return NewDirectoryURL(s.URL(), s.fileSystemClient.Pipeline())
}

// Create creates a new file system within a storage account. If a file system with the same name already exists, the operation fails.
// quotaInGB specifies the maximum size of the file system in gigabytes, 0 means you accept service's default quota.
func (s FileSystemURL) Create(ctx context.Context) (*CreateFilesystemResponse, error) {
	return s.fileSystemClient.CreateFilesystem(ctx, s.name, FileSystemResourceName, nil, nil, nil, nil)
}

// Delete marks the specified file system for deletion.
// The file system and any files contained within it are later deleted during garbage collection.
func (s FileSystemURL) Delete(ctx context.Context) (*DeleteFilesystemResponse, error) {
	return s.fileSystemClient.DeleteFilesystem(ctx, s.name, FileSystemResourceName, nil, nil, nil, nil, nil)
}

// GetProperties returns all user-defined metadata and system properties for the specified file system or file system snapshot.
func (s FileSystemURL) GetProperties(ctx context.Context) (*GetFilesystemPropertiesResponse, error) {
	return s.fileSystemClient.GetFilesystemProperties(ctx, s.name, FileSystemResourceName, nil, nil, nil)
}
