package azbfs

import (
	"context"
	"errors"
	"net/url"
	"strings"

	"github.com/Azure/azure-pipeline-go/pipeline"
)

var directoryResourceName = "directory" // constant value for the resource query parameter

// A DirectoryURL represents a URL to the Azure Storage directory allowing you to manipulate its directories and files.
type DirectoryURL struct {
	directoryClient pathClient
	// filesystem is the filesystem identifier
	filesystem string
	// pathParameter is the file or directory path
	pathParameter string
}

// NewDirectoryURL creates a DirectoryURL object using the specified URL and request policy pipeline.
func NewDirectoryURL(url url.URL, p pipeline.Pipeline) DirectoryURL {
	if p == nil {
		panic("p can't be nil")
	}
	urlParts := NewBfsURLParts(url)
	directoryClient := newPathClient(url, p)
	return DirectoryURL{directoryClient: directoryClient, filesystem: urlParts.FileSystemName, pathParameter: urlParts.DirectoryOrFilePath}
}

func (d DirectoryURL) IsFileSystemRoot() bool {
	return d.pathParameter == ""
}

// URL returns the URL endpoint used by the DirectoryURL object.
func (d DirectoryURL) URL() url.URL {
	return d.directoryClient.URL()
}

// String returns the URL as a string.
func (d DirectoryURL) String() string {
	u := d.URL()
	return u.String()
}

// WithPipeline creates a new DirectoryURL object identical to the source but with the specified request policy pipeline.
func (d DirectoryURL) WithPipeline(p pipeline.Pipeline) DirectoryURL {
	return NewDirectoryURL(d.URL(), p)
}

// NewFileURL creates a new FileURL object by concatenating fileName to the end of
// DirectoryURL's URL. The new FileURL uses the same request policy pipeline as the DirectoryURL.
// To change the pipeline, create the FileURL and then call its WithPipeline method passing in the
// desired pipeline object. Or, call this package's NewFileURL instead of calling this object's
// NewFileURL method.
func (d DirectoryURL) NewFileURL(fileName string) FileURL {
	fileURL := appendToURLPath(d.URL(), fileName)
	return NewFileURL(fileURL, d.directoryClient.Pipeline())
}

// NewDirectoryURL creates a new Directory Url for Sub directory inside the directory of given directory URL.
// The new NewDirectoryURL uses the same request policy pipeline as the DirectoryURL.
// To change the pipeline, create the NewDirectoryUrl and then call its WithPipeline method passing in the
// desired pipeline object.
func (d DirectoryURL) NewDirectoryURL(dirName string) DirectoryURL {
	subDirUrl := appendToURLPath(d.URL(), dirName)
	return NewDirectoryURL(subDirUrl, d.directoryClient.Pipeline())
}

// Create creates a new directory within a File System
func (d DirectoryURL) Create(ctx context.Context, recreateIfExists bool) (*DirectoryCreateResponse, error) {
	return d.CreateWithOptions(ctx, CreateDirectoryOptions{RecreateIfExists: recreateIfExists})
}

// Create creates a new directory within a File System
func (d DirectoryURL) CreateWithOptions(ctx context.Context, options CreateDirectoryOptions) (*DirectoryCreateResponse, error) {
	var ifNoneMatch *string
	if options.RecreateIfExists {
		ifNoneMatch = nil // the default ADLS Gen2 behavior, see https://docs.microsoft.com/en-us/rest/api/storageservices/datalakestoragegen2/path/create
	} else {
		star := "*" // see https://docs.microsoft.com/en-us/rest/api/storageservices/datalakestoragegen2/path/create
		ifNoneMatch = &star
	}
	return d.doCreate(ctx, ifNoneMatch, options.Metadata)
}

func (d DirectoryURL) doCreate(ctx context.Context, ifNoneMatch *string, metadata map[string]string) (*DirectoryCreateResponse, error) {
	resp, err := d.directoryClient.Create(ctx, d.filesystem, d.pathParameter, PathResourceDirectory, nil,
		PathRenameModeNone, nil, nil, nil, nil, nil,
		nil, nil, nil, nil, nil, nil,
		nil, buildMetadataString(metadata), nil, nil, nil, ifNoneMatch,
		nil, nil, nil, nil, nil, nil,
		nil, nil, nil)
	return (*DirectoryCreateResponse)(resp), err
}

// Delete removes the specified empty directory. Note that the directory must be empty before it can be deleted..
// For more information, see https://docs.microsoft.com/rest/api/storageservices/delete-directory.
func (d DirectoryURL) Delete(ctx context.Context, continuationString *string, recursive bool) (*DirectoryDeleteResponse, error) {
	resp, err := d.directoryClient.Delete(ctx, d.filesystem, d.pathParameter, &recursive, continuationString, nil,
		nil, nil, nil, nil, nil, nil, nil)
	return (*DirectoryDeleteResponse)(resp), err
}

// GetProperties returns the directory's metadata and system properties.
func (d DirectoryURL) GetProperties(ctx context.Context) (*DirectoryGetPropertiesResponse, error) {
	// Action MUST be "none", not "getStatus" because the latter does not include the MD5, and
	// sometimes we call this method on things that are actually files
	action := PathGetPropertiesActionNone

	resp, err := d.directoryClient.GetProperties(ctx, d.filesystem, d.pathParameter, action, nil, nil,
		nil, nil, nil, nil, nil, nil, nil)
	return (*DirectoryGetPropertiesResponse)(resp), err
}

// FileSystemURL returns the fileSystemUrl from the directoryUrl
// FileSystemURL is of the FS in which the current directory exists.
func (d DirectoryURL) FileSystemURL() FileSystemURL {
	// Parse Url into FileUrlParts
	// Set the DirectoryOrFilePath empty
	// and generate the Url
	urlParts := NewBfsURLParts(d.URL())
	urlParts.DirectoryOrFilePath = ""
	return NewFileSystemURL(urlParts.URL(), d.directoryClient.Pipeline())
}

// ListDirectorySegment returns files/directories inside the directory. If recursive is set to true then ListDirectorySegment will recursively
// list all files/directories inside the directory. Use an empty Marker to start enumeration from the beginning.
// After getting a segment, process it, and then call ListDirectorySegment again (passing the the previously-returned
// Marker) to get the next segment.
func (d DirectoryURL) ListDirectorySegment(ctx context.Context, marker *string, recursive bool) (*DirectoryListResponse, error) {
	// Since listPath is supported on filesystem Url
	// convert the directory url to fileSystemUrl
	// and listPath for filesystem with directory path set in the path parameter
	var maxEntriesInListOperation = int32(1000)

	resp, err := d.FileSystemURL().fileSystemClient.ListPaths(ctx, recursive, d.filesystem, &d.pathParameter, marker,
		&maxEntriesInListOperation, nil, nil, nil, nil)

	return (*DirectoryListResponse)(resp), err
}

// IsDirectory determines whether the resource at given directoryUrl is a directory Url or not
// It returns false if the directoryUrl is not able to get resource properties
// It returns false if the url represent a file in the filesystem
// TODO reconsider for SDK release
func (d DirectoryURL) IsDirectory(ctx context.Context) (bool, error) {
	grep, err := d.GetProperties(ctx)
	// If the error occurs while getting resource properties return false
	if err != nil {
		return false, err
	}
	// return false if the resource type is not
	if !strings.EqualFold(grep.XMsResourceType(), directoryResourceName) {
		return false, nil
	}
	return true, nil
}

// NewFileUrl converts the current directory Url into the NewFileUrl
// This api is used when the directoryUrl is to represents a file
func (d DirectoryURL) NewFileUrl() FileURL {
	return NewFileURL(d.URL(), d.directoryClient.Pipeline())
}

// Renames the directory to the provided destination
func (d DirectoryURL) Rename(ctx context.Context, options RenameDirectoryOptions) (DirectoryURL, error) {

	// If the destinationFileSystem is not provided, use the current filesystem
	fileSystemName := options.DestinationFileSystem
	if fileSystemName == nil || *fileSystemName == "" {
		fileSystemName = &d.filesystem
	}

	urlParts := NewBfsURLParts(d.directoryClient.URL())
	urlParts.FileSystemName = *fileSystemName
	urlParts.DirectoryOrFilePath = options.DestinationPath
	// ensure we use our source's SAS token in the x-ms-rename-source header
	renameSource := "/" + d.filesystem + "/" + d.pathParameter + "?" + urlParts.SAS.Encode()

	// if we're changing our source SAS to a new SAS in the rename
	// in the case the user wants to have limited permissions per directory: sas1 for directory1 and sas2 for directory2
	if options.DestinationSas != nil && *options.DestinationSas != "" {
		urlParts.SAS = GetSasQueryParams(*options.DestinationSas)
	}
	destinationDirectoryURL := NewDirectoryURL(urlParts.URL(), d.directoryClient.Pipeline())

	_, err := destinationDirectoryURL.directoryClient.Create(ctx, *fileSystemName, options.DestinationPath, PathResourceNone, nil, PathRenameModeLegacy,
		nil, nil, nil, nil, nil, nil, nil, nil, nil, &renameSource, nil, nil, nil, nil, nil, nil, nil, nil, nil,
		nil, nil, nil, nil, nil, nil, nil)

	if err != nil {
		return DirectoryURL{}, err
	}

	return destinationDirectoryURL, nil
}

func (d DirectoryURL) GetAccessControl(ctx context.Context) (BlobFSAccessControl, error) {
	resp, err := d.directoryClient.GetProperties(ctx, d.filesystem, d.pathParameter, PathGetPropertiesActionGetAccessControl, nil,
		nil, nil, nil,
		nil, nil, nil, nil, nil)

	if err != nil {
		return BlobFSAccessControl{}, err
	}

	return BlobFSAccessControl{resp.XMsOwner(), resp.XMsGroup(), resp.XMsACL(), resp.XMsPermissions()}, nil
}

func (d DirectoryURL) SetAccessControl(ctx context.Context, permissions BlobFSAccessControl) (*PathUpdateResponse, error) {
	// TODO: the go http client has a problem with PATCH and content-length header
	//       we should investigate and report the issue
	// See similar todo, with larger comments, in AppendData
	overrideHttpVerb := "PATCH"

	if permissions.ACL != "" && permissions.Permissions != "" {
		return nil, errors.New("specifying both Permissions and ACL conflicts for SetAccessControl")
	}

	var perms, acl *string
	if permissions.Permissions != "" {
		perms = &permissions.Permissions
	} else {
		acl = &permissions.ACL
	}

	// This does not yet have support for recursive updates. But then again, we don't really need it.
	return d.directoryClient.Update(ctx, PathUpdateActionSetAccessControl, d.filesystem, d.pathParameter,
		nil, nil, nil, nil, nil,
		nil, nil, nil, nil, nil,
		nil, nil, &permissions.Owner, &permissions.Group, perms, acl,
		nil, nil, nil, nil, &overrideHttpVerb,
		nil, nil, nil, nil)
}
