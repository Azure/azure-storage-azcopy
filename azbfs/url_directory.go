package azbfs

import (
	"net/url"
	"github.com/Azure/azure-pipeline-go/pipeline"
	"context"
)

var DirectoryResourceType = "directory"
var DeleteDirectoryRecursively = true
var MaxEntriesinListOperation = int32(1000)



// A DirectoryURL represents a URL to the Azure Storage directory allowing you to manipulate its directories and files.
type DirectoryURL struct {
	directoryClient managementClient
	// filesystem is the filesystem identifier
	filesystem string
	// pathParameter is the file or directory path
	pathParameter     string

	p pipeline.Pipeline
}

// NewDirectoryURL creates a DirectoryURL object using the specified URL and request policy pipeline.
func NewDirectoryURL(url url.URL, p pipeline.Pipeline) DirectoryURL {
	if p == nil {
		panic("p can't be nil")
	}
	urlParts := NewFileURLParts(url)
	directoryClient := newManagementClient(url, p)
	return DirectoryURL{directoryClient: directoryClient, filesystem:urlParts.FileSystemName, pathParameter:urlParts.DirectoryOrFilePath, p : p}
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


// NewSubDirectoryUrl creates a new Directory Url for Sub directory inside the directory of given directory URL.
// The new NewSubDirectoryUrl uses the same request policy pipeline as the DirectoryURL.
// To change the pipeline, create the NewDirectoryUrl and then call its WithPipeline method passing in the
// desired pipeline object.
func (d DirectoryURL) NewSubDirectoryUrl(dirName string) DirectoryURL {
	subDirUrl := appendToURLPath(d.URL(), dirName)
	return NewDirectoryURL(subDirUrl, d.directoryClient.Pipeline())
}

// Create creates a new directory within a File System
func (d DirectoryURL) Create(ctx context.Context) (*DirectoryCreateResponse, error) {
		resp, err := d.directoryClient.CreatePath(ctx, d.filesystem, d.pathParameter, &DirectoryResourceType, nil,
		nil, nil, nil, nil, nil, nil,
		nil, nil, nil, nil, nil, nil,
		nil, nil, nil, nil, nil, nil,
		nil, nil, nil, nil, nil, nil,
		nil, nil, nil)
		return (*DirectoryCreateResponse)(resp),err
}


// Delete removes the specified empty directory. Note that the directory must be empty before it can be deleted..
// For more information, see https://docs.microsoft.com/rest/api/storageservices/delete-directory.
func (d DirectoryURL) Delete(ctx context.Context, continuationString *string) (*DirectoryDeleteResponse, error) {
	resp, err :=  d.directoryClient.DeletePath(ctx, d.filesystem, d.pathParameter, &(DeleteDirectoryRecursively), continuationString, nil,
					nil, nil, nil, nil, nil, nil, nil)
	return (*DirectoryDeleteResponse)(resp), err
}


// GetProperties returns the directory's metadata and system properties.
func (d DirectoryURL) GetProperties(ctx context.Context) (*DirectoryGetPropertiesResponse, error) {
	resp, err := d.directoryClient.GetPathProperties(ctx, d.filesystem, d.pathParameter, nil, nil, nil,
		nil, nil, nil, nil)
	return (*DirectoryGetPropertiesResponse)(resp), err
}

// FileSystemUrl returns the fileSystemUrl from the directoryUrl
// FileSystemUrl is of the FS in which the current directory exists.
func (d DirectoryURL) FileSystemUrl() FileSystemURL {
	// Parse Url into FileUrlParts
	// Set the DirectoryOrFilePath empty
	// and generate the Url
	urlParts := NewFileURLParts(d.URL())
	urlParts.DirectoryOrFilePath = ""
	return NewFileSystemURL(urlParts.URL(), d.p)
}

// ListDirectory returns a files inside the directory. If recursive is set to true then ListDirectory will recursively
// list all files inside the directory. Use an empty Marker to start enumeration from the beginning.
// After getting a segment, process it, and then call ListDirectory again (passing the the previously-returned
// Marker) to get the next segment.
func (d DirectoryURL) ListDirectory(ctx context.Context, marker *string, recursive bool) (*DirectoryListResponse, error) {
	// Since listPath is supported on filesystem Url
	// covert the directory url to fileSystemUrl
	// and listPath for filesystem with directory path set in the path parameter
	resp , err := d.FileSystemUrl().fileSystemClient.ListPaths(ctx, recursive, d.filesystem, FileSystemResourceName, &d.pathParameter, nil,
					nil, nil, nil, nil)
	return (*DirectoryListResponse)(resp), err
}


//
//// SetMetadata sets the directory's metadata.
//// For more information, see https://docs.microsoft.com/rest/api/storageservices/set-directory-metadata.
//func (d DirectoryURL) SetMetadata(ctx context.Context, metadata Metadata) (*DirectorySetMetadataResponse, error) {
//	return d.directoryClient.SetMetadata(ctx, nil, metadata)
//}
//
//// ListFilesAndDirectoriesOptions defines options available when calling ListFilesAndDirectoriesSegment.
//type ListFilesAndDirectoriesOptions struct {
//	Prefix     string // No Prefix header is produced if ""
//	MaxResults int32  // 0 means unspecified
//}
//
//func (o *ListFilesAndDirectoriesOptions) pointers() (prefix *string, maxResults *int32) {
//	if o.Prefix != "" {
//		prefix = &o.Prefix
//	}
//	if o.MaxResults != 0 {
//		if o.MaxResults < 0 {
//			panic("MaxResults must be >= 0")
//		}
//		maxResults = &o.MaxResults
//	}
//	return
//}
//
//// toConvenienceModel convert raw response to convenience model.
//// func (r *listFilesAndDirectoriesSegmentResponse) toConvenienceModel() *ListFilesAndDirectoriesSegmentResponse {
//// 	cr := ListFilesAndDirectoriesSegmentResponse{
//// 		rawResponse:     r.rawResponse,
//// 		ServiceEndpoint: r.ServiceEndpoint,
//// 		FileSystemName:       r.FileSystemName,
//// 		ShareSnapshot:   r.ShareSnapshot,
//// 		DirectoryPath:   r.DirectoryPath,
//// 		Prefix:          r.Prefix,
//// 		Marker:          r.Marker,
//// 		MaxResults:      r.MaxResults,
//// 		NextMarker:      r.NextMarker,
//// 	}
//
//// 	for _, e := range r.Entries {
//// 		if f, isFile := e.AsFileEntry(); isFile {
//// 			cr.Files = append(cr.Files, *f)
//// 		} else if d, isDir := e.AsDirectoryEntry(); isDir {
//// 			cr.Directories = append(cr.Directories, *d)
//// 		} else {
//// 			// Logic should not be here, otherwise client is not aligning to latest REST API document
//// 			panic(fmt.Errorf("invalid entry type found, entry info: %v", e))
//// 		}
//
//// 	}
//
//// 	return &cr
//// }
//
//// ListFilesAndDirectoriesSegmentAutoRest is the implementation using Auto Rest generated protocol code.
//// func (d DirectoryURL) ListFilesAndDirectoriesSegmentAutoRest(ctx context.Context, marker Marker, o ListFilesAndDirectoriesOptions) (*ListFilesAndDirectoriesSegmentResponse, error) {
//// 	prefix, maxResults := o.pointers()
//
//// 	rawResponse, error := d.directoryClient.ListFilesAndDirectoriesSegmentAutoRest(ctx, prefix, nil, marker.val, maxResults, nil)
//
//// 	return rawResponse.toConvenienceModel(), error
//// }
//
//// ListFilesAndDirectoriesSegment returns a single segment of files and directories starting from the specified Marker.
//// Use an empty Marker to start enumeration from the beginning. File and directory names are returned in lexicographic order.
//// After getting a segment, process it, and then call ListFilesAndDirectoriesSegment again (passing the the previously-returned
//// Marker) to get the next segment. This method lists the contents only for a single level of the directory hierarchy.
//// For more information, see https://docs.microsoft.com/en-us/rest/api/storageservices/list-directories-and-files.
//func (d DirectoryURL) ListFilesAndDirectoriesSegment(ctx context.Context, marker Marker, o ListFilesAndDirectoriesOptions) (*ListFilesAndDirectoriesSegmentResponse, error) {
//	prefix, maxResults := o.pointers()
//	return d.directoryClient.ListFilesAndDirectoriesSegment(ctx, prefix, nil, marker.val, maxResults, nil)
//}
