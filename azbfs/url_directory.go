package azbfs

import (
	//"context"
	"net/url"

	"github.com/Azure/azure-pipeline-go/pipeline"
)

// A DirectoryURL represents a URL to the Azure Storage directory allowing you to manipulate its directories and files.
type DirectoryURL struct {
	directoryClient managementClient
}

// NewDirectoryURL creates a DirectoryURL object using the specified URL and request policy pipeline.
func NewDirectoryURL(url url.URL, p pipeline.Pipeline) DirectoryURL {
	if p == nil {
		panic("p can't be nil")
	}
	directoryClient := newManagementClient(url, p)
	return DirectoryURL{directoryClient: directoryClient}
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

// NewDirectoryURL creates a new DirectoryURL object by concatenating directoryName to the end of
// DirectoryURL's URL. The new DirectoryURL uses the same request policy pipeline as the DirectoryURL.
// To change the pipeline, create the DirectoryURL and then call its WithPipeline method passing in the
// desired pipeline object. Or, call this package's NewDirectoryURL instead of calling this object's
// NewDirectoryURL method.
func (d DirectoryURL) NewDirectoryURL(directoryName string) DirectoryURL {
	directoryURL := appendToURLPath(d.URL(), directoryName)
	return NewDirectoryURL(directoryURL, d.directoryClient.Pipeline())
}

//// Create creates a new directory within a storage account.
//// For more information, see https://docs.microsoft.com/rest/api/storageservices/create-directory.
//func (d DirectoryURL) Create(ctx context.Context, metadata Metadata) (*DirectoryCreateResponse, error) {
//	return d.directoryClient.Create(ctx, nil, metadata)
//}
//
//// Delete removes the specified empty directory. Note that the directory must be empty before it can be deleted..
//// For more information, see https://docs.microsoft.com/rest/api/storageservices/delete-directory.
//func (d DirectoryURL) Delete(ctx context.Context) (*DirectoryDeleteResponse, error) {
//	return d.directoryClient.Delete(ctx, nil)
//}
//
//// GetProperties returns the directory's metadata and system properties.
//// For more information, see https://docs.microsoft.com/en-us/rest/api/storageservices/get-directory-properties.
//func (d DirectoryURL) GetProperties(ctx context.Context) (*DirectoryGetPropertiesResponse, error) {
//	return d.directoryClient.GetProperties(ctx, nil, nil)
//}
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
