package azbfs

import (
	"context"
	//"io"
	//"net/http"
	"net/url"

	"github.com/Azure/azure-pipeline-go/pipeline"
	"io"
	"net/http"
	"strconv"
)

const (
	fileType = "file"
)

// A FileURL represents a URL to an Azure Storage file.
type FileURL struct {
	fileClient     managementClient
	fileSystemName string
	path           string
}

// NewFileURL creates a FileURL object using the specified URL and request policy pipeline.
func NewFileURL(url url.URL, p pipeline.Pipeline) FileURL {
	if p == nil {
		panic("p can't be nil")
	}
	fileClient := newManagementClient(url, p)

	urlParts := NewFileURLParts(url)
	return FileURL{fileClient: fileClient, fileSystemName: urlParts.FileSystemName, path: urlParts.DirectoryOrFilePath}
}

// URL returns the URL endpoint used by the FileURL object.
func (f FileURL) URL() url.URL {
	return f.fileClient.URL()
}

// String returns the URL as a string.
func (f FileURL) String() string {
	u := f.URL()
	return u.String()
}

// WithPipeline creates a new FileURL object identical to the source but with the specified request policy pipeline.
func (f FileURL) WithPipeline(p pipeline.Pipeline) FileURL {
	return NewFileURL(f.fileClient.URL(), p)
}

// Create creates a new file or replaces a file. Note that this method only initializes the file.
// For more information, see https://docs.microsoft.com/en-us/rest/api/storageservices/create-file.
func (f FileURL) Create(ctx context.Context, body io.ReadSeeker) (*CreatePathResponse, error) {
	fileType := fileType
	return f.fileClient.CreatePath(ctx, f.fileSystemName, f.path, &fileType,
		nil, nil, nil, nil, nil, nil,
		nil, nil, nil, nil, nil,
		nil, nil, nil, nil, nil, nil,
		nil, nil, nil, nil, nil,
		nil, nil, body, nil, nil,
		nil)
}

// Download downloads count bytes of data from the start offset. If count is CountToEnd (0), then data is read from specified offset to the end.
// The response includes all of the file’s properties. However, passing true for rangeGetContentMD5 returns the range’s MD5 in the ContentMD5
// response header/property if the range is <= 4MB; the HTTP request fails with 400 (Bad Request) if the requested range is greater than 4MB.
// For more information, see https://docs.microsoft.com/rest/api/storageservices/get-file.
func (f FileURL) Download(ctx context.Context, offset int64, count int64) (*DownloadResponse, error) {
	dr, err := f.fileClient.ReadPath(ctx, f.fileSystemName, f.path, (&httpRange{offset: offset, count: count}).pointers(),
		nil, nil, nil, nil, nil, nil, nil)
	if err != nil {
		return nil, err
	}

	return &DownloadResponse{
		f:    f,
		dr:   dr,
		ctx:  ctx,
		info: HTTPGetterInfo{Offset: offset, Count: count, ETag: dr.ETag()}, // TODO: Note conditional header is not currently supported in Azure File.
	}, err
}

// Body constructs a stream to read data from with a resilient reader option.
// A zero-value option means to get a raw stream.
func (dr *DownloadResponse) Body(o RetryReaderOptions) io.ReadCloser {
	if o.MaxRetryRequests == 0 {
		return dr.Response().Body
	}

	return NewRetryReader(
		dr.ctx,
		dr.Response(),
		dr.info,
		o,
		func(ctx context.Context, info HTTPGetterInfo) (*http.Response, error) {
			resp, err := dr.f.Download(ctx, info.Offset, info.Count)
			if resp == nil {
				return nil, err
			}
			return resp.Response(), err
		})
}

// Delete immediately removes the file from the storage account.
// For more information, see https://docs.microsoft.com/en-us/rest/api/storageservices/delete-file2.
func (f FileURL) Delete(ctx context.Context) (*DeletePathResponse, error) {
	recursive := false
	return f.fileClient.DeletePath(ctx, f.fileSystemName, f.path, &recursive,
		nil, nil, nil, nil, nil, nil,
		nil, nil, nil)
}

// GetProperties returns the file's metadata and properties.
// For more information, see https://docs.microsoft.com/rest/api/storageservices/get-file-properties.
func (f FileURL) GetProperties(ctx context.Context) (*GetPathPropertiesResponse, error) {
	return f.fileClient.GetPathProperties(ctx, f.fileSystemName, f.path, nil, nil, nil, nil,
		nil, nil, nil)
}

// UploadRange writes bytes to a file.
// offset indiciates the offset at which to begin writing, in bytes.
func (f FileURL) AppendData(ctx context.Context, offset int64, body io.ReadSeeker) (*UpdatePathResponse, error) {
	if offset < 0 {
		panic("offset must be >= 0")
	}
	if body == nil {
		panic("body must not be nil")
	}

	count := validateSeekableStreamAt0AndGetCount(body)
	if count == 0 {
		panic("body must contain readable data whose size is > 0")
	}

	// TODO the go http client has a problem with PATCH and content-length header
	// TODO we should investigate and report the issue
	overrideHttpVerb := "PATCH"

	// TransactionalContentMD5 isn't supported currently.
	return f.fileClient.UpdatePath(ctx, "append", strconv.FormatInt(count, 10), f.fileSystemName, f.path, &offset,
		nil, nil, nil, nil, nil,
		nil, nil, nil, nil, nil,
		nil, nil, nil, &overrideHttpVerb, body, nil, nil, nil)
}

// flushes writes previously uploaded data to a file
func (f FileURL) FlushData(ctx context.Context, fileSize int64) (*UpdatePathResponse, error) {
	if fileSize < 0 {
		panic("fileSize must be >= 0")
	}

	// hardcoded to be false for the moment
	// azcopy does not need this
	retainUncommittedData := false

	// TODO the go http client has a problem with PATCH and content-length header
	// TODO we should investigate and report the issue
	overrideHttpVerb := "PATCH"

	// TransactionalContentMD5 isn't supported currently.
	return f.fileClient.UpdatePath(ctx, "flush", "0", f.fileSystemName, f.path, &fileSize,
		&retainUncommittedData, nil, nil, nil, nil,
		nil, nil, nil, nil, nil,
		nil, nil, nil, &overrideHttpVerb, nil, nil, nil, nil)
}
