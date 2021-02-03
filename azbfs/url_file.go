package azbfs

import (
	"context"
	"encoding/base64"
	"net/url"

	"github.com/Azure/azure-pipeline-go/pipeline"
	"io"
	"net/http"
)

// A FileURL represents a URL to an Azure Storage file.
type FileURL struct {
	fileClient     pathClient
	fileSystemName string
	path           string
}

// BlobFSHTTPHeaders represents the set of custom headers available for defining information about the content.
type BlobFSHTTPHeaders struct {
	ContentType        string
	ContentEncoding    string
	ContentLanguage    string
	ContentDisposition string
	CacheControl       string
}

// NewFileURL creates a FileURL object using the specified URL and request policy pipeline.
func NewFileURL(url url.URL, p pipeline.Pipeline) FileURL {
	if p == nil {
		panic("p can't be nil")
	}
	fileClient := newPathClient(url, p)

	urlParts := NewBfsURLParts(url)
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

func (f FileURL) GetParentDir() (DirectoryURL, error) {
	d, err := removeLastSectionOfPath(f.URL())
	if err != nil {
		return DirectoryURL{}, err
	}
	return NewDirectoryURL(d, f.fileClient.p), nil
}

// Create creates a new file or replaces a file. Note that this method only initializes the file.
// For more information, see https://docs.microsoft.com/en-us/rest/api/storageservices/create-file.
func (f FileURL) Create(ctx context.Context, headers BlobFSHTTPHeaders) (*PathCreateResponse, error) {
	return f.fileClient.Create(ctx, f.fileSystemName, f.path, PathResourceFile,
		nil, PathRenameModeNone, nil, nil, nil, nil,
		&headers.CacheControl, &headers.ContentType, &headers.ContentEncoding, &headers.ContentLanguage, &headers.ContentDisposition,
		nil, nil, nil, nil, nil, nil,
		nil, nil, nil, nil, nil,
		nil, nil, nil, nil, nil,
		nil)
}

// Download downloads count bytes of data from the start offset. If count is CountToEnd (0), then data is read from specified offset to the end.
// The response includes all of the file’s properties. However, passing true for rangeGetContentMD5 returns the range’s MD5 in the ContentMD5
// response header/property if the range is <= 4MB; the HTTP request fails with 400 (Bad Request) if the requested range is greater than 4MB.
// For more information, see https://docs.microsoft.com/rest/api/storageservices/get-file.
func (f FileURL) Download(ctx context.Context, offset int64, count int64) (*DownloadResponse, error) {
	dr, err := f.fileClient.Read(ctx, f.fileSystemName, f.path, (&httpRange{offset: offset, count: count}).pointers(),
		nil, nil, nil, nil, nil, nil, nil, nil)
	if err != nil {
		return nil, err
	}

	return &DownloadResponse{
		f:    f,
		dr:   dr,
		ctx:  ctx,
		info: HTTPGetterInfo{Offset: offset, Count: count, ETag: dr.ETag()},
		// TODO: Note conditional header is not currently supported in Azure File.
		// TODO: review the above todo, since as of 8 Feb 2019 we are on a newer version of the API
	}, err
}

// Body constructs a stream to read data from with a resilient reader option.
// A zero-value option means to get a raw stream.
func (dr *DownloadResponse) Body(o RetryReaderOptions) io.ReadCloser {
	// For internal testing, we  check if injectedError is nil.
	// This allows us to have reader retries
	if o.MaxRetryRequests == 0 && o.injectedError == nil {
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
func (f FileURL) Delete(ctx context.Context) (*PathDeleteResponse, error) {
	recursive := false
	return f.fileClient.Delete(ctx, f.fileSystemName, f.path, &recursive,
		nil, nil, nil, nil, nil, nil,
		nil, nil, nil)
}

// GetProperties returns the file's metadata and properties.
// For more information, see https://docs.microsoft.com/rest/api/storageservices/get-file-properties.
func (f FileURL) GetProperties(ctx context.Context) (*PathGetPropertiesResponse, error) {
	// Action MUST be "none", not "getStatus" because the latter does not include the MD5, and
	// sometimes we call this method on things that are actually files
	action := PathGetPropertiesActionNone

	return f.fileClient.GetProperties(ctx, f.fileSystemName, f.path, action, nil,
		nil, nil, nil,
		nil, nil, nil, nil, nil)
}

// UploadRange writes bytes to a file.
// offset indicates the offset at which to begin writing, in bytes.
// custom headers are not valid on this operation
func (f FileURL) AppendData(ctx context.Context, offset int64, body io.ReadSeeker) (*PathUpdateResponse, error) {
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

	// TODO: the go http client has a problem with PATCH and content-length header
	//                we should investigate and report the issue
	// Note: the "offending" code in the Go SDK is: func (t *transferWriter) shouldSendContentLength() bool
	// That code suggests that a workaround would be to specify a Transfer-Encoding of "identity",
	// but we haven't yet found any way to actually set that header, so that workaround doesn't
	// seem to work. (Just setting Transfer-Encoding like a normal header doesn't seem to work.)
	// Looks like it might actually be impossible to set
	// the Transfer-Encoding header, because bradfitz wrote: "as a general rule of thumb, you don't get to mess
	// with [that field] too much. The net/http package owns much of its behavior."
	// https://grokbase.com/t/gg/golang-nuts/15bg66ryd9/go-nuts-cant-write-encoding-other-than-chunked-in-the-transfer-encoding-field-of-http-request
	overrideHttpVerb := "PATCH"

	// TransactionalContentMD5 isn't supported currently.
	return f.fileClient.Update(ctx, PathUpdateActionAppend, f.fileSystemName, f.path, &offset,
		nil, nil, nil, nil, nil, nil,
		nil, nil, nil, nil, nil, nil, nil,
		nil, nil, nil, nil, nil, nil, &overrideHttpVerb, body, nil, nil, nil)
}

// flushes writes previously uploaded data to a file
// The contentMd5 parameter, if not nil, should represent the MD5 hash that has been computed for the file as whole
func (f FileURL) FlushData(ctx context.Context, fileSize int64, contentMd5 []byte, headers BlobFSHTTPHeaders, retainUncommittedData bool, closeFile bool) (*PathUpdateResponse, error) {
	if fileSize < 0 {
		panic("fileSize must be >= 0")
	}

	var md5InBase64 *string = nil
	if len(contentMd5) > 0 {
		enc := base64.StdEncoding.EncodeToString(contentMd5)
		md5InBase64 = &enc
	}

	// TODO: the go http client has a problem with PATCH and content-length header
	//       we should investigate and report the issue
	// See similar todo, with larger comments, in AppendData
	overrideHttpVerb := "PATCH"

	// TransactionalContentMD5 isn't supported currently.
	return f.fileClient.Update(ctx, PathUpdateActionFlush, f.fileSystemName, f.path, &fileSize,
		&retainUncommittedData, &closeFile, nil, nil,
		&headers.CacheControl, &headers.ContentType, &headers.ContentDisposition, &headers.ContentEncoding, &headers.ContentLanguage,
		md5InBase64, nil, nil, nil, nil, nil, nil, nil,
		nil, nil, &overrideHttpVerb, nil, nil, nil, nil)
}
