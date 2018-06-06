package azbfs
//
//import (
//	"context"
//	"io"
//	"net/http"
//	"net/url"
//
//	"github.com/Azure/azure-pipeline-go/pipeline"
//)
//
//const (
//	fileType = "file"
//
//	// FileMaxUploadRangeBytes indicates the maximum number of bytes that can be sent in a call to UploadRange.
//	FileMaxUploadRangeBytes = 4 * 1024 * 1024 // 4MB
//
//	// FileMaxSizeInBytes indicates the maxiumum file size, in bytes.
//	FileMaxSizeInBytes = 1 * 1024 * 1024 * 1024 * 1024 // 1TB
//)
//
//// A FileURL represents a URL to an Azure Storage file.
//type FileURL struct {
//	fileClient fileClient
//}
//
//// NewFileURL creates a FileURL object using the specified URL and request policy pipeline.
//func NewFileURL(url url.URL, p pipeline.Pipeline) FileURL {
//	if p == nil {
//		panic("p can't be nil")
//	}
//	fileClient := newFileClient(url, p)
//	return FileURL{fileClient: fileClient}
//}
//
//// URL returns the URL endpoint used by the FileURL object.
//func (f FileURL) URL() url.URL {
//	return f.fileClient.URL()
//}
//
//// String returns the URL as a string.
//func (f FileURL) String() string {
//	u := f.URL()
//	return u.String()
//}
//
//// WithPipeline creates a new FileURL object identical to the source but with the specified request policy pipeline.
//func (f FileURL) WithPipeline(p pipeline.Pipeline) FileURL {
//	return NewFileURL(f.fileClient.URL(), p)
//}
//
//// WithSnapshot creates a new FileURL object identical to the source but with the specified share snapshot timestamp.
//// Pass time.Time{} to remove the share snapshot returning a URL to the base file.
//func (f FileURL) WithSnapshot(shareSnapshot string) FileURL {
//	p := NewFileURLParts(f.URL())
//	p.ShareSnapshot = shareSnapshot
//	return NewFileURL(p.URL(), f.fileClient.Pipeline())
//}
//
//// Create creates a new file or replaces a file. Note that this method only initializes the file.
//// For more information, see https://docs.microsoft.com/en-us/rest/api/storageservices/create-file.
//func (f FileURL) Create(ctx context.Context, size int64, h FileHTTPHeaders, metadata Metadata) (*FileCreateResponse, error) {
//	return f.fileClient.Create(ctx, size, nil,
//		&h.ContentType, &h.ContentEncoding, &h.ContentLanguage, &h.CacheControl,
//		h.contentMD5Pointer(), &h.ContentDisposition, metadata)
//}
//
//// StartCopy copies the data at the source URL to a file.
//// For more information, see https://docs.microsoft.com/rest/api/storageservices/copy-file.
//func (f FileURL) StartCopy(ctx context.Context, source url.URL, metadata Metadata) (*FileStartCopyResponse, error) {
//	return f.fileClient.StartCopy(ctx, source.String(), nil, metadata)
//}
//
//// AbortCopy stops a pending copy that was previously started and leaves a destination file with 0 length and metadata.
//// For more information, see https://docs.microsoft.com/rest/api/storageservices/abort-copy-file.
//func (f FileURL) AbortCopy(ctx context.Context, copyID string) (*FileAbortCopyResponse, error) {
//	return f.fileClient.AbortCopy(ctx, copyID, nil)
//}
//
//// Download downloads count bytes of data from the start offset. If count is CountToEnd (0), then data is read from specified offset to the end.
//// The response includes all of the file’s properties. However, passing true for rangeGetContentMD5 returns the range’s MD5 in the ContentMD5
//// response header/property if the range is <= 4MB; the HTTP request fails with 400 (Bad Request) if the requested range is greater than 4MB.
//// For more information, see https://docs.microsoft.com/rest/api/storageservices/get-file.
//func (f FileURL) Download(ctx context.Context, offset int64, count int64, rangeGetContentMD5 bool) (*DownloadResponse, error) {
//	var xRangeGetContentMD5 *bool
//	if rangeGetContentMD5 {
//		if offset == 0 && count == CountToEnd {
//			panic("rangeGetContentMD5 only work with partial data downloading")
//		}
//		xRangeGetContentMD5 = &rangeGetContentMD5
//	}
//	dr, err := f.fileClient.Download(ctx, nil, (&httpRange{offset: offset, count: count}).pointers(), xRangeGetContentMD5)
//	if err != nil {
//		return nil, err
//	}
//
//	return &DownloadResponse{
//		f:    f,
//		dr:   dr,
//		ctx:  ctx,
//		info: HTTPGetterInfo{Offset: offset, Count: count, ETag: dr.ETag()}, // TODO: Note conditional header is not currently supported in Azure File.
//	}, err
//}
//
//// Body constructs a stream to read data from with a resilient reader option.
//// A zero-value option means to get a raw stream.
//func (dr *DownloadResponse) Body(o RetryReaderOptions) io.ReadCloser {
//	if o.MaxRetryRequests == 0 {
//		return dr.Response().Body
//	}
//
//	return NewRetryReader(
//		dr.ctx,
//		dr.Response(),
//		dr.info,
//		o,
//		func(ctx context.Context, info HTTPGetterInfo) (*http.Response, error) {
//			resp, err := dr.f.Download(ctx, info.Offset, info.Count, false)
//			return resp.Response(), err
//		})
//}
//
//// Delete immediately removes the file from the storage account.
//// For more information, see https://docs.microsoft.com/en-us/rest/api/storageservices/delete-file2.
//func (f FileURL) Delete(ctx context.Context) (*FileDeleteResponse, error) {
//	return f.fileClient.Delete(ctx, nil)
//}
//
//// GetProperties returns the file's metadata and properties.
//// For more information, see https://docs.microsoft.com/rest/api/storageservices/get-file-properties.
//func (f FileURL) GetProperties(ctx context.Context) (*FileGetPropertiesResponse, error) {
//	return f.fileClient.GetProperties(ctx, nil, nil)
//}
//
//// SetHTTPHeaders sets file's system properties.
//// For more information, see https://docs.microsoft.com/rest/api/storageservices/set-file-properties.
//func (f FileURL) SetHTTPHeaders(ctx context.Context, h FileHTTPHeaders) (*FileSetHTTPHeadersResponse, error) {
//	return f.fileClient.SetHTTPHeaders(ctx, nil,
//		nil, &h.ContentType, &h.ContentEncoding, &h.ContentLanguage, &h.CacheControl, h.contentMD5Pointer(), &h.ContentDisposition)
//}
//
//// SetMetadata sets a file's metadata.
//// https://docs.microsoft.com/rest/api/storageservices/set-file-metadata.
//func (f FileURL) SetMetadata(ctx context.Context, metadata Metadata) (*FileSetMetadataResponse, error) {
//	return f.fileClient.SetMetadata(ctx, nil, metadata)
//}
//
//// Resize resizes the file to the specified size.
//// For more information, see https://docs.microsoft.com/rest/api/storageservices/set-file-properties.
//func (f FileURL) Resize(ctx context.Context, length int64) (*FileSetHTTPHeadersResponse, error) {
//	return f.fileClient.SetHTTPHeaders(ctx, nil,
//		&length, nil, nil, nil, nil, nil, nil)
//}
//
//// UploadRange writes bytes to a file.
//// offset indiciates the offset at which to begin writing, in bytes.
//// For more information, see https://docs.microsoft.com/en-us/rest/api/storageservices/put-range.
//func (f FileURL) UploadRange(ctx context.Context, offset int64, body io.ReadSeeker) (*FileUploadRangeResponse, error) {
//	if offset < 0 {
//		panic("offset must be >= 0")
//	}
//	if body == nil {
//		panic("body must not be nil")
//	}
//
//	count := validateSeekableStreamAt0AndGetCount(body)
//	if count == 0 {
//		panic("body must contain readable data whose size is > 0")
//	}
//
//	// TransactionalContentMD5 isn't supported currently.
//	return f.fileClient.UploadRange(ctx, *toRange(offset, count), FileRangeWriteUpdate, count, body, nil, nil)
//}
//
//// ClearRange clears the specified range and releases the space used in storage for that range.
//// offset means the start offset of the range to clear.
//// count means count of bytes to clean, it cannot be CountToEnd (0), and must be explictly specified.
//// If the range specified is not 512-byte aligned, the operation will write zeros to
//// the start or end of the range that is not 512-byte aligned and free the rest of the range inside that is 512-byte aligned.
//// For more information, see https://docs.microsoft.com/en-us/rest/api/storageservices/put-range.
//func (f FileURL) ClearRange(ctx context.Context, offset int64, count int64) (*FileUploadRangeResponse, error) {
//	if offset < 0 {
//		panic("offset must be >= 0")
//	}
//	if count <= 0 {
//		panic("count cannot be CountToEnd, and must be > 0")
//	}
//
//	return f.fileClient.UploadRange(ctx, *toRange(offset, count), FileRangeWriteClear, 0, nil, nil, nil)
//}
//
//// GetRangeList returns the list of valid ranges for a file.
//// Use a count with value CountToEnd (0) to indicate the left part of file start from offset.
//// For more information, see https://docs.microsoft.com/en-us/rest/api/storageservices/list-ranges.
//func (f FileURL) GetRangeList(ctx context.Context, offset int64, count int64) (*Ranges, error) {
//	return f.fileClient.GetRangeList(ctx, nil, nil, (&httpRange{offset: offset, count: count}).pointers())
//}
