package azbfs

import (
	"context"
	"net/http"
)

// DirectoryCreateResponse is the CreatePathResponse response type returned for directory specific operations
// The type is used to establish difference in the response for file and directory operations since both type of
// operations has same response type.
type DirectoryCreateResponse PathCreateResponse

// Response returns the raw HTTP response object.
func (dcr DirectoryCreateResponse) Response() *http.Response {
	return PathCreateResponse(dcr).Response()
}

// StatusCode returns the HTTP status code of the response, e.g. 200.
func (dcr DirectoryCreateResponse) StatusCode() int {
	return PathCreateResponse(dcr).StatusCode()
}

// Status returns the HTTP status message of the response, e.g. "200 OK".
func (dcr DirectoryCreateResponse) Status() string {
	return PathCreateResponse(dcr).Status()
}

// ContentLength returns the value for header Content-Length.
func (dcr DirectoryCreateResponse) ContentLength() int64 {
	return PathCreateResponse(dcr).ContentLength()
}

// Date returns the value for header Date.
func (dcr DirectoryCreateResponse) Date() string {
	return PathCreateResponse(dcr).Date()
}

// ETag returns the value for header ETag.
func (dcr DirectoryCreateResponse) ETag() string {
	return PathCreateResponse(dcr).ETag()
}

// LastModified returns the value for header Last-Modified.
func (dcr DirectoryCreateResponse) LastModified() string {
	return PathCreateResponse(dcr).LastModified()
}

// XMsContinuation returns the value for header x-ms-continuation.
func (dcr DirectoryCreateResponse) XMsContinuation() string {
	return PathCreateResponse(dcr).XMsContinuation()
}

// XMsRequestID returns the value for header x-ms-request-id.
func (dcr DirectoryCreateResponse) XMsRequestID() string {
	return PathCreateResponse(dcr).XMsRequestID()
}

// XMsVersion returns the value for header x-ms-version.
func (dcr DirectoryCreateResponse) XMsVersion() string {
	return PathCreateResponse(dcr).XMsVersion()
}

// DirectoryDeleteResponse is the DeletePathResponse response type returned for directory specific operations
// The type is used to establish difference in the response for file and directory operations since both type of
// operations has same response type.
type DirectoryDeleteResponse PathDeleteResponse

// Response returns the raw HTTP response object.
func (ddr DirectoryDeleteResponse) Response() *http.Response {
	return PathDeleteResponse(ddr).Response()
}

// StatusCode returns the HTTP status code of the response, e.g. 200.
func (ddr DirectoryDeleteResponse) StatusCode() int {
	return PathDeleteResponse(ddr).StatusCode()
}

// Status returns the HTTP status message of the response, e.g. "200 OK".
func (ddr DirectoryDeleteResponse) Status() string {
	return PathDeleteResponse(ddr).Status()
}

// Date returns the value for header Date.
func (ddr DirectoryDeleteResponse) Date() string {
	return PathDeleteResponse(ddr).Date()
}

// XMsContinuation returns the value for header x-ms-continuation.
func (ddr DirectoryDeleteResponse) XMsContinuation() string {
	return PathDeleteResponse(ddr).XMsContinuation()
}

// XMsRequestID returns the value for header x-ms-request-id.
func (ddr DirectoryDeleteResponse) XMsRequestID() string {
	return PathDeleteResponse(ddr).XMsRequestID()
}

// XMsVersion returns the value for header x-ms-version.
func (ddr DirectoryDeleteResponse) XMsVersion() string {
	return PathDeleteResponse(ddr).XMsVersion()
}

// DirectoryGetPropertiesResponse is the GetPathPropertiesResponse response type returned for directory specific operations
// The type is used to establish difference in the response for file and directory operations since both type of
// operations has same response type.
type DirectoryGetPropertiesResponse PathGetPropertiesResponse

// Response returns the raw HTTP response object.
func (dgpr DirectoryGetPropertiesResponse) Response() *http.Response {
	return PathGetPropertiesResponse(dgpr).Response()
}

// StatusCode returns the HTTP status code of the response, e.g. 200.
func (dgpr DirectoryGetPropertiesResponse) StatusCode() int {
	return PathGetPropertiesResponse(dgpr).StatusCode()
}

// Status returns the HTTP status message of the response, e.g. "200 OK".
func (dgpr DirectoryGetPropertiesResponse) Status() string {
	return PathGetPropertiesResponse(dgpr).Status()
}

// AcceptRanges returns the value for header Accept-Ranges.
func (dgpr DirectoryGetPropertiesResponse) AcceptRanges() string {
	return PathGetPropertiesResponse(dgpr).AcceptRanges()
}

// CacheControl returns the value for header Cache-Control.
func (dgpr DirectoryGetPropertiesResponse) CacheControl() string {
	return PathGetPropertiesResponse(dgpr).CacheControl()
}

// ContentDisposition returns the value for header Content-Disposition.
func (dgpr DirectoryGetPropertiesResponse) ContentDisposition() string {
	return PathGetPropertiesResponse(dgpr).ContentDisposition()
}

// ContentEncoding returns the value for header Content-Encoding.
func (dgpr DirectoryGetPropertiesResponse) ContentEncoding() string {
	return PathGetPropertiesResponse(dgpr).ContentEncoding()
}

// ContentLanguage returns the value for header Content-Language.
func (dgpr DirectoryGetPropertiesResponse) ContentLanguage() string {
	return PathGetPropertiesResponse(dgpr).ContentLanguage()
}

// ContentLength returns the value for header Content-Length.
func (dgpr DirectoryGetPropertiesResponse) ContentLength() int64 {
	return PathGetPropertiesResponse(dgpr).ContentLength()
}

// ContentRange returns the value for header Content-Range.
func (dgpr DirectoryGetPropertiesResponse) ContentRange() string {
	return PathGetPropertiesResponse(dgpr).ContentRange()
}

// ContentType returns the value for header Content-Type.
func (dgpr DirectoryGetPropertiesResponse) ContentType() string {
	return PathGetPropertiesResponse(dgpr).ContentType()
}

// Date returns the value for header Date.
func (dgpr DirectoryGetPropertiesResponse) Date() string {
	return PathGetPropertiesResponse(dgpr).Date()
}

// ETag returns the value for header ETag.
func (dgpr DirectoryGetPropertiesResponse) ETag() string {
	return PathGetPropertiesResponse(dgpr).ETag()
}

// LastModified returns the value for header Last-Modified.
func (dgpr DirectoryGetPropertiesResponse) LastModified() string {
	return PathGetPropertiesResponse(dgpr).LastModified()
}

// XMsLeaseDuration returns the value for header x-ms-lease-duration.
func (dgpr DirectoryGetPropertiesResponse) XMsLeaseDuration() string {
	return PathGetPropertiesResponse(dgpr).XMsLeaseDuration()
}

// XMsLeaseState returns the value for header x-ms-lease-state.
func (dgpr DirectoryGetPropertiesResponse) XMsLeaseState() string {
	return PathGetPropertiesResponse(dgpr).XMsLeaseState()
}

// XMsLeaseStatus returns the value for header x-ms-lease-status.
func (dgpr DirectoryGetPropertiesResponse) XMsLeaseStatus() string {
	return PathGetPropertiesResponse(dgpr).XMsLeaseStatus()
}

// XMsProperties returns the value for header x-ms-properties.
func (dgpr DirectoryGetPropertiesResponse) XMsProperties() string {
	return PathGetPropertiesResponse(dgpr).XMsProperties()
}

// XMsRequestID returns the value for header x-ms-request-id.
func (dgpr DirectoryGetPropertiesResponse) XMsRequestID() string {
	return PathGetPropertiesResponse(dgpr).XMsRequestID()
}

// XMsResourceType returns the value for header x-ms-resource-type.
func (dgpr DirectoryGetPropertiesResponse) XMsResourceType() string {
	return PathGetPropertiesResponse(dgpr).XMsResourceType()
}

// XMsVersion returns the value for header x-ms-version.
func (dgpr DirectoryGetPropertiesResponse) XMsVersion() string {
	return PathGetPropertiesResponse(dgpr).XMsVersion()
}

// ContentMD5 returns the value for header Content-MD5.
func (dgpr DirectoryGetPropertiesResponse) ContentMD5() []byte {
	return PathGetPropertiesResponse(dgpr).ContentMD5()
}

// DirectoryListResponse is the ListSchema response type. This type declaration is used to implement useful methods on
// ListPath response
type DirectoryListResponse PathList // TODO: Used to by ListPathResponse. Have I changed it to the right thing?

// Response returns the raw HTTP response object.
func (dlr DirectoryListResponse) Response() *http.Response {
	return PathList(dlr).Response()
}

// StatusCode returns the HTTP status code of the response, e.g. 200.
func (dlr DirectoryListResponse) StatusCode() int {
	return PathList(dlr).StatusCode()
}

// Status returns the HTTP status message of the response, e.g. "200 OK".
func (dlr DirectoryListResponse) Status() string {
	return PathList(dlr).Status()
}

// Date returns the value for header Date.
func (dlr DirectoryListResponse) Date() string {
	return PathList(dlr).Date()
}

// ETag returns the value for header ETag.
func (dlr DirectoryListResponse) ETag() string {
	return PathList(dlr).ETag()
}

// LastModified returns the value for header Last-Modified.
func (dlr DirectoryListResponse) LastModified() string {
	return PathList(dlr).LastModified()
}

// XMsContinuation returns the value for header x-ms-continuation.
func (dlr DirectoryListResponse) XMsContinuation() string {
	return PathList(dlr).XMsContinuation()
}

// XMsRequestID returns the value for header x-ms-request-id.
func (dlr DirectoryListResponse) XMsRequestID() string {
	return PathList(dlr).XMsRequestID()
}

// XMsVersion returns the value for header x-ms-version.
func (dlr DirectoryListResponse) XMsVersion() string {
	return PathList(dlr).XMsVersion()
}

// Files returns the slice of all Files in ListDirectorySegment Response.
// It does not include the sub-directory path
func (dlr *DirectoryListResponse) Files() []Path {
	files := []Path{}
	lSchema := PathList(*dlr)
	for _, path := range lSchema.Paths {
		if path.IsDirectory != nil && *path.IsDirectory {
			continue
		}
		files = append(files, path)
	}
	return files
}

// Directories returns the slice of all directories in ListDirectorySegment Response
// It does not include the files inside the directory only returns the sub-directories
func (dlr *DirectoryListResponse) Directories() []string {
	var dir []string
	lSchema := (PathList)(*dlr)
	for _, path := range lSchema.Paths {
		if path.IsDirectory == nil || (path.IsDirectory != nil && !*path.IsDirectory) {
			continue
		}
		dir = append(dir, *path.Name)
	}
	return dir
}

func (dlr *DirectoryListResponse) FilesAndDirectories() []Path {
	var entities []Path
	lSchema := (PathList)(*dlr)
	for _, path := range lSchema.Paths {
		entities = append(entities, path)
	}
	return entities
}

// DownloadResponse wraps AutoRest generated downloadResponse and helps to provide info for retry.
type DownloadResponse struct {
	dr *ReadResponse

	// Fields need for retry.
	ctx  context.Context
	f    FileURL
	info HTTPGetterInfo
}

// Response returns the raw HTTP response object.
func (dr DownloadResponse) Response() *http.Response {
	return dr.dr.Response()
}

// StatusCode returns the HTTP status code of the response, e.g. 200.
func (dr DownloadResponse) StatusCode() int {
	return dr.dr.StatusCode()
}

// Status returns the HTTP status message of the response, e.g. "200 OK".
func (dr DownloadResponse) Status() string {
	return dr.dr.Status()
}

// AcceptRanges returns the value for header Accept-Ranges.
func (dr DownloadResponse) AcceptRanges() string {
	return dr.dr.AcceptRanges()
}

// CacheControl returns the value for header Cache-Control.
func (dr DownloadResponse) CacheControl() string {
	return dr.dr.CacheControl()
}

// ContentDisposition returns the value for header Content-Disposition.
func (dr DownloadResponse) ContentDisposition() string {
	return dr.dr.ContentDisposition()
}

// ContentEncoding returns the value for header Content-Encoding.
func (dr DownloadResponse) ContentEncoding() string {
	return dr.dr.ContentEncoding()
}

// ContentLanguage returns the value for header Content-Language.
func (dr DownloadResponse) ContentLanguage() string {
	return dr.dr.ContentLanguage()
}

// ContentLength returns the value for header Content-Length.
func (dr DownloadResponse) ContentLength() int64 {
	return dr.dr.ContentLength()
}

// ContentRange returns the value for header Content-Range.
func (dr DownloadResponse) ContentRange() string {
	return dr.dr.ContentRange()
}

// ContentType returns the value for header Content-Type.
func (dr DownloadResponse) ContentType() string {
	return dr.dr.ContentType()
}

// Date returns the value for header Date.
func (dr DownloadResponse) Date() string {
	return dr.dr.Date()
}

// ETag returns the value for header ETag.
func (dr DownloadResponse) ETag() string {
	return dr.dr.ETag()
}

// LastModified returns the value for header Last-Modified.
func (dr DownloadResponse) LastModified() string {
	return dr.dr.LastModified()
}

// RequestID returns the value for header x-ms-request-id.
func (dr DownloadResponse) RequestID() string {
	return dr.dr.XMsRequestID()
}

// Version returns the value for header x-ms-version.
func (dr DownloadResponse) Version() string {
	return dr.dr.XMsVersion()
}
