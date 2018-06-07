package azbfs

import "net/http"

// DirectoryCreateResponse is the CreatePathResponse response type returned for directory specific operations
// The type is used to establish difference in the response for file and directory operations since both type of
// operations has same response type.
type DirectoryCreateResponse CreatePathResponse

// Response returns the raw HTTP response object.
func (dcr DirectoryCreateResponse) Response() *http.Response {
	return CreatePathResponse(dcr).Response()
}

// StatusCode returns the HTTP status code of the response, e.g. 200.
func (dcr DirectoryCreateResponse) StatusCode() int {
	return CreatePathResponse(dcr).StatusCode()
}

// Status returns the HTTP status message of the response, e.g. "200 OK".
func (dcr DirectoryCreateResponse) Status() string {
	return CreatePathResponse(dcr).Status()
}

// ContentLength returns the value for header Content-Length.
func (dcr DirectoryCreateResponse) ContentLength() string {
	return CreatePathResponse(dcr).ContentLength()
}

// Date returns the value for header Date.
func (dcr DirectoryCreateResponse) Date() string {
	return CreatePathResponse(dcr).Date()
}

// ETag returns the value for header ETag.
func (dcr DirectoryCreateResponse) ETag() string {
	return CreatePathResponse(dcr).ETag()
}

// LastModified returns the value for header Last-Modified.
func (dcr DirectoryCreateResponse) LastModified() string {
	return CreatePathResponse(dcr).LastModified()
}

// XMsContinuation returns the value for header x-ms-continuation.
func (dcr DirectoryCreateResponse) XMsContinuation() string {
	return CreatePathResponse(dcr).XMsContinuation()
}

// XMsRequestID returns the value for header x-ms-request-id.
func (dcr DirectoryCreateResponse) XMsRequestID() string {
	return CreatePathResponse(dcr).XMsRequestID()
}

// XMsVersion returns the value for header x-ms-version.
func (dcr DirectoryCreateResponse) XMsVersion() string {
	return CreatePathResponse(dcr).XMsVersion()
}

// DirectoryDeleteResponse is the DeletePathResponse response type returned for directory specific operations
// The type is used to establish difference in the response for file and directory operations since both type of
// operations has same response type.
type DirectoryDeleteResponse DeletePathResponse

// Response returns the raw HTTP response object.
func (ddr DirectoryDeleteResponse) Response() *http.Response {
	return DeletePathResponse(ddr).Response()
}

// StatusCode returns the HTTP status code of the response, e.g. 200.
func (ddr DirectoryDeleteResponse) StatusCode() int {
	return DeletePathResponse(ddr).StatusCode()
}

// Status returns the HTTP status message of the response, e.g. "200 OK".
func (ddr DirectoryDeleteResponse) Status() string {
	return DeletePathResponse(ddr).Status()
}

// Date returns the value for header Date.
func (ddr DirectoryDeleteResponse) Date() string {
	return DeletePathResponse(ddr).Date()
}

// XMsContinuation returns the value for header x-ms-continuation.
func (ddr DirectoryDeleteResponse) XMsContinuation() string {
	return DeletePathResponse(ddr).XMsContinuation()
}

// XMsRequestID returns the value for header x-ms-request-id.
func (ddr DirectoryDeleteResponse) XMsRequestID() string {
	return DeletePathResponse(ddr).XMsRequestID()
}

// XMsVersion returns the value for header x-ms-version.
func (ddr DirectoryDeleteResponse) XMsVersion() string {
	return DeletePathResponse(ddr).XMsVersion()
}

// DirectoryGetPropertiesResponse is the GetPathPropertiesResponse response type returned for directory specific operations
// The type is used to establish difference in the response for file and directory operations since both type of
// operations has same response type.
type DirectoryGetPropertiesResponse GetPathPropertiesResponse

// Response returns the raw HTTP response object.
func (dgpr DirectoryGetPropertiesResponse) Response() *http.Response {
	return GetPathPropertiesResponse(dgpr).Response()
}

// StatusCode returns the HTTP status code of the response, e.g. 200.
func (dgpr DirectoryGetPropertiesResponse) StatusCode() int {
	return GetPathPropertiesResponse(dgpr).StatusCode()
}

// Status returns the HTTP status message of the response, e.g. "200 OK".
func (dgpr DirectoryGetPropertiesResponse) Status() string {
	return GetPathPropertiesResponse(dgpr).Status()
}

// AcceptRanges returns the value for header Accept-Ranges.
func (dgpr DirectoryGetPropertiesResponse) AcceptRanges() string {
	return GetPathPropertiesResponse(dgpr).AcceptRanges()
}

// CacheControl returns the value for header Cache-Control.
func (dgpr DirectoryGetPropertiesResponse) CacheControl() string {
	return GetPathPropertiesResponse(dgpr).CacheControl()
}

// ContentDisposition returns the value for header Content-Disposition.
func (dgpr DirectoryGetPropertiesResponse) ContentDisposition() string {
	return GetPathPropertiesResponse(dgpr).ContentDisposition()
}

// ContentEncoding returns the value for header Content-Encoding.
func (dgpr DirectoryGetPropertiesResponse) ContentEncoding() string {
	return GetPathPropertiesResponse(dgpr).ContentEncoding()
}

// ContentLanguage returns the value for header Content-Language.
func (dgpr DirectoryGetPropertiesResponse) ContentLanguage() string {
	return GetPathPropertiesResponse(dgpr).ContentLanguage()
}

// ContentLength returns the value for header Content-Length.
func (dgpr DirectoryGetPropertiesResponse) ContentLength() string {
	return GetPathPropertiesResponse(dgpr).ContentLength()
}

// ContentRange returns the value for header Content-Range.
func (dgpr DirectoryGetPropertiesResponse) ContentRange() string {
	return GetPathPropertiesResponse(dgpr).ContentRange()
}

// ContentType returns the value for header Content-Type.
func (dgpr DirectoryGetPropertiesResponse) ContentType() string {
	return GetPathPropertiesResponse(dgpr).ContentType()
}

// Date returns the value for header Date.
func (dgpr DirectoryGetPropertiesResponse) Date() string {
	return GetPathPropertiesResponse(dgpr).Date()
}

// ETag returns the value for header ETag.
func (dgpr DirectoryGetPropertiesResponse) ETag() string {
	return GetPathPropertiesResponse(dgpr).ETag()
}

// LastModified returns the value for header Last-Modified.
func (dgpr DirectoryGetPropertiesResponse) LastModified() string {
	return GetPathPropertiesResponse(dgpr).LastModified()
}

// XMsLeaseDuration returns the value for header x-ms-lease-duration.
func (dgpr DirectoryGetPropertiesResponse) XMsLeaseDuration() string {
	return GetPathPropertiesResponse(dgpr).XMsLeaseDuration()
}

// XMsLeaseState returns the value for header x-ms-lease-state.
func (dgpr DirectoryGetPropertiesResponse) XMsLeaseState() string {
	return GetPathPropertiesResponse(dgpr).XMsLeaseState()
}

// XMsLeaseStatus returns the value for header x-ms-lease-status.
func (dgpr DirectoryGetPropertiesResponse) XMsLeaseStatus() string {
	return GetPathPropertiesResponse(dgpr).XMsLeaseStatus()
}

// XMsProperties returns the value for header x-ms-properties.
func (dgpr DirectoryGetPropertiesResponse) XMsProperties() string {
	return GetPathPropertiesResponse(dgpr).XMsProperties()
}

// XMsRequestID returns the value for header x-ms-request-id.
func (dgpr DirectoryGetPropertiesResponse) XMsRequestID() string {
	return GetPathPropertiesResponse(dgpr).XMsRequestID()
}

// XMsResourceType returns the value for header x-ms-resource-type.
func (dgpr DirectoryGetPropertiesResponse) XMsResourceType() string {
	return GetPathPropertiesResponse(dgpr).XMsResourceType()
}

// XMsVersion returns the value for header x-ms-version.
func (dgpr DirectoryGetPropertiesResponse) XMsVersion() string {
	return GetPathPropertiesResponse(dgpr).XMsVersion()
}

// DirectoryListResponse is the ListSchema response type. This type declaration is used to implement useful methods on
// ListPath response
type DirectoryListResponse ListSchema

// Response returns the raw HTTP response object.
func (dlr DirectoryListResponse) Response() *http.Response {
	return ListSchema(dlr).Response()
}

// StatusCode returns the HTTP status code of the response, e.g. 200.
func (dlr DirectoryListResponse) StatusCode() int {
	return ListSchema(dlr).StatusCode()
}

// Status returns the HTTP status message of the response, e.g. "200 OK".
func (dlr DirectoryListResponse) Status() string {
	return ListSchema(dlr).Status()
}

// Date returns the value for header Date.
func (dlr DirectoryListResponse) Date() string {
	return ListSchema(dlr).Date()
}

// ETag returns the value for header ETag.
func (dlr DirectoryListResponse) ETag() string {
	return ListSchema(dlr).ETag()
}

// LastModified returns the value for header Last-Modified.
func (dlr DirectoryListResponse) LastModified() string {
	return ListSchema(dlr).LastModified()
}

// XMsContinuation returns the value for header x-ms-continuation.
func (dlr DirectoryListResponse) XMsContinuation() string {
	return ListSchema(dlr).XMsContinuation()
}

// XMsRequestID returns the value for header x-ms-request-id.
func (dlr DirectoryListResponse) XMsRequestID() string {
	return ListSchema(dlr).XMsRequestID()
}

// XMsVersion returns the value for header x-ms-version.
func (dlr DirectoryListResponse) XMsVersion() string {
	return ListSchema(dlr).XMsVersion()
}

// Files returns the slice of all Files in ListDirectory Response.
// It does not include the sub-directory path
func (dlr *DirectoryListResponse) Files() []string {
	files := []string{}
	lSchema :=  ListSchema(*dlr)
	for _, path := range lSchema.Paths {
		if path.IsDirectory != nil && *path.IsDirectory {
			continue
		}
		files = append(files, *path.Name)
	}
	return files
}

// Directories returns the slice of all directories in ListDirectory Response
// It does not include the files inside the directory only returns the sub-directories
func (dlr *DirectoryListResponse) Directories() []string {
	var dir []string
	lSchema :=  (ListSchema)(*dlr)
	for _, path := range lSchema.Paths {
		if path.IsDirectory == nil || (path.IsDirectory != nil && !*path.IsDirectory) {
			continue
		}
		dir = append(dir, *path.Name)
	}
	return dir
}