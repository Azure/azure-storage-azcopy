package ste

import (
	"net/http"

	"github.com/Azure/azure-storage-azcopy/azbfs"
	"github.com/Azure/azure-storage-blob-go/azblob"
	"github.com/Azure/azure-storage-file-go/azfile"
)

type ErrorEx struct {
	error
}

// TODO: consider rolling MSRequestID into this, so that all places that use this can pick up, and log, the request ID too
func (errex ErrorEx) ErrorCodeAndString() (string, int, string) {
	switch e := interface{}(errex.error).(type) {
	case azblob.StorageError:
		return string(e.ServiceCode()), e.Response().StatusCode, e.Response().Status
	case azfile.StorageError:
		return string(e.ServiceCode()), e.Response().StatusCode, e.Response().Status
	case azbfs.StorageError:
		return string(e.ServiceCode()), e.Response().StatusCode, e.Response().Status
	default:
		return "", 0, errex.Error()
	}
}

type hasResponse interface {
	Response() *http.Response
}

// MSRequestID gets the request ID guid associated with the failed request.
// Returns "" if there isn't one (either no request, or there is a request but it doesn't have the header)
func (errex ErrorEx) MSRequestID() string {
	if respErr, ok := errex.error.(hasResponse); ok {
		r := respErr.Response()
		if r != nil {
			return r.Header.Get("X-Ms-Request-Id")
		}
	}
	return ""
}
