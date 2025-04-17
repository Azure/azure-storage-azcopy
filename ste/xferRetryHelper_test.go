package ste

import (
	"errors"
	"github.com/stretchr/testify/assert"
	"net/http"
	"runtime"
	"syscall"
	"testing"
)

func TestGetShouldRetry(t *testing.T) {
	a := assert.New(t)

	//GetShouldRetry returns nil if RetryStatusCodes is nil
	RetryStatusCodes = nil
	shouldRetry := getShouldRetry(nil)
	a.Nil(shouldRetry)

	// TestGetShouldRetry
	RetryStatusCodes = RetryCodes{409: {"ShareAlreadyExists": {}, "ShareBeingDeleted": {}, "BlobAlreadyExists": {}}, 500: {}, 404: {"BlobNotFound": {}}}
	shouldRetry = getShouldRetry(nil)
	a.NotNil(shouldRetry)

	header := make(http.Header)
	header["x-ms-error-code"] = []string{"BlobAlreadyExists"}
	response := &http.Response{Header: header, StatusCode: 409}
	a.True(shouldRetry(response, nil))

	header = make(http.Header)
	header["x-ms-error-code"] = []string{"ServerBusy"}
	response = &http.Response{Header: header, StatusCode: 500}
	a.True(shouldRetry(response, nil))

	header = make(http.Header)
	header["x-ms-error-code"] = []string{"ServerBusy"}
	response = &http.Response{Header: header, StatusCode: 502}
	a.False(shouldRetry(response, nil))

	header = make(http.Header)
	header["x-ms-error-code"] = []string{"ContainerBeingDeleted"}
	response = &http.Response{Header: header, StatusCode: 409}
	a.False(shouldRetry(response, nil))

	if runtime.GOOS == "windows" {
		rawErr := syscall.Errno(10054) // magic number, in reference to windows.WSAECONNRESET, preventing OS specific shenanigans
		strErr := errors.New("wsarecv: An existing connection was forcibly closed by the remote host.")

		a.True(shouldRetry(nil, rawErr))
		a.True(shouldRetry(nil, strErr))
	}
}

func TestGetErrorCode(t *testing.T) {
	a := assert.New(t)

	// Test with no error code
	header := make(http.Header)
	header["x-ms-meta-foo"] = []string{"bar"}
	response := &http.Response{Header: header}
	code := getErrorCode(response)
	a.Equal("", code)

	// Test with error code
	header = make(http.Header)
	header["x-ms-error-code"] = []string{"BlobAlreadyExists"}
	response = &http.Response{Header: header}
	code = getErrorCode(response)
	a.Equal("BlobAlreadyExists", code)

	// Test with error code
	header = make(http.Header)
	header.Set("x-ms-error-code", "BlobBeingDeleted")
	response = &http.Response{Header: header}
	code = getErrorCode(response)
	a.Equal("BlobBeingDeleted", code)
}

func TestParseRetryCodes(t *testing.T) {
	a := assert.New(t)

	code := ""
	rc, err := ParseRetryCodes(code)
	a.Nil(rc)
	a.Empty(rc)
	a.Nil(err)

	code = "500"
	rc, err = ParseRetryCodes(code)
	a.Nil(err)
	a.Len(rc, 1)
	a.Contains(rc, 500)
	a.Empty(rc[500])

	code = "500; 404:; 403:,"
	rc, err = ParseRetryCodes(code)
	a.Nil(err)
	a.Len(rc, 3)
	a.Contains(rc, 500)
	a.Contains(rc, 404)
	a.Contains(rc, 403)
	a.Empty(rc[500])
	a.Empty(rc[404])
	a.Empty(rc[403])

	code = "409:ShareAlreadyExists, ShareBeingDeleted,BlobAlreadyExists"
	rc, err = ParseRetryCodes(code)
	a.Nil(err)
	a.Len(rc, 1)
	a.Contains(rc, 409)
	a.Len(rc[409], 3)
	a.Contains(rc[409], "ShareAlreadyExists")
	a.Contains(rc[409], "ShareBeingDeleted")
	a.Contains(rc[409], "BlobAlreadyExists")

	code = "409:ShareAlreadyExists, ShareBeingDeleted,BlobAlreadyExists; 500; 404: BlobNotFound"
	rc, err = ParseRetryCodes(code)
	a.Nil(err)
	a.Len(rc, 3)
	a.Contains(rc, 409)
	a.Len(rc[409], 3)
	a.Contains(rc[409], "ShareAlreadyExists")
	a.Contains(rc[409], "ShareBeingDeleted")
	a.Contains(rc[409], "BlobAlreadyExists")
	a.Contains(rc, 500)
	a.Empty(rc[500])
	a.Contains(rc, 404)
	a.Len(rc[404], 1)
	a.Contains(rc[404], "BlobNotFound")
}

func TestParseRetryCodesNegative(t *testing.T) {
	a := assert.New(t)

	code := " ;"
	_, err := ParseRetryCodes(code)
	a.NotNil(err)
	a.Contains(err.Error(), "http status code must be an int")

	code = "a:b:c"
	_, err = ParseRetryCodes(code)
	a.NotNil(err)
	a.Contains(err.Error(), "each status code must be followed by a comma separated list of status codes")

	code = "a"
	_, err = ParseRetryCodes(code)
	a.NotNil(err)
	a.Contains(err.Error(), "http status code must be an int")
}

func TestParseStorageErrorCodes(t *testing.T) {
	a := assert.New(t)

	code := ""
	sec := ParseStorageErrorCodes(code)
	a.Nil(sec)
	a.Empty(sec)

	code = ",   , "
	sec = ParseStorageErrorCodes(code)
	a.Empty(sec)

	code = "ShareAlreadyExists,ShareBeingDeleted,BlobAlreadyExists,ContainerBeingDeleted"
	sec = ParseStorageErrorCodes(code)
	a.NotNil(sec)
	a.Len(sec, 4)
	a.Contains(sec, "ShareAlreadyExists")
	a.Contains(sec, "ShareBeingDeleted")
	a.Contains(sec, "BlobAlreadyExists")
	a.Contains(sec, "ContainerBeingDeleted")

	code = "ShareAlreadyExists,   ShareBeingDeleted,BlobAlreadyExists   ,ContainerBeingDeleted"
	sec = ParseStorageErrorCodes(code)
	a.NotNil(sec)
	a.Len(sec, 4)
	a.Contains(sec, "ShareAlreadyExists")
	a.Contains(sec, "ShareBeingDeleted")
	a.Contains(sec, "BlobAlreadyExists")
	a.Contains(sec, "ContainerBeingDeleted")

	code = "UnsupportedHeader"
	sec = ParseStorageErrorCodes(code)
	a.NotNil(sec)
	a.Len(sec, 1)
	a.Contains(sec, "UnsupportedHeader")

	code = "   UnsupportedHeader   "
	sec = ParseStorageErrorCodes(code)
	a.NotNil(sec)
	a.Len(sec, 1)
	a.Contains(sec, "UnsupportedHeader")

	code = ",   "
	sec = ParseStorageErrorCodes(code)
	a.NotNil(sec)
	a.Len(sec, 0)
}
