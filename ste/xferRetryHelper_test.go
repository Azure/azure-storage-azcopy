package ste

import (
	"github.com/stretchr/testify/assert"
	"net/http"
	"testing"
)

//func TestGetShouldRetry(t *testing.T) {
//	a := assert.New(t)
//
//	//GetShouldRetry returns nil if RetryStatusCodes is nil
//	RetryStatusCodes = nil
//	shouldRetry := getShouldRetry()
//	a.Nil(shouldRetry)
//
//	// TestGetShouldRetry
//	RetryStatusCodes = RetryCodes{409: {"ShareAlreadyExists": {}, "ShareBeingDeleted": {}, "BlobAlreadyExists": {}}, 500: {}, 404: {"BlobNotFound": {}}}
//	shouldRetry = getShouldRetry()
//	a.NotNil(shouldRetry)
//
//	header := make(http.Header)
//	header["x-ms-error-code"] = []string{"BlobAlreadyExists"}
//	response := &http.Response{Header: header, StatusCode: 409}
//	a.True(shouldRetry(response, nil))
//
//	header = make(http.Header)
//	header["x-ms-error-code"] = []string{"ServerBusy"}
//	response = &http.Response{Header: header, StatusCode: 500}
//	a.True(shouldRetry(response, nil))
//
//	header = make(http.Header)
//	header["x-ms-error-code"] = []string{"ServerBusy"}
//	response = &http.Response{Header: header, StatusCode: 502}
//	a.False(shouldRetry(response, nil))
//
//	header = make(http.Header)
//	header["x-ms-error-code"] = []string{"ContainerBeingDeleted"}
//	response = &http.Response{Header: header, StatusCode: 409}
//	a.False(shouldRetry(response, nil))
//
//	if runtime.GOOS == "windows" {
//		rawErr := syscall.Errno(10054) // magic number, in reference to windows.WSAECONNRESET, preventing OS specific shenanigans
//		strErr := errors.New("wsarecv: An existing connection was forcibly closed by the remote host.")
//
//		a.True(shouldRetry(nil, rawErr))
//		a.True(shouldRetry(nil, strErr))
//	}
//}

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

	// deltas are always opposite of the default action
	codes := func(defaultAction bool, deltas ...string) StorageErrorCodes {
		out := StorageErrorCodes{
			StorageErrorCodesWildcard: defaultAction,
		}

		for _, v := range deltas {
			out[v] = !defaultAction
		}

		return out
	}

	// input to expected result
	TestMatrix := map[string]RetryCodes{
		"": nil, // no codes
		"500": { // one code
			500: codes(true),
		},
		// several codes with mixed empty data
		"500; 404:; 403:,": {
			500: codes(true),
			404: codes(true),
			403: codes(true),
		},
		// one code with several storage errors
		"409:ShareAlreadyExists, ShareBeingDeleted,BlobAlreadyExists": {
			409: codes(false, "ShareAlreadyExists", "ShareBeingDeleted", "BlobAlreadyExists"),
		},
		// multiple codes with mixed data
		"409:ShareAlreadyExists, ShareBeingDeleted,BlobAlreadyExists; 500; 404: BlobNotFound": {
			409: codes(false, "ShareAlreadyExists", "ShareBeingDeleted", "BlobAlreadyExists"),
			500: codes(true),
			404: codes(false, "BlobNotFound"),
		},
		// redacting a full code
		"400; 500; -400": {
			400: codes(false),
			500: codes(true),
		},
		// redacting part of a code
		"404; 500; -404: BlobNotFound": {
			404: codes(true, "BlobNotFound"),
			500: codes(true),
		},
		// stacking effects
		"-500; 500: FooBar; 500: Baz; 500: Asdf; -500: Asdf": {
			500: codes(false, "FooBar", "Baz"),
		},
	}

	for k, v := range TestMatrix {
		rc, err := ParseRetryCodes(k)
		a.NoError(err, "Parsing error codes `"+k+"`")
		a.Equal(v, rc, "input: `"+k+"`")
	}
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

//func TestParseStorageErrorCodes(t *testing.T) {
//	a := assert.New(t)
//
//	code := ""
//	sec := ParseStorageErrorCodes(code)
//	a.Nil(sec)
//	a.Empty(sec)
//
//	code = ",   , "
//	sec = ParseStorageErrorCodes(code)
//	a.Empty(sec)
//
//	code = "ShareAlreadyExists,ShareBeingDeleted,BlobAlreadyExists,ContainerBeingDeleted"
//	sec = ParseStorageErrorCodes(code)
//	a.NotNil(sec)
//	a.Len(sec, 4)
//	a.Contains(sec, "ShareAlreadyExists")
//	a.Contains(sec, "ShareBeingDeleted")
//	a.Contains(sec, "BlobAlreadyExists")
//	a.Contains(sec, "ContainerBeingDeleted")
//
//	code = "ShareAlreadyExists,   ShareBeingDeleted,BlobAlreadyExists   ,ContainerBeingDeleted"
//	sec = ParseStorageErrorCodes(code)
//	a.NotNil(sec)
//	a.Len(sec, 4)
//	a.Contains(sec, "ShareAlreadyExists")
//	a.Contains(sec, "ShareBeingDeleted")
//	a.Contains(sec, "BlobAlreadyExists")
//	a.Contains(sec, "ContainerBeingDeleted")
//
//	code = "UnsupportedHeader"
//	sec = ParseStorageErrorCodes(code)
//	a.NotNil(sec)
//	a.Len(sec, 1)
//	a.Contains(sec, "UnsupportedHeader")
//
//	code = "   UnsupportedHeader   "
//	sec = ParseStorageErrorCodes(code)
//	a.NotNil(sec)
//	a.Len(sec, 1)
//	a.Contains(sec, "UnsupportedHeader")
//
//	code = ",   "
//	sec = ParseStorageErrorCodes(code)
//	a.NotNil(sec)
//	a.Len(sec, 0)
//}
