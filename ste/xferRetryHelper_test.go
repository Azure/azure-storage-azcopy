package ste

import (
	"errors"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/bloberror"
	"github.com/stretchr/testify/assert"
	"net/http"
	"runtime"
	"syscall"
	"testing"
)

func TestGetShouldRetry(t *testing.T) {
	type ResponseRetryPair struct {
		Resp        *http.Response
		Err         error
		ShouldRetry bool
	}
	type TestEntry struct {
		TestCond func() bool

		Rules RetryCodes
		Tests []*ResponseRetryPair // pointers so they can be excluded by OS
		// CustomTest overrides Tests
		CustomTest func(t *testing.T, retryFunc RetryFunc)
	}

	ParseRules := func(in string) RetryCodes {
		o, err := ParseRetryCodes(in)
		assert.NoErrorf(t, err, "failed to parse rule `%s`", in)
		return o
	}

	RespTest := func(status int, code string, retry bool) *ResponseRetryPair {
		return &ResponseRetryPair{
			Resp: &http.Response{
				StatusCode: status,
				Header: map[string][]string{
					"x-ms-error-code": {code},
				},
			},
			ShouldRetry: retry,
		}
	}

	ErrorTest := func(err error, retry bool) *ResponseRetryPair {
		return &ResponseRetryPair{
			Err:         err,
			ShouldRetry: retry,
		}
	}

	_ = ParseRules

	matrix := []TestEntry{
		{ // porting the original test in
			Rules: nil,
			CustomTest: func(t *testing.T, retryFunc RetryFunc) {
				assert.Nil(t, retryFunc)
			},
		},
		{
			Rules: ParseRules("409: ShareAlreadyExists, ShareBeingDeleted, BlobAlreadyExists; 500; 404: BlobNotFound"),
			Tests: []*ResponseRetryPair{
				RespTest(409, string(bloberror.BlobAlreadyExists), true),
				RespTest(500, string(bloberror.ServerBusy), true),  // matches 500 cond
				RespTest(502, string(bloberror.ServerBusy), false), // does not match status code
				RespTest(409, string(bloberror.ContainerBeingDeleted), false),
			},
		},
		{
			TestCond: func() bool {
				return runtime.GOOS == "windows"
			},
			Rules: ParseRules("409: ShareAlreadyExists, ShareBeingDeleted, BlobAlreadyExists; 500; 404: BlobNotFound"),
			Tests: []*ResponseRetryPair{
				ErrorTest(syscall.Errno(10054), true),
				ErrorTest(errors.New("wsarecv: An existing connection was forcibly closed by the remote host."), true),
				ErrorTest(syscall.Errno(10048), true),
				ErrorTest(errors.New("Only one usage of each socket address (protocol/network address/port) is normally permitted."), true),
				ErrorTest(syscall.Errno(10060), true),
				ErrorTest(errors.New("A connection attempt failed because the connected party did not properly respond after a period of time, or established connection failed because connected host has failed to respond."), true),
			},
		},
		{ // test full code removal
			Rules: ParseRules("400; 500; -400"),
			Tests: []*ResponseRetryPair{
				RespTest(400, "asdf", false),
				RespTest(500, "asdf", true),
			},
		},
		{ // Partial code removal
			Rules: ParseRules("400: foo, bar; -400: foo"),
			Tests: []*ResponseRetryPair{
				RespTest(400, "foo", false),
				RespTest(400, "bar", true),
			},
		},
		{ // Inverse code removal
			Rules: ParseRules("-400: foo"),
			Tests: []*ResponseRetryPair{
				RespTest(400, "foo", false),
				RespTest(400, "bar", true),
				RespTest(400, "asdf", true),
			},
		},
	}

	for entryNum, v := range matrix {
		if v.TestCond != nil && !v.TestCond() {
			continue // ignore this test
		}

		RetryStatusCodes = v.Rules
		shouldRetry := GetShouldRetry(nil)

		if v.CustomTest != nil {
			v.CustomTest(t, shouldRetry)
		}

		for testNum, v := range v.Tests {
			res := shouldRetry(v.Resp, v.Err)
			assert.Equalf(t, v.ShouldRetry, res, "expected result mismatch on entry %d test %d \n %v", entryNum, testNum,
				matrix[entryNum].Tests[testNum])
		}
	}
}

func TestGetErrorCode(t *testing.T) {
	a := assert.New(t)

	// Test with no error code
	header := make(http.Header)
	header["x-ms-meta-foo"] = []string{"bar"}
	response := &http.Response{Header: header}
	codeNil := getErrorCodes(response)
	a.Nil(codeNil)

	// Test with error code
	header = make(http.Header)
	header["x-ms-error-code"] = []string{"BlobAlreadyExists"}
	response = &http.Response{Header: header}
	code := getErrorCodes(response)[0]
	a.Equal("BlobAlreadyExists", code)

	// Test with error code
	header = make(http.Header)
	header.Set("x-ms-error-code", "BlobBeingDeleted")
	response = &http.Response{Header: header}
	code = getErrorCodes(response)[0]
	a.Equal("BlobBeingDeleted", code)

	// Test with copy source error code
	header = make(http.Header)
	header.Set("x-ms-copy-source-error-code", "CopySourceErrCode")
	response = &http.Response{Header: header}
	code = getErrorCodes(response)[0]
	a.Equal("CopySourceErrCode", code)

	// Test with error code and copy source error code
	header = make(http.Header)
	header.Set("X-Ms-Copy-Source-Error-Code", "AccessDenied")
	header.Set("x-ms-error-code", "CouldNotVerifyCopySource")
	response = &http.Response{Header: header}
	codes := getErrorCodes(response)
	a.Equal([]string{"CouldNotVerifyCopySource", "AccessDenied"}, codes)

	// Test with wrong header and copy source error code
	header = make(http.Header)
	header.Set("X-Ms-Copy-Source-Error-Code", "AccessDenied")
	header.Set("x-ms-foo-bar", "RandomErrCode")
	response = &http.Response{Header: header}
	codes = getErrorCodes(response)
	a.Equal([]string{"AccessDenied"}, codes)
}

func TestGetCopySourceStatusCodes(t *testing.T) {
	a := assert.New(t)

	// Test with no status code
	header := make(http.Header)
	header.Set("x-ms-foo-bar", "200")
	response := &http.Response{Header: header}
	code := getCopySourceStatusCode(response)
	a.Empty(code)

	// Test with copy source status code
	header = make(http.Header)
	header.Set("x-ms-copy-source-status-code", "408")
	response = &http.Response{Header: header}
	code = getCopySourceStatusCode(response)
	a.Equal("408", code)

	// Test with copy source status code
	header = make(http.Header)
	header.Set("X-Ms-Copy-Source-Status-Code", "429")
	response = &http.Response{Header: header}
	code = getCopySourceStatusCode(response)
	a.Equal("429", code)
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
