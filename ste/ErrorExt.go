package ste

import (
	"errors"
	"net/http"

	"github.com/Azure/azure-sdk-for-go/sdk/azcore"
)

type ErrorEx struct {
	error
}

// TODO: consider rolling MSRequestID into this, so that all places that use this can pick up, and log, the request ID too
func (errex ErrorEx) ErrorCodeAndString() (string, int, string) {
	var respErr *azcore.ResponseError
	if errors.As(errex.error, &respErr) {
		return respErr.ErrorCode, respErr.StatusCode, respErr.RawResponse.Status
	}
	return "", 0, errex.Error()

}

type hasResponse interface {
	Response() *http.Response
}

// MSRequestID gets the request ID guid associated with the failed request.
// Returns "" if there isn't one (either no request, or there is a request but it doesn't have the header)
func (errex ErrorEx) MSRequestID() string {
	var respErr *azcore.ResponseError
	if errors.As(errex.error, &respErr) {
		return respErr.RawResponse.Header.Get("x-ms-request-id")
	}
	if respErr, ok := errex.error.(hasResponse); ok {
		r := respErr.Response()
		if r != nil {
			return r.Header.Get("X-Ms-Request-Id")
		}
	}
	return ""
}
