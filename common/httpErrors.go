// Copyright © 2017 Microsoft <wastore@microsoft.com>
//
// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files (the "Software"), to deal
// in the Software without restriction, including without limitation the rights
// to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
// copies of the Software, and to permit persons to whom the Software is
// furnished to do so, subject to the following conditions:
//
// The above copyright notice and this permission notice shall be included in
// all copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
// THE SOFTWARE.

package common

import (
	"fmt"
	"io"
	"net/http"
	"strings"
	"sync"
)

// ==============================================================================================
// HTTP Error Handling
// ==============================================================================================

// Buffer pool for reading response bodies to reduce GC pressure
var responseBodyBufferPool = sync.Pool{
	New: func() interface{} {
		// Pre-allocate 4KB buffers
		buf := make([]byte, 4096)
		return &buf
	},
}

// HTTPStatusError represents a generic HTTP error with status code
// This is protocol-agnostic and doesn't depend on S3 or any specific service
type HTTPStatusError struct {
	StatusCode  int
	Status      string
	RawBody     string
	IsClientErr bool // 4xx errors
	IsServerErr bool // 5xx errors
	IsRetryable bool // Whether this error should trigger a retry
}

// DetectHTTPStatusError checks if the HTTP response indicates an error and parses it
// Returns nil if the status code is 2xx (success)
// Optimized to read response body only once and reuse bytes for parsing
func DetectHTTPStatusError(resp *http.Response) *HTTPStatusError {
	if resp == nil {
		return nil
	}

	// Status codes 200-299 are successful
	if resp.StatusCode >= 200 && resp.StatusCode < 300 {
		return nil
	}

	httpErr := &HTTPStatusError{
		StatusCode:  resp.StatusCode,
		Status:      resp.Status,
		IsClientErr: resp.StatusCode >= 400 && resp.StatusCode < 500,
		IsServerErr: resp.StatusCode >= 500 && resp.StatusCode < 600,
	}

	// Determine if the error is retryable
	// Generally, 5xx server errors and some 4xx errors are retryable
	httpErr.IsRetryable = httpErr.IsServerErr ||
		resp.StatusCode == 408 || // Request Timeout
		resp.StatusCode == 429 || // Too Many Requests
		resp.StatusCode == 503 || // Service Unavailable
		resp.StatusCode == 504 // Gateway Timeout

	// Try to read the response body (optimized with buffer pool)
	if resp.Body != nil {
		// Get buffer from pool
		bufPtr := responseBodyBufferPool.Get().(*[]byte)
		defer responseBodyBufferPool.Put(bufPtr)

		buf := *bufPtr
		n, err := io.ReadFull(resp.Body, buf)

		// ReadFull returns ErrUnexpectedEOF if body is smaller than buffer
		// This is expected and not an error condition
		if err != nil && err != io.ErrUnexpectedEOF && err != io.EOF {
			// Real read error, but continue with partial data if any
			if n == 0 {
				return httpErr
			}
		}

		if n > 0 {
			bodyBytes := buf[:n]
			httpErr.RawBody = string(bodyBytes)
		}
	}

	return httpErr
}

// String formats the HTTPStatusError for logging
func (e *HTTPStatusError) String() string {
	if e == nil {
		return ""
	}

	errType := "Unknown"
	if e.IsClientErr {
		errType = "Client Error"
	} else if e.IsServerErr {
		errType = "Server Error"
	}

	if e.RawBody != "" {
		return fmt.Sprintf("HTTP %d (%s) - %s: %s",
			e.StatusCode, errType, e.Status, strings.TrimSpace(e.RawBody))
	}

	return fmt.Sprintf("HTTP %d (%s) - %s", e.StatusCode, errType, e.Status)
}

// GetErrorCode returns the error code for tracking
// Returns S3 error code if available, otherwise HTTP status code
func (e *HTTPStatusError) GetErrorCode() int {
	if e == nil {
		return 0
	}
	return e.StatusCode
}

// GetErrorMessage returns the error message for tracking
// Returns the raw body if available, otherwise HTTP status
func (e *HTTPStatusError) GetErrorMessage() string {
	if e == nil {
		return ""
	}

	if e.RawBody != "" {
		return strings.TrimSpace(e.RawBody)
	}

	return e.Status
}
