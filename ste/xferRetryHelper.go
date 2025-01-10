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

package ste

import (
	"errors"
	"fmt"
	"net/http"
	"strconv"
	"strings"
)

var RetryStatusCodes RetryCodes

func getShouldRetry() func(*http.Response, error) bool {
	if len(RetryStatusCodes) == 0 {
		return nil
	}
	return func(resp *http.Response, err error) bool {
		if storageErrorCodes, ok := RetryStatusCodes[resp.StatusCode]; ok {
			// no status codes specified to compare to
			if len(storageErrorCodes) == 0 {
				return true
			}
			// compare to status codes
			errorCode := getErrorCode(resp)
			if errorCode != "" {
				if _, ok = storageErrorCodes[errorCode]; ok {
					return true
				}
			}
		}

		// This is probably something in between us and azure storage dropping our traffic.
		// Should be retryable, since it's possibly a one-off network error.
		if strings.Contains(err.Error(), "wsarecv: An existing connection was forcibly closed by the remote host.") {
			return true
		}

		return false
	}
}

func getErrorCode(resp *http.Response) string {
	if resp.Header["x-ms-error-code"] != nil { //nolint:staticcheck
		return resp.Header["x-ms-error-code"][0] //nolint:staticcheck
	} else if resp.Header["X-Ms-Error-Code"] != nil {
		return resp.Header["X-Ms-Error-Code"][0]
	}
	return ""
}

type StorageErrorCodes map[string]struct{} // where map[string]struct{} is the set of storage error codes
type RetryCodes map[int]StorageErrorCodes  // where int is the HTTP status code

// ParseRetryCodes takes a string and returns a RetryCodes object
// Format: <http status code>: <storage error code>, <storage error code>; <http status code>: <storage error code>; <http status code>
func ParseRetryCodes(s string) (RetryCodes, error) {
	if len(s) == 0 {
		return nil, nil
	}
	rcs := make(RetryCodes)
	codes := strings.Split(s, ";")
	for _, code := range codes {
		code = strings.Trim(code, " ")
		tuple := strings.Split(code, ":")
		// tuple must have at least one element
		if len(tuple) > 2 {
			return nil, errors.New("invalid retry code format, each status code must be followed by a comma separated list of status codes")
		} else {
			// first element must be an int
			c := strings.Trim(tuple[0], " ")
			httpStatusCode, err := strconv.Atoi(c)
			if err != nil {
				return nil, fmt.Errorf("invalid retry code format, http status code must be an int (%s)", err)
			}
			if len(tuple) == 1 {
				rcs[httpStatusCode] = nil
			} else if len(tuple) == 2 {
				rcs[httpStatusCode] = ParseStorageErrorCodes(tuple[1])
			}
		}
	}
	return rcs, nil
}

// ParseStorageErrorCodes takes a string and returns a StorageErrorCodes object
// Format: comma separated list of strings that represent storage error codes
func ParseStorageErrorCodes(s string) StorageErrorCodes {
	s = strings.Trim(s, " ")
	if len(s) == 0 {
		return nil
	}
	codes := strings.Split(s, ",")
	secs := make(StorageErrorCodes)
	for _, code := range codes {
		code = strings.Trim(code, " ")
		if len(code) != 0 {
			secs[code] = struct{}{}
		}
	}
	return secs
}
