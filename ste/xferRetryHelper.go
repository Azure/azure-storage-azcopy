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
	"github.com/Azure/azure-storage-azcopy/v10/common"
	"net/http"
	"strconv"
	"strings"
	"syscall"
)

// Defines the retry policy rules
var RetryStatusCodes RetryCodes

var platformRetriedErrnos []syscall.Errno

type RetryFunc = func(*http.Response, error) bool

func IsOSErrors(err error, errnos ...syscall.Errno) bool {
	if err == nil {
		return false
	}

	for _, v := range errnos {
		if errors.Is(err, v) ||
			strings.Contains(strings.ToLower(err.Error()), strings.ToLower(v.Error())) {
			return true
		}
	}

	return false
}

func GetShouldRetry(log *LogOptions) RetryFunc {
	if len(RetryStatusCodes) == 0 {
		return nil
	}

	return func(resp *http.Response, err error) bool {
		if resp != nil {
			if storageErrorCodes, ok := RetryStatusCodes[resp.StatusCode]; ok {
				// compare to status codes
				errorCodes := getErrorCodes(resp)
				for _, errorCode := range errorCodes {
					if policy, ok := storageErrorCodes[errorCode]; ok {
						if policy && log != nil && log.ShouldLog(common.ELogLevel.Debug()) {
							log.Log(
								common.ELogLevel.Debug(),
								fmt.Sprintf("Request %s retried on custom condition %s",
									resp.Header.Get("x-ms-client-request-id"),
									errorCode))
						}

						if policy {
							return policy
						}
					} else if !ok && storageErrorCodes["*"] {
						return true
					}

				}
			}
			// Check if copy source status code is present
			respStatusCode := getCopySourceStatusCode(resp)
			if respStatusCode != "" {
				if copyStatusCode, err := strconv.Atoi(respStatusCode); err == nil {
					if _, exists := RetryStatusCodes[copyStatusCode]; exists {
						if log != nil && log.ShouldLog(common.ELogLevel.Debug()) {
							log.Log(
								common.ELogLevel.Debug(),
								fmt.Sprintf("Request %s retried on copy source status code %s",
									resp.Header.Get("x-ms-client-request-id"), respStatusCode))
						}
						return true
					}
				}
			}
		}

		// Check if we're in a retriable error defined by the OS specific file
		if len(platformRetriedErrnos) > 0 &&
			IsOSErrors(err, platformRetriedErrnos...) {
			return true
		}

		return false
	}
}

func getErrorCodes(resp *http.Response) []string {
	// There can be multiple error code headers per response
	var errorCodes []string
	if resp.Header["x-ms-error-code"] != nil { //nolint:staticcheck
		errorCodes = append(errorCodes, resp.Header["x-ms-error-code"][0]) //nolint:staticcheck
	} else if resp.Header["X-Ms-Error-Code"] != nil { //nolint:staticcheck
		errorCodes = append(errorCodes, resp.Header["X-Ms-Error-Code"][0]) //nolint:staticcheck
	}
	if resp.Header["x-ms-copy-source-error-code"] != nil { //nolint:staticcheck
		errorCodes = append(errorCodes, resp.Header["x-ms-copy-source-error-code"][0]) //nolint:staticcheck
	} else if resp.Header["X-Ms-Copy-Source-Error-Code"] != nil { //nolint:staticcheck
		errorCodes = append(errorCodes, resp.Header["X-Ms-Copy-Source-Error-Code"][0]) //nolint:staticcheck
	}
	return errorCodes
}

func getCopySourceStatusCode(resp *http.Response) string {
	if resp.Header["x-ms-copy-source-status-code"] != nil { //nolint:staticcheck
		return resp.Header["x-ms-copy-source-status-code"][0] //nolint:staticcheck
	} else if resp.Header["X-Ms-Copy-Source-Status-Code"] != nil { //nolint:staticcheck
		return resp.Header["X-Ms-Copy-Source-Status-Code"][0] //nolint:staticcheck
	}
	return ""
}

type StorageErrorCodes map[string]bool // where map[string]bool is the set of storage error codes; true = retry,  = no retry

/*
	Prior to adjusting parsing to allow for the *removal* of codes, default functionality was:
		- If it was specified without storage codes, retry all storage codes.
		- If it was specified with storage codes, retry only the specific codes.

	When adding the ability to remove retry cases, new functionality logically adds
		- If it was specified without storage codes, remove that entire code from the blacklist.
		- If it was specified with storage codes, remove those specific codes from the blacklist.

	But what if the HTTP code already exists and we're just trying to ignore one storage code?
	What if our default policy targets a specific code we want to ignore?

	Introducing, the humble wildcard policy. Instead of creating a nil StorageErrorCodes, one with a wildcard policy will be created.
	Specifying a positive code alone sets this policy to true, with negations applying.
	Specifying a negative code alone sets this policy to false, with additions applying.

	imagine:
	500; -500: FooBarError

	All 500 errors would be retried, except FooBarError.

	500; -500

	No 500 errors would be retried.

	500: FooBarError, BazError; -500: BazError

	Only FooBarError would be retried.
*/

const StorageErrorCodesWildcard = "*"

func (s StorageErrorCodes) GetWildcardPolicy() bool {
	return s[StorageErrorCodesWildcard]
}

func (s StorageErrorCodes) SetWildcardPolicy(policy bool) {
	s[StorageErrorCodesWildcard] = policy
}

type RetryCodes map[int]StorageErrorCodes // where int is the HTTP status code

// ParseRetryCodes takes a string and returns a RetryCodes object
// Format: <http status code>: <storage error code>, <storage error code>; <http status code>: <storage error code>; <http status code>
// Remove entire status codes with a negative status code.
// Remove storage error codes by specifying them under a status code.
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

			doRetry := httpStatusCode >= 0
			if !doRetry { // revert the status code to the real one
				httpStatusCode = -httpStatusCode
			}

			var parsedErrorCodes = make(StorageErrorCodes)
			switch { // Use a switch so we can fallthrough and avoid code dupe
			case len(tuple) == 2:
				if p := ParseStorageErrorCodes(tuple[1], doRetry); len(p) > 0 {
					// catching something means we have a non-nil output, copy it into our parsed field
					parsedErrorCodes = p
					// If we have delta objects, this means our default retry policy _opposes_ them.
					// e.g. `500: FooBarError` means only retry FooBarError, do not retry everything else.
					doRetry = !doRetry
				}

				fallthrough // Write the wildcard in case it gets used
			default:
				parsedErrorCodes[StorageErrorCodesWildcard] = doRetry
			}

			// MergeStorageErrorCodes will ignore the extra wildcard if it isn't necessary
			rcs[httpStatusCode] = MergeStorageErrorCodes(rcs[httpStatusCode], parsedErrorCodes)
		}
	}
	return rcs, nil
}

func MergeStorageErrorCodes(original, delta StorageErrorCodes) StorageErrorCodes {
	if original == nil { // if the original didn't exist, insert the delta.
		return delta
	}

	originalWildcard := original[StorageErrorCodesWildcard]

	// if only the wildcard was passed, overwrite with the delta.
	if _, deltaIncludesWildcard := delta[StorageErrorCodesWildcard]; len(delta) == 1 && deltaIncludesWildcard {
		return delta
	}

	out := make(StorageErrorCodes)
	// In the final case, we have a "real" delta. We'll ignore the delta wildcard,
	// and add or replace the additional values in opposition with the original wildcard.

	// First, clone the old data.
	for k, v := range original {
		out[k] = v
	}

	// Then, apply the delta.
	for k, v := range delta {
		if k == StorageErrorCodesWildcard {
			continue // ignore the new wildcard, that's only intended for full replaces.
		}

		_, originalPresent := out[k]

		// if we overlap into the original wildcard, remove the code.
		if v == originalWildcard {
			if originalPresent {
				delete(out, k)
			}
		} else { // If we go against the grain, write it.
			out[k] = v
		}
	}

	return out
}

// ParseStorageErrorCodes takes a string and returns a StorageErrorCodes object
// Format: comma separated list of strings that represent storage error codes
func ParseStorageErrorCodes(s string, doRetry bool) StorageErrorCodes {
	s = strings.Trim(s, " ")
	if len(s) == 0 {
		return nil
	}
	codes := strings.Split(s, ",")
	secs := make(StorageErrorCodes)
	for _, code := range codes {
		code = strings.Trim(code, " ")
		if len(code) != 0 {
			secs[code] = doRetry
		}
	}
	return secs
}
