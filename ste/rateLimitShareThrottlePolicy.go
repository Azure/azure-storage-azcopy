// Copyright © Microsoft <wastore@microsoft.com>
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
	"fmt"
	"net/http"
	"strconv"

	"github.com/Azure/azure-sdk-for-go/sdk/azcore/policy"
	"github.com/Azure/azure-storage-azcopy/v10/common"
)

// storageThrottlePolicy feeds Azure Files and registered destination Blob
// responses into their resource controllers. Blob requests also consume one
// account TPS token per retry attempt.
type storageThrottlePolicy struct{}

func newStorageThrottlePolicy() policy.Policy { return storageThrottlePolicy{} }

func newAzureFilesThrottlePolicy() policy.Policy { return newStorageThrottlePolicy() }

func (storageThrottlePolicy) Do(req *policy.Request) (*http.Response, error) {
	rawURL := req.Raw().URL.String()
	shareKey := common.ShareKeyFromRawURL(rawURL)
	blobKey := common.BlobAccountKeyFromRawURL(rawURL)
	blobPacer := common.GetBlobPacer(rawURL)
	if blobPacer != nil {
		if err := blobPacer.AcquireIO(req.Raw().Context(), 0, 1); err != nil {
			return nil, fmt.Errorf("acquiring destination Blob account TPS token: %w", err)
		}
	}

	response, err := req.Next()
	if response == nil {
		return response, err
	}

	if shareKey == "" && (blobKey == "" || blobPacer == nil) {
		return response, err
	}

	var body string
	if response.StatusCode == http.StatusServiceUnavailable || response.StatusCode == http.StatusTooManyRequests {
		body = transparentlyReadBody(response)
	}

	kind := classifyAzureFilesThrottle(response.StatusCode, body)
	retryAfter := parseRetryAfterSeconds(response)
	if shareKey != "" {
		common.HandleShareResponse(shareKey, kind, retryAfter)
	}
	if blobPacer != nil {
		common.HandleBlobResponse(blobKey, kind, retryAfter)
	}
	return response, err
}

// parseRetryAfterSeconds returns the Retry-After header value in seconds, or 0
// when absent/unparseable. Only the delta-seconds form is honored (the
// HTTP-date form is uncommon for Azure Storage throttling responses).
func parseRetryAfterSeconds(resp *http.Response) float64 {
	if resp == nil {
		return 0
	}
	v := resp.Header.Get("Retry-After")
	if v == "" {
		return 0
	}
	if secs, err := strconv.ParseFloat(v, 64); err == nil && secs > 0 {
		return secs
	}
	return 0
}
