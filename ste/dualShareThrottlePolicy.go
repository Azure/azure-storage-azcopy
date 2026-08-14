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
	"net/http"
	"strconv"

	"github.com/Azure/azure-sdk-for-go/sdk/azcore/policy"
	"github.com/Azure/azure-storage-azcopy/v10/common"
)

// azureFilesThrottlePolicy is a per-retry pipeline policy that feeds every
// Azure Files HTTP outcome into that share's DualResourceController fast path
// (HandleResponse). This is the real-time reactive signal that lets a share's
// bandwidth/IOPS targets drop before the next ~30s GetShareStats poll.
//
// It is shared across all clients (blob included) but is a cheap no-op for any
// request that is not an Azure Files endpoint, or whose share has no controller
// registered yet.
type azureFilesThrottlePolicy struct{}

func newAzureFilesThrottlePolicy() policy.Policy { return azureFilesThrottlePolicy{} }

func (azureFilesThrottlePolicy) Do(req *policy.Request) (*http.Response, error) {
	response, err := req.Next()
	if response == nil {
		return response, err
	}

	shareKey := common.ShareKeyFromRawURL(req.Raw().URL.String())
	if shareKey == "" {
		return response, err // not an Azure Files request
	}

	// Only read the (short XML error) body on throttle responses; successes are
	// reported as ThrottleNone so the controller can track the success ratio.
	var body string
	if response.StatusCode == http.StatusServiceUnavailable || response.StatusCode == http.StatusTooManyRequests {
		body = transparentlyReadBody(response)
	}

	// Classify here (Azure Files-specific) and hand the controller a neutral kind.
	kind := classifyAzureFilesThrottle(response.StatusCode, body)
	common.HandleShareResponse(shareKey, kind, parseRetryAfterSeconds(response))
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
