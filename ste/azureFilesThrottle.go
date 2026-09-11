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
	"strings"

	"github.com/Azure/azure-storage-azcopy/v10/common"
)

// isAzureFilesThrottleResponse reports whether an HTTP status signals server
// overload. AzCopy already classifies 503 (and 429) as retriable throttling
// elsewhere; this mirrors that convention for the dual-resource controller.
func isAzureFilesThrottleResponse(statusCode int) bool {
	return statusCode == http.StatusTooManyRequests || statusCode == http.StatusServiceUnavailable
}

// classifyAzureFilesThrottle maps an Azure Files HTTP status + error body onto
// the storage-service-agnostic common.ThrottleKind the RateLimitController
// consumes. It reuses the AzCopy xferStatsPolicy convention:
//
//	"Operations per second is over the account limit" -> IOPS
//	"...gress is over the account limit"               -> bandwidth (In/Egress)
//
// A 429/503 whose body matches neither phrase is reported as ThrottleUnknown so
// the caller can apply it conservatively to both dimensions. Non-throttle status
// codes return ThrottleNone. Keeping this Azure Files-specific parsing in ste
// (not common) lets the controller serve other services (e.g. Blob) too.
func classifyAzureFilesThrottle(statusCode int, errBody string) common.ThrottleKind {
	if !isAzureFilesThrottleResponse(statusCode) {
		return common.ThrottleNone
	}
	b := strings.ToLower(errBody)
	switch {
	case strings.Contains(b, "operations per second is over the account limit"):
		return common.ThrottleIops
	case strings.Contains(b, "gress is over the account limit"): // ingress or egress
		return common.ThrottleBandwidth
	default:
		return common.ThrottleUnknown
	}
}
