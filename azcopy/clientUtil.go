// Copyright © 2025 Microsoft <wastore@microsoft.com>
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

package azcopy

import (
	"net/http"

	"github.com/Azure/azure-sdk-for-go/sdk/azcore"
	"github.com/Azure/azure-sdk-for-go/sdk/azcore/policy"
	"github.com/Azure/azure-storage-azcopy/v10/common"
	"github.com/Azure/azure-storage-azcopy/v10/ste"
)

// CreateClientOptions creates generic client options which are required to create any
// client to interact with storage service. Default options are modified to suit azcopy.
// srcCred is required in cases where source is authenticated via oAuth for S2S transfers
func CreateClientOptions(logger common.ILoggerResetable, srcCred *common.ScopedToken, reauthCred *common.ScopedAuthenticator) azcore.ClientOptions {
	logOptions := ste.LogOptions{}

	if logger != nil {
		logOptions.RequestLogOptions.SyslogDisabled = common.IsForceLoggingDisabled()
		logOptions.Log = logger.Log
		logOptions.ShouldLog = logger.ShouldLog
	}
	return ste.NewClientOptions(policy.RetryOptions{
		MaxRetries:    ste.UploadMaxTries,
		TryTimeout:    ste.UploadTryTimeout,
		RetryDelay:    ste.UploadRetryDelay,
		MaxRetryDelay: ste.UploadMaxRetryDelay,
	}, policy.TelemetryOptions{
		ApplicationID: common.AddUserAgentPrefix(common.UserAgent),
	}, ste.NewAzcopyHTTPClient(frontEndMaxIdleConnectionsPerHost), logOptions, srcCred, reauthCred)
}

const frontEndMaxIdleConnectionsPerHost = http.DefaultMaxIdleConnsPerHost
