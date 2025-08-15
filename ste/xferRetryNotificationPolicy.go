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
	"context"
	"github.com/Azure/azure-sdk-for-go/sdk/azcore/policy"
	"net/http"
)

// retryNotificationReceiver should be implemented by code that wishes to be notified when a retry
// happens. Such code must register itself into the context, using withRetryNotification,
// so that the RetryNotificationPolicy can invoke the callback when necessary.
type retryNotificationReceiver interface {
	RetryCallback()
}

// withRetryNotifier returns a context that contains a retry notifier. The retryNotificationPolicy
// will then invoke the callback when a retry happens
func withRetryNotification(ctx context.Context, r retryNotificationReceiver) context.Context {
	return context.WithValue(ctx, retryNotifyContextKey, r)
}

var timeoutNotifyContextKey = contextKey{"timeoutNotify"}

// withTimeoutNotification returns a context that contains indication of a timeout. The retryNotificationPolicy
// will then set the timeout flag when a timeout happens
func withTimeoutNotification(ctx context.Context, timeout *bool) context.Context {
	return context.WithValue(ctx, timeoutNotifyContextKey, timeout)
}

type contextKey struct {
	name string
}

var retryNotifyContextKey = contextKey{"retryNotify"}

type retryNotificationPolicy struct {
}

func newRetryNotificationPolicy() policy.Policy {
	return &retryNotificationPolicy{}
}

func (r *retryNotificationPolicy) Do(req *policy.Request) (*http.Response, error) {
	response, err := req.Next() // Make the request

	if response != nil {
		if response.StatusCode == http.StatusServiceUnavailable {
			// Grab the notification callback out of the context and, if its there, call it
			notifier, ok := req.Raw().Context().Value(retryNotifyContextKey).(retryNotificationReceiver)
			if ok {
				notifier.RetryCallback()
			}
		}
		if timeout, ok := req.Raw().Context().Value(timeoutNotifyContextKey).(*bool); ok {
			if response.StatusCode == http.StatusInternalServerError && ((response.Header["x-ms-error-code"] != nil && response.Header["x-ms-error-code"][0] == "OperationTimedOut") || (response.Header["X-Ms-OnError-Code"] != nil && response.Header["X-Ms-OnError-Code"][0] == "OperationTimedOut")) { //nolint:staticcheck
				*timeout = true
			}
		}
	}

	return response, err
}
