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
	"github.com/Azure/azure-pipeline-go/pipeline"
	"net/http"
)

// retryNotificationReceiver should be implemented by code that wishes to be notified when a retry
// happens. Such code must register itself into the context, using withRetryNotification,
// so that the retryNotificationPolicy can invoke the callback when necessary.
type retryNotificationReceiver interface {
	RetryCallback()
}

// withRetryNotifier returns a context that contains a retry notifier.  The retryNotificationPolicy
// will then invoke the callback when a retry happens
func withRetryNotification(ctx context.Context, r retryNotificationReceiver) context.Context {
	return context.WithValue(ctx, retryNotifyContextKey, r)
}

type contextKey struct {
	name string
}

var retryNotifyContextKey = contextKey{"retryNotify"}

type retryNotificationPolicy struct {
	next pipeline.Policy
}

// Do invokes the registered notification callback if there's a retry (503) status.
// This is to notify any interested party that a retry status has been returned in an HTTP response.
// (We can't just let the top-level caller look at the status of the HTTP response, because by that
// time our RetryPolicy will have actually DONE the retry, so the status will be successful. That's why, if the
// top level caller wants to be informed, they have to get informed by this callback mechanism.)
func (r *retryNotificationPolicy) Do(ctx context.Context, request pipeline.Request) (pipeline.Response, error) {

	resp, err := r.next.Do(ctx, request)

	if resp != nil {
		if rr := resp.Response(); rr != nil && rr.StatusCode == http.StatusServiceUnavailable {
			// Grab the notification callback out of the context and, if its there, call it
			notifier, ok := ctx.Value(retryNotifyContextKey).(retryNotificationReceiver)
			if ok {
				notifier.RetryCallback()
			}
		}
	}

	return resp, err
}

func newRetryNotificationPolicyFactory() pipeline.Factory {
	return pipeline.FactoryFunc(func(next pipeline.Policy, po *pipeline.PolicyOptions) pipeline.PolicyFunc {
		r := retryNotificationPolicy{next: next}
		return r.Do
	})
}
