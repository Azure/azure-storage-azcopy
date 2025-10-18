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
	"context"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/Azure/azure-sdk-for-go/sdk/azcore/policy"
	"github.com/Azure/azure-sdk-for-go/sdk/azcore/runtime"
	"github.com/stretchr/testify/assert"
)

func Test_RetryNotificationPolicy_Timeout(t *testing.T) {
	a := assert.New(t)

	// generate a test server so we can capture and inspect the request
	srv := httptest.NewServer(http.HandlerFunc(func(res http.ResponseWriter, req *http.Request) {
		res.Header()["x-ms-error-code"] = []string{"OperationTimedOut"}
		res.WriteHeader(http.StatusInternalServerError)
	}))
	defer func() { srv.Close() }()

	p := newRetryNotificationPolicy()
	pl := runtime.NewPipeline("", "",
		runtime.PipelineOptions{PerRetry: []policy.Policy{p}},
		&policy.ClientOptions{Transport: http.DefaultClient},
	)
	var timeoutFromCtx bool
	ctx := withTimeoutNotification(context.Background(), &timeoutFromCtx)
	req, err := runtime.NewRequest(ctx, http.MethodPut, srv.URL)
	a.NoError(err)
	_, err = pl.Do(req)
	a.NoError(err)
	a.True(timeoutFromCtx)
}

func Test_RetryNotificationPolicy_TimeoutNegativeErrorCodeHeader(t *testing.T) {
	a := assert.New(t)

	// generate a test server so we can capture and inspect the request
	srv := httptest.NewServer(http.HandlerFunc(func(res http.ResponseWriter, req *http.Request) {
		res.Header()["x-ms-error-code"] = []string{"CannotVerifyCopySource"}
		res.WriteHeader(http.StatusInternalServerError)
	}))
	defer func() { srv.Close() }()

	p := newRetryNotificationPolicy()
	pl := runtime.NewPipeline("", "",
		runtime.PipelineOptions{PerRetry: []policy.Policy{p}},
		&policy.ClientOptions{Transport: http.DefaultClient},
	)
	var timeoutFromCtx bool
	ctx := withTimeoutNotification(context.Background(), &timeoutFromCtx)
	req, err := runtime.NewRequest(ctx, http.MethodPut, srv.URL)
	a.NoError(err)
	_, err = pl.Do(req)
	a.NoError(err)
	a.False(timeoutFromCtx)
}

func Test_RetryNotificationPolicy_TimeoutNegativeStatusCodeFail(t *testing.T) {
	a := assert.New(t)

	// generate a test server so we can capture and inspect the request
	srv := httptest.NewServer(http.HandlerFunc(func(res http.ResponseWriter, req *http.Request) {
		res.WriteHeader(http.StatusNotFound)
	}))
	defer func() { srv.Close() }()

	p := newRetryNotificationPolicy()
	pl := runtime.NewPipeline("", "",
		runtime.PipelineOptions{PerRetry: []policy.Policy{p}},
		&policy.ClientOptions{Transport: http.DefaultClient},
	)
	var timeoutFromCtx bool
	ctx := withTimeoutNotification(context.Background(), &timeoutFromCtx)
	req, err := runtime.NewRequest(ctx, http.MethodPut, srv.URL)
	a.NoError(err)
	_, err = pl.Do(req)
	a.NoError(err)
	a.False(timeoutFromCtx)
}

func Test_RetryNotificationPolicy_TimeoutNegativeStatusCodeSuccess(t *testing.T) {
	a := assert.New(t)

	// generate a test server so we can capture and inspect the request
	srv := httptest.NewServer(http.HandlerFunc(func(res http.ResponseWriter, req *http.Request) {
		res.WriteHeader(http.StatusOK)
	}))
	defer func() { srv.Close() }()

	p := newRetryNotificationPolicy()
	pl := runtime.NewPipeline("", "",
		runtime.PipelineOptions{PerRetry: []policy.Policy{p}},
		&policy.ClientOptions{Transport: http.DefaultClient},
	)
	var timeoutFromCtx bool
	ctx := withTimeoutNotification(context.Background(), &timeoutFromCtx)
	req, err := runtime.NewRequest(ctx, http.MethodPut, srv.URL)
	a.NoError(err)
	_, err = pl.Do(req)
	a.NoError(err)
	a.False(timeoutFromCtx)
}
