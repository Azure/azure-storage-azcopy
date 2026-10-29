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

package cmd

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"strings"
	"testing"

	"github.com/Azure/azure-sdk-for-go/sdk/azcore"
	"github.com/Azure/azure-sdk-for-go/sdk/azcore/policy"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/service"
	"github.com/stretchr/testify/assert"
)

// lookupTransport answers blob GetProperties with status/code and container
// listings with an empty result, or with listStatus/listCode when set.
type lookupTransport struct {
	status int
	code   string

	listStatus int
	listCode   string
}

func (tr lookupTransport) Do(req *http.Request) (*http.Response, error) {
	resp := &http.Response{Request: req, Header: http.Header{}, Body: io.NopCloser(strings.NewReader(""))}
	if req.Method == http.MethodHead {
		resp.StatusCode = tr.status
		resp.Status = http.StatusText(tr.status)
		resp.Header.Set("x-ms-error-code", tr.code)
		return resp, nil
	}
	if tr.listStatus != 0 {
		resp.StatusCode = tr.listStatus
		resp.Status = http.StatusText(tr.listStatus)
		resp.Header.Set("x-ms-error-code", tr.listCode)
		return resp, nil
	}
	resp.StatusCode = http.StatusOK
	resp.Header.Set("Content-Type", "application/xml")
	resp.Body = io.NopCloser(strings.NewReader(`<?xml version="1.0" encoding="utf-8"?>` +
		`<EnumerationResults ServiceEndpoint="https://acct.blob.core.windows.net/" ContainerName="c"><Blobs /><NextMarker /></EnumerationResults>`))
	return resp, nil
}

func newLookupTestTraverser(a *assert.Assertions, status int, code string) *blobTraverser {
	return newLookupTestTraverserWith(a, lookupTransport{status: status, code: code})
}

func newLookupTestTraverserWith(a *assert.Assertions, tr lookupTransport) *blobTraverser {
	sc, err := service.NewClientWithNoCredential("https://acct.blob.core.windows.net/", &service.ClientOptions{
		ClientOptions: azcore.ClientOptions{
			Transport: tr,
			Retry:     policy.RetryOptions{MaxRetries: -1},
		},
	})
	a.NoError(err)
	return &blobTraverser{
		rawURL:                      "https://acct.blob.core.windows.net/c/dir/file.txt",
		serviceClient:               sc,
		ctx:                         context.Background(),
		incrementEnumerationCounter: enumerationCounterFuncNoop,
		failOnLookupError:           true,
	}
}

func TestBlobTraverserFailsOnLookupError(t *testing.T) {
	for _, tc := range []struct {
		status int
		code   string
	}{
		{http.StatusForbidden, "AuthorizationPermissionMismatch"},
		{http.StatusServiceUnavailable, "ServerBusy"},
	} {
		t.Run(tc.code, func(t *testing.T) {
			a := assert.New(t)
			err := newLookupTestTraverser(a, tc.status, tc.code).Traverse(nil, func(StoredObject) error { return nil }, nil)
			var respErr *azcore.ResponseError
			if a.ErrorAs(err, &respErr) {
				a.Equal(tc.status, respErr.StatusCode)
				a.Equal(tc.code, respErr.ErrorCode)
			}
		})
	}
}

// Errors the traverser returns for a failed request wrap the service error,
// so callers can read its status and code with errors.As.
func TestBlobTraverserReturnedErrorsWrapServiceError(t *testing.T) {
	for _, tc := range []struct {
		name   string
		tr     lookupTransport
		status int
		code   string
	}{
		{"lookup forbidden", lookupTransport{status: http.StatusForbidden, code: "AuthorizationPermissionMismatch"},
			http.StatusForbidden, "AuthorizationPermissionMismatch"},
		{"parallel listing fails", lookupTransport{status: http.StatusNotFound, code: "BlobNotFound",
			listStatus: http.StatusServiceUnavailable, listCode: "ServerBusy"},
			http.StatusServiceUnavailable, "ServerBusy"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			a := assert.New(t)
			tr := newLookupTestTraverserWith(a, tc.tr)
			tr.failOnLookupError = false
			tr.parallelListing = true
			tr.suppressCrawlStats = true
			err := tr.Traverse(noPreProccessor, func(StoredObject) error { return nil }, nil)
			var respErr *azcore.ResponseError
			if a.ErrorAs(err, &respErr) {
				a.Equal(tc.status, respErr.StatusCode)
				a.Equal(tc.code, respErr.ErrorCode)
			}
		})
	}
}

func TestBlobTraverserLookupNotFoundIsNotAnError(t *testing.T) {
	a := assert.New(t)
	var scheduled int
	err := newLookupTestTraverser(a, http.StatusNotFound, "BlobNotFound").Traverse(nil, func(StoredObject) error {
		scheduled++
		return nil
	}, nil)
	a.NoError(err)
	a.Zero(scheduled)
}

// With cacheSingleBlobLookup, IsDirectory followed by Traverse reads the
// blob's properties once instead of twice.
func TestBlobTraverserCachesSingleBlobLookup(t *testing.T) {
	for _, cache := range []bool{false, true} {
		t.Run(fmt.Sprintf("cache=%t", cache), func(t *testing.T) {
			a := assert.New(t)
			tr := &hnsDirTransport{}
			sc, err := service.NewClientWithNoCredential("https://acct.blob.core.windows.net/", &service.ClientOptions{
				ClientOptions: azcore.ClientOptions{Transport: tr, Retry: policy.RetryOptions{MaxRetries: -1}},
			})
			a.NoError(err)
			bt := newBlobTraverser("https://acct.blob.core.windows.net/c/file.txt", sc, context.Background(), InitResourceTraverserOptions{
				Recursive:             true,
				IncrementEnumeration:  enumerationCounterFuncNoop,
				cacheSingleBlobLookup: cache,
			})

			isDir, err := bt.IsDirectory(true)
			a.NoError(err)
			a.False(isDir)
			var got []string
			a.NoError(bt.Traverse(nil, func(o StoredObject) error { got = append(got, o.name); return nil }, nil))
			a.Len(got, 1)

			want := 2
			if cache {
				want = 1
			}
			a.Equal(want, tr.heads("file.txt"), "properties requests")
		})
	}
}
