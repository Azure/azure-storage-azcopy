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
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestShouldPreserveRequestServiceVersion(t *testing.T) {
	tests := []struct {
		name     string
		method   string
		url      string
		version  string
		expected bool
	}{
		{
			name:     "GetBlobHash preserves SDK version",
			method:   http.MethodGet,
			url:      "https://acct.blob.core.windows.net/c/b?comp=hash",
			version:  "2025-11-05",
			expected: true,
		},
		{
			name:     "GetBlobHash without SDK version uses override",
			method:   http.MethodGet,
			url:      "https://acct.blob.core.windows.net/c/b?comp=hash",
			expected: false,
		},
		{
			name:     "Other GET uses override",
			method:   http.MethodGet,
			url:      "https://acct.blob.core.windows.net/c/b?comp=blocklist",
			version:  "2025-11-05",
			expected: false,
		},
		{
			name:     "Non-GET hash request uses override",
			method:   http.MethodPut,
			url:      "https://acct.blob.core.windows.net/c/b?comp=hash",
			version:  "2025-11-05",
			expected: false,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			req, err := http.NewRequest(test.method, test.url, nil)
			assert.NoError(t, err)
			if test.version != "" {
				req.Header.Set("x-ms-version", test.version)
			}
			assert.Equal(t, test.expected, shouldPreserveRequestServiceVersion(req))
		})
	}
}

func TestSetServiceVersionHeaderRemovesCaseVariants(t *testing.T) {
	req, err := http.NewRequest(http.MethodGet, "https://acct.blob.core.windows.net/c", nil)
	assert.NoError(t, err)
	req.Header["x-ms-version"] = []string{"2025-11-05"}
	req.Header["X-Ms-Version"] = []string{"duplicate"}

	setServiceVersionHeader(req, "2025-05-05")

	var values []string
	for name, headerValues := range req.Header {
		if strings.EqualFold(name, "x-ms-version") {
			values = append(values, headerValues...)
		}
	}
	assert.Equal(t, []string{"2025-05-05"}, values)
}
