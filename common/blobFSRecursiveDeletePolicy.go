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

package common

import (
	"context"
	"net/http"
	"strconv"

	"github.com/Azure/azure-sdk-for-go/sdk/azcore/policy"
)

// CtxRecursiveKey is used as a context key to apply the recursive query parameter.
type CtxRecursiveKey struct{}

// WithRecursive applies the recursive parameter to the request.
func WithRecursive(parent context.Context, recursive bool) context.Context {
	return context.WithValue(parent, CtxRecursiveKey{}, recursive)
}

type recursivePolicy struct {
}

// NewRecursivePolicy creates a policy that applies the recursive parameter to the request.
func NewRecursivePolicy() policy.Policy {
	return &recursivePolicy{}
}

func (p *recursivePolicy) Do(req *policy.Request) (*http.Response, error) {
	if recursive := req.Raw().Context().Value(CtxRecursiveKey{}); recursive != nil {
		if req.Raw().URL.Query().Has("recursive") {
			query := req.Raw().URL.Query()
			query.Set("recursive", strconv.FormatBool(recursive.(bool)))
			req.Raw().URL.RawQuery = query.Encode()
		}
	}
	return req.Next()
}
