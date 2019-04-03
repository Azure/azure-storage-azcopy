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

package cmd

import (
	"context"
	"errors"
	"github.com/Azure/azure-storage-azcopy/ste"
	"net/url"
	"strings"
)

func newBlobTraverserForRemove(cca *cookedCopyCmdArgs) (t *blobTraverser, err error) {
	ctx := context.WithValue(context.TODO(), ste.ServiceAPIVersionOverride, ste.DefaultServiceApiVersion)

	rawURL, err := url.Parse(cca.source)
	if err == nil && cca.sourceSAS != "" {
		copyHandlerUtil{}.appendQueryParamToUrl(rawURL, cca.sourceSAS)
	}

	if err != nil {
		return
	}

	if strings.Contains(rawURL.Path, "*") {
		return nil, errors.New("illegal URL, no pattern matching allowed for remove command")
	}

	p, err := createBlobPipeline(ctx, cca.credentialInfo)
	if err != nil {
		return
	}

	return newBlobTraverser(rawURL, p, ctx, cca.recursive, nil), nil
}

func newFileTraverserForRemove(cca *cookedCopyCmdArgs) (t *fileTraverser, err error) {
	ctx := context.WithValue(context.TODO(), ste.ServiceAPIVersionOverride, ste.DefaultServiceApiVersion)

	rawURL, err := url.Parse(cca.source)
	if err == nil && cca.sourceSAS != "" {
		copyHandlerUtil{}.appendQueryParamToUrl(rawURL, cca.sourceSAS)
	}

	if err != nil {
		return
	}

	if strings.Contains(rawURL.Path, "*") {
		return nil, errors.New("illegal URL, no pattern matching allowed for remove command")
	}

	p, err := createFilePipeline(ctx, cca.credentialInfo)
	if err != nil {
		return
	}

	return newFileTraverser(rawURL, p, ctx, cca.recursive, nil), nil
}
