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
	"sync/atomic"
)

func newLocalTraverserForSync(cca *cookedSyncCmdArgs, isSource bool) (*localTraverser, error) {
	var fullPath string

	if isSource {
		fullPath = cca.source
	} else {
		fullPath = cca.destination
	}

	//Trim the offending prefix and then check if it has any pattern matching
	if strings.ContainsAny(strings.TrimPrefix(fullPath, `\\?\`), "*?") {
		return nil, errors.New("illegal local path, no pattern matching allowed for sync command")
	}

	incrementEnumerationCounter := func() {
		var counterAddr *uint64

		if isSource {
			counterAddr = &cca.atomicSourceFilesScanned
		} else {
			counterAddr = &cca.atomicDestinationFilesScanned
		}

		atomic.AddUint64(counterAddr, 1)
	}

	traverser := newLocalTraverser(fullPath, cca.recursive, incrementEnumerationCounter)

	return traverser, nil
}

func newBlobTraverserForSync(cca *cookedSyncCmdArgs, isSource bool) (t *blobTraverser, err error) {
	ctx := context.WithValue(context.TODO(), ste.ServiceAPIVersionOverride, ste.DefaultServiceApiVersion)

	// figure out the right URL
	var rawURL *url.URL
	if isSource {
		rawURL, err = url.Parse(cca.source)
		if err == nil && cca.sourceSAS != "" {
			copyHandlerUtil{}.appendQueryParamToUrl(rawURL, cca.sourceSAS)
		}
	} else {
		rawURL, err = url.Parse(cca.destination)
		if err == nil && cca.destinationSAS != "" {
			copyHandlerUtil{}.appendQueryParamToUrl(rawURL, cca.destinationSAS)
		}
	}

	if err != nil {
		return
	}

	if strings.Contains(rawURL.Path, "*") {
		return nil, errors.New("illegal URL, no pattern matching allowed for sync command")
	}

	p, err := createBlobPipeline(ctx, cca.credentialInfo)
	if err != nil {
		return
	}

	incrementEnumerationCounter := func() {
		var counterAddr *uint64

		if isSource {
			counterAddr = &cca.atomicSourceFilesScanned
		} else {
			counterAddr = &cca.atomicDestinationFilesScanned
		}

		atomic.AddUint64(counterAddr, 1)
	}

	return newBlobTraverser(rawURL, p, ctx, cca.recursive, incrementEnumerationCounter), nil
}
