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
	"net/url"
	"strings"
	"sync/atomic"

	"github.com/Azure/azure-storage-azcopy/common"
	"github.com/Azure/azure-storage-azcopy/ste"
)

// TODO: Get rid of highly situational constructors.
// Why? Because they create a MASSIVE amount of code duplication for a very specific situation.
// This is going to be more painful to clean up the longer we wait to do this, and the more we keep using these.
// PLEASE, do not create any more of these. In the future, use initResourceTraverser.
func newLocalTraverserForSync(cca *cookedSyncCmdArgs, isSource bool) (*localTraverser, error) {
	var fullPath string

	if isSource {
		fullPath = cca.source
	} else {
		fullPath = cca.destination
	}

	if strings.ContainsAny(strings.TrimPrefix(fullPath, common.EXTENDED_PATH_PREFIX), "*?") {
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

	// TODO: Implement this flag (followSymlinks).
	// It's extra work and would require testing at the moment, hence why I didn't do it.
	// Though in hindsight, copy is already getting this testing so, your choice.
	traverser := newLocalTraverser(fullPath, cca.recursive, false, incrementEnumerationCounter)

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
