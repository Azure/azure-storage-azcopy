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

	if strings.ContainsAny(fullPath, "*?") {
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
