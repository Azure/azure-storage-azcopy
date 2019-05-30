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
	"fmt"
	"net/url"
	"strings"

	"github.com/Azure/azure-pipeline-go/pipeline"
	"github.com/Azure/azure-storage-azcopy/common"
	"github.com/Azure/azure-storage-blob-go/azblob"
)

// allow us to iterate through a path pointing to the blob endpoint
type blobTraverser struct {
	rawURL    *url.URL
	p         pipeline.Pipeline
	ctx       context.Context
	recursive bool

	// a generic function to notify that a new stored object has been enumerated
	incrementEnumerationCounter func()
}

func (t *blobTraverser) getPropertiesIfSingleBlob() (*azblob.BlobGetPropertiesResponse, bool) {
	blobURL := azblob.NewBlobURL(*t.rawURL, t.p)
	blobProps, blobPropertiesErr := blobURL.GetProperties(t.ctx, azblob.BlobAccessConditions{})

	// if there was no problem getting the properties, it means that we are looking at a single blob
	if blobPropertiesErr == nil && !gCopyUtil.doesBlobRepresentAFolder(blobProps.NewMetadata()) {
		return blobProps, true
	}

	return nil, false
}

func (t *blobTraverser) traverse(processor objectProcessor, filters []objectFilter) (err error) {
	blobUrlParts := azblob.NewBlobURLParts(*t.rawURL)
	util := copyHandlerUtil{}

	// check if the url points to a single blob
	blobProperties, isBlob := t.getPropertiesIfSingleBlob()
	if isBlob {
		storedObject := newStoredObject(
			getObjectNameOnly(blobUrlParts.BlobName),
			"", // relative path makes no sense when the full path already points to the file
			blobProperties.LastModified(),
			blobProperties.ContentLength(),
			blobProperties.ContentMD5(),
			blobProperties.BlobType(),
		)

		if t.incrementEnumerationCounter != nil {
			t.incrementEnumerationCounter()
		}
		return processIfPassedFilters(filters, storedObject, processor)
	}

	// get the container URL so that we can list the blobs
	containerRawURL := copyHandlerUtil{}.getContainerUrl(blobUrlParts)
	containerURL := azblob.NewContainerURL(containerRawURL, t.p)

	// get the search prefix to aid in the listing
	// example: for a url like https://test.blob.core.windows.net/test/foo/bar/bla
	// the search prefix would be foo/bar/bla
	searchPrefix := blobUrlParts.BlobName

	// append a slash if it is not already present
	// example: foo/bar/bla becomes foo/bar/bla/ so that we only list children of the virtual directory
	if searchPrefix != "" && !strings.HasSuffix(searchPrefix, common.AZCOPY_PATH_SEPARATOR_STRING) {
		searchPrefix += common.AZCOPY_PATH_SEPARATOR_STRING
	}

	for marker := (azblob.Marker{}); marker.NotDone(); {
		// look for all blobs that start with the prefix
		// TODO optimize for the case where recursive is off
		listBlob, err := containerURL.ListBlobsFlatSegment(t.ctx, marker,
			azblob.ListBlobsSegmentOptions{Prefix: searchPrefix, Details: azblob.BlobListingDetails{Metadata: true}})
		if err != nil {
			return fmt.Errorf("cannot list blobs. Failed with error %s", err.Error())
		}

		// process the blobs returned in this result segment
		for _, blobInfo := range listBlob.Segment.BlobItems {
			// if the blob represents a hdi folder, then skip it
			if util.doesBlobRepresentAFolder(blobInfo.Metadata) {
				continue
			}

			//format relative path for current path separator
			var relativePath string
			if common.AZCOPY_PATH_SEPARATOR_STRING == `\` {
				relativePath = strings.Replace(strings.TrimPrefix(blobInfo.Name, searchPrefix), `/`, common.AZCOPY_PATH_SEPARATOR_STRING, -1)
			} else {
				relativePath = strings.TrimPrefix(blobInfo.Name, searchPrefix)
			}

			// if recursive
			if !t.recursive && strings.Contains(relativePath, common.AZCOPY_PATH_SEPARATOR_STRING) {
				continue
			}

			storedObject := newStoredObject(
				getObjectNameOnly(blobInfo.Name),
				relativePath,
				blobInfo.Properties.LastModified,
				*blobInfo.Properties.ContentLength,
				blobInfo.Properties.ContentMD5,
				blobInfo.Properties.BlobType,
			)

			if t.incrementEnumerationCounter != nil {
				t.incrementEnumerationCounter()
			}

			processErr := processIfPassedFilters(filters, storedObject, processor)
			if processErr != nil {
				return processErr
			}
		}

		marker = listBlob.NextMarker
	}

	return
}

func newBlobTraverser(rawURL *url.URL, p pipeline.Pipeline, ctx context.Context, recursive bool, incrementEnumerationCounter func()) (t *blobTraverser) {
	t = &blobTraverser{rawURL: rawURL, p: p, ctx: ctx, recursive: recursive, incrementEnumerationCounter: incrementEnumerationCounter}
	return
}
