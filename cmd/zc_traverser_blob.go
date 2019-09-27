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
	"github.com/Azure/azure-storage-blob-go/azblob"
	"github.com/pkg/errors"

	"github.com/Azure/azure-storage-azcopy/common"
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

func (t *blobTraverser) isDirectory(isSource bool) bool {
	isDirDirect := copyHandlerUtil{}.urlIsContainerOrVirtualDirectory(t.rawURL)

	// Skip the single blob check if we're checking a destination.
	// This is an individual exception for blob because blob supports virtual directories and blobs sharing the same name.
	if isDirDirect || !isSource {
		return isDirDirect
	}

	_, isSingleBlob, err := t.getPropertiesIfSingleBlob()

	if stgErr, ok := err.(azblob.StorageError); ok {
		// We know for sure this is a single blob still, let it walk on through to the traverser.
		if stgErr.ServiceCode() == common.CPK_ERROR_SERVICE_CODE {
			return false
		}
	}

	return !isSingleBlob
}

func (t *blobTraverser) getPropertiesIfSingleBlob() (*azblob.BlobGetPropertiesResponse, bool, error) {
	blobURL := azblob.NewBlobURL(*t.rawURL, t.p)
	blobProps, blobPropertiesErr := blobURL.GetProperties(t.ctx, azblob.BlobAccessConditions{})

	// if there was no problem getting the properties, it means that we are looking at a single blob
	if blobPropertiesErr == nil && !gCopyUtil.doesBlobRepresentAFolder(blobProps.NewMetadata()) {
		return blobProps, true, blobPropertiesErr
	}

	return nil, false, blobPropertiesErr
}

func (t *blobTraverser) traverse(preprocessor objectMorpher, processor objectProcessor, filters []objectFilter) (err error) {
	blobUrlParts := azblob.NewBlobURLParts(*t.rawURL)
	util := copyHandlerUtil{}

	// check if the url points to a single blob
	blobProperties, isBlob, propErr := t.getPropertiesIfSingleBlob()

	if stgErr, ok := propErr.(azblob.StorageError); ok {
		// Don't error out unless it's a CPK error just yet
		// If it's a CPK error, we know it's a single blob and that we can't get the properties on it anyway.
		if stgErr.ServiceCode() == common.CPK_ERROR_SERVICE_CODE {
			return errors.New("this blob uses customer provided encryption keys (CPK). At the moment, AzCopy does not support CPK-encrypted blobs. " +
				"If you wish to make use of this blob, we recommend using one of the Azure Storage SDKs")
		}
	}

	if isBlob {
		// sanity checking so highlighting doesn't highlight things we're not worried about.
		if blobProperties == nil {
			panic("isBlob should never be set if getting properties is an error")
		}

		storedObject := newStoredObject(
			preprocessor,
			getObjectNameOnly(blobUrlParts.BlobName),
			"",
			blobProperties.LastModified(),
			blobProperties.ContentLength(),
			blobProperties.ContentMD5(),
			blobProperties.BlobType(),
			blobUrlParts.ContainerName,
		)

		storedObject.contentDisposition = blobProperties.ContentDisposition()
		storedObject.cacheControl = blobProperties.CacheControl()
		storedObject.contentLanguage = blobProperties.ContentLanguage()
		storedObject.contentEncoding = blobProperties.ContentEncoding()
		storedObject.contentType = blobProperties.ContentType()

		// .NewMetadata() seems odd to call, but it does actually retrieve the metadata from the blob properties.
		storedObject.Metadata = common.FromAzBlobMetadataToCommonMetadata(blobProperties.NewMetadata())
		storedObject.blobAccessTier = azblob.AccessTierType(blobProperties.AccessTier())

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

			relativePath := strings.TrimPrefix(blobInfo.Name, searchPrefix)

			// if recursive
			if !t.recursive && strings.Contains(relativePath, common.AZCOPY_PATH_SEPARATOR_STRING) {
				continue
			}

			storedObject := newStoredObject(
				preprocessor,
				getObjectNameOnly(blobInfo.Name),
				relativePath,
				blobInfo.Properties.LastModified,
				*blobInfo.Properties.ContentLength,
				blobInfo.Properties.ContentMD5,
				blobInfo.Properties.BlobType,
				blobUrlParts.ContainerName,
			)

			storedObject.contentDisposition = common.IffStringNotNil(blobInfo.Properties.ContentDisposition, "")
			storedObject.cacheControl = common.IffStringNotNil(blobInfo.Properties.CacheControl, "")
			storedObject.contentLanguage = common.IffStringNotNil(blobInfo.Properties.ContentLanguage, "")
			storedObject.contentEncoding = common.IffStringNotNil(blobInfo.Properties.ContentEncoding, "")
			storedObject.contentType = common.IffStringNotNil(blobInfo.Properties.ContentType, "")

			storedObject.Metadata = common.FromAzBlobMetadataToCommonMetadata(blobInfo.Metadata)

			storedObject.blobAccessTier = blobInfo.Properties.AccessTier

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
