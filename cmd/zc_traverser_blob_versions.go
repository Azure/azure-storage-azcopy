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
	"net/url"
	"strings"

	"github.com/Azure/azure-pipeline-go/pipeline"
	"github.com/Azure/azure-storage-azcopy/v10/common"
	"github.com/Azure/azure-storage-blob-go/azblob"
)

type blobVersionsTraverser struct {
	rawURL                      *url.URL
	p                           pipeline.Pipeline
	ctx                         context.Context
	includeDirectoryStubs       bool
	incrementEnumerationCounter enumerationCounterFunc
	listOfVersionIds            chan string
}

func (t *blobVersionsTraverser) isDirectory(isSource bool) bool {
	isDirDirect := copyHandlerUtil{}.urlIsContainerOrVirtualDirectory(t.rawURL)

	// Skip the single blob check if we're checking a destination.
	// This is an individual exception for blob because blob supports virtual directories and blobs sharing the same name.
	if isDirDirect || !isSource {
		return isDirDirect
	}

	// The base blob may not exist in some cases.
	return false
}

func (t *blobVersionsTraverser) getBlobProperties(versionID string) (props *azblob.BlobGetPropertiesResponse, err error) {
	blobURLParts := azblob.NewBlobURLParts(*t.rawURL)
	blobURLParts.BlobName = strings.TrimSuffix(blobURLParts.BlobName, common.AZCOPY_PATH_SEPARATOR_STRING)
	if versionID != "" {
		blobURLParts.VersionID = versionID
	}

	blobURL := azblob.NewBlobURL(blobURLParts.URL(), t.p)
	props, err = blobURL.GetProperties(t.ctx, azblob.BlobAccessConditions{})
	return props, err
}

func (t *blobVersionsTraverser) traverse(preprocessor objectMorpher, processor objectProcessor, filters []objectFilter) (err error) {
	blobURLParts := azblob.NewBlobURLParts(*t.rawURL)

	versionID, ok := <-t.listOfVersionIds
	for ; ok; versionID, ok = <-t.listOfVersionIds {
		blobProperties, err := t.getBlobProperties(versionID)

		if err != nil {
			return err
		}

		if blobProperties == nil {
			panic("isBlob should never be set if getting properties is an error")
		}
		blobURLParts.VersionID = versionID
		storedObject := newStoredObject(
			preprocessor,
			getObjectNameOnly(strings.TrimSuffix(blobURLParts.BlobName, common.AZCOPY_PATH_SEPARATOR_STRING)),
			"",
			common.EEntityType.File(),
			blobProperties.LastModified(),
			blobProperties.ContentLength(),
			blobProperties,
			blobPropertiesResponseAdapter{blobProperties},
			common.FromAzBlobMetadataToCommonMetadata(blobProperties.NewMetadata()),
			blobURLParts.ContainerName,
		)
		storedObject.blobVersionID = versionID

		if t.incrementEnumerationCounter != nil {
			t.incrementEnumerationCounter(common.EEntityType.File())
		}

		err = processIfPassedFilters(filters, storedObject, processor)
		if err != nil {
			return err
		}
	}
	return nil
}

func newBlobVersionsTraverser(rawURL *url.URL, p pipeline.Pipeline, ctx context.Context, recursive, includeDirectoryStubs bool,
	incrementEnumerationCounter enumerationCounterFunc, listOfVersionIds chan string) (t *blobVersionsTraverser) {
	return &blobVersionsTraverser{
		rawURL:                      rawURL,
		p:                           p,
		ctx:                         ctx,
		includeDirectoryStubs:       includeDirectoryStubs,
		incrementEnumerationCounter: incrementEnumerationCounter,
		listOfVersionIds:            listOfVersionIds,
	}
}
