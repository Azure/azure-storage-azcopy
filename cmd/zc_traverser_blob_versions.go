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
	// "fmt"
	// "github.com/Azure/azure-storage-azcopy/common/parallel"
	"net/url"
	"strings"

	"github.com/Azure/azure-pipeline-go/pipeline"
	"github.com/Azure/azure-storage-blob-go/azblob"
	// "github.com/pkg/errors"
	"github.com/Azure/azure-storage-azcopy/common"
)

type blobVersionsTraverser struct {
	rawURL                      *url.URL
	p                           pipeline.Pipeline
	ctx                         context.Context
	recursive                   bool
	includeDirectoryStubs       bool
	incrementEnumerationCounter enumerationCounterFunc
	listOfVersionIds            []string
}

func (t *blobVersionsTraverser) isDirectory(isSource bool) bool {
	isDirDirect := copyHandlerUtil{}.urlIsContainerOrVirtualDirectory(t.rawURL)

	// Skip the single blob check if we're checking a destination.
	// This is an individual exception for blob because blob supports virtual directories and blobs sharing the same name.
	if isDirDirect || !isSource {
		return isDirDirect
	}

	_, err := t.getBlobProperties("")

	return err != nil
}

func (t *blobVersionsTraverser) getBlobProperties(versionID string) (props *azblob.BlobGetPropertiesResponse, err error) {
	blobURLParts := azblob.NewBlobURLParts(*t.rawURL)
	blobURLParts.BlobName = strings.TrimSuffix(blobURLParts.BlobName, common.AZCOPY_PATH_SEPARATOR_STRING)
	if versionID != "" {
		blobURLParts.VersionID = versionID
	}

	blobURL := azblob.NewBlobURL(blobURLParts.URL(), t.p)
	props, err = blobURL.GetProperties(t.ctx, azblob.BlobAccessConditions{})

	if err != nil {
		return props, err
	}
	
	// if there was no problem getting the properties, it implies that we are looking at a single blob
	if gCopyUtil.doesBlobRepresentAFolder(props.NewMetadata()) {
		return props, errors.New("This is not a blob")
	}
	return props, nil
	
}

func (t *blobVersionsTraverser) traverse(preprocessor objectMorpher, processor objectProcessor, filters []objectFilter) (err error) {
	blobURLParts := azblob.NewBlobURLParts(*t.rawURL)

	for _, versionID := range t.listOfVersionIds {
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
			versionID,
		)

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
	incrementEnumerationCounter enumerationCounterFunc, listOfVersionIds []string) (t *blobVersionsTraverser) {
	return &blobVersionsTraverser{
		rawURL:                rawURL,
		p:                     p,
		ctx:                   ctx,
		recursive:             recursive,
		includeDirectoryStubs: includeDirectoryStubs, 
		incrementEnumerationCounter: incrementEnumerationCounter, 
		listOfVersionIds: listOfVersionIds,
	}
}
