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
	"strings"

	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/blob"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/service"

	"github.com/Azure/azure-storage-azcopy/v10/common"
)

type blobVersionsTraverser struct {
	rawURL                      string
	serviceClient               *service.Client
	ctx                         context.Context
	includeDirectoryStubs       bool
	incrementEnumerationCounter enumerationCounterFunc
	listOfVersionIds            <-chan string
	cpkOptions                  common.CpkOptions
}

func (t *blobVersionsTraverser) IsDirectory(isSource bool) (bool, error) {
	isDirDirect := copyHandlerUtil{}.urlIsContainerOrVirtualDirectory(t.rawURL)

	// Skip the single blob check if we're checking a destination.
	// This is an individual exception for blob because blob supports virtual directories and blobs sharing the same name.
	if isDirDirect || !isSource {
		return isDirDirect, nil
	}

	// The base blob may not exist in some cases.
	return false, nil
}

func (t *blobVersionsTraverser) getBlobProperties(versionID string) (*blob.GetPropertiesResponse, error) {
	blobURLParts, err := blob.ParseURL(t.rawURL)
	if err != nil {
		return nil, err
	}
	blobURLParts.BlobName = strings.TrimSuffix(blobURLParts.BlobName, common.AZCOPY_PATH_SEPARATOR_STRING)
	if versionID != "" {
		blobURLParts.VersionID = versionID
	}

	blobClient, err := createBlobClientFromServiceClient(blobURLParts, t.serviceClient)
	if err != nil {
		return nil, err
	}
	props, err := blobClient.GetProperties(t.ctx, &blob.GetPropertiesOptions{CPKInfo: t.cpkOptions.GetCPKInfo()})
	return &props, err
}

func (t *blobVersionsTraverser) Traverse(preprocessor objectMorpher, processor objectProcessor, filters []ObjectFilter) (err error) {
	blobURLParts, err := blob.ParseURL(t.rawURL)
	if err != nil {
		return err
	}

	versionID, ok := <-t.listOfVersionIds
	for ; ok; versionID, ok = <-t.listOfVersionIds {
		blobProperties, err := t.getBlobProperties(versionID)

		if err != nil {
			return err
		}

		if blobProperties == nil {
			panic("isBlob should never be set if getting properties is an error")
		}

		blobPropsAdapter := blobPropertiesResponseAdapter{blobProperties}
		blobURLParts.VersionID = versionID
		storedObject := newStoredObject(
			preprocessor,
			getObjectNameOnly(strings.TrimSuffix(blobURLParts.BlobName, common.AZCOPY_PATH_SEPARATOR_STRING)),
			"",
			common.EEntityType.File(),
			blobPropsAdapter.LastModified(),
			blobPropsAdapter.ContentLength(),
			blobPropsAdapter,
			blobPropsAdapter,
			blobPropsAdapter.Metadata,
			blobURLParts.ContainerName,
		)
		storedObject.blobVersionID = versionID

		if t.incrementEnumerationCounter != nil {
			t.incrementEnumerationCounter(common.EEntityType.File(), common.SymlinkHandlingType(0), common.DefaultHardlinkHandlingType)
		}

		err = processIfPassedFilters(filters, storedObject, processor)
		if err != nil {
			return err
		}
	}
	return nil
}

func newBlobVersionsTraverser(rawURL string, serviceClient *service.Client, ctx context.Context, opts InitResourceTraverserOptions) (t *blobVersionsTraverser) {
	return &blobVersionsTraverser{
		rawURL:                      rawURL,
		serviceClient:               serviceClient,
		ctx:                         ctx,
		includeDirectoryStubs:       opts.IncludeDirectoryStubs,
		incrementEnumerationCounter: opts.IncrementEnumeration,
		listOfVersionIds:            opts.ListOfVersionIDs,
		cpkOptions:                  opts.CpkOptions,
	}
}
