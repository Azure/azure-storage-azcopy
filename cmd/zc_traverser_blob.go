package cmd

import (
	"context"
	"fmt"
	"github.com/Azure/azure-pipeline-go/pipeline"
	"github.com/Azure/azure-storage-azcopy/common"
	"github.com/Azure/azure-storage-blob-go/azblob"
	"net/url"
	"strings"
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

func (t *blobTraverser) getPropertiesIfSingleBlob() (blobProps *azblob.BlobGetPropertiesResponse, isBlob bool) {
	blobURL := azblob.NewBlobURL(*t.rawURL, t.p)
	blobProps, blobPropertiesErr := blobURL.GetProperties(t.ctx, azblob.BlobAccessConditions{})

	// if there was no problem getting the properties, it means that we are looking at a single blob
	if blobPropertiesErr == nil {
		isBlob = true
		return
	}

	return
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
		)
		t.incrementEnumerationCounter()
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

			relativePath := strings.Replace(blobInfo.Name, searchPrefix, "", 1)

			// if recursive
			if !t.recursive && strings.Contains(relativePath, common.AZCOPY_PATH_SEPARATOR_STRING) {
				continue
			}

			storedObject := storedObject{
				name:             getObjectNameOnly(blobInfo.Name),
				relativePath:     relativePath,
				lastModifiedTime: blobInfo.Properties.LastModified,
				size:             *blobInfo.Properties.ContentLength,
			}
			t.incrementEnumerationCounter()
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
