package common

import (
	"context"
	"net/url"

	"github.com/Azure/azure-storage-blob-go/azblob"
)

func IsSourcePublicBlob(sourceURI string, ctx context.Context) bool {
	uri, err := url.Parse(sourceURI)
	if err != nil {
		// this should never, would never be hit.
		// a job plan file couldn't be created by AzCopy with an invalid URI.
		panic("Source URI was invalid.")
	}

	blobParts := azblob.NewBlobURLParts(*uri)

	// only containers can be public access
	if blobParts.ContainerName != "" {
		if blobParts.BlobName != "" {
			// first test that it's a blob
			bURL := azblob.NewBlobURL(*uri, azblob.NewPipeline(azblob.NewAnonymousCredential(), azblob.PipelineOptions{}))
			_, err := bURL.GetProperties(ctx, azblob.BlobAccessConditions{}, azblob.ClientProvidedKeyOptions{})
			if err == nil {
				return true
			}

			// since that failed, maybe it doesn't exist and is public to list?
			blobParts.BlobName = ""
		}

		cURL := azblob.NewContainerURL(blobParts.URL(), azblob.NewPipeline(azblob.NewAnonymousCredential(), azblob.PipelineOptions{}))

		_, err := cURL.ListBlobsFlatSegment(ctx, azblob.Marker{}, azblob.ListBlobsSegmentOptions{})
		if err == nil {
			return true
		}
	}

	return false
}
