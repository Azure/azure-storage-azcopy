package common

import (
	"context"

	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/blob"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/container"
)

func IsSourcePublicBlob(sourceURI string, ctx context.Context) bool {
	blobParts, err := blob.ParseURL(sourceURI)
	if err != nil {
		// this should never, would never be hit.
		// a job plan file couldn't be created by AzCopy with an invalid URI.
		panic("Source URI was invalid.")
	}

	// only containers can be public access
	if blobParts.ContainerName != "" {
		if blobParts.BlobName != "" {
			// first test that it's a blob
			blobClient, err := blob.NewClientWithNoCredential(sourceURI, nil)
			if err != nil {
				// this should never, would never be hit.
				// a job plan file couldn't be created by AzCopy with an invalid URI.
				panic("Blob client was unable to be created.")
			}
			_, err = blobClient.GetProperties(ctx, nil)
			if err == nil {
				return true
			}

			// since that failed, maybe it doesn't exist and is public to list?
			blobParts.BlobName = ""
			blobParts.Snapshot = ""
			blobParts.VersionID = ""
		}

		containerClient, err := container.NewClientWithNoCredential(blobParts.String(), nil)
		if err != nil {
			// this should never, would never be hit.
			// a job plan file couldn't be created by AzCopy with an invalid URI.
			panic("Container client was unable to be created.")
		}

		pager := containerClient.NewListBlobsFlatPager(nil)
		_, err = pager.NextPage(ctx)
		if err == nil {
			return true
		}
	}

	return false
}
