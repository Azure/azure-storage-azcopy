package cmd

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/url"

	"github.com/Azure/azure-pipeline-go/pipeline"
	"github.com/Azure/azure-storage-azcopy/common"
	"github.com/Azure/azure-storage-blob-go/2018-03-28/azblob"
)

// copyS2SEnumerator is the base of other service to service copy enumerators,
// which contains common functions and properties.
type copyS2SEnumerator struct {
	common.CopyJobPartOrderRequest

	// object used for destination pre-operations: e.g. create container/share/bucket and etc.
	destBlobPipeline pipeline.Pipeline
}

// initDestPipeline inits destination pipelines shared for destination operations.
func (e *copyS2SEnumerator) initDestPipeline(ctx context.Context) error {
	switch e.FromTo {
	// Currently, e.CredentialInfo is always for the target needs to trigger copy API.
	// In this case, blob destination will use it which needs to call StageBlockFromURL later.
	case common.EFromTo.BlobBlob():
		p, err := createBlobPipeline(ctx, e.CredentialInfo)
		if err != nil {
			return err
		}
		e.destBlobPipeline = p
	}
	return nil
}

// createDestBucket creates bucket level resource for destination, e.g. container for blob, share for file, and etc.
// TODO: Ensure if metadata in bucket level need be copied, currently not copy metadata in bucket level as azcopy-v1.
func (e *copyS2SEnumerator) createDestBucket(ctx context.Context, destURL url.URL, metadata common.Metadata) error {
	switch e.FromTo {
	case common.EFromTo.BlobBlob():
		if e.destBlobPipeline == nil {
			panic(errors.New("invalid state, blob type destination's pipeline is not initialized"))
		}
		tmpContainerURL := blobURLPartsExtension{azblob.NewBlobURLParts(destURL)}.getContainerURL()
		containerURL := azblob.NewContainerURL(tmpContainerURL, e.destBlobPipeline)
		// Create the container, in case of it doesn't exist.
		_, err := containerURL.Create(ctx, metadata.ToAzBlobMetadata(), azblob.PublicAccessNone)
		if err != nil {
			// Skip the error, when container already exists, or hasn't permission to create container(container might already exists).
			if stgErr, ok := err.(azblob.StorageError); !ok ||
				(stgErr.ServiceCode() != azblob.ServiceCodeContainerAlreadyExists &&
					stgErr.Response().StatusCode != http.StatusForbidden) {
				return fmt.Errorf("fail to create container, %v", err)
			}
			// the case error is container already exists
		}
	}
	return nil
}

// validateDestIsService check if destination is a service level URL.
func (e *copyS2SEnumerator) validateDestIsService(ctx context.Context, destURL url.URL) error {
	switch e.FromTo {
	case common.EFromTo.BlobBlob():
		if e.destBlobPipeline == nil {
			panic(errors.New("invalid state, blob type destination's pipeline is not initialized"))
		}
		destServiceURL := azblob.NewServiceURL(destURL, e.destBlobPipeline)
		if _, err := destServiceURL.GetProperties(ctx); err != nil {
			return fmt.Errorf("invalid source and destination combination for service to service copy: "+
				"destination must point to service account when source is a service account, %v", err)
		}
	}

	return nil
}
