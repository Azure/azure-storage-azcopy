package cmd

import (
	"context"
	"errors"
	"fmt"
	"net/url"
	"sync"

	"github.com/Azure/azure-storage-azcopy/common"
	"github.com/Azure/azure-storage-blob-go/2018-03-28/azblob"
)

// copyBlobToNEnumerator enumerates blob source, and submit request for copy blob to blob/file/blobFS (Currently only blob is supported)
// The source could be single blob/container/blob account (Currently only single blob/container is supported)
type copyBlobToNEnumerator common.CopyJobPartOrderRequest

// TODO: add logic to validating source&dest combination, e.g. for account src, the destination could only be account.
func (e *copyBlobToNEnumerator) enumerate(srcURLStr string, isRecursiveOn bool, destURLStr string,
	wg *sync.WaitGroup, waitUntilJobCompletion func(jobID common.JobID, wg *sync.WaitGroup)) error {
	ctx := context.TODO()

	// Create pipeline for source Blob service.
	// TODO: Note: only anonymous credential is supported for blob source(i.e. SAS or public) now.
	// credential validation logic for both source and destination
	// Note: e.CredentialInfo is for destination
	srcCredInfo := common.CredentialInfo{CredentialType: common.ECredentialType.Anonymous()}
	p, err := createBlobPipeline(ctx, srcCredInfo)
	if err != nil {
		return err
	}

	// attempt to parse the source url
	sourceURL, err := url.Parse(srcURLStr)
	if err != nil {
		return errors.New("cannot parse source URL")
	}
	destURL, err := url.Parse(destURLStr)
	if err != nil {
		return errors.New("cannot parse destination URL")
	}

	// Verify if source is service account
	srcServiceURL := azblob.NewServiceURL(*sourceURL, p)
	_, err = srcServiceURL.GetProperties(ctx)
	// Case-1: Source is account
	if err == nil {
		// Validate destination
		// Currently only support blob case
		// TODO: other type destination URLs, e.g: BlobFS, File and etc
		destServiceURL := azblob.NewServiceURL(*destURL, p)
		_, err = destServiceURL.GetProperties(ctx)
		if err != nil {
			return errors.New("invalid source and destination combination for service to service copy: " +
				"destination must point to service account when source is a service account.")
		}

		// List containers
		err = e.enumerateBlobContainers(ctx, srcServiceURL, destServiceURL.URL(), wg, waitUntilJobCompletion)
		if err != nil {
			return err
		}
	}

	// Verify if source is a single blob
	srcBlobURL := azblob.NewBlobURL(*sourceURL, p)
	blobProperties, err := srcBlobURL.GetProperties(ctx, azblob.BlobAccessConditions{})
	// Case-2: Source is single blob
	// Note: Currently only support single to single, considering the destination could be Blob/File/BlobFS and etc
	// Each could be a directory or single file destination
	if err == nil {
		// TODO: if both directory and single file could be supported as destination.
		if endWithSlashOrBackSlash(destURL.Path) {
			return errors.New("invalid source and destination combination for service to service copy: " +
				"destination must point to a single file, when source is a single file.")
		}
		// directly use destURL as destination
		if err := e.addTransferInternal2(srcBlobURL.String(), destURL.String(), blobProperties, wg, waitUntilJobCompletion); err != nil {
			return err
		}
		return e.dispatchFinalPart()
	}

	// Source should be a blob container or directory
	// Case-3: Source is a blob container or directory
	return e.enumerateBlobs(ctx, azblob.NewContainerURL(*sourceURL, p), *destURL, wg, waitUntilJobCompletion)
}

// TODO: search prefix?
func (e *copyBlobToNEnumerator) enumerateBlobContainers(ctx context.Context, srcServiceURL azblob.ServiceURL, destBaseURL url.URL,
	wg *sync.WaitGroup, waitUntilJobCompletion func(jobID common.JobID, wg *sync.WaitGroup)) error {
	for marker := (azblob.Marker{}); marker.NotDone(); {
		listSvcResp, err := srcServiceURL.ListContainersSegment(ctx, marker,
			azblob.ListContainersSegmentOptions{})
		if err != nil {
			return fmt.Errorf("cannot list containers for copy, %v", err)
		}

		// Process the containers returned in this result segment (if the segment is empty, the loop body won't execute)
		for _, containerItem := range listSvcResp.ContainerItems {
			// TODO: Does container's metadata need be copied
			tmpDestURL := destBaseURL
			tmpDestURL.Path = gCopyUtil.generateObjectPath(tmpDestURL.Path, containerItem.Name)
			// List container
			e.enumerateBlobs(
				ctx,
				srcServiceURL.NewContainerURL(containerItem.Name),
				tmpDestURL,
				wg,
				waitUntilJobCompletion)
		}
		marker = listSvcResp.NextMarker
	}
	return nil
}

// TODO: search prefix, when directly list container?
func (e *copyBlobToNEnumerator) enumerateBlobs(ctx context.Context, srcContainerURL azblob.ContainerURL, destBaseURL url.URL,
	wg *sync.WaitGroup, waitUntilJobCompletion func(jobID common.JobID, wg *sync.WaitGroup)) error {
	// TODO: search prefix?
	for marker := (azblob.Marker{}); marker.NotDone(); {
		listContainerResp, err := srcContainerURL.ListBlobsFlatSegment(ctx, marker,
			azblob.ListBlobsSegmentOptions{Details: azblob.BlobListingDetails{Metadata: true}})
		if err != nil {
			return fmt.Errorf("cannot list blobs for copy, %v", err)
		}

		// Process the blobs returned in this result segment (if the segment is empty, the loop body won't execute)
		for _, blobItem := range listContainerResp.Segment.BlobItems {
			// If the blob represents a folder as per the conditions mentioned in the
			// api doesBlobRepresentAFolder, then skip the blob.
			if gCopyUtil.doesBlobRepresentAFolder(blobItem) {
				continue
			}
			// TODO: include, exclude, special char (naming resolution)
			tmpDestURL := destBaseURL
			tmpDestURL.Path = gCopyUtil.generateObjectPath(tmpDestURL.Path, blobItem.Name)
			err = e.addTransferInternal(
				srcContainerURL.NewBlobURL(blobItem.Name).String(),
				tmpDestURL.String(),
				&blobItem.Properties,
				wg,
				waitUntilJobCompletion)
			if err != nil {
				return err // TODO: List error, directly return, make the list mechanism much robust
			}
		}
		marker = listContainerResp.NextMarker
	}

	// If part number is 0 && number of transfer queued is 0
	// it means that no job part has been dispatched and there are no
	// transfer in Job to dispatch a JobPart.
	if e.PartNum == 0 && len(e.Transfers) == 0 {
		return fmt.Errorf("no transfer queued to download. Please verify the source / destination")
	}

	// dispatch the JobPart as Final Part of the Job
	err := e.dispatchFinalPart()
	if err != nil {
		return err
	}
	return nil
}

func (e *copyBlobToNEnumerator) addTransferInternal(source, dest string, properties *azblob.BlobProperties,
	wg *sync.WaitGroup, waitUntilJobCompletion func(jobID common.JobID, wg *sync.WaitGroup)) error {
	if properties.BlobType != azblob.BlobBlockBlob {
		return fmt.Errorf(
			"invalid blob type %q found in Service to Service copy, only block blob is supported now when source is Blob",
			properties.BlobType)
	}

	return e.addTransfer(common.CopyTransfer{
		Source:             source,
		Destination:        dest,
		LastModifiedTime:   properties.LastModified,
		SourceSize:         *properties.ContentLength,
		ContentType:        *properties.ContentType,
		ContentEncoding:    *properties.ContentEncoding,
		ContentDisposition: *properties.ContentDisposition,
		ContentLanguage:    *properties.ContentLanguage,
		CacheControl:       *properties.CacheControl,
		ContentMD5:         string(properties.ContentMD5),
		BlobTier:           string(properties.AccessTier)}, // TODO: blob tier setting correctly
		wg,
		waitUntilJobCompletion)
}

func (e *copyBlobToNEnumerator) addTransferInternal2(source, dest string, properties *azblob.BlobGetPropertiesResponse,
	wg *sync.WaitGroup, waitUntilJobCompletion func(jobID common.JobID, wg *sync.WaitGroup)) error {
	if properties.BlobType() != azblob.BlobBlockBlob {
		return fmt.Errorf(
			"invalid blob type %q found in Service to Service copy, only block blob is supported now when source is Blob",
			properties.BlobType())
	}

	return e.addTransfer(common.CopyTransfer{
		Source:             source,
		Destination:        dest,
		LastModifiedTime:   properties.LastModified(),
		SourceSize:         properties.ContentLength(),
		ContentType:        properties.ContentType(),
		ContentEncoding:    properties.ContentEncoding(),
		ContentDisposition: properties.ContentDisposition(),
		ContentLanguage:    properties.ContentLanguage(),
		CacheControl:       properties.CacheControl(),
		ContentMD5:         string(properties.ContentMD5()),
		BlobTier:           properties.AccessTier()}, // TODO: blob tier setting correctly
		wg,
		waitUntilJobCompletion)
}

func (e *copyBlobToNEnumerator) addTransfer(transfer common.CopyTransfer, wg *sync.WaitGroup,
	waitUntilJobCompletion func(jobID common.JobID, wg *sync.WaitGroup)) error {
	return addTransfer((*common.CopyJobPartOrderRequest)(e), transfer, wg, waitUntilJobCompletion)
}

func (e *copyBlobToNEnumerator) dispatchFinalPart() error {
	return dispatchFinalPart((*common.CopyJobPartOrderRequest)(e))
}

func (e *copyBlobToNEnumerator) partNum() common.PartNumber {
	return e.PartNum
}
