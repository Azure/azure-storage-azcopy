package cmd

import (
	"context"
	"encoding/base64"
	"errors"
	"fmt"
	"net/url"
	"sync"

	"github.com/Azure/azure-storage-azcopy/common"
	"github.com/Azure/azure-storage-blob-go/2018-03-28/azblob"
)

// copyBlobToNEnumerator enumerates blob source, and submit request for copy blob to blob/file/blobFS (Currently only blob is supported)
// The source could be single blob/container/blob account
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
	sourceURL, err := url.Parse(gCopyUtil.replaceBackSlashWithSlash(srcURLStr))
	if err != nil {
		return errors.New("cannot parse source URL")
	}
	destURL, err := url.Parse(gCopyUtil.replaceBackSlashWithSlash(destURLStr))
	if err != nil {
		return errors.New("cannot parse destination URL")
	}

	srcBlobURLPart := azblob.NewBlobURLParts(*sourceURL)
	// Case-1: Source is account, currently only support blob destination
	if isAccountLevel, searchPrefix, pattern := gCopyUtil.isAccountLevelSearch(srcBlobURLPart); isAccountLevel {
		if pattern == "*" && !isRecursiveOn {
			return fmt.Errorf("cannot copy the entire account without recursive flag, please use recursive flag")
		}

		// Switch URL https://<account-name>/containerprefix* to ServiceURL "https://<account-name>"
		tmpSrcBlobURLPart := srcBlobURLPart
		tmpSrcBlobURLPart.ContainerName = ""
		srcServiceURL := azblob.NewServiceURL(tmpSrcBlobURLPart.URL(), p)
		// Validate destination
		// TODO: other type destination URLs, e.g: BlobFS, File and etc
		destServiceURL := azblob.NewServiceURL(*destURL, p)
		_, err = destServiceURL.GetProperties(ctx)
		if err != nil {
			return errors.New("invalid source and destination combination for service to service copy: " +
				"destination must point to service account when source is a service account.")
		}

		// List containers
		err = e.enumerateContainersInAccount(ctx, srcServiceURL, *destURL, searchPrefix, wg, waitUntilJobCompletion)
		if err != nil {
			return err
		}

		// If part number is 0 && number of transfer queued is 0
		// it means that no job part has been dispatched and there are no
		// transfer in Job to dispatch a JobPart.
		if e.PartNum == 0 && len(e.Transfers) == 0 {
			return fmt.Errorf("no transfer queued to copy. Please verify the source / destination")
		}

		// dispatch the JobPart as Final Part of the Job
		err := e.dispatchFinalPart()
		if err != nil {
			return err
		}
		return nil
	}

	// Case-2: Source is single blob
	// Verify if source is a single blob
	srcBlobURL := azblob.NewBlobURL(*sourceURL, p)
	blobProperties, err := srcBlobURL.GetProperties(ctx, azblob.BlobAccessConditions{})
	// Note: Currently only support single to single, and not support single to directory.
	if err == nil {
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

	// Case-3: Source is a blob container or directory
	// Switch URL https://<account-name>/container/blobprefix* to ContainerURL "https://<account-name>/container"
	searchPrefix, pattern := gCopyUtil.searchPrefixFromUrl(srcBlobURLPart)
	if pattern == "*" && !isRecursiveOn {
		return fmt.Errorf("cannot copy the entire container or directory without recursive flag, please use recursive flag")
	}
	tmpSrcBlobURLPart := srcBlobURLPart
	tmpSrcBlobURLPart.BlobName = ""
	err = e.enumerateBlobsInContainer(
		ctx,
		azblob.NewContainerURL(tmpSrcBlobURLPart.URL(), p),
		*destURL,
		searchPrefix,
		wg,
		waitUntilJobCompletion)
	if err != nil {
		return err
	}

	// If part number is 0 && number of transfer queued is 0
	// it means that no job part has been dispatched and there are no
	// transfer in Job to dispatch a JobPart.
	if e.PartNum == 0 && len(e.Transfers) == 0 {
		return fmt.Errorf("no transfer queued to copy. Please verify the source / destination")
	}

	// dispatch the JobPart as Final Part of the Job
	return e.dispatchFinalPart()
}

// TODO: whether to customize public level?
func (e *copyBlobToNEnumerator) createBucket(ctx context.Context, destURL url.URL, metadata azblob.Metadata) error {
	switch e.FromTo {
	case common.EFromTo.BlobBlob():
		// Create pipeline for destination bucket cr
		// TODO: refactor the logic to make blob pipeline shared, note there could be different destinations
		p, err := createBlobPipeline(ctx, e.CredentialInfo)
		if err != nil {
			return err
		}
		containerURL := azblob.NewContainerURL(destURL, p)
		// Create the container, in case of it doesn't exist.
		_, err = containerURL.Create(ctx, metadata, azblob.PublicAccessNone)
		if err != nil {
			if stgErr, ok := err.(azblob.StorageError); !ok || stgErr.ServiceCode() != azblob.ServiceCodeContainerAlreadyExists {
				return fmt.Errorf("fail to create container during enumerate containers in account, %v", err)
			}
			// the case error is container already exists
		}
	}
	return nil
}

// enumerateContainersInAccount enumerates containers in blob service account.
func (e *copyBlobToNEnumerator) enumerateContainersInAccount(ctx context.Context, srcServiceURL azblob.ServiceURL, destBaseURL url.URL,
	srcSearchPattern string, wg *sync.WaitGroup, waitUntilJobCompletion func(jobID common.JobID, wg *sync.WaitGroup)) error {
	for marker := (azblob.Marker{}); marker.NotDone(); {
		listSvcResp, err := srcServiceURL.ListContainersSegment(ctx, marker,
			azblob.ListContainersSegmentOptions{Prefix: srcSearchPattern})
		if err != nil {
			return fmt.Errorf("cannot list containers for copy, %v", err)
		}

		// Process the containers returned in this result segment (if the segment is empty, the loop body won't execute)
		for _, containerItem := range listSvcResp.ContainerItems {
			// TODO: Note case for different destination URL.
			// Whatever the destination type is, it should be equivalent to account level,
			// directoy append container name to it.
			tmpDestURL := destBaseURL
			tmpDestURL.Path = gCopyUtil.generateObjectPath(tmpDestURL.Path, containerItem.Name)
			containerURL := srcServiceURL.NewContainerURL(containerItem.Name)

			// TODO: Create share/bucket and etc.
			// Currently only support blob to blob, so only create container
			e.createBucket(ctx, tmpDestURL, containerItem.Metadata)

			// List source container
			// TODO: List in parallel to speed up.
			e.enumerateBlobsInContainer(
				ctx,
				containerURL,
				tmpDestURL,
				"",
				wg,
				waitUntilJobCompletion)
		}
		marker = listSvcResp.NextMarker
	}
	return nil
}

// enumerateBlobsInContainer enumerates blobs in container.
func (e *copyBlobToNEnumerator) enumerateBlobsInContainer(ctx context.Context, srcContainerURL azblob.ContainerURL, destBaseURL url.URL,
	srcSearchPattern string, wg *sync.WaitGroup, waitUntilJobCompletion func(jobID common.JobID, wg *sync.WaitGroup)) error {
	for marker := (azblob.Marker{}); marker.NotDone(); {
		listContainerResp, err := srcContainerURL.ListBlobsFlatSegment(ctx, marker,
			azblob.ListBlobsSegmentOptions{Details: azblob.BlobListingDetails{Metadata: true}, Prefix: srcSearchPattern})
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
			// TODO: special char (naming resolution) for special directions
			blobRelativePath := gCopyUtil.getRelativePath(srcSearchPattern, blobItem.Name, "/")
			tmpDestURL := destBaseURL
			tmpDestURL.Path = gCopyUtil.generateObjectPath(tmpDestURL.Path, blobRelativePath)
			err = e.addTransferInternal(
				srcContainerURL.NewBlobURL(blobItem.Name).String(),
				tmpDestURL.String(),
				&blobItem.Properties,
				wg,
				waitUntilJobCompletion)
			if err != nil {
				return err // TODO: Ensure for list errors, directly return or do logging but not return, make the list mechanism much robust
			}
		}
		marker = listContainerResp.NextMarker
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

	// work around an existing client bug, the contentMD5 returned from list is base64 encoded bytes, and should be base64 decoded bytes.
	md5DecodedBytes, err := base64.StdEncoding.DecodeString(string(properties.ContentMD5))
	if err != nil {
		return err
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
		ContentMD5:         string(md5DecodedBytes)},
		//BlobTier:           string(properties.AccessTier)}, // TODO: blob tier setting correctly
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
		ContentMD5:         string(properties.ContentMD5())},
		//BlobTier:           properties.AccessTier()}, // TODO: blob tier setting correctly
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
