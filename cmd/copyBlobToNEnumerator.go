package cmd

import (
	"context"
	"encoding/base64"
	"errors"
	"fmt"
	"net/http"
	"net/url"

	"github.com/Azure/azure-pipeline-go/pipeline"
	"github.com/Azure/azure-storage-azcopy/common"
	"github.com/Azure/azure-storage-blob-go/2018-03-28/azblob"
)

// copyBlobToNEnumerator enumerates blob source, and submit request for copy blob to N,
// where N stands for blob/file/blobFS (Currently only blob is supported).
// The source could be a single blob/container/blob account
type copyBlobToNEnumerator common.CopyJobPartOrderRequest

// destination helper info for destination pre-operations: e.g. create container/share/bucket and etc.
type destHelperInfo struct {
	destBlobPipeline pipeline.Pipeline
	// info for other dest type
}

var destInfo destHelperInfo

func (e *copyBlobToNEnumerator) enumerate(cca *cookedCopyCmdArgs) error {
	ctx := context.TODO()

	// Create pipeline for source Blob service.
	// For copy source with blob type, only anonymous credential is supported now(i.e. SAS or public).
	// So directoy create anonymous credential for source.
	// Note: If traditional copy(download first, then upload need be supported), more logic should be added to parse and validate
	// credential for both source and destination.
	srcCredInfo := common.CredentialInfo{CredentialType: common.ECredentialType.Anonymous()}
	srcBlobPipeline, err := createBlobPipeline(ctx, srcCredInfo)
	if err != nil {
		return err
	}
	err = e.initiateDestHelperInfo(ctx)
	if err != nil {
		return err
	}

	// attempt to parse the source and destination url
	sourceURL, err := url.Parse(gCopyUtil.replaceBackSlashWithSlash(cca.source))
	if err != nil {
		return errors.New("cannot parse source URL")
	}
	sourceURL = gCopyUtil.appendQueryParamToUrl(sourceURL, cca.sourceSAS)

	destURL, err := url.Parse(gCopyUtil.replaceBackSlashWithSlash(cca.destination))
	if err != nil {
		return errors.New("cannot parse destination URL")
	}
	destURL = gCopyUtil.appendQueryParamToUrl(destURL, cca.destinationSAS)

	srcBlobURLPartExtension := blobURLPartsExtension{azblob.NewBlobURLParts(*sourceURL)}
	// Case-1: Source is account, currently only support blob destination
	if isAccountLevel, searchPrefix, pattern := srcBlobURLPartExtension.isBlobAccountLevelSearch(); isAccountLevel {
		if pattern == "*" && !cca.recursive {
			return fmt.Errorf("cannot copy the entire account without recursive flag, please use recursive flag")
		}

		// Switch URL https://<account-name>/containerprefix* to ServiceURL "https://<account-name>"
		tmpSrcBlobURLPart := srcBlobURLPartExtension
		tmpSrcBlobURLPart.ContainerName = ""
		srcServiceURL := azblob.NewServiceURL(tmpSrcBlobURLPart.URL(), srcBlobPipeline)
		// Validate destination
		// TODO: other type destination URLs, e.g: BlobFS, File and etc
		destServiceURL := azblob.NewServiceURL(*destURL, srcBlobPipeline)
		_, err = destServiceURL.GetProperties(ctx)
		if err != nil {
			return errors.New("invalid source and destination combination for service to service copy: " +
				"destination must point to service account when source is a service account.")
		}

		// List containers
		err = e.enumerateContainersInAccount(ctx, srcServiceURL, *destURL, searchPrefix, cca)
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
		err := e.dispatchFinalPart(cca)
		if err != nil {
			return err
		}
		return nil
	}

	// Case-2: Source is single blob
	// Verify if source is a single blob
	srcBlobURL := azblob.NewBlobURL(*sourceURL, srcBlobPipeline)
	blobProperties, err := srcBlobURL.GetProperties(ctx, azblob.BlobAccessConditions{})
	// Note: Currently only support single to single, and not support single to directory.
	if err == nil {
		if endWithSlashOrBackSlash(destURL.Path) {
			return errors.New("invalid source and destination combination for service to service copy: " +
				"destination must point to a single file, when source is a single file.")
		}
		err := e.createBucket(ctx, *destURL, nil)
		if err != nil {
			return err
		}

		// directly use destURL as destination
		if err := e.addTransferInternal2(srcBlobURL.URL(), *destURL, blobProperties, cca); err != nil {
			return err
		}
		return e.dispatchFinalPart(cca)
	}

	// Case-3: Source is a blob container or directory
	// Switch URL https://<account-name>/container/blobprefix* to ContainerURL "https://<account-name>/container"
	searchPrefix, pattern := srcBlobURLPartExtension.searchPrefixFromBlobURL()
	if pattern == "*" && !cca.recursive {
		return fmt.Errorf("cannot copy the entire container or directory without recursive flag, please use recursive flag")
	}
	err = e.createBucket(ctx, *destURL, nil)
	if err != nil {
		return err
	}
	err = e.enumerateBlobsInContainer(
		ctx,
		azblob.NewContainerURL(srcBlobURLPartExtension.getContainerURL(), srcBlobPipeline),
		*destURL,
		searchPrefix,
		cca)
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
	return e.dispatchFinalPart(cca)
}

// destination helper info for destination pre-operations: e.g. create container/share/bucket and etc.
// The info, such as blob pipeline is created once, and reused multiple times.
func (e *copyBlobToNEnumerator) initiateDestHelperInfo(ctx context.Context) error {
	switch e.FromTo {
	// Currently, e.CredentialInfo is always for the target needs to trigger copy API.
	// In this case, blob destination will use it which needs to call StageBlockFromURL later.
	case common.EFromTo.BlobBlob():
		p, err := createBlobPipeline(ctx, e.CredentialInfo)
		if err != nil {
			return err
		}
		destInfo.destBlobPipeline = p
	}
	return nil
}

// createBucket creats bucket level object for the destination, e.g. container for blob, share for file, and etc.
// TODO: Create share/bucket and etc. Currently only support blob destination.
// TODO: Ensure if metadata in bucket level need be copied, currently not copy metadata in bucket level as azcopy-v1 do.
func (e *copyBlobToNEnumerator) createBucket(ctx context.Context, destURL url.URL, metadata common.Metadata) error {
	switch e.FromTo {
	case common.EFromTo.BlobBlob():
		if destInfo.destBlobPipeline == nil {
			panic(errors.New("invalid state, blob type destination's pipeline is not initialized"))
		}
		tmpContainerURL := blobURLPartsExtension{azblob.NewBlobURLParts(destURL)}.getContainerURL()
		containerURL := azblob.NewContainerURL(tmpContainerURL, destInfo.destBlobPipeline)
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

// enumerateContainersInAccount enumerates containers in blob service account.
func (e *copyBlobToNEnumerator) enumerateContainersInAccount(ctx context.Context, srcServiceURL azblob.ServiceURL, destBaseURL url.URL,
	srcSearchPattern string, cca *cookedCopyCmdArgs) error {
	for marker := (azblob.Marker{}); marker.NotDone(); {
		listSvcResp, err := srcServiceURL.ListContainersSegment(ctx, marker,
			azblob.ListContainersSegmentOptions{Prefix: srcSearchPattern})
		if err != nil {
			return fmt.Errorf("cannot list containers for copy, %v", err)
		}

		// Process the containers returned in this result segment (if the segment is empty, the loop body won't execute)
		for _, containerItem := range listSvcResp.ContainerItems {
			// Whatever the destination type is, it should be equivalent to account level,
			// directly append container name to it.
			tmpDestURL := destBaseURL
			tmpDestURL.Path = gCopyUtil.generateObjectPath(tmpDestURL.Path, containerItem.Name)
			containerURL := srcServiceURL.NewContainerURL(containerItem.Name)

			// Transfer azblob's metadata to common metadata, note common metadata can be transferred to other types of metadata.
			e.createBucket(ctx, tmpDestURL, nil)

			// List source container
			// TODO: List in parallel to speed up.
			e.enumerateBlobsInContainer(
				ctx,
				containerURL,
				tmpDestURL,
				"",
				cca)
		}
		marker = listSvcResp.NextMarker
	}
	return nil
}

// enumerateBlobsInContainer enumerates blobs in container.
func (e *copyBlobToNEnumerator) enumerateBlobsInContainer(ctx context.Context, srcContainerURL azblob.ContainerURL, destBaseURL url.URL,
	srcSearchPattern string, cca *cookedCopyCmdArgs) error {
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
			blobRelativePath := gCopyUtil.getRelativePath(srcSearchPattern, blobItem.Name)
			tmpDestURL := destBaseURL
			tmpDestURL.Path = gCopyUtil.generateObjectPath(tmpDestURL.Path, blobRelativePath)
			err = e.addTransferInternal(
				srcContainerURL.NewBlobURL(blobItem.Name).URL(),
				tmpDestURL,
				&blobItem.Properties,
				blobItem.Metadata,
				cca)
			if err != nil {
				return err // TODO: Ensure for list errors, directly return or do logging but not return, make the list mechanism much robust
			}
		}
		marker = listContainerResp.NextMarker
	}
	return nil
}

func (e *copyBlobToNEnumerator) addTransferInternal(srcURL, destURL url.URL, properties *azblob.BlobProperties, metadata azblob.Metadata,
	cca *cookedCopyCmdArgs) error {
	if properties.BlobType != azblob.BlobBlockBlob {
		glcm.Info(fmt.Sprintf(
			"Skipping %v: %s. This version of AzCopy only supports BlockBlob transfer.",
			properties.BlobType,
			common.URLExtension{URL: srcURL}.RedactSigQueryParamForLogging()))
	}

	// work around an existing client bug, the contentMD5 returned from list is base64 encoded bytes, and should be base64 decoded bytes.
	md5DecodedBytes, err := base64.StdEncoding.DecodeString(string(properties.ContentMD5))
	if err != nil {
		return err
	}

	return e.addTransfer(common.CopyTransfer{
		Source:             gCopyUtil.stripSASFromBlobUrl(srcURL).String(),
		Destination:        gCopyUtil.stripSASFromBlobUrl(destURL).String(),
		LastModifiedTime:   properties.LastModified,
		SourceSize:         *properties.ContentLength,
		ContentType:        *properties.ContentType,
		ContentEncoding:    *properties.ContentEncoding,
		ContentDisposition: *properties.ContentDisposition,
		ContentLanguage:    *properties.ContentLanguage,
		CacheControl:       *properties.CacheControl,
		ContentMD5:         md5DecodedBytes,
		Metadata:           common.FromAzBlobMetadataToCommonMetadata(metadata),
		BlobType:           properties.BlobType},
		//BlobTier:           string(properties.AccessTier)}, // TODO: blob tier setting correctly
		cca)
}

func (e *copyBlobToNEnumerator) addTransferInternal2(srcURL, destURL url.URL, properties *azblob.BlobGetPropertiesResponse,
	cca *cookedCopyCmdArgs) error {
	if properties.BlobType() != azblob.BlobBlockBlob {
		glcm.Info(fmt.Sprintf(
			"Skipping %v: %s. This version of AzCopy only supports BlockBlob transfer.",
			properties.BlobType(),
			common.URLExtension{URL: srcURL}.RedactSigQueryParamForLogging()))
	}

	return e.addTransfer(common.CopyTransfer{
		Source:             gCopyUtil.stripSASFromBlobUrl(srcURL).String(),
		Destination:        gCopyUtil.stripSASFromBlobUrl(destURL).String(),
		LastModifiedTime:   properties.LastModified(),
		SourceSize:         properties.ContentLength(),
		ContentType:        properties.ContentType(),
		ContentEncoding:    properties.ContentEncoding(),
		ContentDisposition: properties.ContentDisposition(),
		ContentLanguage:    properties.ContentLanguage(),
		CacheControl:       properties.CacheControl(),
		ContentMD5:         properties.ContentMD5(),
		Metadata:           common.FromAzBlobMetadataToCommonMetadata(properties.NewMetadata()),
		BlobType:           properties.BlobType()},
		//BlobTier:           properties.AccessTier()}, // TODO: blob tier setting correctly
		cca)
}

func (e *copyBlobToNEnumerator) addTransfer(transfer common.CopyTransfer, cca *cookedCopyCmdArgs) error {
	return addTransfer((*common.CopyJobPartOrderRequest)(e), transfer, cca)
}

func (e *copyBlobToNEnumerator) dispatchFinalPart(cca *cookedCopyCmdArgs) error {
	return dispatchFinalPart((*common.CopyJobPartOrderRequest)(e), cca)
}

func (e *copyBlobToNEnumerator) partNum() common.PartNumber {
	return e.PartNum
}
