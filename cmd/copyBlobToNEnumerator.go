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

// destPreProceed is used for destination pre-operations: e.g. create container/share/bucket and etc.
type destPreProceed struct {
	destBlobPipeline pipeline.Pipeline
	// info for other dest type
}

var destInfo destPreProceed

func (e *copyBlobToNEnumerator) enumerate(cca *cookedCopyCmdArgs) error {
	ctx := context.TODO()

	// attempt to parse the source and destination url
	sourceURL, err := url.Parse(gCopyUtil.replaceBackSlashWithSlash(cca.source))
	if err != nil {
		return errors.New("cannot parse source URL")
	}
	destURL, err := url.Parse(gCopyUtil.replaceBackSlashWithSlash(cca.destination))
	if err != nil {
		return errors.New("cannot parse destination URL")
	}

	// append the sas at the end of query params.
	sourceURL = gCopyUtil.appendQueryParamToUrl(sourceURL, cca.sourceSAS)
	destURL = gCopyUtil.appendQueryParamToUrl(destURL, cca.destinationSAS)

	// Create pipeline for source Blob service.
	// For copy source with blob type, only anonymous credential is supported now(i.e. SAS or public).
	// So directoy create anonymous credential for source.
	// Note: If traditional copy(download first, then upload need be supported), more logic should be added to parse and validate
	// credential for both source and destination.
	srcBlobPipeline, err := createBlobPipeline(ctx,
		common.CredentialInfo{CredentialType: common.ECredentialType.Anonymous()})
	if err != nil {
		return err
	}

	if err = e.initDestPreProceed(ctx); err != nil {
		return err
	}

	srcBlobURLPartExtension := blobURLPartsExtension{azblob.NewBlobURLParts(*sourceURL)}

	// Case-1: Source is a single blob
	// Verify if source is a single blob
	srcBlobURL := azblob.NewBlobURL(*sourceURL, srcBlobPipeline)
	// Note: Currently only support single to single, and not support single to directory.
	if blobProperties, err := srcBlobURL.GetProperties(ctx, azblob.BlobAccessConditions{}); err == nil {
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

	// Case-2: Source is account level, e.g.:
	// a: https://<blob-service>/container
	// b: https://<blob-service>/containerprefix*/vd/blob
	if isAccountLevel, containerPrefix := srcBlobURLPartExtension.isBlobAccountLevelSearch(); isAccountLevel {
		if !cca.recursive {
			return fmt.Errorf("cannot copy the entire account without recursive flag, please use recursive flag")
		}

		// Validate IF destination is service level account.
		if err := e.validateDestIsService(ctx, *destURL); err != nil {
			return err
		}

		srcServiceURL := azblob.NewServiceURL(srcBlobURLPartExtension.getServiceURL(), srcBlobPipeline)
		// List containers, find specific containers and add transfers for these containers.
		if err := enumerateContainersInAccount(
			ctx,
			srcServiceURL,
			containerPrefix,
			func(containerItem azblob.ContainerItem) error {
				// Whatever the destination type is, it should be equivalent to account level,
				// so directly append container name to it.
				tmpDestURL := urlExtension{URL: *destURL}.generateObjectPath(containerItem.Name)
				// create bucket for destination, in case bucket doesn't exist.
				if err := e.createBucket(ctx, tmpDestURL, nil); err != nil {
					return err
				}

				// After enumerating the containers according to container prefix in account level,
				// do container level enumerating and add transfers.
				searchPrefix, blobNamePattern := srcBlobURLPartExtension.searchPrefixFromBlobURL()
				return e.addTransfersFromContainer(
					ctx,
					srcServiceURL.NewContainerURL(containerItem.Name),
					tmpDestURL,
					searchPrefix,
					blobNamePattern,
					"",
					cca)
			}); err != nil {
			return err
		}
	} else { // Case-3: Source is a blob container or directory
		searchPrefix, blobNamePattern := srcBlobURLPartExtension.searchPrefixFromBlobURL()
		if searchPrefix == "*" && !cca.recursive {
			return fmt.Errorf("cannot copy the entire container or directory without recursive flag, please use recursive flag")
		}
		// create bucket for destination, in case bucket doesn't exist.
		if err := e.createBucket(ctx, *destURL, nil); err != nil {
			return err
		}

		if err := e.addTransfersFromContainer(
			ctx,
			azblob.NewContainerURL(srcBlobURLPartExtension.getContainerURL(), srcBlobPipeline),
			*destURL,
			searchPrefix,
			blobNamePattern,
			srcBlobURLPartExtension.getParentSourcePath(),
			cca); err != nil {
			return err
		}
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

// initDestPreProceed inits common dest objects for e.g. destination pre-operations
// like create container/share/bucket and etc.
// The info such as blob pipeline which is created once and can be reused for multiple times.
func (e *copyBlobToNEnumerator) initDestPreProceed(ctx context.Context) error {
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

// validateDestIsService check if destination is an URL point to service.
func (e *copyBlobToNEnumerator) validateDestIsService(ctx context.Context, destURL url.URL) error {
	// TODO: validate other type's destination URLs, e.g: BlobFS, File.
	switch e.FromTo {
	case common.EFromTo.BlobBlob():
		destBlobPipeline, err := createBlobPipeline(ctx, e.CredentialInfo)
		if err != nil {
			return err
		}
		destServiceURL := azblob.NewServiceURL(destURL, destBlobPipeline)
		if _, err := destServiceURL.GetProperties(ctx); err != nil {
			return fmt.Errorf("invalid source and destination combination for service to service copy: "+
				"destination must point to service account when source is a service account, %v", err)
		}
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

// addTransfersFromContainer enumerates blobs in container, and adds matched blob into transfer.
func (e *copyBlobToNEnumerator) addTransfersFromContainer(ctx context.Context, srcContainerURL azblob.ContainerURL, destBaseURL url.URL,
	blobNamePrefix, blobNamePattern, parentSourcePath string, cca *cookedCopyCmdArgs) error {

	blobFilter := func(blobItem azblob.BlobItem) bool {
		// If the blobName doesn't matches the blob name pattern, then blob is not included
		// queued for transfer.
		if !gCopyUtil.matchBlobNameAgainstPattern(blobNamePattern, blobItem.Name, cca.recursive) {
			return false
		}

		// Check the blob should be included or not.
		if !gCopyUtil.resourceShouldBeIncluded(parentSourcePath, e.Include, blobItem.Name) {
			return false
		}

		// Check the blob should be excluded or not.
		if gCopyUtil.resourceShouldBeExcluded(parentSourcePath, e.Exclude, blobItem.Name) {
			return false
		}

		return true
	}

	// enumerate blob in containers, and add matched blob into transfer.
	return enumerateBlobsInContainer(
		ctx,
		srcContainerURL,
		blobNamePrefix,
		blobFilter,
		func(blobItem azblob.BlobItem) error {
			blobRelativePath := gCopyUtil.getRelativePath(blobNamePrefix, blobItem.Name)
			return e.addTransferInternal(
				srcContainerURL.NewBlobURL(blobItem.Name).URL(),
				urlExtension{URL: destBaseURL}.generateObjectPath(blobRelativePath),
				&blobItem.Properties,
				blobItem.Metadata,
				cca)
		})
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
