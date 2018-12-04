package cmd

import (
	"context"
	"errors"
	"fmt"
	"net/url"
	"strings"

	"github.com/Azure/azure-storage-azcopy/common"
	"github.com/Azure/azure-storage-blob-go/azblob"
)

// copyBlobToNEnumerator enumerates blob source, and submit request for copy blob to N,
// where N stands for blob/file/blobFS (Currently only blob is supported).
// The source could be a single blob/container/blob account
type copyBlobToNEnumerator struct {
	copyS2SEnumerator
}

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
	if err := e.initDestPipeline(ctx); err != nil {
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
		err := e.createDestBucket(ctx, *destURL, nil)
		if err != nil {
			return err
		}

		// directly use destURL as destination
		if err := e.addBlobToNTransfer2(srcBlobURL.URL(), *destURL, blobProperties, cca); err != nil {
			return err
		}
		return e.dispatchFinalPart(cca)
	}

	// Case-2: Source is account level, e.g.:
	// a: https://<blob-service>/container
	// b: https://<blob-service>/containerprefix*/vd/blob
	if isAccountLevel, containerPrefix := srcBlobURLPartExtension.isBlobAccountLevelSearch(); isAccountLevel {
		if !cca.recursive {
			return fmt.Errorf("cannot copy the entire account without recursive flag. Please use --recursive flag")
		}

		// Validate If destination is service level account.
		if err := e.validateDestIsService(ctx, *destURL); err != nil {
			return err
		}

		srcServiceURL := azblob.NewServiceURL(srcBlobURLPartExtension.getServiceURL(), srcBlobPipeline)
		blobPrefix, blobNamePattern, _ := srcBlobURLPartExtension.searchPrefixFromBlobURL()
		// List containers and add transfers for these containers.
		if err := e.addTransferFromAccount(ctx, srcServiceURL, *destURL,
			containerPrefix, blobPrefix, blobNamePattern, cca); err != nil {
			return err
		}

	} else { // Case-3: Source is a blob container or directory
		blobPrefix, blobNamePattern, isWildcardSearch := srcBlobURLPartExtension.searchPrefixFromBlobURL()
		if blobNamePattern == "*" && !cca.recursive && !isWildcardSearch {
			return fmt.Errorf("cannot copy the entire container or directory without recursive flag. Please use --recursive flag")
		}
		// create bucket for destination, in case bucket doesn't exist.
		if err := e.createDestBucket(ctx, *destURL, nil); err != nil {
			return err
		}

		if err := e.addTransfersFromContainer(ctx,
			azblob.NewContainerURL(srcBlobURLPartExtension.getContainerURL(), srcBlobPipeline),
			*destURL,
			blobPrefix,
			blobNamePattern,
			srcBlobURLPartExtension.getParentSourcePath(),
			false,
			isWildcardSearch,
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

// addTransferFromAccount enumerates containers, and adds matched blob into transfer.
func (e *copyBlobToNEnumerator) addTransferFromAccount(ctx context.Context,
	srcServiceURL azblob.ServiceURL, destBaseURL url.URL,
	containerPrefix, blobPrefix, blobNamePattern string, cca *cookedCopyCmdArgs) error {
	return enumerateContainersInAccount(
		ctx,
		srcServiceURL,
		containerPrefix,
		func(containerItem azblob.ContainerItem) error {
			// Whatever the destination type is, it should be equivalent to account level,
			// so directly append container name to it.
			tmpDestURL := urlExtension{URL: destBaseURL}.generateObjectPath(containerItem.Name)
			// create bucket for destination, in case bucket doesn't exist.
			if err := e.createDestBucket(ctx, tmpDestURL, nil); err != nil {
				return err
			}

			// Two cases for exclude/include which need to match container names in account:
			// a. https://<blobservice>/container*/blob*.vhd
			// b. https://<blobservice>/ which equals to https://<blobservice>/*
			return e.addTransfersFromContainer(
				ctx,
				srcServiceURL.NewContainerURL(containerItem.Name),
				tmpDestURL,
				blobPrefix,
				blobNamePattern,
				"",
				true,
				true,
				cca)
		})
}

// addTransfersFromContainer enumerates blobs in container, and adds matched blob into transfer.
func (e *copyBlobToNEnumerator) addTransfersFromContainer(ctx context.Context, srcContainerURL azblob.ContainerURL, destBaseURL url.URL,
	blobNamePrefix, blobNamePattern, parentSourcePath string, includExcludeContainer, isWildcardSearch bool, cca *cookedCopyCmdArgs) error {

	blobFilter := func(blobItem azblob.BlobItem) bool {
		// If the blobName doesn't matches the blob name pattern, then blob is not included
		// queued for transfer.
		if !gCopyUtil.matchBlobNameAgainstPattern(blobNamePattern, blobItem.Name, cca.recursive) {
			return false
		}
		includeExcludeMatchPath := common.IffString(includExcludeContainer,
			azblob.NewBlobURLParts(srcContainerURL.URL()).ContainerName+"/"+blobItem.Name,
			blobItem.Name)
		// Check the blob should be included or not.
		if !gCopyUtil.resourceShouldBeIncluded(parentSourcePath, e.Include, includeExcludeMatchPath) {
			return false
		}

		// Check the blob should be excluded or not.
		if gCopyUtil.resourceShouldBeExcluded(parentSourcePath, e.Exclude, includeExcludeMatchPath) {
			return false
		}
		// check if blobType of the current blob is present in the list of blob type to exclude.
		for _, blobType := range e.ExcludeBlobType {
			if blobItem.Properties.BlobType == blobType {
				return false
			}
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
			var blobRelativePath = ""
			// As downloading logic temporarily, refactor after scenario ensured.
			if isWildcardSearch {
				blobRelativePath = strings.Replace(blobItem.Name, blobNamePrefix[:strings.LastIndex(blobNamePrefix, common.AZCOPY_PATH_SEPARATOR_STRING)+1], "", 1)
			} else {
				blobRelativePath = gCopyUtil.getRelativePath(blobNamePrefix, blobItem.Name)
			}

			return e.addBlobToNTransfer(
				srcContainerURL.NewBlobURL(blobItem.Name).URL(),
				urlExtension{URL: destBaseURL}.generateObjectPath(blobRelativePath),
				&blobItem.Properties,
				blobItem.Metadata,
				cca)
		})
}

func (e *copyBlobToNEnumerator) addBlobToNTransfer(srcURL, destURL url.URL, properties *azblob.BlobProperties, metadata azblob.Metadata,
	cca *cookedCopyCmdArgs) error {
	// if properties.BlobType != azblob.BlobBlockBlob {
	// 	glcm.Info(fmt.Sprintf(
	// 		"Skipping %v: %s. This version of AzCopy only supports BlockBlob transfer.",
	// 		properties.BlobType,
	// 		common.URLExtension{URL: srcURL}.RedactSigQueryParamForLogging()))
	// }

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
		ContentMD5:         properties.ContentMD5,
		Metadata:           common.FromAzBlobMetadataToCommonMetadata(metadata),
		BlobType:           getBlobType(properties.BlobType, cca.blobType)},
		//BlobTier:           string(properties.AccessTier)}, // TODO: blob tier setting correctly
		cca)
}

func (e *copyBlobToNEnumerator) addBlobToNTransfer2(srcURL, destURL url.URL, properties *azblob.BlobGetPropertiesResponse,
	cca *cookedCopyCmdArgs) error {
	// if properties.BlobType() != azblob.BlobBlockBlob {
	// 	glcm.Info(fmt.Sprintf(
	// 		"Skipping %v: %s. This version of AzCopy only supports BlockBlob transfer.",
	// 		properties.BlobType(),
	// 		common.URLExtension{URL: srcURL}.RedactSigQueryParamForLogging()))
	// }

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
		BlobType:           getBlobType(properties.BlobType(), cca.blobType)},
		//BlobTier:           properties.AccessTier()}, // TODO: blob tier setting correctly
		cca)
}

func getBlobType(srcBlobType azblob.BlobType, specifiedBlobType common.BlobType) azblob.BlobType {
	blobType := srcBlobType
	if specifiedBlobType != common.EBlobType.None() {
		blobType = azblob.BlobType(specifiedBlobType.String())

		glcm.Info(fmt.Sprintf("Saving blob as %v.", blobType))
	}

	return blobType
}

func (e *copyBlobToNEnumerator) addTransfer(transfer common.CopyTransfer, cca *cookedCopyCmdArgs) error {
	return addTransfer(&(e.CopyJobPartOrderRequest), transfer, cca)
}

func (e *copyBlobToNEnumerator) dispatchFinalPart(cca *cookedCopyCmdArgs) error {
	return dispatchFinalPart(&(e.CopyJobPartOrderRequest), cca)
}

func (e *copyBlobToNEnumerator) partNum() common.PartNumber {
	return e.PartNum
}
