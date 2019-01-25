package cmd

import (
	"context"
	"errors"
	"fmt"
	"net/url"
	"strings"

	"github.com/Azure/azure-pipeline-go/pipeline"
	"github.com/Azure/azure-storage-azcopy/common"
	"github.com/jiacfan/azure-storage-blob-go/azblob"
)

// copyS2SMigrationBlobEnumerator enumerates blob source, and submit request for copy blob to N,
// where N stands for blob/file/blobFS (Currently only blob is supported).
// The source could be a single blob/container/blob account
type copyS2SMigrationBlobEnumerator struct {
	copyS2SMigrationEnumeratorBase

	// source Azure Blob resources
	srcBlobPipeline         pipeline.Pipeline
	srcBlobURLPartExtension blobURLPartsExtension
}

func (e *copyS2SMigrationBlobEnumerator) initEnumerator(ctx context.Context, cca *cookedCopyCmdArgs) (err error) {
	if err = e.initEnumeratorCommon(ctx, cca); err != nil {
		return err
	}

	// append the sas at the end of query params.
	e.sourceURL = gCopyUtil.appendQueryParamToUrl(e.sourceURL, cca.sourceSAS)
	e.destURL = gCopyUtil.appendQueryParamToUrl(e.destURL, cca.destinationSAS)

	// Create pipeline for source Blob service.
	// For copy source with blob type, only anonymous credential is supported now(i.e. SAS or public).
	// So directoy create anonymous credential for source.
	// Note: If traditional copy(download first, then upload need be supported), more logic should be added to parse and validate
	// credential for both source and destination.
	e.srcBlobPipeline, err = createBlobPipeline(ctx,
		common.CredentialInfo{CredentialType: common.ECredentialType.Anonymous()})
	if err != nil {
		return err
	}

	e.srcBlobURLPartExtension = blobURLPartsExtension{azblob.NewBlobURLParts(*e.sourceURL)}

	return nil
}

func (e *copyS2SMigrationBlobEnumerator) enumerate(cca *cookedCopyCmdArgs) error {
	ctx := context.TODO()

	if err := e.initEnumerator(ctx, cca); err != nil {
		return err
	}

	// Case-1: Source is a single blob
	// Verify if source is a single blob
	srcBlobURL := azblob.NewBlobURL(*e.sourceURL, e.srcBlobPipeline)
	// Note: Currently only support single to single, and not support single to directory.
	if blobProperties, err := srcBlobURL.GetProperties(ctx, azblob.BlobAccessConditions{}); err == nil {
		if endWithSlashOrBackSlash(e.destURL.Path) {
			return errors.New("invalid source and destination combination for service to service copy: " +
				"destination must point to a single file, when source is a single file.")
		}
		err := e.createDestBucket(ctx, *e.destURL, nil)
		if err != nil {
			return err
		}

		// directly use destURL as destination
		if err := e.addBlobToNTransfer2(srcBlobURL.URL(), *e.destURL, blobProperties, cca); err != nil {
			return err
		}
		return e.dispatchFinalPart(cca)
	}

	// Case-2: Source is account level, e.g.:
	// a: https://<blob-service>/
	// b: https://<blob-service>/containerprefix*/vd/blob
	if isAccountLevel, containerPrefix := e.srcBlobURLPartExtension.isBlobAccountLevelSearch(); isAccountLevel {
		if !cca.recursive {
			return fmt.Errorf("cannot copy the entire account without recursive flag. Please use --recursive flag")
		}

		// Validate If destination is service level account.
		if err := e.validateDestIsService(ctx, *e.destURL); err != nil {
			return err
		}

		srcServiceURL := azblob.NewServiceURL(e.srcBlobURLPartExtension.getServiceURL(), e.srcBlobPipeline)
		blobPrefix, blobNamePattern, _ := e.srcBlobURLPartExtension.searchPrefixFromBlobURL()
		// List containers and add transfers for these containers.
		if err := e.addTransferFromAccount(ctx, srcServiceURL, *e.destURL,
			containerPrefix, blobPrefix, blobNamePattern, cca); err != nil {
			return err
		}
	} else { // Case-3: Source is a blob container or directory
		blobPrefix, blobNamePattern, isWildcardSearch := e.srcBlobURLPartExtension.searchPrefixFromBlobURL()
		if blobNamePattern == "*" && !cca.recursive && !isWildcardSearch {
			return fmt.Errorf("cannot copy the entire container or directory without recursive flag. Please use --recursive flag")
		}
		// create bucket for destination, in case bucket doesn't exist.
		if err := e.createDestBucket(ctx, *e.destURL, nil); err != nil {
			return err
		}

		if err := e.addTransfersFromContainer(ctx,
			azblob.NewContainerURL(e.srcBlobURLPartExtension.getContainerURL(), e.srcBlobPipeline),
			*e.destURL,
			blobPrefix,
			blobNamePattern,
			e.srcBlobURLPartExtension.getParentSourcePath(),
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
func (e *copyS2SMigrationBlobEnumerator) addTransferFromAccount(ctx context.Context,
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
func (e *copyS2SMigrationBlobEnumerator) addTransfersFromContainer(ctx context.Context, srcContainerURL azblob.ContainerURL, destBaseURL url.URL,
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

func (e *copyS2SMigrationBlobEnumerator) addBlobToNTransfer(srcURL, destURL url.URL, properties *azblob.BlobProperties, metadata azblob.Metadata,
	cca *cookedCopyCmdArgs) error {
	return e.addTransfer(
		common.CopyTransfer{
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
			BlobType:           properties.BlobType,
			BlobTier:           e.getAccessTier(properties.AccessTier, cca.preserveS2SAccessTier),
		},
		cca)
}

func (e *copyS2SMigrationBlobEnumerator) addBlobToNTransfer2(srcURL, destURL url.URL, properties *azblob.BlobGetPropertiesResponse,
	cca *cookedCopyCmdArgs) error {
	return e.addTransfer(
		common.CopyTransfer{
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
			BlobType:           properties.BlobType(),
			BlobTier:           e.getAccessTier(azblob.AccessTierType(properties.AccessTier()), cca.preserveS2SAccessTier),
		},
		cca)
}

func (e *copyS2SMigrationBlobEnumerator) getAccessTier(accessTier azblob.AccessTierType, preserveS2SAccessTier bool) azblob.AccessTierType {
	return azblob.AccessTierType(common.IffString(preserveS2SAccessTier, string(accessTier), string(azblob.AccessTierNone)))
}

func (e *copyS2SMigrationBlobEnumerator) addTransfer(transfer common.CopyTransfer, cca *cookedCopyCmdArgs) error {
	return addTransfer(&(e.CopyJobPartOrderRequest), transfer, cca)
}

func (e *copyS2SMigrationBlobEnumerator) dispatchFinalPart(cca *cookedCopyCmdArgs) error {
	return dispatchFinalPart(&(e.CopyJobPartOrderRequest), cca)
}

func (e *copyS2SMigrationBlobEnumerator) partNum() common.PartNumber {
	return e.PartNum
}
