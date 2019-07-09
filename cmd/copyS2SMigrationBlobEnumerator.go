package cmd

import (
	"context"
	"errors"
	"fmt"
	"net/url"
	"strings"
	"time"

	"github.com/Azure/azure-pipeline-go/pipeline"
	"github.com/Azure/azure-storage-blob-go/azblob"

	"github.com/Azure/azure-storage-azcopy/common"
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

	sourceURLParts := azblob.NewBlobURLParts(*e.sourceURL)
	if sourceURLParts.SAS.Encode() == "" {
		// Grab OAuth token and craft pipeline to create user delegation SAS token
		hasToken, err := GetUserOAuthTokenManagerInstance().HasCachedToken()

		if err != nil {
			return errors.New(fmt.Sprintf("no viable form of auth present on source: couldn't get cached token: %s", err))
		}

		if !hasToken {
			return errors.New("no viable form of auth present on source: no cached token present")
		}

		OAuthToken, err := GetUserOAuthTokenManagerInstance().GetTokenInfo(ctx)

		if OAuthToken == nil || err != nil {
			return errors.New(fmt.Sprintf("no viable form of auth present on source: couldn't load OAuth token: %s", err))
		}

		udkp, err := createBlobPipeline(ctx, common.CredentialInfo{
			CredentialType: common.ECredentialType.OAuthToken(),
			OAuthTokenInfo: *OAuthToken,
		})

		if err != nil {
			return errors.New(fmt.Sprintf("no viable form of auth present on source: couldn't create blob pipeline: %s", err))
		}

		// Attempt to create a user delegation SAS token and append it to the source.
		rawURL := *e.sourceURL
		rawURL.Path = ""
		currentTime := time.Now()
		tomorrow := currentTime.Add(time.Hour*24)

		sourceServiceURL := azblob.NewServiceURL(rawURL, udkp)
		udc, err := sourceServiceURL.GetUserDelegationCredential(ctx, azblob.NewKeyInfo(currentTime, tomorrow), nil, nil)

		if err != nil {
			return errors.New(fmt.Sprintf("no viable form of auth present on source: failed to create user delegation credential: %s", err))
		}

		SASValues := azblob.BlobSASSignatureValues{
			Protocol: azblob.SASProtocolHTTPS,
			StartTime: currentTime,
			ExpiryTime: tomorrow,
			Permissions: "rl",
			ContainerName: sourceURLParts.ContainerName,
		}

		SASParams, err := SASValues.NewSASQueryParameters(udc)

		if err != nil {
			return errors.New(fmt.Sprintf("no viable form of auth present on source: failed to create user delegation SAS token: %s", err))
		}

		// Yes all of these have to be set. Consistency would be nice.
		sourceURLParts.SAS = SASParams
		*e.sourceURL = sourceURLParts.URL()
		e.SourceSAS = SASParams.Encode()
		e.srcBlobURLPartExtension = blobURLPartsExtension{ sourceURLParts } // For directories
		cca.sourceSAS = SASParams.Encode()
	}

	// Case-1: Source is a single blob
	// Verify if source is a single blob
	srcBlobURL := azblob.NewBlobURL(*e.sourceURL, e.srcBlobPipeline)
	if e.srcBlobURLPartExtension.isBlobSyntactically() {
		if blobProperties, err := srcBlobURL.GetProperties(ctx, azblob.BlobAccessConditions{}); err == nil {
			if e.isDestServiceSyntactically() {
				return errSingleToAccountCopy
			}
			if endWithSlashOrBackSlash(e.destURL.Path) || e.isDestBucketSyntactically() {
				fileName := gCopyUtil.getFileNameFromPath(e.srcBlobURLPartExtension.BlobName)
				*e.destURL = urlExtension{*e.destURL}.generateObjectPath(fileName)
			}
			if cca.blobType == cca.blobType.PageBlob() && blobProperties.ContentLength()%512 != 0 {
				return errors.New("page blobs must be a multiple of 512 bytes in length")
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
		} else {
			if isFatal := handleSingleFileValidationErrorForBlob(err); isFatal {
				return err
			}
		}
	}

	// Case-2: Source is account level, e.g.:
	// a: https://<blob-service>/
	// b: https://<blob-service>/containerprefix*/vd/blob
	if isAccountLevel, containerPrefix := e.srcBlobURLPartExtension.isBlobAccountLevelSearch(); isAccountLevel {
		glcm.Info(infoCopyFromAccount)
		if cca.blobType == cca.blobType.PageBlob() {
			glcm.Info("Copying entire account to page blobs will fail on files without a content length that is a multiple of 512 bytes")
		}
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
		glcm.Info(infoCopyFromContainerDirectoryListOfFiles)
		if cca.blobType == cca.blobType.PageBlob() {
			glcm.Info("Copying directory to page blobs will fail on files without a content length that is a multiple of 512 bytes")
		}
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
			BlobTier:           e.getAccessTier(properties.AccessTier, cca.s2sPreserveAccessTier),
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
			BlobTier:           e.getAccessTier(azblob.AccessTierType(properties.AccessTier()), cca.s2sPreserveAccessTier),
		},
		cca)
}

func (e *copyS2SMigrationBlobEnumerator) getAccessTier(accessTier azblob.AccessTierType, s2sPreserveAccessTier bool) azblob.AccessTierType {
	return azblob.AccessTierType(common.IffString(s2sPreserveAccessTier, string(accessTier), string(azblob.AccessTierNone)))
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
