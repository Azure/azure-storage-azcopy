package cmd

import (
	"context"
	"errors"
	"fmt"
	"net/url"
	"strings"

	"github.com/Azure/azure-pipeline-go/pipeline"
	"github.com/Azure/azure-storage-azcopy/common"
	"github.com/Azure/azure-storage-file-go/azfile"
)

// copyS2SFileEnumerator enumerates file source, and submit request for copy file to N,
// where N stands for blob/file/blobFS (Currently only blob is supported).
// The source could be single file/directory/share/file account
type copyS2SFileEnumerator struct {
	copyS2SEnumeratorBase

	// source Azure File resources
	srcFilePipeline         pipeline.Pipeline
	srcFileURLPartExtension fileURLPartsExtension
}

func (e *copyS2SFileEnumerator) initEnumerator(ctx context.Context, cca *cookedCopyCmdArgs) (err error) {
	if err = e.initEnumeratorCommon(ctx, cca); err != nil {
		return err
	}

	// append the sas at the end of query params.
	e.sourceURL = gCopyUtil.appendQueryParamToUrl(e.sourceURL, cca.sourceSAS)
	e.destURL = gCopyUtil.appendQueryParamToUrl(e.destURL, cca.destinationSAS)

	// Create pipeline for source Azure File service.
	// Note: only anonymous credential is supported for file source(i.e. SAS) now.
	// e.CredentialInfo is for destination
	srcCredInfo := common.CredentialInfo{CredentialType: common.ECredentialType.Anonymous()}
	e.srcFilePipeline, err = createFilePipeline(ctx, srcCredInfo)
	if err != nil {
		return err
	}
	if err := e.initDestPipeline(ctx); err != nil {
		return err
	}

	e.srcFileURLPartExtension = fileURLPartsExtension{azfile.NewFileURLParts(*e.sourceURL)}

	return nil
}

func (e *copyS2SFileEnumerator) enumerate(cca *cookedCopyCmdArgs) error {
	ctx := context.TODO()

	if err := e.initEnumerator(ctx, cca); err != nil {
		return err
	}

	// Case-1: Source is single file
	srcFileURL := azfile.NewFileURL(*e.sourceURL, e.srcFilePipeline)
	// Verify if source is a single file
	// Note: Currently only support single to single, and not support single to directory.
	if fileProperties, err := srcFileURL.GetProperties(ctx); err == nil {
		if endWithSlashOrBackSlash(e.destURL.Path) {
			return errors.New("invalid source and destination combination for service to service copy: " +
				"destination must point to a single file, when source is a single file.")
		}
		err := e.createDestBucket(ctx, *e.destURL, nil)
		if err != nil {
			return err
		}
		// directly use destURL as destination
		if err := e.addFileToNTransfer(srcFileURL.URL(), *e.destURL, fileProperties, cca); err != nil {
			return err
		}
		return e.dispatchFinalPart(cca)
	}

	// Case-2: Source is account, currently only support blob destination
	if isAccountLevel, sharePrefix := e.srcFileURLPartExtension.isFileAccountLevelSearch(); isAccountLevel {
		if !cca.recursive {
			return fmt.Errorf("cannot copy the entire account without recursive flag. Please use --recursive flag")
		}

		// Validate If destination is service level account.
		if err := e.validateDestIsService(ctx, *e.destURL); err != nil {
			return err
		}

		srcServiceURL := azfile.NewServiceURL(e.srcFileURLPartExtension.getServiceURL(), e.srcFilePipeline)
		fileOrDirectoryPrefix, fileNamePattern, _ := e.srcFileURLPartExtension.searchPrefixFromFileURL()
		// List shares and add transfers for these shares.
		if err := e.addTransferFromAccount(ctx, srcServiceURL, *e.destURL, sharePrefix, fileOrDirectoryPrefix,
			fileNamePattern, cca); err != nil {
			return err
		}

	} else { // Case-3: Source is a file share or directory
		searchPrefix, fileNamePattern, isWildcardSearch := e.srcFileURLPartExtension.searchPrefixFromFileURL()
		if fileNamePattern == "*" && !cca.recursive && !isWildcardSearch {
			return fmt.Errorf("cannot copy the entire share or directory without recursive flag. Please use --recursive flag")
		}
		if err := e.createDestBucket(ctx, *e.destURL, nil); err != nil {
			return err
		}
		if err := e.addTransfersFromDirectory(ctx,
			azfile.NewShareURL(e.srcFileURLPartExtension.getShareURL(), e.srcFilePipeline).NewRootDirectoryURL(),
			*e.destURL,
			searchPrefix,
			fileNamePattern,
			e.srcFileURLPartExtension.getParentSourcePath(),
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

// addTransferFromAccount enumerates shares in account, and adds matched file into transfer.
func (e *copyS2SFileEnumerator) addTransferFromAccount(ctx context.Context,
	srcServiceURL azfile.ServiceURL, destBaseURL url.URL,
	sharePrefix, fileOrDirectoryPrefix, fileNamePattern string, cca *cookedCopyCmdArgs) error {
	return enumerateSharesInAccount(
		ctx,
		srcServiceURL,
		sharePrefix,
		func(shareItem azfile.ShareItem) error {
			// Whatever the destination type is, it should be equivalent to account level,
			// so directly append share name to it.
			tmpDestURL := urlExtension{URL: destBaseURL}.generateObjectPath(shareItem.Name)
			// create bucket for destination, in case bucket doesn't exist.
			if err := e.createDestBucket(ctx, tmpDestURL, nil); err != nil {
				return err
			}

			// Two cases for exclude/include which need to match share names in account:
			// a. https://<fileservice>/share*/file*.vhd
			// b. https://<fileservice>/ which equals to https://<fileservice>/*
			return e.addTransfersFromDirectory(
				ctx,
				srcServiceURL.NewShareURL(shareItem.Name).NewRootDirectoryURL(),
				tmpDestURL,
				fileOrDirectoryPrefix,
				fileNamePattern,
				"",
				true,
				true,
				cca)
		})
}

// addTransfersFromDirectory enumerates files in directory and sub directoreis,
// and adds matched file into transfer.
func (e *copyS2SFileEnumerator) addTransfersFromDirectory(ctx context.Context,
	srcDirectoryURL azfile.DirectoryURL, destBaseURL url.URL,
	fileOrDirNamePrefix, fileNamePattern, parentSourcePath string,
	includExcludeShare, isWildcardSearch bool, cca *cookedCopyCmdArgs) error {

	fileFilter := func(fileItem azfile.FileItem, fileURL azfile.FileURL) bool {
		fileURLPart := azfile.NewFileURLParts(fileURL.URL())
		// Check if file name matches pattern.
		if !gCopyUtil.matchBlobNameAgainstPattern(fileNamePattern, fileURLPart.DirectoryOrFilePath, cca.recursive) {
			return false
		}

		includeExcludeMatchPath := common.IffString(includExcludeShare,
			fileURLPart.ShareName+"/"+fileURLPart.DirectoryOrFilePath,
			fileURLPart.DirectoryOrFilePath)
		// Check the file should be included or not.
		if !gCopyUtil.resourceShouldBeIncluded(parentSourcePath, e.Include, includeExcludeMatchPath) {
			return false
		}

		// Check the file should be excluded or not.
		if gCopyUtil.resourceShouldBeExcluded(parentSourcePath, e.Exclude, includeExcludeMatchPath) {
			return false
		}

		return true
	}

	// enumerate files and sub directories in directory, and add matched files into transfer.
	return enumerateDirectoriesAndFilesInShare(
		ctx,
		srcDirectoryURL,
		fileOrDirNamePrefix,
		cca.recursive,
		fileFilter,
		func(fileItem azfile.FileItem, fileURL azfile.FileURL) error {
			fileURLPart := azfile.NewFileURLParts(fileURL.URL())
			var fileRelativePath = ""
			// As downloading blob logic temporarily, refactor after scenario ensured.
			if isWildcardSearch {
				fileRelativePath = strings.Replace(fileURLPart.DirectoryOrFilePath,
					fileOrDirNamePrefix[:strings.LastIndex(fileOrDirNamePrefix, common.AZCOPY_PATH_SEPARATOR_STRING)+1], "", 1)
			} else {
				fileRelativePath = gCopyUtil.getRelativePath(fileOrDirNamePrefix, fileURLPart.DirectoryOrFilePath)
			}

			// TODO: Remove get attribute, when file's list method can return property and metadata directly.
			if cca.preserveProperties {
				p, err := fileURL.GetProperties(ctx)
				if err != nil {
					return err
				}

				return e.addFileToNTransfer(
					fileURL.URL(),
					urlExtension{URL: destBaseURL}.generateObjectPath(fileRelativePath),
					p,
					cca)
			} else {
				return e.addFileToNTransfer2(
					fileURL.URL(),
					urlExtension{URL: destBaseURL}.generateObjectPath(fileRelativePath),
					fileItem.Properties,
					cca)
			}
		})
}

func (e *copyS2SFileEnumerator) addFileToNTransfer(srcURL, destURL url.URL, properties *azfile.FileGetPropertiesResponse,
	cca *cookedCopyCmdArgs) error {
	return e.addTransfer(common.CopyTransfer{
		Source:             gCopyUtil.stripSASFromFileShareUrl(srcURL).String(),
		Destination:        gCopyUtil.stripSASFromBlobUrl(destURL).String(), // Optimize this if more target resource types need be supported.
		LastModifiedTime:   properties.LastModified(),
		SourceSize:         properties.ContentLength(),
		ContentType:        properties.ContentType(),
		ContentEncoding:    properties.ContentEncoding(),
		ContentDisposition: properties.ContentDisposition(),
		ContentLanguage:    properties.ContentLanguage(),
		CacheControl:       properties.CacheControl(),
		ContentMD5:         properties.ContentMD5(),
		Metadata:           common.FromAzFileMetadataToCommonMetadata(properties.NewMetadata())},
		cca)
}

func (e *copyS2SFileEnumerator) addFileToNTransfer2(srcURL, destURL url.URL, properties *azfile.FileProperty,
	cca *cookedCopyCmdArgs) error {
	return e.addTransfer(common.CopyTransfer{
		Source:      gCopyUtil.stripSASFromFileShareUrl(srcURL).String(),
		Destination: gCopyUtil.stripSASFromBlobUrl(destURL).String(), // Optimize this if more target resource types need be supported.
		SourceSize:  properties.ContentLength},
		cca)
}

func (e *copyS2SFileEnumerator) addTransfer(transfer common.CopyTransfer, cca *cookedCopyCmdArgs) error {
	return addTransfer(&(e.CopyJobPartOrderRequest), transfer, cca)
}

func (e *copyS2SFileEnumerator) dispatchFinalPart(cca *cookedCopyCmdArgs) error {
	return dispatchFinalPart(&(e.CopyJobPartOrderRequest), cca)
}

func (e *copyS2SFileEnumerator) partNum() common.PartNumber {
	return e.PartNum
}
