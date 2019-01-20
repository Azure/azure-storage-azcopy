package cmd

import (
	"context"
	"errors"
	"fmt"
	"net/url"
	"strings"

	"github.com/Azure/azure-storage-azcopy/common"
	"github.com/Azure/azure-storage-file-go/azfile"
)

// copyFileToNEnumerator enumerates file source, and submit request for copy file to N,
// where N stands for blob/file/blobFS (Currently only blob is supported).
// The source could be single file/directory/share/file account
type copyFileToNEnumerator struct {
	copyS2SEnumerator
}

func (e *copyFileToNEnumerator) enumerate(cca *cookedCopyCmdArgs) error {
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

	// Create pipeline for source Azure File service.
	// Note: only anonymous credential is supported for file source(i.e. SAS) now.
	// e.CredentialInfo is for destination
	srcCredInfo := common.CredentialInfo{CredentialType: common.ECredentialType.Anonymous()}
	srcFilePipeline, err := createFilePipeline(ctx, srcCredInfo)
	if err != nil {
		return err
	}
	if err := e.initDestPipeline(ctx); err != nil {
		return err
	}

	srcFileURLPartExtension := fileURLPartsExtension{azfile.NewFileURLParts(*sourceURL)}

	// Case-1: Source is single file
	srcFileURL := azfile.NewFileURL(*sourceURL, srcFilePipeline)
	// Verify if source is a single file
	// Note: Currently only support single to single, and not support single to directory.
	if fileProperties, err := srcFileURL.GetProperties(ctx); err == nil {
		if endWithSlashOrBackSlash(destURL.Path) {
			return errors.New("invalid source and destination combination for service to service copy: " +
				"destination must point to a single file, when source is a single file.")
		}
		err := e.createDestBucket(ctx, *destURL, nil)
		if err != nil {
			return err
		}
		// directly use destURL as destination
		if err := e.addFileToNTransfer(srcFileURL.URL(), *destURL, fileProperties, cca); err != nil {
			return err
		}
		return e.dispatchFinalPart(cca)
	}

	// Case-2: Source is account, currently only support blob destination
	if isAccountLevel, sharePrefix := srcFileURLPartExtension.isFileAccountLevelSearch(); isAccountLevel {
		if !cca.recursive {
			return fmt.Errorf("cannot copy the entire account without recursive flag. Please use --recursive flag")
		}

		// Validate If destination is service level account.
		if err := e.validateDestIsService(ctx, *destURL); err != nil {
			return err
		}

		srcServiceURL := azfile.NewServiceURL(srcFileURLPartExtension.getServiceURL(), srcFilePipeline)
		fileOrDirectoryPrefix, fileNamePattern, _ := srcFileURLPartExtension.searchPrefixFromFileURL()
		// List shares and add transfers for these shares.
		if err := e.addTransferFromAccount(ctx, srcServiceURL, *destURL, sharePrefix, fileOrDirectoryPrefix,
			fileNamePattern, cca); err != nil {
			return err
		}

	} else { // Case-3: Source is a file share or directory
		searchPrefix, fileNamePattern, isWildcardSearch := srcFileURLPartExtension.searchPrefixFromFileURL()
		if fileNamePattern == "*" && !cca.recursive && !isWildcardSearch {
			return fmt.Errorf("cannot copy the entire share or directory without recursive flag. Please use --recursive flag")
		}
		if err := e.createDestBucket(ctx, *destURL, nil); err != nil {
			return err
		}
		if err := e.addTransfersFromDirectory(ctx,
			azfile.NewShareURL(srcFileURLPartExtension.getShareURL(), srcFilePipeline).NewRootDirectoryURL(),
			*destURL,
			searchPrefix,
			fileNamePattern,
			srcFileURLPartExtension.getParentSourcePath(),
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
func (e *copyFileToNEnumerator) addTransferFromAccount(ctx context.Context,
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
func (e *copyFileToNEnumerator) addTransfersFromDirectory(ctx context.Context,
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

			// TODO: Remove get attribute, when file's list method can return property and metadata.
			p, err := fileURL.GetProperties(ctx)
			if err != nil {
				return err
			}

			return e.addFileToNTransfer(
				fileURL.URL(),
				urlExtension{URL: destBaseURL}.generateObjectPath(fileRelativePath),
				p,
				cca)
		})
}

func (e *copyFileToNEnumerator) addFileToNTransfer(srcURL, destURL url.URL, properties *azfile.FileGetPropertiesResponse,
	cca *cookedCopyCmdArgs) error {
	return e.addTransfer(common.CopyTransfer{
		Source:             gCopyUtil.stripSASFromFileShareUrl(srcURL).String(),
		Destination:        gCopyUtil.stripSASFromFileShareUrl(destURL).String(),
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

func (e *copyFileToNEnumerator) addTransfer(transfer common.CopyTransfer, cca *cookedCopyCmdArgs) error {
	return addTransfer(&(e.CopyJobPartOrderRequest), transfer, cca)
}

func (e *copyFileToNEnumerator) dispatchFinalPart(cca *cookedCopyCmdArgs) error {
	return dispatchFinalPart(&(e.CopyJobPartOrderRequest), cca)
}

func (e *copyFileToNEnumerator) partNum() common.PartNumber {
	return e.PartNum
}
