package cmd

import (
	"context"
	"errors"
	"fmt"
	"net/url"

	"github.com/Azure/azure-storage-azcopy/common"
	"github.com/Azure/azure-storage-file-go/2017-07-29/azfile"
)

type copyDownloadFileEnumerator common.CopyJobPartOrderRequest

// enumerate enumerats file resources, and add transfers accordingly.
// It supports equivalent functions as blob enumerator.
func (e *copyDownloadFileEnumerator) enumerate(cca *cookedCopyCmdArgs) error {
	ctx := context.TODO()

	// attempt to parse the source url
	sourceURL, err := url.Parse(gCopyUtil.replaceBackSlashWithSlash(cca.source))
	if err != nil {
		return errors.New("cannot parse source URL")
	}
	// append the sas at the end of query params.
	sourceURL = gCopyUtil.appendQueryParamToUrl(sourceURL, cca.sourceSAS)

	// Create pipeline for source Azure File service.
	// Note: only anonymous credential is supported for file source(i.e. SAS) now.
	srcCredInfo := common.CredentialInfo{CredentialType: common.ECredentialType.Anonymous()}
	srcFilePipeline, err := createFilePipeline(ctx, srcCredInfo)
	if err != nil {
		return err
	}

	srcFileURLPartExtension := fileURLPartsExtension{azfile.NewFileURLParts(*sourceURL)}

	// Case-1: Source is single file
	srcFileURL := azfile.NewFileURL(*sourceURL, srcFilePipeline)
	// Verify if source is a single file
	// Note: Currently only support single to single, and not support single to directory.
	if fileProperties, err := srcFileURL.GetProperties(ctx); err == nil {
		var singleFileDestinationPath string
		if gCopyUtil.isPathALocalDirectory(cca.destination) {
			singleFileDestinationPath = gCopyUtil.generateLocalPath(
				cca.destination, gCopyUtil.getPossibleFileNameFromURL(sourceURL.Path))
		} else {
			singleFileDestinationPath = cca.destination
		}
		// TODO: Ensure whether to create share here or in Xfer

		if err := e.addDownloadFileTransfer(srcFileURL.URL(), singleFileDestinationPath, fileProperties, cca); err != nil {
			return err
		}
		return e.dispatchFinalPart(cca)
	}

	// Case-2: Source is a file share or directory
	// The destination must be a directory, when source is share or directory.
	if !gCopyUtil.isPathALocalDirectory(cca.destination) {
		return fmt.Errorf("the destination must be an existing directory in this download scenario")
	}

	searchPrefix, fileNamePattern := srcFileURLPartExtension.searchPrefixFromFileURL()
	if searchPrefix == "" && !cca.recursive {
		return fmt.Errorf("cannot copy the entire share or directory without recursive flag, please use recursive flag")
	}
	// TODO: Ensure whether to create share here or in Xfer

	if err := e.addTransfersFromDirectory(ctx,
		azfile.NewShareURL(srcFileURLPartExtension.getShareURL(), srcFilePipeline).NewRootDirectoryURL(),
		cca.destination,
		searchPrefix,
		fileNamePattern,
		srcFileURLPartExtension.getParentSourcePath(),
		cca); err != nil {
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

// addTransfersFromDirectory enumerates files in directory and sub directoreis,
// and adds matched file into transfer.
func (e *copyDownloadFileEnumerator) addTransfersFromDirectory(
	ctx context.Context, srcDirectoryURL azfile.DirectoryURL,
	destBasePath, fileOrDirNamePrefix, fileNamePattern, parentSourcePath string,
	cca *cookedCopyCmdArgs) error {

	fileFilter := func(fileItem azfile.FileItem, fileURL azfile.FileURL) bool {
		fileURLPart := azfile.NewFileURLParts(fileURL.URL())
		// Check if file name matches pattern.
		if !gCopyUtil.matchBlobNameAgainstPattern(fileNamePattern, fileURLPart.DirectoryOrFilePath, cca.recursive) {
			return false
		}

		// Check the file should be included or not.
		if !gCopyUtil.resourceShouldBeIncluded(parentSourcePath, e.Include, fileURLPart.DirectoryOrFilePath) {
			return false
		}

		// Check the file should be excluded or not.
		if gCopyUtil.resourceShouldBeExcluded(parentSourcePath, e.Exclude, fileURLPart.DirectoryOrFilePath) {
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
			fileRelativePath := gCopyUtil.getRelativePath(fileOrDirNamePrefix, fileURLPart.DirectoryOrFilePath)

			// TODO: Remove get attribute, when file's list method can return property and metadata.
			p, err := fileURL.GetProperties(ctx)
			if err != nil {
				return err
			}

			return e.addDownloadFileTransfer(
				fileURL.URL(),
				gCopyUtil.generateLocalPath(destBasePath, fileRelativePath),
				p,
				cca)
		})
}

func (e *copyDownloadFileEnumerator) addDownloadFileTransfer(srcURL url.URL, destPath string,
	properties *azfile.FileGetPropertiesResponse, cca *cookedCopyCmdArgs) error {
	return e.addTransfer(common.CopyTransfer{
		Source:           gCopyUtil.stripSASFromFileShareUrl(srcURL).String(),
		Destination:      destPath,
		LastModifiedTime: properties.LastModified(),
		SourceSize:       properties.ContentLength()},
		cca)
}

func (e *copyDownloadFileEnumerator) addTransfer(transfer common.CopyTransfer, cca *cookedCopyCmdArgs) error {
	return addTransfer((*common.CopyJobPartOrderRequest)(e), transfer, cca)
}

func (e *copyDownloadFileEnumerator) dispatchFinalPart(cca *cookedCopyCmdArgs) error {
	return dispatchFinalPart((*common.CopyJobPartOrderRequest)(e), cca)
}
