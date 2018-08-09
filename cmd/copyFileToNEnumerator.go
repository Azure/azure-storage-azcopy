package cmd

import (
	"context"
	"errors"
	"fmt"
	"net/url"

	"github.com/Azure/azure-storage-azcopy/common"
	"github.com/Azure/azure-storage-file-go/2017-07-29/azfile"
)

// copyFileToNEnumerator enumerates file source, and submit request for copy file to N,
// where N stands for blob/file/blobFS (Currently only blob is supported).
// The source could be single file/directory/share/file account
type copyFileToNEnumerator struct {
	copyS2SEnumerator
}

func (e *copyFileToNEnumerator) enumerate(cca *cookedCopyCmdArgs) error {
	ctx := context.TODO()

	// Create pipeline for source Azure File service.
	// Note: only anonymous credential is supported for file source(i.e. SAS) now.
	// e.CredentialInfo is for destination
	srcCredInfo := common.CredentialInfo{CredentialType: common.ECredentialType.Anonymous()}
	srcFilePipeline, err := createFilePipeline(ctx, srcCredInfo)
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

	if err := e.initDestPipeline(ctx); err != nil {
		return err
	}

	srcFileURLPartExtension := fileURLPartsExtension{azfile.NewFileURLParts(*sourceURL)}
	// Case-1: Source is account, currently only support blob destination
	if isAccountLevel, searchPrefix, _ := srcFileURLPartExtension.isFileAccountLevelSearch(); isAccountLevel {
		if !cca.recursive {
			return fmt.Errorf("cannot copy the entire account without recursive flag, please use recursive flag")
		}

		// Validate If destination is service level account.
		if err := e.validateDestIsService(ctx, *destURL); err != nil {
			return err
		}

		// Switch URL https://<account-name>/shareprefix* to ServiceURL "https://<account-name>"
		tmpSrcFileURLPart := srcFileURLPartExtension
		tmpSrcFileURLPart.ShareName = ""
		srcServiceURL := azfile.NewServiceURL(tmpSrcFileURLPart.URL(), srcFilePipeline)

		// List shares
		err = e.enumerateSharesInAccount(ctx, srcServiceURL, *destURL, searchPrefix, cca)
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

	// Case-2: Source is single file
	// Verify if source is a single file
	srcFileURL := azfile.NewFileURL(*sourceURL, srcFilePipeline)
	fileProperties, err := srcFileURL.GetProperties(ctx)
	// Note: Currently only support single to single, and not support single to directory.
	if err == nil {
		if endWithSlashOrBackSlash(destURL.Path) {
			return errors.New("invalid source and destination combination for service to service copy: " +
				"destination must point to a single file, when source is a single file.")
		}
		err := e.createDestBucket(ctx, *destURL, nil)
		if err != nil {
			return err
		}
		// directly use destURL as destination
		if err := e.addTransferInternal(srcFileURL.URL(), *destURL, fileProperties, cca); err != nil {
			return err
		}
		return e.dispatchFinalPart(cca)
	}

	// Case-3: Source is a file share or directory
	// Switch URL https://<account-name>/share/prefix* to ShareURL "https://<account-name>/share"
	dirURL, searchPrefix := srcFileURLPartExtension.getDirURLAndSearchPrefixFromFileURL(srcFilePipeline)
	if searchPrefix == "" && !cca.recursive {
		return fmt.Errorf("cannot copy the entire share or directory without recursive flag, please use recursive flag")
	}
	err = e.createDestBucket(ctx, *destURL, nil)
	if err != nil {
		return err
	}
	err = e.enumerateDirectoriesAndFilesInShare(
		ctx,
		dirURL,
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

// enumerateSharesInAccount enumerates containers in blob service account.
func (e *copyFileToNEnumerator) enumerateSharesInAccount(ctx context.Context, srcServiceURL azfile.ServiceURL, destBaseURL url.URL,
	srcSearchPattern string, cca *cookedCopyCmdArgs) error {
	for marker := (azfile.Marker{}); marker.NotDone(); {
		listSvcResp, err := srcServiceURL.ListSharesSegment(ctx, marker,
			azfile.ListSharesOptions{Prefix: srcSearchPattern})
		if err != nil {
			return fmt.Errorf("cannot list shares for copy, %v", err)
		}

		// Process the shares returned in this result segment (if the segment is empty, the loop body won't execute)
		for _, shareItem := range listSvcResp.ShareItems {
			// Whatever the destination type is, it should be equivalent to account level,
			// directoy append share name to it.
			tmpDestURL := destBaseURL
			tmpDestURL.Path = gCopyUtil.generateObjectPath(tmpDestURL.Path, shareItem.Name)
			shareRootDirURL := srcServiceURL.NewShareURL(shareItem.Name).NewRootDirectoryURL()

			// Transfer azblob's metadata to common metadata, note common metadata can be transferred to other types of metadata.
			// Doesn't copy bucket's metadata as AzCopy-v1 do.
			e.createDestBucket(ctx, tmpDestURL, nil)

			// List source share
			// TODO: List in parallel to speed up.
			e.enumerateDirectoriesAndFilesInShare(
				ctx,
				shareRootDirURL,
				tmpDestURL,
				"",
				cca)
		}
		marker = listSvcResp.NextMarker
	}
	return nil
}

// enumerateDirectoriesAndFilesInShare enumerates blobs in container.
func (e *copyFileToNEnumerator) enumerateDirectoriesAndFilesInShare(ctx context.Context, srcDirURL azfile.DirectoryURL, destBaseURL url.URL,
	srcSearchPattern string, cca *cookedCopyCmdArgs) error {
	for marker := (azfile.Marker{}); marker.NotDone(); {
		listDirResp, err := srcDirURL.ListFilesAndDirectoriesSegment(ctx, marker,
			azfile.ListFilesAndDirectoriesOptions{Prefix: srcSearchPattern})
		if err != nil {
			return fmt.Errorf("cannot list files for copy, %v", err)
		}

		// Process the files returned in this result segment (if the segment is empty, the loop body won't execute)
		for _, fileItem := range listDirResp.FileItems {
			srcFileURL := srcDirURL.NewFileURL(fileItem.Name)
			srcFileProperties, err := srcFileURL.GetProperties(ctx) // TODO: the cost is high while otherwise we cannot get the last modified time. As Azure file's PM description, list might get more valuable file properties later, optimize the logic after the change...
			if err != nil {
				return err
			}

			tmpDestURL := destBaseURL
			tmpDestURL.Path = gCopyUtil.generateObjectPath(tmpDestURL.Path, fileItem.Name)
			err = e.addTransferInternal(
				srcFileURL.URL(),
				tmpDestURL,
				srcFileProperties,
				cca)
			if err != nil {
				return err // TODO: Ensure for list errors, directly return or do logging but not return, make the list mechanism much robust
			}
		}

		// Process the directories if the recursive mode is on
		if cca.recursive {
			for _, dirItem := range listDirResp.DirectoryItems {
				tmpSubDirURL := srcDirURL.NewDirectoryURL(dirItem.Name)
				tmpDestURL := destBaseURL
				tmpDestURL.Path = gCopyUtil.generateObjectPath(tmpDestURL.Path, dirItem.Name)
				// Recursive with prefix set to ""
				e.enumerateDirectoriesAndFilesInShare(
					ctx,
					tmpSubDirURL,
					tmpDestURL,
					"",
					cca)
			}
		}

		marker = listDirResp.NextMarker
	}
	return nil
}

func (e *copyFileToNEnumerator) addTransferInternal(srcURL, destURL url.URL, properties *azfile.FileGetPropertiesResponse,
	cca *cookedCopyCmdArgs) error {
	// TODO: This is temp work around for Azure file's content MD5, can be removed whe new File SDK get released.
	contentMD5 := properties.ContentMD5()

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
		ContentMD5:         contentMD5[:]},
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
