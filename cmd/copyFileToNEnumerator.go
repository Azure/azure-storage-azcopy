package cmd

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/url"

	"github.com/Azure/azure-storage-azcopy/common"
	"github.com/Azure/azure-storage-blob-go/2018-03-28/azblob"
	"github.com/Azure/azure-storage-file-go/2017-07-29/azfile"
)

// copyFileToNEnumerator enumerates file source, and submit request for copy file to N,
// where N stands for blob/file/blobFS (Currently only blob is supported).
// The source could be single file/directory/share/file account
type copyFileToNEnumerator common.CopyJobPartOrderRequest

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
	err = e.initiateDestHelperInfo(ctx)
	if err != nil {
		return err
	}

	// attempt to parse the source and destination url
	sourceURL, err := url.Parse(gCopyUtil.replaceBackSlashWithSlash(cca.src))
	if err != nil {
		return errors.New("cannot parse source URL")
	}
	destURL, err := url.Parse(gCopyUtil.replaceBackSlashWithSlash(cca.dst))
	if err != nil {
		return errors.New("cannot parse destination URL")
	}

	srcFileURLPartExtension := fileURLPartsExtension{azfile.NewFileURLParts(*sourceURL)}
	// Case-1: Source is account, currently only support blob destination
	if isAccountLevel, searchPrefix, pattern := srcFileURLPartExtension.isFileAccountLevelSearch(); isAccountLevel {
		if pattern == "*" && !cca.recursive {
			return fmt.Errorf("cannot copy the entire account without recursive flag, please use recursive flag")
		}

		// Switch URL https://<account-name>/shareprefix* to ServiceURL "https://<account-name>"
		tmpSrcFileURLPart := srcFileURLPartExtension
		tmpSrcFileURLPart.ShareName = ""
		srcServiceURL := azfile.NewServiceURL(tmpSrcFileURLPart.URL(), srcFilePipeline)
		// Validate destination, currently only support blob destination
		// TODO: other type destination URLs, e.g: BlobFS, File and etc
		destServiceURL := azfile.NewServiceURL(*destURL, srcFilePipeline)
		_, err = destServiceURL.GetProperties(ctx)
		if err != nil {
			return errors.New("invalid source and destination combination for service to service copy: " +
				"destination must point to service account when source is a service account.")
		}

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
		err := e.dispatchFinalPart()
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
		err := e.createBucket(ctx, *destURL, nil)
		if err != nil {
			return err
		}
		// directly use destURL as destination
		if err := e.addTransferInternal(srcFileURL.URL(), *destURL, fileProperties, cca); err != nil {
			return err
		}
		return e.dispatchFinalPart()
	}

	// Case-3: Source is a file share or directory
	// Switch URL https://<account-name>/share/prefix* to ShareURL "https://<account-name>/share"
	dirURL, searchPrefix := srcFileURLPartExtension.getDirURLAndSearchPrefixFromFileURL(srcFilePipeline)
	if searchPrefix == "" && !cca.recursive {
		return fmt.Errorf("cannot copy the entire share or directory without recursive flag, please use recursive flag")
	}
	err = e.createBucket(ctx, *destURL, nil)
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
	return e.dispatchFinalPart()
}

// destination helper info for destination pre-operations: e.g. create container/share/bucket and etc.
// The info, such as blob pipeline is created once, and reused multiple times.
func (e *copyFileToNEnumerator) initiateDestHelperInfo(ctx context.Context) error {
	switch e.FromTo {
	case common.EFromTo.FileBlob():
		p, err := createBlobPipeline(ctx, e.CredentialInfo)
		if err != nil {
			return err
		}
		destInfo.destBlobPipeline = p
	}
	return nil
}

// TODO: Create share/bucket and etc. Currently only support blob destination, so create container.
func (e *copyFileToNEnumerator) createBucket(ctx context.Context, destURL url.URL, metadata common.Metadata) error {
	switch e.FromTo {
	case common.EFromTo.FileBlob():
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
		// Here could be other cases, e.g.: creating share and etc.
	}
	return nil
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
			e.createBucket(ctx, tmpDestURL, nil)

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
		Source:             srcURL.String(),
		Destination:        destURL.String(),
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
	return addTransfer((*common.CopyJobPartOrderRequest)(e), transfer, cca)
}

func (e *copyFileToNEnumerator) dispatchFinalPart() error {
	return dispatchFinalPart((*common.CopyJobPartOrderRequest)(e))
}

func (e *copyFileToNEnumerator) partNum() common.PartNumber {
	return e.PartNum
}
