package cmd

import (
	"context"
	"fmt"
	"math/rand"
	"strings"
	"time"

	"github.com/Azure/azure-storage-azcopy/common"
	"github.com/Azure/azure-storage-blob-go/2018-03-28/azblob"
	"github.com/Azure/azure-storage-file-go/2017-07-29/azfile"
)

// addTransfer accepts a new transfer, if the threshold is reached, dispatch a job part order.
func addTransfer(e *common.CopyJobPartOrderRequest, transfer common.CopyTransfer, cca *cookedCopyCmdArgs) error {
	// dispatch the transfers once the number reaches NumOfFilesPerDispatchJobPart
	// we do this so that in the case of large transfer, the transfer engine can get started
	// while the frontend is still gathering more transfers
	if len(e.Transfers) == NumOfFilesPerDispatchJobPart {
		shuffleTransfers(e.Transfers)
		resp := common.CopyJobPartOrderResponse{}
		Rpc(common.ERpcCmd.CopyJobPartOrder(), (*common.CopyJobPartOrderRequest)(e), &resp)

		if !resp.JobStarted {
			return fmt.Errorf("copy job part order with JobId %s and part number %d failed because %s", e.JobID, e.PartNum, resp.ErrorMsg)
		}
		// if the current part order sent to engine is 0, then start fetching the Job Progress summary.
		if e.PartNum == 0 {
			cca.waitUntilJobCompletion(false)
		}
		e.Transfers = []common.CopyTransfer{}
		e.PartNum++
	}

	e.Transfers = append(e.Transfers, transfer)

	return nil
}

// this function shuffles the transfers before they are dispatched
// this is done to avoid hitting the same partition continuously in an append only pattern
// TODO this should probably be removed after the high throughput block blob feature is implemented on the service side
func shuffleTransfers(transfers []common.CopyTransfer) {
	rand.Seed(time.Now().UnixNano())
	rand.Shuffle(len(transfers), func(i, j int) { transfers[i], transfers[j] = transfers[j], transfers[i] })
}

// we need to send a last part with isFinalPart set to true, along with whatever transfers that still haven't been sent
// dispatchFinalPart sends a last part with isFinalPart set to true, along with whatever transfers that still haven't been sent.
func dispatchFinalPart(e *common.CopyJobPartOrderRequest, cca *cookedCopyCmdArgs) error {
	shuffleTransfers(e.Transfers)
	e.IsFinalPart = true
	var resp common.CopyJobPartOrderResponse
	Rpc(common.ERpcCmd.CopyJobPartOrder(), (*common.CopyJobPartOrderRequest)(e), &resp)

	if !resp.JobStarted {
		return fmt.Errorf("copy job part order with JobId %s and part number %d failed because %s", e.JobID, e.PartNum, resp.ErrorMsg)
	}

	// set the flag on cca, to indicate the enumeration is done
	cca.isEnumerationComplete = true
	return nil
}

//////////////////////////////////////////////////////////////////////////////////////////
// Blob service enumerators.
//////////////////////////////////////////////////////////////////////////////////////////
// enumerateBlobsInContainer enumerates blobs in container.
func enumerateBlobsInContainer(ctx context.Context, containerURL azblob.ContainerURL,
	blobPrefix string, filter func(blobItem azblob.BlobItem) bool,
	callback func(blobItem azblob.BlobItem) error) error {
	for marker := (azblob.Marker{}); marker.NotDone(); {
		listContainerResp, err := containerURL.ListBlobsFlatSegment(
			ctx, marker,
			azblob.ListBlobsSegmentOptions{
				Details: azblob.BlobListingDetails{Metadata: true},
				Prefix:  blobPrefix})
		if err != nil {
			return fmt.Errorf("cannot list blobs, %v", err)
		}

		// Process the blobs returned in this result segment (if the segment is empty, the loop body won't execute)
		for _, blobItem := range listContainerResp.Segment.BlobItems {
			// If the blob represents a folder as per the conditions mentioned in the
			// api doesBlobRepresentAFolder, then skip the blob.
			if gCopyUtil.doesBlobRepresentAFolder(blobItem) {
				continue
			}

			if !filter(blobItem) {
				continue
			}

			if err := callback(blobItem); err != nil {
				return err
			}
		}
		marker = listContainerResp.NextMarker
	}
	return nil
}

// enumerateContainersInAccount enumerates containers in blob service account.
func enumerateContainersInAccount(ctx context.Context, srcServiceURL azblob.ServiceURL,
	containerPrefix string, callback func(containerItem azblob.ContainerItem) error) error {
	for marker := (azblob.Marker{}); marker.NotDone(); {
		listSvcResp, err := srcServiceURL.ListContainersSegment(ctx, marker,
			azblob.ListContainersSegmentOptions{Prefix: containerPrefix})
		if err != nil {
			return fmt.Errorf("cannot list containers, %v", err)
		}

		// Process the containers returned in this result segment (if the segment is empty, the loop body won't execute)
		for _, containerItem := range listSvcResp.ContainerItems {
			if err := callback(containerItem); err != nil {
				return err
			}
		}
		marker = listSvcResp.NextMarker
	}
	return nil
}

//////////////////////////////////////////////////////////////////////////////////////////
// File service enumerators.
//////////////////////////////////////////////////////////////////////////////////////////
// enumerateSharesInAccount enumerates shares in file service account.
func enumerateSharesInAccount(ctx context.Context, srcServiceURL azfile.ServiceURL,
	sharePrefix string, callback func(shareItem azfile.ShareItem) error) error {
	for marker := (azfile.Marker{}); marker.NotDone(); {
		listSvcResp, err := srcServiceURL.ListSharesSegment(ctx, marker,
			azfile.ListSharesOptions{Prefix: sharePrefix})
		if err != nil {
			return fmt.Errorf("cannot list shares, %v", err)
		}

		// Process the shares returned in this result segment (if the segment is empty, the loop body won't execute)
		for _, shareItem := range listSvcResp.ShareItems {
			if err := callback(shareItem); err != nil {
				return err
			}
		}
		marker = listSvcResp.NextMarker
	}
	return nil
}

// enumerateDirectoriesAndFilesInShare enumerates files in share.
// filePrefix could be:
// a. File with parent directories and file prefix: /d1/d2/fileprefix
// b. File with pure file prefix: fileprefix
// c. File with pur parent directories: /d1/d2/
func enumerateDirectoriesAndFilesInShare(ctx context.Context, srcDirURL azfile.DirectoryURL,
	filePrefix string, recursive bool, filter func(fileItem azfile.FileItem) bool,
	callback func(fileItem azfile.FileItem) error) error {

	// Process the filePrefix, if the file prefix starts with parent directory,
	// then it wishes to enumerate the directory with specific sub-directory,
	// append the sub-directory to the src directory URL.
	// e.g.: searching https://<azfile>/share/basedir, and prefix is /d1/d2/file
	// the new source directory URL will be https://<azfile>/share/basedir/d1/d2
	if len(filePrefix) > 0 && filePrefix[0] == common.AZCOPY_PATH_SEPARATOR_CHAR {
		filePrefix = filePrefix[1:]
	}
	if lastSepIndex := strings.LastIndex(filePrefix, common.AZCOPY_PATH_SEPARATOR_STRING); lastSepIndex > 0 {
		subDirStr := filePrefix[:lastSepIndex]
		srcDirURL = srcDirURL.NewDirectoryURL(subDirStr)
		filePrefix = filePrefix[lastSepIndex+1:]
	}

	// After preprocess, file prefix will no more contains '/'. It will be the prefix of
	// file or dir in current dir level.
	for marker := (azfile.Marker{}); marker.NotDone(); {
		listDirResp, err := srcDirURL.ListFilesAndDirectoriesSegment(ctx, marker,
			azfile.ListFilesAndDirectoriesOptions{Prefix: filePrefix})
		if err != nil {
			return fmt.Errorf("cannot list files and directories, %v", err)
		}

		// Process the files returned in this result segment (if the segment is empty, the loop body won't execute)
		for _, fileItem := range listDirResp.FileItems {
			if !filter(fileItem) {
				continue
			}

			if err := callback(fileItem); err != nil {
				return err
			}
		}

		// Process the directories if the recursive mode is on
		if recursive {
			for _, dirItem := range listDirResp.DirectoryItems {
				// Recursive with prefix set to ""
				enumerateDirectoriesAndFilesInShare(
					ctx,
					srcDirURL.NewDirectoryURL(dirItem.Name),
					"",
					recursive,
					filter,
					callback)
			}
		}

		marker = listDirResp.NextMarker
	}
	return nil
}
