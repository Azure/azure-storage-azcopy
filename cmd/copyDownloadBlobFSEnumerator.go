package cmd

import (
	"context"
	"errors"
	"fmt"
	"net/url"

	"net/http"
	"time"

	"github.com/Azure/azure-storage-azcopy/azbfs"
	"github.com/Azure/azure-storage-azcopy/common"
)

type copyDownloadBlobFSEnumerator common.CopyJobPartOrderRequest

func (e *copyDownloadBlobFSEnumerator) enumerate(cca *cookedCopyCmdArgs) error {
	util := copyHandlerUtil{}
	ctx := context.Background()

	// Create blob FS pipeline.
	p, err := createBlobFSPipeline(ctx, e.CredentialInfo)
	if err != nil {
		return err
	}

	// attempt to parse the source url
	sourceUrl, err := url.Parse(cca.src)
	if err != nil {
		return errors.New("cannot parse source URL")
	}

	// parse the given into fileUrl Parts.
	// fileUrl can be further used to get the filesystem name , directory path and other pieces of Info.
	fsUrlParts := azbfs.NewBfsURLParts(*sourceUrl)

	// Create the directory Url and list the entities inside the path
	directoryUrl := azbfs.NewDirectoryURL(*sourceUrl, p)

	// keep the continuation marker as empty
	continuationMarker := ""
	// firstListing is temporary bool variables which tracks whether listing of the Url
	// is done for the first time or not.
	// since first time continuation marker is also empty
	// so add this bool flag which doesn't terminates the loop on first listing.
	firstListing := true

	dListResp, err := directoryUrl.ListDirectorySegment(ctx, &continuationMarker, true)
	if err != nil {
		return fmt.Errorf("error listing the files inside the given source url %s", directoryUrl.String())
	}

	// Loop will continue unless the continuationMarker received in the response is empty
	for continuationMarker != "" || firstListing {
		firstListing = false
		continuationMarker = dListResp.XMsContinuation()
		// Get only the files inside the given path
		// since azcopy creates the parent directory in the path of file
		// so directories will be created unless the directory is empty.
		// TODO: currently empty directories are not created
		resources := dListResp.Files()
		for _, path := range resources {
			var destination = ""
			// If the destination is not directory that is existing
			// It is expected that the resource to be downloaded is downloaded at the destination provided
			if util.isPathALocalDirectory(cca.dst) {
				destination = util.generateLocalPath(cca.dst, util.getRelativePath(fsUrlParts.DirectoryOrFilePath, *path.Name, "/"))
			} else {
				destination = cca.dst
			}
			// convert the time of path to time format
			// If path.LastModified is nil then lastModified time is set to current time
			lModifiedTime := time.Now()
			// else parse the modified to time format and persist it as lastModifiedTime
			if path.LastModified != nil {
				lModifiedTime, err = time.Parse(http.TimeFormat, *path.LastModified)
				if err != nil {
					return fmt.Errorf("error parsing the modified %s time for file / dir %s. Failed with error %s", *path.LastModified, *path.Name, err.Error())
				}
			}
			// Queue the transfer
			e.addTransfer(common.CopyTransfer{
				Source:           directoryUrl.FileSystemURL().NewDirectoryURL(*path.Name).String(),
				Destination:      destination,
				LastModifiedTime: lModifiedTime,
				SourceSize:       *path.ContentLength,
			}, cca)
		}
		dListResp, err = directoryUrl.ListDirectorySegment(ctx, &continuationMarker, true)
		if err != nil {
			return fmt.Errorf("error listing the files inside the given source url %s", directoryUrl.String())
		}
	}
	// dispatch the JobPart as Final Part of the Job
	err = e.dispatchFinalPart()
	if err != nil {
		return err
	}
	return nil
}

func (e *copyDownloadBlobFSEnumerator) addTransfer(transfer common.CopyTransfer, cca *cookedCopyCmdArgs) error {
	return addTransfer((*common.CopyJobPartOrderRequest)(e), transfer, cca)
}

func (e *copyDownloadBlobFSEnumerator) dispatchFinalPart() error {
	return dispatchFinalPart((*common.CopyJobPartOrderRequest)(e))
}

func (e *copyDownloadBlobFSEnumerator) partNum() common.PartNumber {
	return e.PartNum
}
