package cmd

import (
	"context"
	"errors"
	"fmt"
	"net/url"

	"net/http"
	"time"

	"strings"

	"strconv"

	"github.com/Azure/azure-storage-azcopy/azbfs"
	"github.com/Azure/azure-storage-azcopy/common"
)

type copyDownloadBlobFSEnumerator common.CopyJobPartOrderRequest

func (e *copyDownloadBlobFSEnumerator) enumerate(cca *cookedCopyCmdArgs) error {
	util := copyHandlerUtil{}
	ctx := context.Background()

	// create blob FS pipeline.
	p, err := createBlobFSPipeline(ctx, e.CredentialInfo)
	if err != nil {
		return err
	}

	// attempt to parse the source url
	sourceURL, err := url.Parse(cca.source)
	if err != nil {
		return errors.New("cannot parse source URL")
	}

	// parse the given source URL into fsUrlParts, which separates the filesystem name and directory/file path
	fsUrlParts := azbfs.NewBfsURLParts(*sourceURL)

	// we do not know if the source is a file or a directory
	// we assume it is a directory and get its properties
	directoryURL := azbfs.NewDirectoryURL(*sourceURL, p)
	props, err := directoryURL.GetProperties(ctx)

	// if the source URL is actually a file
	// then we should short-circuit and simply download that file
	if err == nil && strings.EqualFold(props.XMsResourceType(), "file") {
		var destination = ""
		// if the destination is an existing directory, then put the file under it
		// otherwise assume the user has provided a specific path for the destination file
		if util.isPathALocalDirectory(cca.destination) {
			destination = util.generateLocalPath(cca.destination, util.getPossibleFileNameFromURL(fsUrlParts.DirectoryOrFilePath))
		} else {
			destination = cca.destination
		}

		fileSize, err := strconv.ParseInt(props.ContentLength(), 10, 64)
		if err != nil {
			panic(err)
		}

		// Queue the transfer
		e.addTransfer(common.CopyTransfer{
			Source:           cca.source,
			Destination:      destination,
			LastModifiedTime: e.parseLmt(props.LastModified()),
			SourceSize:       fileSize,
		}, cca)

	} else {
		// if downloading entire file system, then create a local directory with the file system's name
		if fsUrlParts.DirectoryOrFilePath == "" {
			cca.destination = util.generateLocalPath(cca.destination, fsUrlParts.FileSystemName)
		}

		// initialize an empty continuation marker
		continuationMarker := ""

		// list out the directory and download its files
		// loop will continue unless the continuationMarker received in the response is empty
		for {
			dListResp, err := directoryURL.ListDirectorySegment(ctx, &continuationMarker, true)
			if err != nil {
				return fmt.Errorf("error listing the files inside the given source url %s", directoryURL.String())
			}

			// get only the files inside the given path
			// TODO: currently empty directories are not created, consider creating them
			for _, path := range dListResp.Files() {
				// Queue the transfer
				e.addTransfer(common.CopyTransfer{
					Source:           directoryURL.FileSystemURL().NewDirectoryURL(*path.Name).String(),
					Destination:      util.generateLocalPath(cca.destination, util.getRelativePath(fsUrlParts.DirectoryOrFilePath, *path.Name)),
					LastModifiedTime: e.parseLmt(*path.LastModified),
					SourceSize:       *path.ContentLength,
				}, cca)
			}

			// update the continuation token for the next list operation
			continuationMarker = dListResp.XMsContinuation()

			// determine whether listing should be done
			if continuationMarker == "" {
				break
			}
		}
	}

	// dispatch the JobPart as Final Part of the Job
	err = e.dispatchFinalPart(cca)
	if err != nil {
		return err
	}
	return nil
}

func (e *copyDownloadBlobFSEnumerator) parseLmt(lastModifiedTime string) time.Time {
	// if last modified time is available, parse it
	// otherwise use the current time as last modified time
	lmt := time.Now()
	if lastModifiedTime != "" {
		parsedLmt, err := time.Parse(http.TimeFormat, lastModifiedTime)
		if err == nil {
			lmt = parsedLmt
		}
	}

	return lmt
}

func (e *copyDownloadBlobFSEnumerator) addTransfer(transfer common.CopyTransfer, cca *cookedCopyCmdArgs) error {
	return addTransfer((*common.CopyJobPartOrderRequest)(e), transfer, cca)
}

func (e *copyDownloadBlobFSEnumerator) dispatchFinalPart(cca *cookedCopyCmdArgs) error {
	return dispatchFinalPart((*common.CopyJobPartOrderRequest)(e), cca)
}
