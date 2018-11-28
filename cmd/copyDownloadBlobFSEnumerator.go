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

	// Case-1: If the source URL is actually a file
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

		return e.dispatchFinalPart(cca)
	}

	// Case-2: Source is a filesystem or directory
	// In this case, the destination should be a directory.
	if !gCopyUtil.isPathALocalDirectory(cca.destination) {
		return fmt.Errorf("the destination must be an existing directory in this download scenario")
	}

	srcADLSGen2PathURLPartExtension := adlsGen2PathURLPartsExtension{fsUrlParts}
	parentSourcePath := srcADLSGen2PathURLPartExtension.getParentSourcePath()
	// The case when user provide list of files to copy. It is used by internal integration.
	if len(cca.listOfFilesToCopy) > 0 {
		for _, fileOrDir := range cca.listOfFilesToCopy {
			tempURLPartsExtension := srcADLSGen2PathURLPartExtension
			if len(parentSourcePath) > 0 && parentSourcePath[len(parentSourcePath)-1] == common.AZCOPY_PATH_SEPARATOR_CHAR {
				parentSourcePath = parentSourcePath[0 : len(parentSourcePath)-1]
			}

			// Try to see if this is a file path, and download the file if it is.
			// Create the path using the given source and files mentioned with listOfFile flag.
			// For Example:
			// 1. source = "https://sdksampleperftest.dfs.core.windows.net/bigdata" file = "file1.txt" blobPath= "file1.txt"
			// 2. source = "https://sdksampleperftest.dfs.core.windows.net/bigdata/dir-1" file = "file1.txt" blobPath= "dir-1/file1.txt"
			filePath := fmt.Sprintf("%s%s%s", parentSourcePath, common.AZCOPY_PATH_SEPARATOR_STRING, fileOrDir)
			if len(filePath) > 0 && filePath[0] == common.AZCOPY_PATH_SEPARATOR_CHAR {
				filePath = filePath[1:]
			}
			tempURLPartsExtension.DirectoryOrFilePath = filePath
			fileURL := azbfs.NewFileURL(tempURLPartsExtension.URL(), p)
			if fileProperties, err := fileURL.GetProperties(ctx); err == nil && strings.EqualFold(fileProperties.XMsResourceType(), "file") {
				// file exists
				fileSize, err := strconv.ParseInt(fileProperties.ContentLength(), 10, 64)
				if err != nil {
					panic(err)
				}

				// assembling the file relative path
				fileRelativePath := fileOrDir
				// ensure there is no additional AZCOPY_PATH_SEPARATOR_CHAR at the start of file name
				if len(fileRelativePath) > 0 && fileRelativePath[0] == common.AZCOPY_PATH_SEPARATOR_CHAR {
					fileRelativePath = fileRelativePath[1:]
				}
				// check for the special character in blob relative path and get path without special character.
				fileRelativePath = util.blobPathWOSpecialCharacters(fileRelativePath)

				srcURL := tempURLPartsExtension.createADLSGen2PathURLFromFileSystem(filePath)
				e.addTransfer(common.CopyTransfer{
					Source:           srcURL.String(),
					Destination:      util.generateLocalPath(cca.destination, fileRelativePath),
					LastModifiedTime: e.parseLmt(fileProperties.LastModified()),
					SourceSize:       fileSize}, cca)
				continue
			}

			if !cca.recursive {
				glcm.Info(fmt.Sprintf("error fetching properties of %s. Either it is a directory or getting the file properties failed. For directories try using the recursive flag.", filePath))
				continue
			}

			// Try to see if this is a directory, and download the directory if it is.
			dirURL := azbfs.NewDirectoryURL(tempURLPartsExtension.URL(), p)
			err := enumerateFilesInADLSGen2Directory(
				ctx,
				dirURL,
				func(fileItem azbfs.ListEntrySchema) bool { // filter always return true in this case
					return true
				},
				func(fileItem azbfs.ListEntrySchema) error {
					relativePath := strings.Replace(*fileItem.Name, parentSourcePath, "", 1)
					if len(relativePath) > 0 && relativePath[0] == common.AZCOPY_PATH_SEPARATOR_CHAR {
						relativePath = relativePath[1:]
					}
					relativePath = util.blobPathWOSpecialCharacters(relativePath)
					return e.addTransfer(common.CopyTransfer{
						Source:           dirURL.FileSystemURL().NewDirectoryURL(*fileItem.Name).String(), // This point to file
						Destination:      util.generateLocalPath(cca.destination, relativePath),
						LastModifiedTime: e.parseLmt(*fileItem.LastModified),
						SourceSize:       *fileItem.ContentLength,
					}, cca)
				},
			)
			if err != nil {
				glcm.Info(fmt.Sprintf("cannot list files inside directory %s mentioned", filePath))
				continue
			}
		}
		// If there are no transfer to queue up, exit with message
		if len(e.Transfers) == 0 {
			glcm.Exit(fmt.Sprintf("no transfer queued for copying data from %s to %s", cca.source, cca.destination), 1)
			return nil
		}
		// dispatch the JobPart as Final Part of the Job
		err = e.dispatchFinalPart(cca)
		if err != nil {
			return err
		}
		return nil
	}

	// Following is original code path, which handles the case when list of files is not specified
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
