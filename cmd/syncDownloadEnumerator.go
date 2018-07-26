package cmd

import (
	"context"
	"fmt"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"strings"

	"github.com/Azure/azure-pipeline-go/pipeline"
	"github.com/Azure/azure-storage-azcopy/common"
	"github.com/Azure/azure-storage-azcopy/ste"
	"github.com/Azure/azure-storage-blob-go/2018-03-28/azblob"
)

type syncDownloadEnumerator common.SyncJobPartOrderRequest

// accept a new transfer, if the threshold is reached, dispatch a job part order
func (e *syncDownloadEnumerator) addTransferToUpload(transfer common.CopyTransfer, cca *cookedSyncCmdArgs) error {

	if len(e.CopyJobRequest.Transfers) == NumOfFilesPerDispatchJobPart {
		resp := common.CopyJobPartOrderResponse{}
		e.CopyJobRequest.PartNum = e.PartNumber
		Rpc(common.ERpcCmd.CopyJobPartOrder(), (*common.CopyJobPartOrderRequest)(&e.CopyJobRequest), &resp)

		if !resp.JobStarted {
			return fmt.Errorf("copy job part order with JobId %s and part number %d failed because %s", e.JobID, e.PartNumber, resp.ErrorMsg)
		}
		// if the current part order sent to engine is 0, then start fetching the Job Progress summary.
		if e.PartNumber == 0 {
			go glcm.WaitUntilJobCompletion(cca)
		}
		e.CopyJobRequest.Transfers = []common.CopyTransfer{}
		e.PartNumber++
	}
	e.CopyJobRequest.Transfers = append(e.CopyJobRequest.Transfers, transfer)
	return nil
}

// we need to send a last part with isFinalPart set to true, along with whatever transfers that still haven't been sent
func (e *syncDownloadEnumerator) dispatchFinalPart() error {
	numberOfCopyTransfers := len(e.CopyJobRequest.Transfers)
	numberOfDeleteTransfers := len(e.DeleteJobRequest.Transfers)
	// If the numberoftransfer to copy / delete both are 0
	// means no transfer has been to queue to send to STE
	if numberOfCopyTransfers == 0 && numberOfDeleteTransfers == 0 {
		// If there are some files that were deleted locally
		// display the files
		if e.FilesDeletedLocally > 0 {
			return fmt.Errorf("%d files deleted locally. No transfer to upload or download ", e.FilesDeletedLocally)
		} else {
			return fmt.Errorf("cannot start job because there are no transfer to upload or delete. " +
				"The source and destination are in sync")
		}
	} else if numberOfCopyTransfers > 0 && numberOfDeleteTransfers > 0 {
		//If there are transfer to upload and download both
		// Send the CopyJob Part Order first
		// Increment the Part Number
		// Send the DeleteJob Part are the final Part
		var resp common.CopyJobPartOrderResponse
		e.CopyJobRequest.PartNum = e.PartNumber
		Rpc(common.ERpcCmd.CopyJobPartOrder(), (*common.CopyJobPartOrderRequest)(&e.CopyJobRequest), &resp)
		if !resp.JobStarted {
			return fmt.Errorf("copy job part order with JobId %s and part number %d failed because %s", e.JobID, e.PartNumber, resp.ErrorMsg)
		}
		e.PartNumber++
		e.DeleteJobRequest.IsFinalPart = true
		e.DeleteJobRequest.PartNum = e.PartNumber
		Rpc(common.ERpcCmd.CopyJobPartOrder(), (*common.CopyJobPartOrderRequest)(&e.DeleteJobRequest), &resp)
		if !resp.JobStarted {
			return fmt.Errorf("delete job part order with JobId %s and part number %d failed because %s", e.JobID, e.PartNumber, resp.ErrorMsg)
		}
	} else if numberOfCopyTransfers > 0 {
		// Only CopyJobPart Order needs to be sent
		e.CopyJobRequest.IsFinalPart = true
		e.CopyJobRequest.PartNum = e.PartNumber
		var resp common.CopyJobPartOrderResponse
		Rpc(common.ERpcCmd.CopyJobPartOrder(), (*common.CopyJobPartOrderRequest)(&e.CopyJobRequest), &resp)
		if !resp.JobStarted {
			return fmt.Errorf("copy job part order with JobId %s and part number %d failed because %s", e.JobID, e.PartNumber, resp.ErrorMsg)
		}
	} else {
		// Only DeleteJob Part Order needs to be sent
		e.DeleteJobRequest.IsFinalPart = true
		e.DeleteJobRequest.PartNum = e.PartNumber
		var resp common.CopyJobPartOrderResponse
		Rpc(common.ERpcCmd.CopyJobPartOrder(), (*common.CopyJobPartOrderRequest)(&e.DeleteJobRequest), &resp)
		if !resp.JobStarted {
			return fmt.Errorf("delete job part order with JobId %s and part number %d failed because %s", e.JobID, e.PartNumber, resp.ErrorMsg)
		}
	}
	return nil
}

// compareRemoteAgainstLocal api compares the blob at given destination Url and
// compare with blobs locally. If the blobs locally doesn't exists, then destination
// blobs are downloaded locally.
func (e *syncDownloadEnumerator) compareRemoteAgainstLocal(cca *cookedSyncCmdArgs, p pipeline.Pipeline) error {
	util := copyHandlerUtil{}

	ctx := context.WithValue(context.Background(), ste.ServiceAPIVersionOverride, ste.DefaultServiceApiVersion)

	destinationUrl, err := url.Parse(cca.source)
	if err != nil {
		return fmt.Errorf("error parsing the destinatio url")
	}

	// since source is a remote url, it will have sas parameter
	// since sas parameter will be stripped from the source url
	// while cooking the raw command arguments
	destinationUrl = util.appendQueryParamToUrl(destinationUrl, cca.sourceSAS)

	blobUrlParts := azblob.NewBlobURLParts(*destinationUrl)

	blobURLPartsExtension := blobURLPartsExtension{blobUrlParts}

	containerUrl := util.getContainerUrl(blobUrlParts)
	searchPrefix, pattern := blobURLPartsExtension.searchPrefixFromBlobURL()

	containerBlobUrl := azblob.NewContainerURL(containerUrl, p)
	// virtual directory is the entire virtual directory path before the blob name
	// passed in the searchPrefix
	// Example: cca.destination = https://<container-name>/vd-1?<sig> searchPrefix = vd-1/
	// virtualDirectory = vd-1
	// Example: cca.destination = https://<container-name>/vd-1/vd-2/fi*.txt?<sig> searchPrefix = vd-1/vd-2/fi*.txt
	// virtualDirectory = vd-1/vd-2/
	virtualDirectory := util.getLastVirtualDirectoryFromPath(searchPrefix)
	// strip away the leading / in the closest virtual directory
	if len(virtualDirectory) > 0 && virtualDirectory[0:1] == "/" {
		virtualDirectory = virtualDirectory[1:]
	}

	// Get the source path without the wildcards
	// This is defined since the files mentioned with exclude flag
	// & include flag are relative to the Source
	// If the source has wildcards, then files are relative to the
	// parent source path which is the path of last directory in the source
	// without wildcards
	// For Example: src = "/home/user/dir1" parentSourcePath = "/home/user/dir1"
	// For Example: src = "/home/user/dir*" parentSourcePath = "/home/user"
	// For Example: src = "/home/*" parentSourcePath = "/home"
	parentSourcePath := blobUrlParts.BlobName
	wcIndex := util.firstIndexOfWildCard(parentSourcePath)
	if wcIndex != -1 {
		parentSourcePath = parentSourcePath[:wcIndex]
		pathSepIndex := strings.LastIndex(parentSourcePath, "/")
		if pathSepIndex == -1 {
			parentSourcePath = ""
		}
		parentSourcePath = parentSourcePath[:pathSepIndex]
	}

	for marker := (azblob.Marker{}); marker.NotDone(); {
		// look for all blobs that start with the prefix
		listBlob, err := containerBlobUrl.ListBlobsFlatSegment(ctx, marker,
			azblob.ListBlobsSegmentOptions{Prefix: searchPrefix})
		if err != nil {
			return fmt.Errorf("cannot list blobs for download. Failed with error %s", err.Error())
		}

		// Process the blobs returned in this result segment (if the segment is empty, the loop body won't execute)
		for _, blobInfo := range listBlob.Segment.BlobItems {
			// If blob name doesn't match the pattern
			// This check supports the Use wild cards
			// SearchPrefix is used to list to all the blobs inside the destination
			// and pattern is used to identify which blob to compare further
			if !util.blobNameMatchesThePattern(pattern, blobInfo.Name) {
				continue
			}

			if !util.resourceShouldBeIncluded(parentSourcePath, e.Include, blobInfo.Name) {
				continue
			}

			if util.resourceShouldBeExcluded(parentSourcePath, e.Exclude, blobInfo.Name) {
				continue
			}
			// relativePathofBlobLocally is the local path relative to source at which blob should be downloaded
			// Example: cca.source ="C:\User1\user-1" cca.destination = "https://<container-name>/virtual-dir?<sig>" blob name = "virtual-dir/a.txt"
			// relativePathofBlobLocally = virtual-dir/a.txt
			// remove the virtual directory from the relativePathofBlobLocally
			blobRootPath, _ := util.sourceRootPathWithoutWildCards(blobUrlParts.BlobName)
			relativePathofBlobLocally := util.relativePathToRoot(blobRootPath, blobInfo.Name, '/')
			relativePathofBlobLocally = strings.Replace(relativePathofBlobLocally, virtualDirectory, "", 1)
			blobLocalPath := util.generateLocalPath(cca.destination, relativePathofBlobLocally)
			// Check if the blob exists locally or not
			_, err := os.Stat(blobLocalPath)
			if err == nil {
				// If the blob exists locally, then we don't need to compare the modified time
				// since it has already been compared in compareLocalAgainstRemote api
				continue
			}
			// if the blob doesn't exits locally, then we need to download blob.
			if err != nil && os.IsNotExist(err) {
				// download the blob
				err = e.addTransferToUpload(common.CopyTransfer{
					Source:           util.stripSASFromBlobUrl(util.generateBlobUrl(containerUrl, blobInfo.Name)).String(),
					Destination:      blobLocalPath,
					LastModifiedTime: blobInfo.Properties.LastModified,
					SourceSize:       *blobInfo.Properties.ContentLength,
				}, cca)
				if err != nil {
					return err
				}
			}
		}
		marker = listBlob.NextMarker
	}
	return nil
}

// compareLocalAgainstRemote iterates through each files/dir inside the source and compares
// them against blobs on container. If the blobs doesn't exists but exists locally, then delete
// the files locally
func (e *syncDownloadEnumerator) compareLocalAgainstRemote(cca *cookedSyncCmdArgs, p pipeline.Pipeline) (error, bool) {
	util := copyHandlerUtil{}

	ctx := context.WithValue(context.Background(), ste.ServiceAPIVersionOverride, ste.DefaultServiceApiVersion)
	// attempt to parse the destination url
	sourceUrl, err := url.Parse(cca.source)
	if err != nil {
		// the destination should have already been validated, it would be surprising if it cannot be parsed at this point
		panic(err)
	}
	// since source is a remote url, it will have sas parameter
	// since sas parameter will be stripped from the source url
	// while cooking the raw command arguments
	sourceUrl = util.appendQueryParamToUrl(sourceUrl, cca.sourceSAS)

	blobUrl := azblob.NewBlobURL(*sourceUrl, p)
	// Get the local file Info
	f, ferr := os.Stat(cca.destination)
	// Get the destination blob properties
	bProperties, berr := blobUrl.GetProperties(ctx, azblob.BlobAccessConditions{})
	// get the blob url parts
	blobUrlParts := azblob.NewBlobURLParts(*sourceUrl)
	// If the error occurs while fetching the fileInfo of the source
	// return the error
	if ferr != nil {
		return fmt.Errorf("cannot access the source %s. Failed with error %s", cca.destination, err.Error()), false
	}
	// If the destination is a file locally and source is not a blob
	// it means that it could be a virtual directory / container
	// sync cannot happen between a file and a virtual directory / container
	if !f.IsDir() && berr != nil {
		return fmt.Errorf("cannot perform sync since source is a file and destination "+
			"is not a blob. Listing blob failed with error %s", berr.Error()), false
	}
	// If the source is an existing blob and the destination is a directory
	// need to check if the blob exists in the destination or not
	if berr == nil && f.IsDir() {
		// Get the blob name without the any virtual directory as path in the blobName
		// for example: cca.source = https://<container-name>/a1/a2/f1.txt blobName = f1.txt
		bName := blobUrlParts.BlobName
		// Find the Index of the last Separator
		sepIndex := strings.LastIndex(bName, "/")
		// If there is no separator in the blobName
		// blobName is the complete blobName
		// for example: cca.source = https://<container-name>/f1.txt blobName = f1.txt
		// If there exists a path separator in the blobName
		// Get the lastIndex of Path Separator
		// bName with blob name content from the last path separator till the end.
		// for example: cca.source = https://<container-name>/a1/f1.txt blobName = a1/f1.txt bName = f1.txt
		if sepIndex != -1 {
			bName = bName[sepIndex+1:]
		}
		blobLocalPath := util.generateObjectPath(cca.destination, bName)
		blobLocalInfo, err := os.Stat(blobLocalPath)
		// If the blob does not exists locally ||
		// If it exists and the blobModified time is after modified time of blob existing locally
		// download is required.
		if (err != nil && os.IsNotExist(err)) ||
			(err == nil && bProperties.LastModified().After(blobLocalInfo.ModTime())) {
			e.addTransferToUpload(common.CopyTransfer{
				Source:           util.stripSASFromBlobUrl(*sourceUrl).String(),
				Destination:      blobLocalPath,
				LastModifiedTime: bProperties.LastModified(),
				SourceSize:       bProperties.ContentLength(),
			}, cca)
			return nil, true
		}
		return fmt.Errorf("cannot perform the sync since source %s "+
			"is a directory and destination %s are already in sync", cca.destination, sourceUrl.String()), true
	}
	// If the source is a file and destination is a blob
	// For Example: "cca.dstString = C:\User\user-1\a.txt" && "cca.source = https://<container-name>/vd-1/a.txt"
	if berr == nil && !f.IsDir() {
		// Get the blob name from the destination url
		// blobName refers to the last name of the blob with which it is stored as file locally
		// Example1: "cca.source = https://<container-name>/blob1?<sig>  blobName = blob1"
		// Example1: "cca.source = https://<container-name>/dir1/blob1?<sig>  blobName = blob1"
		blobName := sourceUrl.Path[strings.LastIndex(sourceUrl.Path, "/")+1:]
		// Compare the blob name and file name
		// blobName and filename should be same for sync to happen
		if strings.Compare(blobName, f.Name()) != 0 {
			return fmt.Errorf("sync cannot be done since blob %s and filename %s doesn't match", blobName, f.Name()), true
		}
		// If the modified time of file local is before than that of blob
		// sync needs to happen. The transfer is queued
		if f.ModTime().Before(bProperties.LastModified()) {
			e.addTransferToUpload(common.CopyTransfer{
				Source:           util.stripSASFromBlobUrl(*sourceUrl).String(),
				Destination:      cca.destination,
				SourceSize:       bProperties.ContentLength(),
				LastModifiedTime: bProperties.LastModified(),
			}, cca)
		}
		return nil, true
	}
	var sourcePattern = ""
	// get the root path without wildCards and get the source Pattern
	// For Example: source = <container-name>/a*/*/*
	// rootPath = <container-name> sourcePattern = a*/*/*
	blobUrlParts.BlobName, sourcePattern = util.sourceRootPathWithoutWildCards(blobUrlParts.BlobName)
	//sourcePattern = strings.Replace(sourcePattern, "/", string(os.PathSeparator), -1)
	// checkAndQueue is an internal function which check the modified time of file locally
	// and on container and then decideds whether to queue transfer for upload or not.
	checkAndQueue := func(root string, pathToFile string, f os.FileInfo) error {
		// localfileRelativePath is the path of file relative to root directory
		// Example1: root = C:\User\user1\dir-1  fileAbsolutePath = :\User\user1\dir-1\a.txt localfileRelativePath = \a.txt
		// Example2: root = C:\User\user1\dir-1  fileAbsolutePath = :\User\user1\dir-1\dir-2\a.txt localfileRelativePath = \dir-2\a.txt
		localfileRelativePath := strings.Replace(pathToFile, root, "", 1)
		// remove the path separator at the start of relative path
		if len(localfileRelativePath) > 0 && localfileRelativePath[0] == common.AZCOPY_PATH_SEPARATOR_CHAR {
			localfileRelativePath = localfileRelativePath[1:]
		}
		// if the localfileRelativePath does not match the source pattern, then it is not compared
		if !util.blobNameMatchesThePattern(sourcePattern, localfileRelativePath) {
			return nil
		}

		// Appending the fileRelativePath to the sourceUrl
		// root = C:\User\user1\dir-1  cca.source = https://<container-name>/<vir-d>?<sig>
		// fileAbsolutePath = C:\User\user1\dir-1\dir-2\a.txt localfileRelativePath = \dir-2\a.txt
		// filedestinationUrl =  https://<container-name>/<vir-d>/dir-2/a.txt?<sig>
		filedestinationUrl, _ := util.appendBlobNameToUrl(blobUrlParts, localfileRelativePath)
		// Get the properties of given on container
		blobUrl := azblob.NewBlobURL(filedestinationUrl, p)
		blobProperties, err := blobUrl.GetProperties(ctx, azblob.BlobAccessConditions{})

		if err != nil {
			if stError, ok := err.(azblob.StorageError); !ok || (ok && stError.Response().StatusCode != http.StatusNotFound) {
				return fmt.Errorf("error sync up the blob %s because it failed to get the properties. Failed with error %s", localfileRelativePath, err.Error())
			}
			// If the blobUrl.GetProperties failed with StatusNotFound, it means blob doesn't exists
			// delete the blob locally
			if stError, ok := err.(azblob.StorageError); !ok || (ok && stError.Response().StatusCode == http.StatusNotFound) {
				err := os.Remove(pathToFile)
				if err != nil {
					return fmt.Errorf("error deleting the file %s. Failed with error %s", pathToFile, err.Error())
				}
				e.FilesDeletedLocally++
				return nil
			}
			return err
		}
		// If the local file modified time was after the remote blob
		// then sync is  required
		if err == nil && !blobProperties.LastModified().After(f.ModTime()) {
			return nil
		}

		// File exists locally but the modified time of file locally was before the modified
		// time of blob, so sync is required
		err = e.addTransferToUpload(common.CopyTransfer{
			Source:           util.stripSASFromBlobUrl(filedestinationUrl).String(),
			Destination:      pathToFile,
			LastModifiedTime: blobProperties.LastModified(),
			SourceSize:       blobProperties.ContentLength(),
		}, cca)
		if err != nil {
			return err
		}
		return nil
	}

	listOfFilesAndDir, err := filepath.Glob(cca.destination)

	if err != nil {
		return fmt.Errorf("error listing the file name inside the source %s", cca.destination), false
	}

	// Get the destination path without the wildcards
	// This is defined since the files mentioned with exclude flag
	// & include flag are relative to the Destination
	// If the Destination has wildcards, then files are relative to the
	// parent Destination path which is the path of last directory in the Destination
	// without wildcards
	// For Example: dst = "/home/user/dir1" parentSourcePath = "/home/user/dir1"
	// For Example: dst = "/home/user/dir*" parentSourcePath = "/home/user"
	// For Example: dst = "/home/*" parentSourcePath = "/home"
	parentDestinationPath := cca.destination
	wcIndex := util.firstIndexOfWildCard(parentDestinationPath)
	if wcIndex != -1 {
		parentDestinationPath = parentDestinationPath[:wcIndex]
		pathSepIndex := strings.LastIndex(parentDestinationPath, common.AZCOPY_PATH_SEPARATOR_STRING)
		parentDestinationPath = parentDestinationPath[:pathSepIndex]
	}

	// Iterate through each file / dir inside the source
	// and then checkAndQueue
	for _, fileOrDir := range listOfFilesAndDir {
		f, err := os.Stat(fileOrDir)
		if err == nil {
			// directories are uploaded only if recursive is on
			if f.IsDir() {
				// walk goes through the entire directory tree
				err = filepath.Walk(fileOrDir, func(pathToFile string, f os.FileInfo, err error) error {
					if err != nil {
						return err
					}
					if f.IsDir() {
						return nil
					} else {
						// replace the OS path separator in pathToFile string with AZCOPY_PATH_SEPARATOR
						// this replacement is done to handle the windows file paths where path separator "\\"
						pathToFile = strings.Replace(pathToFile, common.OS_PATH_SEPARATOR, common.AZCOPY_PATH_SEPARATOR_STRING, -1)

						if !util.resourceShouldBeIncluded(parentDestinationPath, e.Include, pathToFile) {
							return nil
						}
						if util.resourceShouldBeExcluded(parentDestinationPath, e.Exclude, pathToFile) {
							return nil
						}
						return checkAndQueue(cca.destination, pathToFile, f)
					}
				})
			} else if !f.IsDir() {
				// replace the OS path separator in fileOrDir string with AZCOPY_PATH_SEPARATOR
				// this replacement is done to handle the windows file paths where path separator "\\"
				fileOrDir = strings.Replace(fileOrDir, common.OS_PATH_SEPARATOR, common.AZCOPY_PATH_SEPARATOR_STRING, -1)

				if !util.resourceShouldBeIncluded(parentDestinationPath, e.Include, fileOrDir) {
					continue
				}
				if util.resourceShouldBeExcluded(parentDestinationPath, e.Exclude, fileOrDir) {
					continue
				}
				err = checkAndQueue(cca.destination, fileOrDir, f)
			}
		}
	}
	return nil, false
}

// this function accepts the list of files/directories to transfer and processes them
func (e *syncDownloadEnumerator) enumerate(cca *cookedSyncCmdArgs) error {
	p := ste.NewBlobPipeline(azblob.NewAnonymousCredential(), azblob.PipelineOptions{
		Telemetry: azblob.TelemetryOptions{
			Value: common.UserAgent,
		},
	},
		ste.XferRetryOptions{
			Policy:        0,
			MaxTries:      ste.UploadMaxTries,
			TryTimeout:    ste.UploadTryTimeout,
			RetryDelay:    ste.UploadRetryDelay,
			MaxRetryDelay: ste.UploadMaxRetryDelay},
		nil)

	// Copying the JobId of sync job to individual copyJobRequest
	e.CopyJobRequest.JobID = e.JobID
	// Copying the FromTo of sync job to individual copyJobRequest
	e.CopyJobRequest.FromTo = e.FromTo

	// set the sas of user given Source
	e.CopyJobRequest.SourceSAS = e.SourceSAS

	// set the sas of user given destination
	e.CopyJobRequest.DestinationSAS = e.DestinationSAS

	// Set the preserve-last-modified-time to true in CopyJobRequest
	e.CopyJobRequest.BlobAttributes.PreserveLastModifiedTime = true

	// Copying the JobId of sync job to individual deleteJobRequest.
	e.DeleteJobRequest.JobID = e.JobID
	// FromTo of DeleteJobRequest will be BlobTrash.
	e.DeleteJobRequest.FromTo = common.EFromTo.BlobTrash()

	// set the sas of user given Source
	e.DeleteJobRequest.SourceSAS = e.SourceSAS

	// set the sas of user given destination
	e.DeleteJobRequest.DestinationSAS = e.DestinationSAS

	// set force wriet flag to true
	e.CopyJobRequest.ForceWrite = true

	//Initialize the number of transfer deleted locally to Zero
	e.FilesDeletedLocally = 0

	//Set the log level
	e.CopyJobRequest.LogLevel = e.LogLevel
	e.DeleteJobRequest.LogLevel = e.LogLevel

	// Copy the sync Command String to the CopyJobPartRequest and DeleteJobRequest
	e.CopyJobRequest.CommandString = e.CommandString
	e.DeleteJobRequest.CommandString = e.CommandString

	err, isSourceABlob := e.compareLocalAgainstRemote(cca, p)
	if err != nil {
		return err
	}
	// If the source provided is a blob, then remote doesn't needs to be compared against the local
	// since single blob already has been compared against the file
	if !isSourceABlob {
		err = e.compareRemoteAgainstLocal(cca, p)
		if err != nil {
			return err
		}
	}
	// No Job Part has been dispatched, then dispatch the JobPart.
	if e.PartNumber == 0 ||
		len(e.CopyJobRequest.Transfers) > 0 ||
		len(e.DeleteJobRequest.Transfers) > 0 {
		err = e.dispatchFinalPart()
		if err != nil {
			return err
		}
		glcm.WaitUntilJobCompletion(cca)
	}
	return nil
}
