package cmd

import (
	"context"
	"fmt"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"time"

	"github.com/Azure/azure-pipeline-go/pipeline"
	"github.com/Azure/azure-storage-azcopy/common"
	"github.com/Azure/azure-storage-azcopy/ste"
	"github.com/Azure/azure-storage-blob-go/azblob"
)

type syncUploadEnumerator common.SyncJobPartOrderRequest

// accepts a new transfer which is to delete the blob on container.
func (e *syncUploadEnumerator) addTransferToDelete(transfer common.CopyTransfer, cca *cookedSyncCmdArgs) error {
	e.DeleteJobRequest.Transfers = append(e.DeleteJobRequest.Transfers, transfer)
	return nil
}

// accept a new transfer, if the threshold is reached, dispatch a job part order
func (e *syncUploadEnumerator) addTransferToUpload(transfer common.CopyTransfer, cca *cookedSyncCmdArgs) error {

	if len(e.CopyJobRequest.Transfers) == NumOfFilesPerDispatchJobPart {
		resp := common.CopyJobPartOrderResponse{}
		e.CopyJobRequest.PartNum = e.PartNumber
		Rpc(common.ERpcCmd.CopyJobPartOrder(), (*common.CopyJobPartOrderRequest)(&e.CopyJobRequest), &resp)

		if !resp.JobStarted {
			return fmt.Errorf("copy job part order with JobId %s and part number %d failed because %s", e.JobID, e.PartNumber, resp.ErrorMsg)
		}
		// if the current part order sent to engine is 0, then start fetching the Job Progress summary.
		if e.PartNumber == 0 {
			//update this atomic counter which is monitored by another go routine
			//reporting numbers to the user
			cca.setFirstPartOrdered()
		}
		e.CopyJobRequest.Transfers = []common.CopyTransfer{}
		e.PartNumber++
	}
	e.CopyJobRequest.Transfers = append(e.CopyJobRequest.Transfers, transfer)
	return nil
}

// we need to send a last part with isFinalPart set to true, along with whatever transfers that still haven't been sent
func (e *syncUploadEnumerator) dispatchFinalPart(cca *cookedSyncCmdArgs) error {
	numberOfCopyTransfers := uint64(len(e.CopyJobRequest.Transfers))
	numberOfDeleteTransfers := uint64(len(e.DeleteJobRequest.Transfers))
	if numberOfCopyTransfers == 0 && numberOfDeleteTransfers == 0 {
		glcm.Exit("cannot start job because there are no files to upload or delete. "+
			"The source and destination are in sync", 0)
		return nil
	}
	// sendDeleteTransfers is an internal function which creates JobPartRequest for all the delete transfers enumerated.
	// It creates requests for group of 10000 transfers.
	sendDeleteTransfers := func() error {
		currentCount := uint64(0)
		// If the user agrees to delete the transfers, then break the entire deleteJobRequest into parts of 10000 size and send them
		for numberOfDeleteTransfers > 0 {
			// number of transfers in the current request can be either 10,000 or less than that.
			numberOfTransfers := common.Iffuint64(numberOfDeleteTransfers > NumOfFilesPerDispatchJobPart, NumOfFilesPerDispatchJobPart, numberOfDeleteTransfers)
			// create a copy of DeleteJobRequest
			deleteJobRequest := e.DeleteJobRequest
			// Reset the transfer list in the copy of DeleteJobRequest
			deleteJobRequest.Transfers = []common.CopyTransfer{}
			// Copy the transfers from currentCount till number of transfer calculated for current iteration
			deleteJobRequest.Transfers = e.DeleteJobRequest.Transfers[currentCount : currentCount+numberOfTransfers]
			// Set the part number
			deleteJobRequest.PartNum = e.PartNumber
			// Increment the part number
			e.PartNumber++
			// Increment the current count
			currentCount += numberOfTransfers
			// Decrease the numberOfDeleteTransfer by the number Of transfers calculated for the current iteration
			numberOfDeleteTransfers -= numberOfTransfers
			// If the number of delete transfer is 0, it means it is the last part that needs to be sent.
			// Set the IsFinalPart for the current request to true
			if numberOfDeleteTransfers == 0 {
				deleteJobRequest.IsFinalPart = true
			}
			var resp common.CopyJobPartOrderResponse
			Rpc(common.ERpcCmd.CopyJobPartOrder(), (*common.CopyJobPartOrderRequest)(&deleteJobRequest), &resp)
			if !resp.JobStarted {
				return fmt.Errorf("copy job part order with JobId %s and part number %d failed because %s", e.JobID, e.PartNumber, resp.ErrorMsg)
			}
			// If the part sent above was the first, then set setFirstPartOrdered, so that progress can be fetched.
			if deleteJobRequest.PartNum == 0 {
				cca.setFirstPartOrdered()
			}
		}
		return nil
	}
	if numberOfCopyTransfers > 0 && numberOfDeleteTransfers > 0 {
		var resp common.CopyJobPartOrderResponse
		e.CopyJobRequest.PartNum = e.PartNumber
		answer := ""
		if cca.nodelete {
			answer = "n"
		} else if cca.force {
			answer = "y"
		} else {
			answer = glcm.Prompt(fmt.Sprintf("Sync has enumerated %v files to delete from destination. Do you want to delete these files ? Please confirm with y/n: ", numberOfDeleteTransfers))
		}
		// read a line from stdin, if the answer is not yes, then is No, then ignore the transfers queued for deletion and continue
		if !strings.EqualFold(answer, "y") {
			e.CopyJobRequest.IsFinalPart = true
			Rpc(common.ERpcCmd.CopyJobPartOrder(), (*common.CopyJobPartOrderRequest)(&e.CopyJobRequest), &resp)
			if !resp.JobStarted {
				return fmt.Errorf("copy job part order with JobId %s and part number %d failed because %s", e.JobID, e.PartNumber, resp.ErrorMsg)
			}
			return nil
		}
		Rpc(common.ERpcCmd.CopyJobPartOrder(), (*common.CopyJobPartOrderRequest)(&e.CopyJobRequest), &resp)
		if !resp.JobStarted {
			return fmt.Errorf("copy job part order with JobId %s and part number %d failed because %s", e.JobID, e.PartNumber, resp.ErrorMsg)
		}
		// If the part sent above was the first, then setFirstPartOrdered, so that progress can be fetched.
		if e.PartNumber == 0 {
			cca.setFirstPartOrdered()
		}
		e.PartNumber++
		err := sendDeleteTransfers()
		cca.isEnumerationComplete = true
		return err
	} else if numberOfCopyTransfers > 0 {
		e.CopyJobRequest.IsFinalPart = true
		e.CopyJobRequest.PartNum = e.PartNumber
		var resp common.CopyJobPartOrderResponse
		Rpc(common.ERpcCmd.CopyJobPartOrder(), (*common.CopyJobPartOrderRequest)(&e.CopyJobRequest), &resp)
		if !resp.JobStarted {
			return fmt.Errorf("copy job part order with JobId %s and part number %d failed because %s", e.JobID, e.PartNumber, resp.ErrorMsg)
		}
		cca.isEnumerationComplete = true
		return nil
	}
	answer := ""
	// If the user set the force flag to true, then prompt is not required and file will be deleted.
	if cca.nodelete {
		answer = "n"
	} else if cca.force {
		answer = "y"
	} else {
		answer = glcm.Prompt(fmt.Sprintf("Sync has enumerated %v files to delete from destination. Do you want to delete these files ? Please confirm with y/n: ", numberOfDeleteTransfers))
	}
	// read a line from stdin, if the answer is not yes, then is No, then ignore the transfers queued for deletion and continue
	if !strings.EqualFold(answer, "y") {
		return fmt.Errorf("cannot start job because there are no transfer to upload or delete. " +
			"The source and destination are in sync")
	}
	error := sendDeleteTransfers()
	cca.isEnumerationComplete = true
	return error
}

func (e *syncUploadEnumerator) listTheSourceIfRequired(cca *cookedSyncCmdArgs, p pipeline.Pipeline) (bool, error) {
	ctx := context.WithValue(context.Background(), ste.ServiceAPIVersionOverride, ste.DefaultServiceApiVersion)
	util := copyHandlerUtil{}

	// attempt to parse the destination url
	destinationUrl, err := url.Parse(cca.destination)
	// the destination should have already been validated, it would be surprising if it cannot be parsed at this point
	common.PanicIfErr(err)

	// since destination is a remote url, it will have sas parameter
	// since sas parameter will be stripped from the destination url
	// while cooking the raw command arguments
	// destination sas is added to url for listing the blobs.
	destinationUrl = util.appendQueryParamToUrl(destinationUrl, cca.destinationSAS)

	blobUrl := azblob.NewBlobURL(*destinationUrl, p)

	// Get the files and directories for the given source pattern
	listOfFilesAndDir, lofaderr := filepath.Glob(cca.source)
	if lofaderr != nil {
		return false, fmt.Errorf("error getting the files and directories for source pattern %s", cca.source)
	}

	// Get the blob Properties
	bProperties, bPropertiesError := blobUrl.GetProperties(ctx, azblob.BlobAccessConditions{})

	// isSourceASingleFile is used to determine whether given source pattern represents single file or not
	// If the source is a single file, this pointer will not be nil
	// if it is nil, it means the source is a directory or list of file
	var isSourceASingleFile os.FileInfo = nil

	if len(listOfFilesAndDir) == 0 {
		fInfo, fError := os.Stat(listOfFilesAndDir[0])
		if fError != nil {
			return false, fmt.Errorf("cannot get the information of the %s. Failed with error %s", listOfFilesAndDir[0], fError)
		}
		if fInfo.Mode().IsRegular() {
			isSourceASingleFile = fInfo
		}
	}

	// sync only happens between the source and destination of same type i.e between blob and blob or between Directory and Virtual Folder / Container
	// If the source is a file and destination is not a blob, sync fails.
	if isSourceASingleFile != nil && bPropertiesError != nil {
		glcm.Exit(fmt.Sprintf("Cannot perform sync between file %s and non blob destination %s. sync only happens between source and destination of same type", cca.source, cca.destination), 1)
	}
	// If the source is a directory and destination is a blob
	if isSourceASingleFile == nil && bPropertiesError == nil {
		glcm.Exit(fmt.Sprintf("Cannot perform sync between directory %s and blob destination %s. sync only happens between source and destination of same type", cca.source, cca.destination), 1)
	}

	// If both source is a file and destination is a blob, then we need to do the comparison and queue the transfer if required.
	if isSourceASingleFile != nil && bPropertiesError == nil {
		blobName := destinationUrl.Path[strings.LastIndex(destinationUrl.Path, "/")+1:]
		// Compare the blob name and file name
		// blobName and filename should be same for sync to happen
		if strings.Compare(blobName, isSourceASingleFile.Name()) != 0 {
			glcm.Exit(fmt.Sprintf("sync cannot be done since blob %s and filename %s doesn't match", blobName, isSourceASingleFile.Name()), 1)
		}

		// If the modified time of file local is not later than that of blob
		// sync does not needs to happen.
		if !isSourceASingleFile.ModTime().After(bProperties.LastModified()) {
			glcm.Exit(fmt.Sprintf("blob %s and file %s already in sync", blobName, isSourceASingleFile.Name()), 1)
		}

		e.addTransferToUpload(common.CopyTransfer{
			Source:           cca.source,
			Destination:      util.stripSASFromBlobUrl(*destinationUrl).String(),
			SourceSize:       isSourceASingleFile.Size(),
			LastModifiedTime: isSourceASingleFile.ModTime(),
		}, cca)
		return true, nil
	}

	if len(listOfFilesAndDir) == 1 && !cca.recursive {
		glcm.Exit(fmt.Sprintf("error performing between source %s and destination %s is a directory. recursive flag is turned off.", cca.source, cca.destination), 1)
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
	parentSourcePath := cca.source
	wcIndex := util.firstIndexOfWildCard(parentSourcePath)
	if wcIndex != -1 {
		parentSourcePath = parentSourcePath[:wcIndex]
		pathSepIndex := strings.LastIndex(parentSourcePath, common.AZCOPY_PATH_SEPARATOR_STRING)
		parentSourcePath = parentSourcePath[:pathSepIndex]
	}

	// Iterate through each file / dir inside the source
	// and then checkAndQueue
	for _, fileOrDir := range listOfFilesAndDir {
		f, err := os.Stat(fileOrDir)
		if err != nil {
			glcm.Info(fmt.Sprintf("cannot get the file Info for %s. failed with error %s", fileOrDir, err.Error()))
		}
		// directories are uploaded only if recursive is on
		if f.IsDir() && cca.recursive {
			// walk goes through the entire directory tree
			err = filepath.Walk(fileOrDir, func(pathToFile string, fileInfo os.FileInfo, err error) error {
				if err != nil {
					return err
				}
				if fileInfo.IsDir() {
					return nil
				} else {
					// replace the OS path separator in pathToFile string with AZCOPY_PATH_SEPARATOR
					// this replacement is done to handle the windows file paths where path separator "\\"
					pathToFile = strings.Replace(pathToFile, common.OS_PATH_SEPARATOR, common.AZCOPY_PATH_SEPARATOR_STRING, -1)

					if util.resourceShouldBeExcluded(parentSourcePath, e.Exclude, pathToFile) {
						e.SourceFilesToExclude[pathToFile] = f.ModTime()
						return nil
					}
					if len(e.SourceFiles) > MaxNumberOfFilesAllowedInSync {
						glcm.Exit(fmt.Sprintf("cannot sync the source %s with more than %v number of files", cca.source, MaxNumberOfFilesAllowedInSync), 1)
					}
					e.SourceFiles[pathToFile] = fileInfo.ModTime()
					// Increment the sync counter.
					atomic.AddUint64(&cca.atomicSourceFilesScanned, 1)
				}
				return nil
			})
		} else if !f.IsDir() {
			// replace the OS path separator in fileOrDir string with AZCOPY_PATH_SEPARATOR
			// this replacement is done to handle the windows file paths where path separator "\\"
			fileOrDir = strings.Replace(fileOrDir, common.OS_PATH_SEPARATOR, common.AZCOPY_PATH_SEPARATOR_STRING, -1)

			if util.resourceShouldBeExcluded(parentSourcePath, e.Exclude, fileOrDir) {
				e.SourceFilesToExclude[fileOrDir] = f.ModTime()
				continue
			}
			if len(e.SourceFiles) > MaxNumberOfFilesAllowedInSync {
				glcm.Exit(fmt.Sprintf("cannot sync the source %s with more than %v number of files", cca.source, MaxNumberOfFilesAllowedInSync), 1)
			}
			e.SourceFiles[fileOrDir] = f.ModTime()
			// Increment the sync counter.
			atomic.AddUint64(&cca.atomicSourceFilesScanned, 1)
		}
	}
	return false, nil
}

// listDestinationAndCompare lists the blob under the destination mentioned and verifies whether the blob
// exists locally or not by checking the expected localPath of blob in the sourceFiles map. If the blob
// does exists, it compares the last modified time. If it does not exists, it queues the blob for deletion.
func (e *syncUploadEnumerator) listDestinationAndCompare(cca *cookedSyncCmdArgs, p pipeline.Pipeline) error {
	util := copyHandlerUtil{}

	ctx := context.WithValue(context.Background(), ste.ServiceAPIVersionOverride, ste.DefaultServiceApiVersion)

	// rootPath is the path of source without wildCards
	// sourcePattern is the filePath pattern inside the source
	// For Example: cca.source = C:\a1\a* , so rootPath = C:\a1 and filePattern is a*
	// This is to avoid enumerator to compare any file inside the destination directory
	// that doesn't match the pattern
	// For Example: cca.source = C:\a1\a* des = https://<container-name>?<sig>
	// Only files that follow pattern a* will be compared
	rootPath, sourcePattern := util.sourceRootPathWithoutWildCards(cca.source)
	//replace the os path separator  with path separator "/" which is path separator for blobs
	//sourcePattern = strings.Replace(sourcePattern, string(os.PathSeparator), "/", -1)
	destinationUrl, err := url.Parse(cca.destination)
	if err != nil {
		return fmt.Errorf("error parsing the destinatio url")
	}

	// since destination is a remote url, it will have sas parameter
	// since sas parameter will be stripped from the destination url
	// while cooking the raw command arguments
	// destination sas is added to url for listing the blobs.
	destinationUrl = util.appendQueryParamToUrl(destinationUrl, cca.destinationSAS)

	blobUrlParts := azblob.NewBlobURLParts(*destinationUrl)
	blobURLPartsExtension := blobURLPartsExtension{blobUrlParts}

	containerUrl := util.getContainerUrl(blobUrlParts)
	searchPrefix, _, _ := blobURLPartsExtension.searchPrefixFromBlobURL()

	containerBlobUrl := azblob.NewContainerURL(containerUrl, p)

	// Get the destination path without the wildcards
	// This is defined since the files mentioned with exclude flag
	// & include flag are relative to the Destination
	// If the Destination has wildcards, then files are relative to the
	// parent Destination path which is the path of last directory in the Destination
	// without wildcards
	// For Example: dst = "/home/user/dir1" parentSourcePath = "/home/user/dir1"
	// For Example: dst = "/home/user/dir*" parentSourcePath = "/home/user"
	// For Example: dst = "/home/*" parentSourcePath = "/home"
	parentDestinationPath := blobUrlParts.BlobName
	wcIndex := util.firstIndexOfWildCard(parentDestinationPath)
	if wcIndex != -1 {
		parentDestinationPath = parentDestinationPath[:wcIndex]
		pathSepIndex := strings.LastIndex(parentDestinationPath, common.AZCOPY_PATH_SEPARATOR_STRING)
		parentDestinationPath = parentDestinationPath[:pathSepIndex]
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
			// realtivePathofBlobLocally is the local path relative to source at which blob should be downloaded
			// Example: cca.source ="C:\User1\user-1" cca.destination = "https://<container-name>/virtual-dir?<sig>" blob name = "virtual-dir/a.txt"
			// realtivePathofBlobLocally = virtual-dir/a.txt
			realtivePathofBlobLocally := util.relativePathToRoot(searchPrefix, blobInfo.Name, '/')

			// check if the listed blob segment matches the sourcePath pattern
			// if it does not comparison is not required
			if !util.matchBlobNameAgainstPattern(sourcePattern, realtivePathofBlobLocally, cca.recursive) {
				continue
			}

			// Increment the number of files scanned at the destination.
			atomic.AddUint64(&cca.atomicDestinationFilesScanned, 1)

			// calculate the expected local path of the blob
			blobLocalPath := util.generateLocalPath(rootPath, realtivePathofBlobLocally)

			// If the files is found in the list of files to be excluded, then it is not compared
			_, found := e.SourceFilesToExclude[blobLocalPath]
			if found {
				continue
			}
			// Check if the blob exists in the map of source Files. If the file is
			// found, compare the modified time of the file against the blob's last
			// modified time. If the modified time of file is later than the blob's
			// modified time, then queue transfer for upload. If not, then delete
			// blobLocalPath from the map of sourceFiles.
			localFileTime, found := e.SourceFiles[blobLocalPath]
			if found {
				if localFileTime.After(blobInfo.Properties.LastModified) {
					e.addTransferToUpload(common.CopyTransfer{
						Source:      blobLocalPath,
						Destination: util.stripSASFromBlobUrl(util.generateBlobUrl(containerUrl, blobInfo.Name)).String(),
						SourceSize:  *blobInfo.Properties.ContentLength,
					}, cca)
				}
				delete(e.SourceFiles, blobLocalPath)
			} else {
				// If the blob is not found in the map of source Files, queue it for
				// delete
				e.addTransferToDelete(common.CopyTransfer{
					Source:      util.stripSASFromBlobUrl(util.generateBlobUrl(containerUrl, blobInfo.Name)).String(),
					Destination: "", // no destination in case of Delete JobPartOrder
					SourceSize:  *blobInfo.Properties.ContentLength,
				}, cca)
			}
		}
		marker = listBlob.NextMarker
	}
	return nil
}

// queueSourceFilesForUpload
func (e *syncUploadEnumerator) queueSourceFilesForUpload(cca *cookedSyncCmdArgs) {
	util := copyHandlerUtil{}
	// rootPath will be the parent source directory before the first wildcard
	// For Example: cca.source = C:\a\b* rootPath = C:\a
	// For Example: cca.source = C:\*\a* rootPath = c:\
	// In case of no wildCard, rootPath is equal to the source directory
	// This rootPath is effective when wildCards are provided
	// Using this rootPath, path of file on blob is calculated
	rootPath, _ := util.sourceRootPathWithoutWildCards(cca.source)

	// attempt to parse the destination url
	destinationUrl, err := url.Parse(cca.destination)
	// the destination should have already been validated, it would be surprising if it cannot be parsed at this point
	common.PanicIfErr(err)

	// since destination is a remote url, it will have sas parameter
	// since sas parameter will be stripped from the destination url
	// while cooking the raw command arguments
	// destination sas is added to url for listing the blobs.
	destinationUrl = util.appendQueryParamToUrl(destinationUrl, cca.destinationSAS)

	blobUrlParts := azblob.NewBlobURLParts(*destinationUrl)

	for file, _ := range e.SourceFiles {
		// get the file Info
		f, err := os.Stat(file)
		if err != nil {
			glcm.Info(fmt.Sprintf("Error %s getting the file info for file %s", err.Error(), file))
			continue
		}
		// localfileRelativePath is the path of file relative to root directory
		// Example1: rootPath = C:\User\user1\dir-1  fileAbsolutePath = C:\User\user1\dir-1\a.txt localfileRelativePath = \a.txt
		// Example2: rootPath = C:\User\user1\dir-1  fileAbsolutePath = C:\User\user1\dir-1\dir-2\a.txt localfileRelativePath = \dir-2\a.txt
		localfileRelativePath := strings.Replace(file, rootPath, "", 1)
		// remove the path separator at the start of relative path
		if len(localfileRelativePath) > 0 && localfileRelativePath[0] == common.AZCOPY_PATH_SEPARATOR_CHAR {
			localfileRelativePath = localfileRelativePath[1:]
		}
		// Appending the fileRelativePath to the destinationUrl
		// root = C:\User\user1\dir-1  cca.destination = https://<container-name>/<vir-d>?<sig>
		// fileAbsolutePath = C:\User\user1\dir-1\dir-2\a.txt localfileRelativePath = \dir-2\a.txt
		// filedestinationUrl =  https://<container-name>/<vir-d>/dir-2/a.txt?<sig>
		filedestinationUrl, _ := util.appendBlobNameToUrl(blobUrlParts, localfileRelativePath)

		err = e.addTransferToUpload(common.CopyTransfer{
			Source:           file,
			Destination:      util.stripSASFromBlobUrl(filedestinationUrl).String(),
			LastModifiedTime: f.ModTime(),
			SourceSize:       f.Size(),
		}, cca)
		if err != nil {
			glcm.Info(fmt.Sprintf("Error %s uploading transfer source :%s and destination %s", err.Error(), file, util.stripSASFromBlobUrl(filedestinationUrl).String()))
		}

	}
}

// this function accepts the list of files/directories to transfer and processes them
func (e *syncUploadEnumerator) enumerate(cca *cookedSyncCmdArgs) error {
	ctx := context.WithValue(context.TODO(), ste.ServiceAPIVersionOverride, ste.DefaultServiceApiVersion)

	// Create the new azblob pipeline
	p, err := createBlobPipeline(ctx, e.CredentialInfo)
	if err != nil {
		return err
	}

	// Copying the JobId of sync job to individual copyJobRequest
	e.CopyJobRequest.JobID = e.JobID
	// Copying the FromTo of sync job to individual copyJobRequest
	e.CopyJobRequest.FromTo = e.FromTo

	// set the sas of user given Source
	e.CopyJobRequest.SourceSAS = e.SourceSAS

	// set the sas of user given destination
	e.CopyJobRequest.DestinationSAS = e.DestinationSAS

	// Copying the JobId of sync job to individual deleteJobRequest.
	e.DeleteJobRequest.JobID = e.JobID
	// FromTo of DeleteJobRequest will be BlobTrash.
	e.DeleteJobRequest.FromTo = common.EFromTo.BlobTrash()

	// For delete the source is the destination in case of sync upload
	// For Example: source = /home/user destination = https://container/vd-1?<sig>
	// For deleting the blobs, Source in Delete Job Source will be the blob url
	// and source sas is the destination sas which is url sas.
	// set the destination sas as the source sas
	e.DeleteJobRequest.SourceSAS = e.DestinationSAS

	// Set the Log Level
	e.CopyJobRequest.LogLevel = e.LogLevel
	e.DeleteJobRequest.LogLevel = e.LogLevel

	// Set the force flag to true
	e.CopyJobRequest.ForceWrite = true

	// Copy the sync Command String to the CopyJobPartRequest and DeleteJobRequest
	e.CopyJobRequest.CommandString = e.CommandString
	e.DeleteJobRequest.CommandString = e.CommandString

	// Set credential info properly
	e.CopyJobRequest.CredentialInfo = e.CredentialInfo
	e.DeleteJobRequest.CredentialInfo = e.CredentialInfo

	e.SourceFiles = make(map[string]time.Time)

	e.SourceFilesToExclude = make(map[string]time.Time)

	cca.waitUntilJobCompletion(false)

	// list the source files and store in the map.
	// While listing the source files, it applies the exclude filter
	// and stores them into a separate map "sourceFilesToExclude"
	isSourceAFile, err := e.listTheSourceIfRequired(cca, p)
	if err != nil {
		return err
	}
	// isSourceAFile defines whether source is a file or not.
	// If source is a file and destination is a blob, then destination doesn't needs to be compared against local.
	if !isSourceAFile {
		err = e.listDestinationAndCompare(cca, p)
		if err != nil {
			return err
		}
	}
	e.queueSourceFilesForUpload(cca)

	// No Job Part has been dispatched, then dispatch the JobPart.
	if e.PartNumber == 0 ||
		len(e.CopyJobRequest.Transfers) > 0 ||
		len(e.DeleteJobRequest.Transfers) > 0 {
		err = e.dispatchFinalPart(cca)
		if err != nil {
			return err
		}
		//cca.waitUntilJobCompletion(true)
		cca.setFirstPartOrdered()
	}
	cca.setScanningComplete()
	return nil
}
