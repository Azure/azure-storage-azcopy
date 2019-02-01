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
		// if the current part order sent to engine is 0, then set atomicSyncStatus
		// variable to 1
		if e.PartNumber == 0 {
			//cca.waitUntilJobCompletion(false)
			cca.setFirstPartOrdered()
		}
		e.CopyJobRequest.Transfers = []common.CopyTransfer{}
		e.PartNumber++
	}
	e.CopyJobRequest.Transfers = append(e.CopyJobRequest.Transfers, transfer)
	return nil
}

// addTransferToDelete adds the filePath to the list of files to delete locally.
func (e *syncDownloadEnumerator) addTransferToDelete(filePath string) {
	e.FilesToDeleteLocally = append(e.FilesToDeleteLocally, filePath)
}

// we need to send a last part with isFinalPart set to true, along with whatever transfers that still haven't been sent
func (e *syncDownloadEnumerator) dispatchFinalPart(cca *cookedSyncCmdArgs) error {
	numberOfCopyTransfers := len(e.CopyJobRequest.Transfers)
	numberOfDeleteTransfers := len(e.FilesToDeleteLocally)
	// If the numberoftransfer to copy / delete both are 0
	// means no transfer has been to queue to send to STE
	if numberOfCopyTransfers == 0 && numberOfDeleteTransfers == 0 {
		glcm.Exit("cannot start job because there are no transfer to upload or delete. "+
			"The source and destination are in sync", 0)
		return nil
	}
	if numberOfCopyTransfers > 0 {
		// Only CopyJobPart Order needs to be sent
		e.CopyJobRequest.IsFinalPart = true
		e.CopyJobRequest.PartNum = e.PartNumber
		var resp common.CopyJobPartOrderResponse
		Rpc(common.ERpcCmd.CopyJobPartOrder(), (*common.CopyJobPartOrderRequest)(&e.CopyJobRequest), &resp)
		if !resp.JobStarted {
			return fmt.Errorf("copy job part order with JobId %s and part number %d failed because %s", e.JobID, e.PartNumber, resp.ErrorMsg)
		}
		// If the JobPart sent was the first part, then set atomicSyncStatus to 1, so that progress reporting can start.
		if e.PartNumber == 0 {
			cca.setFirstPartOrdered()
		}
	}
	if numberOfDeleteTransfers > 0 {
		answer := ""
		if cca.nodelete {
			answer = "n"
		} else if cca.force {
			answer = "y"
		} else {
			answer = glcm.Prompt(fmt.Sprintf("Sync has enumerated %v files to delete locally. Do you want to delete these files ? Please confirm with y/n: ", numberOfDeleteTransfers))
		}
		// read a line from stdin, if the answer is not yes, then is No, then ignore the transfers queued for deletion and continue
		if !strings.EqualFold(answer, "y") {
			if numberOfCopyTransfers == 0 {
				glcm.Exit("cannot start job because there are no transfer to upload or delete. "+
					"The source and destination are in sync", 0)
			}
			cca.isEnumerationComplete = true
			return nil
		}
		for _, file := range e.FilesToDeleteLocally {
			err := os.Remove(file)
			if err != nil {
				glcm.Info(fmt.Sprintf("error %s deleting the file %s", err.Error(), file))
			}
		}
		if numberOfCopyTransfers == 0 {
			glcm.Exit(fmt.Sprintf("sync completed. Deleted %v files locally ", len(e.FilesToDeleteLocally)), 0)
		}
	}
	cca.isEnumerationComplete = true
	return nil
}

// listDestinationAndCompare lists the blob under the destination mentioned and verifies whether the blob
// exists locally or not by checking the expected localPath of blob in the sourceFiles map. If the blob
// does exists, it compares the last modified time. If it does not exists, it queues the blob for deletion.
func (e *syncDownloadEnumerator) listSourceAndCompare(cca *cookedSyncCmdArgs, p pipeline.Pipeline) error {
	util := copyHandlerUtil{}

	ctx := context.WithValue(context.Background(), ste.ServiceAPIVersionOverride, ste.DefaultServiceApiVersion)

	// rootPath is the path of destination without wildCards
	// For Example: cca.source = C:\a1\a* , so rootPath = C:\a1
	rootPath, _ := util.sourceRootPathWithoutWildCards(cca.destination)
	//replace the os path separator  with path separator "/" which is path separator for blobs
	//sourcePattern = strings.Replace(sourcePattern, string(os.PathSeparator), "/", -1)
	sourceURL, err := url.Parse(cca.source)
	if err != nil {
		return fmt.Errorf("error parsing the destinatio url")
	}

	// since source is a remote url, it will have sas parameter
	// since sas parameter will be stripped from the source url
	// while cooking the raw command arguments
	// source sas is added to url for listing the blobs.
	sourceURL = util.appendQueryParamToUrl(sourceURL, cca.sourceSAS)

	blobUrlParts := azblob.NewBlobURLParts(*sourceURL)
	blobURLPartsExtension := blobURLPartsExtension{blobUrlParts}

	containerUrl := util.getContainerUrl(blobUrlParts)
	searchPrefix, pattern, _ := blobURLPartsExtension.searchPrefixFromBlobURL()

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

	// Get the destination path without the wildcards
	// This is defined since the files mentioned with exclude flag
	// & include flag are relative to the Destination
	// If the Destination has wildcards, then files are relative to the
	// parent Destination path which is the path of last directory in the Destination
	// without wildcards
	// For Example: dst = "/home/user/dir1" parentSourcePath = "/home/user/dir1"
	// For Example: dst = "/home/user/dir*" parentSourcePath = "/home/user"
	// For Example: dst = "/home/*" parentSourcePath = "/home"
	parentSourcePath := blobUrlParts.BlobName
	wcIndex := util.firstIndexOfWildCard(parentSourcePath)
	if wcIndex != -1 {
		parentSourcePath = parentSourcePath[:wcIndex]
		pathSepIndex := strings.LastIndex(parentSourcePath, common.AZCOPY_PATH_SEPARATOR_STRING)
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
			// check if the listed blob segment does not matches the sourcePath pattern
			// if it does not comparison is not required
			if !util.matchBlobNameAgainstPattern(pattern, blobInfo.Name, cca.recursive) {
				continue
			}
			// realtivePathofBlobLocally is the local path relative to source at which blob should be downloaded
			// Example: cca.source ="C:\User1\user-1" cca.destination = "https://<container-name>/virtual-dir?<sig>" blob name = "virtual-dir/a.txt"
			// realtivePathofBlobLocally = virtual-dir/a.txt
			relativePathofBlobLocally := util.relativePathToRoot(parentSourcePath, blobInfo.Name, '/')
			relativePathofBlobLocally = strings.Replace(relativePathofBlobLocally, virtualDirectory, "", 1)

			blobLocalPath := util.generateLocalPath(cca.destination, relativePathofBlobLocally)

			// Increment the number of files scanned at the destination.
			atomic.AddUint64(&cca.atomicSourceFilesScanned, 1)

			// calculate the expected local path of the blob
			blobLocalPath = util.generateLocalPath(rootPath, relativePathofBlobLocally)

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
				if !blobInfo.Properties.LastModified.After(localFileTime) {
					delete(e.SourceFiles, blobLocalPath)
					continue
				}
			}
			e.addTransferToUpload(common.CopyTransfer{
				Source:           util.stripSASFromBlobUrl(util.generateBlobUrl(containerUrl, blobInfo.Name)).String(),
				Destination:      blobLocalPath,
				SourceSize:       *blobInfo.Properties.ContentLength,
				LastModifiedTime: blobInfo.Properties.LastModified,
			}, cca)

			delete(e.SourceFiles, blobLocalPath)
		}
		marker = listBlob.NextMarker
	}
	return nil
}

func (e *syncDownloadEnumerator) listTheDestinationIfRequired(cca *cookedSyncCmdArgs, p pipeline.Pipeline) (bool, error) {
	ctx := context.WithValue(context.Background(), ste.ServiceAPIVersionOverride, ste.DefaultServiceApiVersion)
	util := copyHandlerUtil{}

	// attempt to parse the destination url
	sourceURL, err := url.Parse(cca.source)
	// the destination should have already been validated, it would be surprising if it cannot be parsed at this point
	common.PanicIfErr(err)

	// since destination is a remote url, it will have sas parameter
	// since sas parameter will be stripped from the destination url
	// while cooking the raw command arguments
	// destination sas is added to url for listing the blobs.
	sourceURL = util.appendQueryParamToUrl(sourceURL, cca.sourceSAS)

	blobUrl := azblob.NewBlobURL(*sourceURL, p)

	// Get the files and directories for the given source pattern
	listOfFilesAndDir, lofaderr := filepath.Glob(cca.destination)
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
		blobName := sourceURL.Path[strings.LastIndex(sourceURL.Path, "/")+1:]
		// Compare the blob name and file name
		// blobName and filename should be same for sync to happen
		if strings.Compare(blobName, isSourceASingleFile.Name()) != 0 {
			glcm.Exit(fmt.Sprintf("sync cannot be done since blob %s and filename %s doesn't match", blobName, isSourceASingleFile.Name()), 1)
		}

		// If the modified time of file local is not later than that of blob
		// sync does not needs to happen.
		if isSourceASingleFile.ModTime().After(bProperties.LastModified()) {
			glcm.Exit(fmt.Sprintf("blob %s and file %s already in sync", blobName, isSourceASingleFile.Name()), 1)
		}

		e.addTransferToUpload(common.CopyTransfer{
			Source:           util.stripSASFromBlobUrl(*sourceURL).String(),
			Destination:      cca.source,
			SourceSize:       bProperties.ContentLength(),
			LastModifiedTime: bProperties.LastModified(),
		}, cca)
	}

	sourcePattern := ""
	// Parse the source URL into blob URL parts.
	blobUrlParts := azblob.NewBlobURLParts(*sourceURL)
	// get the root path without wildCards and get the source Pattern
	// For Example: source = <container-name>/a*/*/*
	// rootPath = <container-name> sourcePattern = a*/*/*
	blobUrlParts.BlobName, sourcePattern = util.sourceRootPathWithoutWildCards(blobUrlParts.BlobName)

	// Iterate through each file / dir inside the source
	// and then checkAndQueue
	for _, fileOrDir := range listOfFilesAndDir {
		f, err := os.Stat(fileOrDir)
		if err != nil {
			glcm.Info(fmt.Sprintf("cannot get the file info for %s. failed with error %s", fileOrDir, err.Error()))
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

					// localfileRelativePath is the path of file relative to root directory
					// Example1: root = C:\User\user1\dir-1  fileAbsolutePath = :\User\user1\dir-1\a.txt localfileRelativePath = \a.txt
					// Example2: root = C:\User\user1\dir-1  fileAbsolutePath = :\User\user1\dir-1\dir-2\a.txt localfileRelativePath = \dir-2\a.txt
					localfileRelativePath := strings.Replace(pathToFile, cca.destination, "", 1)
					// remove the path separator at the start of relative path
					if len(localfileRelativePath) > 0 && localfileRelativePath[0] == common.AZCOPY_PATH_SEPARATOR_CHAR {
						localfileRelativePath = localfileRelativePath[1:]
					}
					// if the localfileRelativePath does not match the source pattern, then it is not compared
					if !util.matchBlobNameAgainstPattern(sourcePattern, localfileRelativePath, cca.recursive) {
						return nil
					}

					if util.resourceShouldBeExcluded(cca.destination, e.Exclude, pathToFile) {
						e.SourceFilesToExclude[pathToFile] = fileInfo.ModTime()
						return nil
					}
					if len(e.SourceFiles) > MaxNumberOfFilesAllowedInSync {
						glcm.Exit(fmt.Sprintf("cannot sync the source %s with more than %v number of files", cca.source, MaxNumberOfFilesAllowedInSync), 1)
					}
					e.SourceFiles[pathToFile] = fileInfo.ModTime()
					// Increment the sync counter.
					atomic.AddUint64(&cca.atomicDestinationFilesScanned, 1)
				}
				return nil
			})
		} else if !f.IsDir() {
			// replace the OS path separator in fileOrDir string with AZCOPY_PATH_SEPARATOR
			// this replacement is done to handle the windows file paths where path separator "\\"
			fileOrDir = strings.Replace(fileOrDir, common.OS_PATH_SEPARATOR, common.AZCOPY_PATH_SEPARATOR_STRING, -1)

			// localfileRelativePath is the path of file relative to root directory
			// Example1: root = C:\User\user1\dir-1  fileAbsolutePath = :\User\user1\dir-1\a.txt localfileRelativePath = \a.txt
			// Example2: root = C:\User\user1\dir-1  fileAbsolutePath = :\User\user1\dir-1\dir-2\a.txt localfileRelativePath = \dir-2\a.txt
			localfileRelativePath := strings.Replace(fileOrDir, cca.destination, "", 1)
			// remove the path separator at the start of relative path
			if len(localfileRelativePath) > 0 && localfileRelativePath[0] == common.AZCOPY_PATH_SEPARATOR_CHAR {
				localfileRelativePath = localfileRelativePath[1:]
			}
			// if the localfileRelativePath does not match the source pattern, then it is not compared
			if !util.matchBlobNameAgainstPattern(sourcePattern, localfileRelativePath, cca.recursive) {
				continue
			}

			if util.resourceShouldBeExcluded(cca.destination, e.Exclude, fileOrDir) {
				e.SourceFilesToExclude[fileOrDir] = f.ModTime()
				continue
			}

			if len(e.SourceFiles) > MaxNumberOfFilesAllowedInSync {
				glcm.Exit(fmt.Sprintf("cannot sync the source %s with more than %v number of files", cca.source, MaxNumberOfFilesAllowedInSync), 1)
			}
			e.SourceFiles[fileOrDir] = f.ModTime()
			// Increment the sync counter.
			atomic.AddUint64(&cca.atomicDestinationFilesScanned, 1)
		}
	}
	return false, nil
}

// queueSourceFilesForUpload
func (e *syncDownloadEnumerator) queueSourceFilesForUpload(cca *cookedSyncCmdArgs) {
	for file, _ := range e.SourceFiles {
		e.addTransferToDelete(file)
	}
}

// this function accepts the list of files/directories to transfer and processes them
func (e *syncDownloadEnumerator) enumerate(cca *cookedSyncCmdArgs) error {
	ctx := context.WithValue(context.TODO(), ste.ServiceAPIVersionOverride, ste.DefaultServiceApiVersion)

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

	//Set the log level
	e.CopyJobRequest.LogLevel = e.LogLevel
	e.DeleteJobRequest.LogLevel = e.LogLevel

	// Copy the sync Command String to the CopyJobPartRequest and DeleteJobRequest
	e.CopyJobRequest.CommandString = e.CommandString
	e.DeleteJobRequest.CommandString = e.CommandString

	// Set credential info properly
	e.CopyJobRequest.CredentialInfo = e.CredentialInfo
	e.DeleteJobRequest.CredentialInfo = e.CredentialInfo

	e.SourceFiles = make(map[string]time.Time)

	e.SourceFilesToExclude = make(map[string]time.Time)

	cca.waitUntilJobCompletion(false)

	isSourceABlob, err := e.listTheDestinationIfRequired(cca, p)
	if err != nil {
		return err
	}

	// If the source provided is a blob, then remote doesn't needs to be compared against the local
	// since single blob already has been compared against the file
	if !isSourceABlob {
		err = e.listSourceAndCompare(cca, p)
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
		cca.setFirstPartOrdered()
	}
	// scanning all the destination and is complete
	cca.setScanningComplete()
	return nil
}
