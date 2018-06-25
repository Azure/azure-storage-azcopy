package cmd

import (
	"context"
	"fmt"
	"net/http"
	"net/url"
	"os"
	"strings"
	"github.com/Azure/azure-pipeline-go/pipeline"
	"github.com/Azure/azure-storage-azcopy/common"
	"github.com/Azure/azure-storage-blob-go/2017-07-29/azblob"
	"path/filepath"
	"github.com/Azure/azure-storage-azcopy/ste"
)

type syncUploadEnumerator common.SyncJobPartOrderRequest

// accepts a new transfer which is to delete the blob on container.
func (e *syncUploadEnumerator) addTransferToDelete(transfer common.CopyTransfer, cca *cookedSyncCmdArgs) error {
	// If the existing transfers in DeleteJobRequest is equal to NumOfFilesPerDispatchJobPart,
	// then send the JobPartOrder to transfer engine.
	if len(e.DeleteJobRequest.Transfers) == NumOfFilesPerDispatchJobPart {
		resp := common.CopyJobPartOrderResponse{}
		e.DeleteJobRequest.PartNum = e.PartNumber
		Rpc(common.ERpcCmd.CopyJobPartOrder(), (*common.CopyJobPartOrderRequest)(&e.DeleteJobRequest), &resp)

		if !resp.JobStarted {
			return fmt.Errorf("copy job part order with JobId %s and part number %d failed because %s", e.JobID, e.PartNumber, resp.ErrorMsg)
		}
		// if the current part order sent to engine is 0, then start fetching the Job Progress summary.
		if e.PartNumber == 0 {
			go glcm.WaitUntilJobCompletion(cca)
		}
		e.DeleteJobRequest.Transfers = []common.CopyTransfer{}
		e.PartNumber++
	}
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
			go glcm.WaitUntilJobCompletion(cca)
		}
		e.CopyJobRequest.Transfers = []common.CopyTransfer{}
		e.PartNumber++
	}
	e.CopyJobRequest.Transfers = append(e.CopyJobRequest.Transfers, transfer)
	return nil
}

// we need to send a last part with isFinalPart set to true, along with whatever transfers that still haven't been sent
func (e *syncUploadEnumerator) dispatchFinalPart() error {
	numberOfCopyTransfers := len(e.CopyJobRequest.Transfers)
	numberOfDeleteTransfers := len(e.DeleteJobRequest.Transfers)
	if numberOfCopyTransfers == 0 && numberOfDeleteTransfers == 0 {
		return fmt.Errorf("cannot start job because there are no transfer to upload or delete. " +
			"The source and destination are in sync")
	} else if numberOfCopyTransfers > 0 && numberOfDeleteTransfers > 0 {
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
		e.CopyJobRequest.IsFinalPart = true
		e.CopyJobRequest.PartNum = e.PartNumber
		var resp common.CopyJobPartOrderResponse
		Rpc(common.ERpcCmd.CopyJobPartOrder(), (*common.CopyJobPartOrderRequest)(&e.CopyJobRequest), &resp)
		if !resp.JobStarted {
			return fmt.Errorf("copy job part order with JobId %s and part number %d failed because %s", e.JobID, e.PartNumber, resp.ErrorMsg)
		}
	} else {
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
// blobs are deleted.
func (e *syncUploadEnumerator) compareRemoteAgainstLocal(cca *cookedSyncCmdArgs, p pipeline.Pipeline) error {
	util := copyHandlerUtil{}

	ctx := context.WithValue(context.Background(), ste.ServiceAPIVersionOverride, ste.DefaultServiceApiVersion)

	// rootPath is the path of source without wildCards
	// sourcePattern is the filePath pattern inside the source
	// For Example: cca.src = C:\a1\a* , so rootPath = C:\a1 and filePattern is a*
	// This is to avoid enumerator to compare any file inside the destination directory
	// that doesn't match the pattern
	// For Example: cca.src = C:\a1\a* des = https://<container-name>?<sig>
	// Only files that follow pattern a* will be compared
	rootPath, sourcePattern := util.sourceRootPathWithoutWildCards(cca.src, os.PathSeparator)
	//replace the os path separator  with path separator "/" which is path separator for blobs
	sourcePattern = strings.Replace(sourcePattern, string(os.PathSeparator), "/", -1)
	destinationUrl, err := url.Parse(cca.dst)
	if err != nil {
		return fmt.Errorf("error parsing the destinatio url")
	}

	blobUrlParts := azblob.NewBlobURLParts(*destinationUrl)
	containerUrl := util.getContainerUrl(blobUrlParts)
	searchPrefix, pattern := util.searchPrefixFromUrl(blobUrlParts)

	containerBlobUrl := azblob.NewContainerURL(containerUrl, p)

	for marker := (azblob.Marker{}); marker.NotDone(); {
		// look for all blobs that start with the prefix
		listBlob, err := containerBlobUrl.ListBlobsFlatSegment(ctx, marker,
			azblob.ListBlobsSegmentOptions{Prefix: searchPrefix})
		if err != nil {
			return fmt.Errorf("cannot list blobs for download. Failed with error %s", err.Error())
		}

		// Process the blobs returned in this result segment (if the segment is empty, the loop body won't execute)
		for _, blobInfo := range listBlob.Blobs.Blob {
			// If blob name doesn't match the pattern
			// This check supports the Use wild cards
			// SearchPrefix is used to list to all the blobs inside the destination
			// and pattern is used to identify which blob to compare further
			if !util.blobNameMatchesThePattern(pattern, blobInfo.Name) {
				continue
			}

			// realtivePathofBlobLocally is the local path relative to source at which blob should be downloaded
			// Example: cca.src ="C:\User1\user-1" cca.dst = "https://<container-name>/virtual-dir?<sig>" blob name = "virtual-dir/a.txt"
			// realtivePathofBlobLocally = virtual-dir/a.txt
			realtivePathofBlobLocally := util.relativePathToRoot(searchPrefix, blobInfo.Name, '/')

			// check if the listed blob segment matches the sourcePath pattern
			// if it does not comparison is not required
			if !util.blobNameMatchesThePattern(sourcePattern, realtivePathofBlobLocally) {
				continue
			}
			blobLocalPath := util.generateLocalPath(rootPath, realtivePathofBlobLocally)
			// Check if the blob exists locally or not
			_, err := os.Stat(blobLocalPath)
			if err == nil {
				continue
			}
			// if the blob doesn't exits locally, then we need to delete blob.
			if err != nil && os.IsNotExist(err) {
				// delete the blob.
				e.addTransferToDelete(common.CopyTransfer{
					Source:      util.generateBlobUrl(containerUrl, blobInfo.Name),
					Destination: "", // no destination in case of Delete JobPartOrder
					SourceSize:  *blobInfo.Properties.ContentLength,
				}, cca)
			}
		}
		marker = listBlob.NextMarker
	}
	return nil
}

func (e *syncUploadEnumerator) compareLocalAgainstRemote(cca *cookedSyncCmdArgs, p pipeline.Pipeline) (error, bool) {

	ctx := context.WithValue(context.Background(), ste.ServiceAPIVersionOverride, ste.DefaultServiceApiVersion)
	util := copyHandlerUtil{}

	// attempt to parse the destination url
	destinationUrl, err := url.Parse(cca.dst)
	if err != nil {
		// the destination should have already been validated, it would be surprising if it cannot be parsed at this point
		panic(err)
	}
	blobUrl := azblob.NewBlobURL(*destinationUrl, p)
	// Get the files and directories for the given source pattern
	listOfFilesAndDir, lofaderr := filepath.Glob(cca.src)
	if lofaderr != nil {
		return fmt.Errorf("error getting the files and directories for source pattern %s", cca.src), false
	}
	// isSourceASingleFile is used to determine whether given source pattern represents single file or not
	// If the source is a single file, this pointer will not be nil
	// if it is nil, it means the source is a directory or list of file
	var isSourceASingleFile os.FileInfo = nil

	// If the number of files matching the given pattern is 1
	// determine the type of the source
	if len(listOfFilesAndDir) == 1 {
		lofadInfo, lofaderr := os.Stat(listOfFilesAndDir[0])
		if lofaderr != nil {
			return fmt.Errorf("error getting the file info the source %s", listOfFilesAndDir[0]), false
		}
		// If the given source represents a single file, then set isSourceASingleFile pointer to the fileInfo pointer
		if !lofadInfo.IsDir() {
			isSourceASingleFile = lofadInfo
		}
	}
	// Get the destination blob properties
	bProperties, berr := blobUrl.GetProperties(ctx, azblob.BlobAccessConditions{})

	// If the destination is an existing blob and the source is a directory
	// sync cannot happen between an existing blob and a local directory
	if berr == nil && isSourceASingleFile != nil {
		return fmt.Errorf("cannot perform the sync since source %s "+
			"is a directory and destination %s is a blob", cca.src, destinationUrl.String()), false
	}
	// If the source is a file and destination is a blob
	// For Example: "cca.src = C:\User\user-1\a.txt" && "cca.dst = https://<container-name>/vd-1/a.txt"
	if berr == nil && isSourceASingleFile != nil {
		// Get the blob name from the destination url
		// blobName refers to the last name of the blob with which it is stored as file locally
		// Example1: "cca.dst = https://<container-name>/blob1?<sig>  blobName = blob1"
		// Example1: "cca.dst = https://<container-name>/dir1/blob1?<sig>  blobName = blob1"
		blobName := destinationUrl.Path[strings.LastIndex(destinationUrl.Path, "/")+1:]
		// Compare the blob name and file name
		// blobName and filename should be same for sync to happen
		if strings.Compare(blobName, isSourceASingleFile.Name()) != 0 {
			return fmt.Errorf("sync cannot be done since blob %s and filename %s doesn't match", blobName, isSourceASingleFile.Name()), true
		}
		// If the modified time of file local is later than that of blob
		// sync needs to happen. The transfer is queued
		if isSourceASingleFile.ModTime().After(bProperties.LastModified()) {
			e.addTransferToUpload(common.CopyTransfer{
				Source:           cca.src,
				Destination:      destinationUrl.String(),
				SourceSize:       isSourceASingleFile.Size(),
				LastModifiedTime: isSourceASingleFile.ModTime(),
			}, cca)
		}
		return nil, true
	}

	blobUrlParts := azblob.NewBlobURLParts(*destinationUrl)
	// If the source is a file and destination is not a blob, it could be a container or directory
	// then compare the file against the possible blob. If file doesn't exists as a blob upload it
	// it it exists then compare it.
	if isSourceASingleFile != nil && berr != nil {
		filedestinationUrl, _ := util.appendBlobNameToUrl(blobUrlParts, isSourceASingleFile.Name())
		blobUrl := azblob.NewBlobURL(filedestinationUrl, p)
		bProperties, err := blobUrl.GetProperties(ctx, azblob.BlobAccessConditions{})
		// If err is not nil, it means the blob does not exists
		if err != nil {
			if stError, ok := err.(azblob.StorageError); !ok || (ok && stError.Response().StatusCode != http.StatusNotFound) {
				return fmt.Errorf("error sync up the blob %s because it failed to get the properties. Failed with error %s", filedestinationUrl.String(), err.Error()), true
			}
		}
		if err == nil && !isSourceASingleFile.ModTime().After(bProperties.LastModified()) {
			return fmt.Errorf("sync is not required since the source %s modified time is before the destinaton %s modified time ", cca.src, filedestinationUrl.String()), true
		}
		e.addTransferToUpload(common.CopyTransfer{
			Source:           cca.src,
			Destination:      filedestinationUrl.String(),
			LastModifiedTime: isSourceASingleFile.ModTime(),
			SourceSize:       isSourceASingleFile.Size(),
		}, cca)
		return nil, true
	}

	// checkAndQueue is an internal function which check the modified time of file locally
	// and on container and then decideds whether to queue transfer for upload or not.
	checkAndQueue := func(root string, pathToFile string, f os.FileInfo) error {
		// If root path equals the pathToFile it means that source passed was a filePath
		// For Example: root = C:\a1\a2\f1.txt, pathToFile: C:\a1\a2\f1.txt
		// localFileRelativePath = f1.txt
		// remove the last component in the root path
		// root = C:\a1\a2
		if strings.EqualFold(root, pathToFile) {
			pathSepIndex := strings.LastIndex(root, string(os.PathSeparator))
			if pathSepIndex <= 0 {
				root = ""
			} else {
				root = root[:pathSepIndex]
			}
		}
		// localfileRelativePath is the path of file relative to root directory
		// Example1: root = C:\User\user1\dir-1  fileAbsolutePath = :\User\user1\dir-1\a.txt localfileRelativePath = \a.txt
		// Example2: root = C:\User\user1\dir-1  fileAbsolutePath = :\User\user1\dir-1\dir-2\a.txt localfileRelativePath = \dir-2\a.txt
		localfileRelativePath := strings.Replace(pathToFile, root, "", 1)
		// remove the path separator at the start of relative path
		if len(localfileRelativePath) > 0 && localfileRelativePath[0] == os.PathSeparator {
			localfileRelativePath = localfileRelativePath[1:]
		}
		// Appending the fileRelativePath to the destinationUrl
		// root = C:\User\user1\dir-1  cca.dst = https://<container-name>/<vir-d>?<sig>
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
		}
		// If the local file modified time was behind the remote
		// then sync is not required
		if err == nil && !f.ModTime().After(blobProperties.LastModified()) {
			return nil
		}
		err = e.addTransferToUpload(common.CopyTransfer{
			Source:           pathToFile,
			Destination:      filedestinationUrl.String(),
			LastModifiedTime: f.ModTime(),
			SourceSize:       f.Size(),
		}, cca)
		if err != nil {
			return err
		}
		return nil
	}
	// rootPath will be the parent source directory before the first wildcard
	// For Example: cca.src = C:\a\b* rootPath = C:\a
	// For Example: cca.src = C:\*\a* rootPath = c:\
	// In case of no wildCard, rootPath is equal to the source directory
	// This rootPath is effective when wildCards are provided
	// Using this rootPath, path of file on blob is calculated
	// for ex: cca.src := C:\a*\f*.txt rootPath = C:\
	// path of file C:\a1\f1.txt on the destination path will be destination/a1/f1.txt
	rootPath, _ := util.sourceRootPathWithoutWildCards(cca.src, os.PathSeparator)
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
						return checkAndQueue(rootPath, pathToFile, f)
					}
				})
			} else if !f.IsDir() {
				err = checkAndQueue(rootPath, fileOrDir, f)
			}
		}
	}
	return nil, false
}

// this function accepts the list of files/directories to transfer and processes them
func (e *syncUploadEnumerator) enumerate(cca *cookedSyncCmdArgs) error {
	// Create the new azblob pipeline
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
	// Copying the JobId of sync job to individual deleteJobRequest.
	e.DeleteJobRequest.JobID = e.JobID
	// FromTo of DeleteJobRequest will be BlobTrash.
	e.DeleteJobRequest.FromTo = common.EFromTo.BlobTrash()

	// Set the Log Level
	e.CopyJobRequest.LogLevel = e.LogLevel
	e.DeleteJobRequest.LogLevel = e.LogLevel

	// Set the force flag to true
	e.CopyJobRequest.ForceWrite = true

	// Copy the sync Command String to the CopyJobPartRequest and DeleteJobRequest
	e.CopyJobRequest.CommandString = e.CommandString
	e.DeleteJobRequest.CommandString = e.CommandString

	err, isSourceAFile := e.compareLocalAgainstRemote(cca, p)
	if err != nil {
		return err
	}
	// isSourceAFile defines whether source is a file or not.
	// If source is a file and destination is a blob, then destination doesn't needs to be compared against local.
	if !isSourceAFile {
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
