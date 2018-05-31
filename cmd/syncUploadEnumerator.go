package cmd

import (
	"context"
	"fmt"
	"io"
	"io/ioutil"
	"net/http"
	"net/url"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/Azure/azure-pipeline-go/pipeline"
	"github.com/Azure/azure-storage-azcopy/common"
	"github.com/Azure/azure-storage-blob-go/2017-07-29/azblob"
	"path/filepath"
)

type syncUploadEnumerator common.SyncJobPartOrderRequest

// accepts a new transfer which is to delete the blob on container.
func (e *syncUploadEnumerator) addTransferToDelete(transfer common.CopyTransfer, wg *sync.WaitGroup,
	waitUntilJobCompletion func(jobID common.JobID, wg *sync.WaitGroup)) error {
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
			wg.Add(1)
			go waitUntilJobCompletion(e.JobID, wg)
		}
		e.DeleteJobRequest.Transfers = []common.CopyTransfer{}
		e.PartNumber++
	}
	e.DeleteJobRequest.Transfers = append(e.DeleteJobRequest.Transfers, transfer)
	return nil
}

// accept a new transfer, if the threshold is reached, dispatch a job part order
func (e *syncUploadEnumerator) addTransferToUpload(transfer common.CopyTransfer, wg *sync.WaitGroup,
	waitUntilJobCompletion func(jobID common.JobID, wg *sync.WaitGroup)) error {

	if len(e.CopyJobRequest.Transfers) == NumOfFilesPerDispatchJobPart {
		resp := common.CopyJobPartOrderResponse{}
		e.CopyJobRequest.PartNum = e.PartNumber
		Rpc(common.ERpcCmd.CopyJobPartOrder(), (*common.CopyJobPartOrderRequest)(&e.CopyJobRequest), &resp)

		if !resp.JobStarted {
			return fmt.Errorf("copy job part order with JobId %s and part number %d failed because %s", e.JobID, e.PartNumber, resp.ErrorMsg)
		}
		// if the current part order sent to engine is 0, then start fetching the Job Progress summary.
		if e.PartNumber == 0 {
			wg.Add(1)
			go waitUntilJobCompletion(e.JobID, wg)
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
func (e *syncUploadEnumerator) compareRemoteAgainstLocal(
	sourcePath string, isRecursiveOn bool,
	destinationUrlString string, p pipeline.Pipeline,
	wg *sync.WaitGroup, waitUntilJobCompletion func(jobID common.JobID, wg *sync.WaitGroup)) error {

	util := copyHandlerUtil{}

	destinationUrl, err := url.Parse(destinationUrlString)
	if err != nil {
		return fmt.Errorf("error parsing the destinatio url")
	}

	blobUrlParts := azblob.NewBlobURLParts(*destinationUrl)
	containerUrl := util.getContainerUrl(blobUrlParts)
	searchPrefix, pattern := util.searchPrefixFromUrl(blobUrlParts)

	containerBlobUrl := azblob.NewContainerURL(containerUrl, p)
	// virtual directory is the entire virtual directory path before the blob name
	// passed in the searchPrefix
	// Example: dst = https://<container-name>/vd-1?<sig> searchPrefix = vd-1/
	// virtualDirectory = vd-1
	// Example: dst = https://<container-name>/vd-1/vd-2/fi*.txt?<sig> searchPrefix = vd-1/vd-2/fi*.txt
	// virtualDirectory = vd-1/vd-2/
	virtualDirectory := util.getLastVirtualDirectoryFromPath(searchPrefix)
	// strip away the leading / in the closest virtual directory
	if len(virtualDirectory) > 0 && virtualDirectory[0:1] == "/" {
		virtualDirectory = virtualDirectory[1:]
	}

	for marker := (azblob.Marker{}); marker.NotDone(); {
		// look for all blobs that start with the prefix
		listBlob, err := containerBlobUrl.ListBlobsFlatSegment(context.TODO(), marker,
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
			// Example: src ="C:\User1\user-1" dst = "https://<container-name>/virtual-dir?<sig>" blob name = "virtual-dir/a.txt"
			// realtivePathofBlobLocally = virtual-dir/a.txt
			// remove the virtual directory from the realtivePathofBlobLocally
			realtivePathofBlobLocally := util.getRelativePath(searchPrefix, blobInfo.Name, "/")
			realtivePathofBlobLocally = strings.Replace(realtivePathofBlobLocally, virtualDirectory, "",1)
			blobLocalPath := util.generateLocalPath(sourcePath, realtivePathofBlobLocally)
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
				}, wg, waitUntilJobCompletion)
			}
		}
		marker = listBlob.NextMarker
	}
	return nil
}

func (e *syncUploadEnumerator) compareLocalAgainstRemote(src string, isRecursiveOn bool, dst string, wg *sync.WaitGroup, p pipeline.Pipeline,
	waitUntilJobCompletion func(jobID common.JobID, wg *sync.WaitGroup)) (error, bool) {
	util := copyHandlerUtil{}

	// attempt to parse the destination url
	destinationUrl, err := url.Parse(dst)
	if err != nil {
		// the destination should have already been validated, it would be surprising if it cannot be parsed at this point
		panic(err)
	}
	blobUrl := azblob.NewBlobURL(*destinationUrl, p)
	// Get the local file Info
	f, ferr := os.Stat(src)
	// Get the destination blob properties
	bProperties, berr := blobUrl.GetProperties(context.Background(), azblob.BlobAccessConditions{})
	// If the error occurs while fetching the fileInfo of the source
	// return the error
	if ferr != nil {
		return fmt.Errorf("cannot access the source %s. Failed with error %s", src, err.Error()), false
	}
	// If the source is a file locally and destination is not a blob
	// it means that it could be a virtual directory / container
	// sync cannot happen between a file and a virtual directory / container
	if !f.IsDir() && berr != nil {
		return fmt.Errorf("cannot perform sync since source is a file and destination "+
			"is not a blob. Listing blob failed with error %s", berr.Error()), true
	}
	// If the destination is an existing blob and the source is a directory
	// sync cannot happen between an existing blob and a local directory
	if berr == nil && f.IsDir() {
		return fmt.Errorf("cannot perform the sync since source %s "+
			"is a directory and destination %s is a blob", src, destinationUrl.String()), false
	}
	// If the source is a file and destination is a blob
	// For Example: "src = C:\User\user-1\a.txt" && "dst = https://<container-name>/vd-1/a.txt"
	if berr == nil && !f.IsDir() {
		// Get the blob name from the destination url
		// blobName refers to the last name of the blob with which it is stored as file locally
		// Example1: "dst = https://<container-name>/blob1?<sig>  blobName = blob1"
		// Example1: "dst = https://<container-name>/dir1/blob1?<sig>  blobName = blob1"
		blobName := destinationUrl.Path[strings.LastIndex(destinationUrl.Path, "/")+1:]
		// Compare the blob name and file name
		// blobName and filename should be same for sync to happen
		if strings.Compare(blobName, f.Name()) != 0 {
			return fmt.Errorf("sync cannot be done since blob %s and filename %s doesn't match", blobName, f.Name()), true
		}
		// If the modified time of file local is later than that of blob
		// sync needs to happen. The transfer is queued
		if f.ModTime().After(bProperties.LastModified()) {
			e.addTransferToUpload(common.CopyTransfer{
				Source:      src,
				Destination: destinationUrl.String(),
				SourceSize:  f.Size(),
				LastModifiedTime:f.ModTime(),
			}, wg, waitUntilJobCompletion)
		}
		return nil, true
	}

	blobUrlParts := azblob.NewBlobURLParts(*destinationUrl)

	// checkAndQueue is an internal function which check the modified time of file locally
	// and on container and then decideds whether to queue transfer for upload or not.
	checkAndQueue := func(root string, pathToFile string, f os.FileInfo) error {
		// localfileRelativePath is the path of file relative to root directory
		// Example1: root = C:\User\user1\dir-1  fileAbsolutePath = :\User\user1\dir-1\a.txt localfileRelativePath = \a.txt
		// Example2: root = C:\User\user1\dir-1  fileAbsolutePath = :\User\user1\dir-1\dir-2\a.txt localfileRelativePath = \dir-2\a.txt
		localfileRelativePath := strings.Replace(pathToFile, root, "", 1)
		// remove the path separator at the start of relative path
		if len(localfileRelativePath) > 0  && localfileRelativePath[0] == os.PathSeparator {
			localfileRelativePath = localfileRelativePath[1:]
		}
		// Appending the fileRelativePath to the destinationUrl
		// root = C:\User\user1\dir-1  dst = https://<container-name>/<vir-d>?<sig>
		// fileAbsolutePath = C:\User\user1\dir-1\dir-2\a.txt localfileRelativePath = \dir-2\a.txt
		// filedestinationUrl =  https://<container-name>/<vir-d>/dir-2/a.txt?<sig>
		filedestinationUrl, _ := util.appendBlobNameToUrl(blobUrlParts, localfileRelativePath)

		// Get the properties of given on container
		blobUrl := azblob.NewBlobURL(filedestinationUrl, p)
		blobProperties, err := blobUrl.GetProperties(context.Background(), azblob.BlobAccessConditions{})

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
		// Closing the blob Properties response body if not nil.
		if blobProperties != nil && blobProperties.Response() != nil {
			io.Copy(ioutil.Discard, blobProperties.Response().Body)
			blobProperties.Response().Body.Close()
		}
		err = e.addTransferToUpload(common.CopyTransfer{
			Source:           pathToFile,
			Destination:      filedestinationUrl.String(),
			LastModifiedTime: f.ModTime(),
			SourceSize:       f.Size(),
		}, wg, waitUntilJobCompletion)
		if err != nil {
			return err
		}
		return nil
	}

	listOfFilesAndDir, err := filepath.Glob(src)

	if err != nil {
		return fmt.Errorf("error listing the file name inside the source %s", src), false
	}

	// Iterate through each file / dir inside the source
	// and then checkAndQueue
	for _, fileOrDir := range listOfFilesAndDir {
		f, err := os.Stat(fileOrDir)
		if err == nil {
			// directories are uploaded only if recursive is on
			if f.IsDir()  {
				// walk goes through the entire directory tree
				err = filepath.Walk(fileOrDir, func(pathToFile string, f os.FileInfo, err error) error {
					if err != nil {
						return err
					}
					if f.IsDir(){
						return nil
					} else {
						return checkAndQueue(src, pathToFile, f)
					}
				})
			} else if !f.IsDir() {
				err = checkAndQueue(src, fileOrDir, f)
			}
		}
	}
	return nil, false
}

// this function accepts the list of files/directories to transfer and processes them
func (e *syncUploadEnumerator) enumerate(src string, isRecursiveOn bool, dst string, wg *sync.WaitGroup,
	waitUntilJobCompletion func(jobID common.JobID, wg *sync.WaitGroup)) error {
	p := azblob.NewPipeline(
		azblob.NewAnonymousCredential(),
		azblob.PipelineOptions{
			Retry: azblob.RetryOptions{
				Policy:        azblob.RetryPolicyExponential,
				MaxTries:      5,
				TryTimeout:    time.Minute * 1,
				RetryDelay:    time.Second * 1,
				MaxRetryDelay: time.Second * 3,
			},
		})
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

	err, isSourceAFile := e.compareLocalAgainstRemote(src, isRecursiveOn, dst, wg, p, waitUntilJobCompletion)
	if err != nil {
		return err
	}
	// isSourceAFile defines whether source is a file or not.
	// If source is a file and destination is a blob, then destination doesn't needs to be compared against local.
	if !isSourceAFile {
		err = e.compareRemoteAgainstLocal(src, isRecursiveOn, dst, p, wg, waitUntilJobCompletion)
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
		wg.Add(1)
		waitUntilJobCompletion(e.JobID, wg)
	}
	return nil
}
