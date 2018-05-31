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

func (e *syncUploadEnumerator) compareRemoteAgainstLocal(
	sourcePath string, isRecursiveOn bool,
	destinationUrlString string, p pipeline.Pipeline,
	wg *sync.WaitGroup, waitUntilJobCompletion func(jobID common.JobID, wg *sync.WaitGroup)) error {

	util := copyHandlerUtil{}

	destinationUrl, err := url.Parse(destinationUrlString)
	if err != nil {
		return fmt.Errorf("error parsing the destinatio url")
	}
	var containerUrl url.URL
	var searchPrefix string
	if !util.urlIsContainerOrShare(destinationUrl) {
		containerUrl = util.getContainerURLFromString(*destinationUrl)
		// get the search prefix to query the service
		searchPrefix = util.getBlobNameFromURL(destinationUrl.Path)
		searchPrefix = searchPrefix[:len(searchPrefix)-1] // strip away the * at the end
	} else {
		containerUrl = *destinationUrl
		searchPrefix = ""
	}

	// if the user did not specify / at the end of the virtual directory, add it before doing the prefix search
	if strings.LastIndex(searchPrefix, "/") != len(searchPrefix)-1 {
		searchPrefix += "/"
	}

	containerBlobUrl := azblob.NewContainerURL(containerUrl, p)

	closestVirtualDirectory := util.getLastVirtualDirectoryFromPath(searchPrefix)
	// strip away the leading / in the closest virtual directory
	if len(closestVirtualDirectory) > 0 && closestVirtualDirectory[0:1] == "/" {
		closestVirtualDirectory = closestVirtualDirectory[1:]
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
			blobNameAfterPrefix := blobInfo.Name[len(closestVirtualDirectory):]
			// If there is a "/" at the start of blobName, then strip "/" separator.
			if len(blobNameAfterPrefix) > 0 && blobNameAfterPrefix[0:1] == "/" {
				blobNameAfterPrefix = blobNameAfterPrefix[1:]
			}
			if !isRecursiveOn && strings.Contains(blobNameAfterPrefix, "/") {
				continue
			}
			blobLocalPath := util.generateLocalPath(sourcePath, blobNameAfterPrefix)
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
		//err = e.dispatchPart(false)
		if err != nil {
			return err
		}
	}
	return nil
}

func (e *syncUploadEnumerator) compareLocalAgainstRemote(src string, isRecursiveOn bool, dst string, wg *sync.WaitGroup, p pipeline.Pipeline,
	waitUntilJobCompletion func(jobID common.JobID, wg *sync.WaitGroup)) error {
	util := copyHandlerUtil{}

	// attempt to parse the destination url
	destinationUrl, err := url.Parse(dst)
	if err != nil {
		// the destination should have already been validated, it would be surprising if it cannot be parsed at this point
		panic(err)
	}
	blobUrl := azblob.NewBlobURL(*destinationUrl, p)
	f, ferr := os.Stat(src)
	bProperties, berr := blobUrl.GetProperties(context.Background(), azblob.BlobAccessConditions{})
	if ferr != nil {
		return fmt.Errorf("cannot access the source %s. Failed with error %s", src, err.Error())
	}
	if !f.IsDir() && berr != nil {
		return fmt.Errorf("cannot perform sync since source is a file and destination "+
			"is not a blob. Listing blob failed with error %s", berr.Error())
	}
	if berr == nil && f.IsDir() {
		return fmt.Errorf("cannot perform the sync since source %s "+
			"is a directory and destination %s is a blob", src, destinationUrl.String())
	}
	// If the source is a file and destination is a blob
	if berr == nil && !f.IsDir() {
		blobName := destinationUrl.Path[strings.LastIndex(destinationUrl.Path, "/"):]
		if strings.Compare(blobName, f.Name()) != 0 {
			return fmt.Errorf("sync cannot be done since blob %s and filename %s doesn't match", blobName, f.Name())
		}
		if f.ModTime().After(bProperties.LastModified()) {
			e.addTransferToUpload(common.CopyTransfer{
				Source:      src,
				Destination: destinationUrl.String(),
				SourceSize:  f.Size(),
			}, wg, waitUntilJobCompletion)
		}
		return nil
	}

	// verify the source path provided is valid or not.
	_, err = os.Stat(src)
	if err != nil {
		return fmt.Errorf("cannot find source to sync")
	}

	var containerPath string
	var destinationSuffixAfterContainer string

	// If destination url is not container, then get container Url from destination string.
	if !util.urlIsContainerOrShare(destinationUrl) {
		containerPath, destinationSuffixAfterContainer = util.getConatinerUrlAndSuffix(*destinationUrl)
	} else {
		containerPath = util.getContainerURLFromString(*destinationUrl).Path
		destinationSuffixAfterContainer = ""
	}

	var dirIterateFunction func(dirPath string, currentDirString string) error
	dirIterateFunction = func(dirPath string, currentDirString string) error {
		files, err := ioutil.ReadDir(dirPath)
		if err != nil {
			return err
		}
		// Iterate through all files and directories.
		for i := 0; i < len(files); i++ {
			if files[i].IsDir() {
				dirIterateFunction(dirPath+string(os.PathSeparator)+files[i].Name(), currentDirString+files[i].Name()+"/")
			} else {
				// the path in the blob name started at the given fileOrDirectoryPath
				// example: fileOrDirectoryPath = "/dir1/dir2/dir3" pathToFile = "/dir1/dir2/dir3/file1.txt" result = "dir3/file1.txt"
				destinationUrl.Path = containerPath + destinationSuffixAfterContainer + currentDirString + files[i].Name()
				localFilePath := dirPath + string(os.PathSeparator) + files[i].Name()
				blobUrl := azblob.NewBlobURL(*destinationUrl, p)
				blobProperties, err := blobUrl.GetProperties(context.Background(), azblob.BlobAccessConditions{})

				if err != nil {
					if stError, ok := err.(azblob.StorageError); !ok || (ok && stError.Response().StatusCode != http.StatusNotFound) {
						return fmt.Errorf("error sync up the blob %s because it failed to get the properties. Failed with error %s", localFilePath, err.Error())
					}
				}
				if err == nil && !files[i].ModTime().After(blobProperties.LastModified()) {
					continue
				}

				// Closing the blob Properties response body if not nil.
				if blobProperties != nil && blobProperties.Response() != nil {
					io.Copy(ioutil.Discard, blobProperties.Response().Body)
					blobProperties.Response().Body.Close()
				}

				err = e.addTransferToUpload(common.CopyTransfer{
					Source:           localFilePath,
					Destination:      destinationUrl.String(),
					LastModifiedTime: files[i].ModTime(),
					SourceSize:       files[i].Size(),
				}, wg, waitUntilJobCompletion)
				if err != nil {
					return err
				}
			}
		}
		return nil
	}
	return dirIterateFunction(src, "/")
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

	err := e.compareLocalAgainstRemote(src, isRecursiveOn, dst, wg, p, waitUntilJobCompletion)
	if err != nil {
		return nil
	}
	err = e.compareRemoteAgainstLocal(src, isRecursiveOn, dst, p, wg, waitUntilJobCompletion)
	if err != nil {
		return err
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
