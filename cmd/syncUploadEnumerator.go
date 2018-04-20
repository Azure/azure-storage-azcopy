package cmd

import (
	"context"
	"fmt"
	"github.com/Azure/azure-pipeline-go/pipeline"
	"github.com/Azure/azure-storage-azcopy/common"
	"github.com/Azure/azure-storage-azcopy/ste"
	"github.com/Azure/azure-storage-blob-go/2017-07-29/azblob"
	"io"
	"io/ioutil"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"sync"
)

type syncUploadEnumerator common.SyncJobPartOrderRequest

// accepts a new transfer which is to delete the blob on container.
func (e *syncUploadEnumerator) addTransferToDelete(transfer common.CopyTransfer, wg *sync.WaitGroup,
	waitUntilJobCompletion func(jobID common.JobID, wg *sync.WaitGroup)) error {
	// If the existing transfers in DeleteJobRequest is equal to NumOfFilesPerUploadJobPart,
	// then send the JobPartOrder to transfer engine.
	if len(e.DeleteJobRequest.Transfers) == NumOfFilesPerUploadJobPart {
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

	if len(e.CopyJobRequest.Transfers) == NumOfFilesPerUploadJobPart {
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
			"The source and destination  and in sync")
	} else if numberOfCopyTransfers > 0 && numberOfDeleteTransfers > 0 {
		var resp common.CopyJobPartOrderResponse
		e.CopyJobRequest.PartNum = e.PartNumber
		Rpc(common.ERpcCmd.CopyJobPartOrder(), (*common.CopyJobPartOrderRequest)(&e.CopyJobRequest), &resp)
		if !resp.JobStarted {
			return fmt.Errorf("copy job part order with JobId %s and part number %d failed because %s", e.JobID, e.PartNumber, resp.ErrorMsg)
		}
		e.PartNumber++
		e.DeleteJobRequest.IsFinalPart = true
		Rpc(common.ERpcCmd.CopyJobPartOrder(), (*common.CopyJobPartOrderRequest)(&e.DeleteJobRequest), &resp)
		if !resp.JobStarted {
			return fmt.Errorf("delete job part order with JobId %s and part number %d failed because %s", e.JobID, e.PartNumber, resp.ErrorMsg)
		}
	} else if numberOfCopyTransfers > 0 {
		e.CopyJobRequest.IsFinalPart = true
		var resp common.CopyJobPartOrderResponse
		Rpc(common.ERpcCmd.CopyJobPartOrder(), (*common.CopyJobPartOrderRequest)(&e.CopyJobRequest), &resp)
		if !resp.JobStarted {
			return fmt.Errorf("copy job part order with JobId %s and part number %d failed because %s", e.JobID, e.PartNumber, resp.ErrorMsg)
		}
	} else {
		e.DeleteJobRequest.IsFinalPart = true
		var resp common.CopyJobPartOrderResponse
		Rpc(common.ERpcCmd.CopyJobPartOrder(), (*common.CopyJobPartOrderRequest)(&e.DeleteJobRequest), &resp)
		if !resp.JobStarted {
			return fmt.Errorf("delete job part order with JobId %s and part number %d failed because %s", e.JobID, e.PartNumber, resp.ErrorMsg)
		}
	}
	return nil
}

func (e *syncUploadEnumerator) compareRemoteFilesAgainstLocal(
	sourcePath string, isRecursiveOn bool,
	destinationUrlString string, p pipeline.Pipeline,
	wg *sync.WaitGroup, waitUntilJobCompletion func(jobID common.JobID, wg *sync.WaitGroup)) error {
	//util := copyHandlerUtil{}

	return nil
}

func (e *syncUploadEnumerator) checkLocalFilesOnContainer(src string, isRecursiveOn bool, dst string, wg *sync.WaitGroup, p pipeline.Pipeline,
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
	// list the source files and directories
	listOfFilesAndDirectories, err := filepath.Glob(src)
	if err != nil || len(listOfFilesAndDirectories) == 0 {
		return fmt.Errorf("cannot find source to sync")
	}

	var containerPath string
	var destinationSufferAfterContainer string

	// If destination url is not container, then get container Url from destination string.
	if !util.urlIsContainer(destinationUrl) {
		containerPath, destinationSufferAfterContainer = util.getConatinerUrlAndSuffix(*destinationUrl)
	} else {
		containerPath = util.getContainerURLFromString(*destinationUrl).Path
		destinationSufferAfterContainer = ""
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
				destinationUrl.Path = containerPath + destinationSufferAfterContainer + currentDirString + files[i].Name()
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
	// walk through every file and directory
	// upload every file
	// upload directory recursively if recursive option is on
	for _, fileOrDirectoryPath := range listOfFilesAndDirectories {
		f, err := os.Stat(fileOrDirectoryPath)
		if err == nil {
			// directories are uploaded only if recursive is on
			if f.IsDir() && isRecursiveOn {
				err = dirIterateFunction(fileOrDirectoryPath, "/")
				if err != nil {
					return err
				}
			} else if !f.IsDir() {
				// files are uploaded using their file name as blob name
				destinationUrl.Path = containerPath + "/" + destinationSufferAfterContainer + "/" + f.Name()
				blobUrl := azblob.NewBlobURL(*destinationUrl, p)
				blobProperties, err := blobUrl.GetProperties(context.Background(), azblob.BlobAccessConditions{})

				if err != nil {
					if stError, ok := err.(azblob.StorageError); !ok || (ok && stError.Response().StatusCode != http.StatusNotFound) {
						return fmt.Errorf("error sync up the blob %s because it failed to get the properties. Failed with error %s", f.Name(), err.Error())
					}
				}
				if !f.ModTime().After(blobProperties.LastModified()) {
					return fmt.Errorf("sync not required since the destination is same as source or was modified later than source")
				}
				// Closing the blob Properties response body if not nil.
				if blobProperties != nil && blobProperties.Response() != nil {
					io.Copy(ioutil.Discard, blobProperties.Response().Body)
					blobProperties.Response().Body.Close()
				}
				err = e.addTransferToUpload(common.CopyTransfer{
					Source:           fileOrDirectoryPath,
					Destination:      destinationUrl.String(),
					LastModifiedTime: f.ModTime(),
					SourceSize:       f.Size(),
				}, wg, waitUntilJobCompletion)
				if err != nil {
					return err
				}
			}
		}
	}
	return nil
}

// this function accepts the list of files/directories to transfer and processes them
func (e *syncUploadEnumerator) enumerate(src string, isRecursiveOn bool, dst string, wg *sync.WaitGroup,
	waitUntilJobCompletion func(jobID common.JobID, wg *sync.WaitGroup)) error {
	p := azblob.NewPipeline(
		azblob.NewAnonymousCredential(),
		azblob.PipelineOptions{
			Retry: azblob.RetryOptions{
				Policy:        azblob.RetryPolicyExponential,
				MaxTries:      ste.UploadMaxTries,
				TryTimeout:    ste.UploadTryTimeout,
				RetryDelay:    ste.UploadRetryDelay,
				MaxRetryDelay: ste.UploadMaxRetryDelay,
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

	err := e.checkLocalFilesOnContainer(src, isRecursiveOn, dst, wg, p, waitUntilJobCompletion)
	if err != nil {
		return nil
	}
	return nil
}
