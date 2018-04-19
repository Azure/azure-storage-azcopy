package cmd

import (
	"errors"
	"fmt"
	"github.com/Azure/azure-storage-azcopy/common"
	"net/url"
	"os"
	"path/filepath"
	"sync"
	"github.com/Azure/azure-storage-azcopy/ste"
	"github.com/Azure/azure-storage-blob-go/2017-07-29/azblob"
	"context"
	"github.com/Azure/azure-pipeline-go/pipeline"
	"io"
	"io/ioutil"
	"net/http"
)

type syncUploadEnumerator common.SyncJobPartOrderRequest

// accept a new transfer, if the threshold is reached, dispatch a job part order
func (e *syncUploadEnumerator) addTransfer(transfer common.CopyTransfer, wg *sync.WaitGroup,
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
	e.CopyJobRequest.IsFinalPart = true
	var resp common.CopyJobPartOrderResponse
	Rpc(common.ERpcCmd.CopyJobPartOrder(), (*common.CopyJobPartOrderRequest)(&e.CopyJobRequest), &resp)

	if !resp.JobStarted {
		return fmt.Errorf("copy job part order with JobId %s and part number %d failed because %s", e.JobID, e.PartNumber, resp.ErrorMsg)
	}
	return nil
}

func (e *syncUploadEnumerator) checkLocalFilesOnContainer(src string, isRecursiveOn bool, dst string, wg *sync.WaitGroup, p pipeline.Pipeline,
	waitUntilJobCompletion func(jobID common.JobID, wg *sync.WaitGroup)) error{
	util := copyHandlerUtil{}

	// attempt to parse the destination url
	destinationUrl, err := url.Parse(dst)
	if err != nil {
		// the destination should have already been validated, it would be surprising if it cannot be parsed at this point
		panic(err)
	}

	// list the source files and directories
	listOfFilesAndDirectories, err := filepath.Glob(src)
	if err != nil || len(listOfFilesAndDirectories) == 0 {
		return fmt.Errorf("cannot find source to upload")
	}

	// when a single file is being uploaded, we need to treat this case differently, as the destinationUrl might be a blob
	if len(listOfFilesAndDirectories) == 1 {
		f, err := os.Stat(listOfFilesAndDirectories[0])
		if err != nil {
			return errors.New("cannot find source to upload")
		}

		if !f.IsDir() {
			// append file name as blob name in case the given URL is a container
			if util.urlIsContainer(destinationUrl) {
				destinationUrl.Path = util.generateBlobPath(destinationUrl.Path, f.Name())
			}
			blobUrl := azblob.NewBlobURL(*destinationUrl, p)
			blobProperties, err := blobUrl.GetProperties(context.Background(), azblob.BlobAccessConditions{})
			// if the existing blob does'nt exists, then sync is not possible.
			// sync is not supported from source to container.
			if err != nil{
				return fmt.Errorf("given destination is not an existing blob. Hence cannot sync the source to destination ")
			}
			// If there is no difference in the modified time, then sync is not required.
			if err == nil && !f.ModTime().After(blobProperties.LastModified()) {
				return fmt.Errorf("given source and destination are all are already in sync. Hence no sync is not required")
			}
			// Closing the blob Properties response body if not nil.
			if blobProperties != nil && blobProperties.Response() != nil{
				io.Copy(ioutil.Discard, blobProperties.Response().Body)
				blobProperties.Response().Body.Close()
			}
			err = e.addTransfer(common.CopyTransfer{
				Source:           listOfFilesAndDirectories[0],
				Destination:      destinationUrl.String(),
				LastModifiedTime: f.ModTime(),
				SourceSize:       f.Size(),
			}, wg, waitUntilJobCompletion)

			if err != nil {
				return err
			}
			return e.dispatchFinalPart()
		}
	}

	// in any other case, the destination url must point to a container
	if !util.urlIsContainer(destinationUrl) {
		return errors.New("please provide a valid container URL as destination")
	}

	// temporarily save the path of the container
	cleanContainerPath := destinationUrl.Path

	// walk through every file and directory
	// upload every file
	// upload directory recursively if recursive option is on
	for _, fileOrDirectoryPath := range listOfFilesAndDirectories {
		f, err := os.Stat(fileOrDirectoryPath)
		if err == nil {
			// directories are uploaded only if recursive is on
			if f.IsDir() && isRecursiveOn {
				// walk goes through the entire directory tree
				err = filepath.Walk(fileOrDirectoryPath, func(pathToFile string, f os.FileInfo, err error) error {
					if err != nil {
						return err
					}

					if f.IsDir() {
						// skip the subdirectories, we only care about files
						return nil
					} else { // upload the files
						// the path in the blob name started at the given fileOrDirectoryPath
						// example: fileOrDirectoryPath = "/dir1/dir2/dir3" pathToFile = "/dir1/dir2/dir3/file1.txt" result = "dir3/file1.txt"
						destinationUrl.Path = util.generateBlobPath(cleanContainerPath, util.getRelativePath(fileOrDirectoryPath, pathToFile, string(os.PathSeparator)))
						blobUrl := azblob.NewBlobURL(*destinationUrl, p)
						blobProperties, err := blobUrl.GetProperties(context.Background(), azblob.BlobAccessConditions{})

						if err != nil {
							if stError, ok := err.(azblob.StorageError); !ok || (ok && stError.Response().StatusCode != http.StatusNotFound) {
								return fmt.Errorf("error sync up the blob %s because it failed to get the properties. Failed with error %s", pathToFile, err.Error())
							}
						}
						if err == nil && !f.ModTime().After(blobProperties.LastModified()){
							return nil
						}
						// Closing the blob Properties response body if not nil.
						if blobProperties != nil && blobProperties.Response() != nil{
							io.Copy(ioutil.Discard, blobProperties.Response().Body)
							blobProperties.Response().Body.Close()
						}
						err = e.addTransfer(common.CopyTransfer{
							Source:           pathToFile,
							Destination:      destinationUrl.String(),
							LastModifiedTime: f.ModTime(),
							SourceSize:       f.Size(),
						}, wg, waitUntilJobCompletion)
						if err != nil {
							return err
						}
					}
					return nil
				})
			} else if !f.IsDir() {
				// files are uploaded using their file name as blob name
				destinationUrl.Path = util.generateBlobPath(cleanContainerPath, f.Name())
				destinationUrl.Path = util.generateBlobPath(cleanContainerPath, util.getRelativePath(fileOrDirectoryPath, f.Name(), string(os.PathSeparator)))
				blobUrl := azblob.NewBlobURL(*destinationUrl, p)
				blobProperties, err := blobUrl.GetProperties(context.Background(), azblob.BlobAccessConditions{})

				if err != nil {
					if stError, ok := err.(azblob.StorageError); !ok || (ok && stError.Response().StatusCode != http.StatusNotFound) {
						return fmt.Errorf("error sync up the blob %s because it failed to get the properties. Failed with error %s", f.Name(), err.Error())
					}
				}
				if !f.ModTime().After(blobProperties.LastModified()){
					return fmt.Errorf("sync not required since the destination is same as source or was modified later than source")
				}
				// Closing the blob Properties response body if not nil.
				if blobProperties != nil && blobProperties.Response() != nil{
					io.Copy(ioutil.Discard, blobProperties.Response().Body)
					blobProperties.Response().Body.Close()
				}
				err = e.addTransfer(common.CopyTransfer{
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
	if e.PartNumber == 0 && len(e.CopyJobRequest.Transfers) == 0 {
		return errors.New("nothing can be uploaded, please use --recursive to upload directories")
	}
	return e.dispatchFinalPart()
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

	return e.checkLocalFilesOnContainer(src, isRecursiveOn, dst, wg, p,waitUntilJobCompletion)
}
