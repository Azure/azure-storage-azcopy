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
	"strings"
)

type syncUploadEnumerator common.SyncJobPartOrderRequest


// accepts a new transfer which is to delete the blob on container.
func (e *syncUploadEnumerator) addTransferToDelete(transfer common.CopyTransfer, wg *sync.WaitGroup,
	waitUntilJobCompletion func(jobID common.JobID, wg *sync.WaitGroup)) error{
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
	}else if numberOfCopyTransfers > 0 && numberOfDeleteTransfers > 0 {
		var resp common.CopyJobPartOrderResponse
		e.CopyJobRequest.PartNum = e.PartNumber
		Rpc(common.ERpcCmd.CopyJobPartOrder(), (*common.CopyJobPartOrderRequest)(&e.CopyJobRequest), &resp)
		if !resp.JobStarted {
			return fmt.Errorf("copy job part order with JobId %s and part number %d failed because %s", e.JobID, e.PartNumber, resp.ErrorMsg)
		}
		e.PartNumber ++
		e.DeleteJobRequest.IsFinalPart = true
		Rpc(common.ERpcCmd.CopyJobPartOrder(), (*common.CopyJobPartOrderRequest)(&e.DeleteJobRequest), &resp)
		if !resp.JobStarted {
			return fmt.Errorf("delete job part order with JobId %s and part number %d failed because %s", e.JobID, e.PartNumber, resp.ErrorMsg)
		}
	}else if numberOfCopyTransfers > 0 {
		e.CopyJobRequest.IsFinalPart = true
		var resp common.CopyJobPartOrderResponse
		Rpc(common.ERpcCmd.CopyJobPartOrder(), (*common.CopyJobPartOrderRequest)(&e.CopyJobRequest), &resp)
		if !resp.JobStarted {
			return fmt.Errorf("copy job part order with JobId %s and part number %d failed because %s", e.JobID, e.PartNumber, resp.ErrorMsg)
		}
	}else {
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
								wg *sync.WaitGroup, waitUntilJobCompletion func(jobID common.JobID, wg *sync.WaitGroup)) error{
	util := copyHandlerUtil{}

	// attempt to parse the source url
	destUrl, err := url.Parse(destinationUrlString)
	if err != nil {
		return errors.New("cannot parse source URL")
	}

	// get the container url to be used later for listing
	literalContainerUrl := util.getContainerURLFromString(*destUrl)
	containerUrl := azblob.NewContainerURL(literalContainerUrl, p)

	numOfStarInUrlPath := util.numOfStarInUrl(destUrl.Path)
	if numOfStarInUrlPath == 1 { // prefix search

		// the * must be at the end of the path
		if strings.LastIndex(destUrl.Path, "*") != len(destUrl.Path)-1 {
			return errors.New("the * in the source URL must be at the end of the path")
		}

		// the destination must be a directory,
		// otherwise we don't know from where to list and compare the files.
		if !util.isPathDirectory(sourcePath) {
			return errors.New("the destination must be an existing directory in this download scenario")
		}

		// get the search prefix to query the service
		_, searchPrefix := util.getDirNameFromSource(sourcePath)
		searchPrefix = searchPrefix[:len(searchPrefix)-1] // strip away the * at the end

		closestVirtualDirectory := util.getLastVirtualDirectoryFromPath(searchPrefix)

		// strip away the leading / in the closest virtual directory
		if len(closestVirtualDirectory) > 0 && closestVirtualDirectory[0:1] == "/" {
			closestVirtualDirectory = closestVirtualDirectory[1:]
		}

		// perform a list blob
		for marker := (azblob.Marker{}); marker.NotDone(); {
			// look for all blobs that start with the prefix
			listBlob, err := containerUrl.ListBlobsFlatSegment(context.TODO(), marker,
				azblob.ListBlobsSegmentOptions{Prefix: searchPrefix})
			if err != nil {
				return fmt.Errorf("cannot list blobs for download. Failed with error %s", err.Error())
			}

			// Process the blobs returned in this result segment (if the segment is empty, the loop body won't execute)
			for _, blobInfo := range listBlob.Blobs.Blob {
				blobNameAfterPrefix := blobInfo.Name[len(closestVirtualDirectory):]
				if !isRecursiveOn && strings.Contains(blobNameAfterPrefix, "/") {
					continue
				}
				sourceLocalPath := util.generateLocalPath(sourcePath, blobNameAfterPrefix)
				f, err := os.Stat(sourceLocalPath)
				if err != nil && os.IsNotExist(err){
					e.addTransferToDelete(common.CopyTransfer{
						Source:util.generateBlobUrl(literalContainerUrl, blobInfo.Name),
						Destination:"",
						SourceSize:*blobInfo.Properties.ContentLength,
					}, wg, waitUntilJobCompletion)
				}else if err == nil && !f.ModTime().After(blobInfo.Properties.LastModified){
					// add to transfer to upload.
					e.addTransferToUpload(common.CopyTransfer{
						Source:           sourceLocalPath,
						Destination:      util.generateBlobUrl(literalContainerUrl, blobInfo.Name),
						LastModifiedTime: blobInfo.Properties.LastModified,
						SourceSize:       *blobInfo.Properties.ContentLength},
						wg, waitUntilJobCompletion)
				}else{
					// Skip the blob.
					continue
				}
			}
			marker = listBlob.NextMarker
			//err = e.dispatchPart(false)
			if err != nil {
				return err
			}
		}

		err = e.dispatchFinalPart()
		if err != nil {
			return err
		}
	} else if numOfStarInUrlPath == 0 { // no prefix search

		// if recursive happens to be turned on, then we will attempt to download a virtual directory
		if isRecursiveOn {
			// recursively download everything that is under the given path, that is a virtual directory
			sourcePath, searchPrefix := util.getDirNameFromSource(sourcePath)
			fmt.Println("search Prefix ", searchPrefix)
			// if the user did not specify / at the end of the virtual directory, add it before doing the prefix search
			if strings.LastIndex(searchPrefix, "/") != len(searchPrefix)-1 {
				searchPrefix += "/"
			}

			fmt.Println("new source path ", sourcePath)
			// the destination must be a directory, otherwise we don't know where to put the files
			if !util.isPathDirectory(sourcePath) {
				return errors.New("the destination must be an existing directory in this sync scenario")
			}

			// perform a list blob
			for marker := (azblob.Marker{}); marker.NotDone(); {
				// look for all blobs that start with the prefix, so that if a blob is under the virtual directory, it will show up
				listBlob, err := containerUrl.ListBlobsFlatSegment(context.Background(), marker,
					azblob.ListBlobsSegmentOptions{Prefix: searchPrefix})
				if err != nil {
					return fmt.Errorf("cannot list blobs for download. Failed with error %s", err.Error())
				}

				// Process the blobs returned in this result segment (if the segment is empty, the loop body won't execute)
				for _, blobInfo := range listBlob.Blobs.Blob {

					sourceLocalPath := util.generateLocalPath(sourcePath, util.getRelativePath(searchPrefix, blobInfo.Name, "/"))
					fmt.Println("source local path ", sourceLocalPath, " blob name ", blobInfo.Name)
					f, err := os.Stat(sourceLocalPath)
					if err != nil && os.IsNotExist(err){
						// add blob to delete
					}else if err == nil && !f.ModTime().After(blobInfo.Properties.LastModified){
						// add to transfer to upload.
						e.addTransferToUpload(common.CopyTransfer{
							Source:           sourceLocalPath,
							Destination:      util.generateBlobUrl(literalContainerUrl, blobInfo.Name),
							LastModifiedTime: blobInfo.Properties.LastModified,
							SourceSize:       *blobInfo.Properties.ContentLength},
							wg,
							waitUntilJobCompletion)
					}else{
						// Skip the blob.
						continue
					}
				}

				marker = listBlob.NextMarker
				//err = e.dispatchPart(false)
				if err != nil {
					return err
				}
			}
		}
		err = e.dispatchFinalPart()
		if err != nil {
			return err
		}

	} else { // more than one * is not supported
		return errors.New("only one * is allowed in the source URL")
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
			err = e.addTransferToUpload(common.CopyTransfer{
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

	// If destination url is not container, then get container Url from destination string.
	if !util.urlIsContainer(destinationUrl) {
		destinationUrl.Path = util.getContainerURLFromString(*destinationUrl).Path
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
							fmt.Println("file time ", f.ModTime())
							fmt.Println("blob time ", blobProperties.LastModified())
							return nil
						}
						if blobProperties != nil{
							fmt.Println("file time ", f.ModTime())
							fmt.Println("blob time ", blobProperties.LastModified())
						}
						// Closing the blob Properties response body if not nil.
						if blobProperties != nil && blobProperties.Response() != nil{
							io.Copy(ioutil.Discard, blobProperties.Response().Body)
							blobProperties.Response().Body.Close()
						}
						fmt.Println("Source String ", pathToFile)
						fmt.Println("destination string ", destinationUrl.String())
						err = e.addTransferToUpload(common.CopyTransfer{
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

	err := e.checkLocalFilesOnContainer(src, isRecursiveOn, dst, wg, p,waitUntilJobCompletion)
	if err != nil{
		return nil
	}
	return e.compareRemoteFilesAgainstLocal(src, isRecursiveOn, dst, p, wg, waitUntilJobCompletion)
}
