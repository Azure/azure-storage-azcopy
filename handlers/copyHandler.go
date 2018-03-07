// Copyright © 2017 Microsoft <wastore@microsoft.com>
//
// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files (the "Software"), to deal
// in the Software without restriction, including without limitation the rights
// to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
// copies of the Software, and to permit persons to whom the Software is
// furnished to do so, subject to the following conditions:
//
// The above copyright notice and this permission notice shall be included in
// all copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
// THE SOFTWARE.

package handlers

import (
	"context"

	"fmt"
	"github.com/Azure/azure-storage-azcopy/common"
	"github.com/Azure/azure-storage-blob-go/2016-05-31/azblob"

	"log"
	"net/url"
	"os"
	"os/signal"
	"path"
	"path/filepath"
	"strings"
	"time"
)

// handles the copy command
// dispatches the job order (in parts) to the storage engine
func HandleCopyCommand(commandLineInput common.CopyCmdArgsAndFlags) {
	jobPartOrder := common.CopyJobPartOrderRequest{}
	copyHandlerUtil{}.applyFlags(&commandLineInput, &jobPartOrder)

	// generate job id
	jobId := common.JobID(common.NewUUID())
	jobPartOrder.ID = jobId
	jobStarted := true

	// not having a valid blob type is a fatal error
	if jobPartOrder.OptionalAttributes.BlobType == common.InvalidBlob {
		fmt.Println("Invalid blob type passed. Please enter the valid blob type - BlockBlob, AppendBlob, PageBlob")
		return
	}

	// depending on the source and destination type, we process the cp command differently
	if commandLineInput.SourceType == common.Local && commandLineInput.DestinationType == common.Blob {
		jobStarted = handleUploadFromLocalToBlobStorage(&commandLineInput, &jobPartOrder)
	} else if commandLineInput.SourceType == common.Blob && commandLineInput.DestinationType == common.Local {
		jobStarted = handleDownloadFromBlobStorageToLocal(&commandLineInput, &jobPartOrder)
	}

	// unexpected errors can happen while communicating with the transfer engine
	if !jobStarted {
		fmt.Print("Job with id", jobId, "was not abe to start. Please try again")
		return
	}

	// in background mode we would spit out the job id and quit
	// in foreground mode we would continuously print out status updates for the job, so the job id is not important
	fmt.Println("Job with id", jobId, "has started.")
	if commandLineInput.IsaBackgroundOp {
		return
	}

	// created a signal channel to receive the Interrupt and Kill signal send to OS
	cancelChannel := make(chan os.Signal, 1)
	// cancelChannel will be notified when os receives os.Interrupt and os.Kill signals
	signal.Notify(cancelChannel, os.Interrupt, os.Kill)

	// waiting for signals from either cancelChannel or timeOut Channel.
	// if no signal received, will fetch/display a job status update then sleep for a bit
	for {
		select {
		case <-cancelChannel:
			fmt.Println("Cancelling Job")
			HandleCancelCommand(jobId.String())
			os.Exit(1)
		default:
			jobStatus := copyHandlerUtil{}.fetchJobStatus(jobId)

			// happy ending to the front end
			if jobStatus == "JobCompleted" {
				os.Exit(1)
			}

			// wait a bit before fetching job status again, as fetching has costs associated with it on the backend
			time.Sleep(500 * time.Millisecond)
		}
	}
	return
}

func handleUploadFromLocalToBlobStorage(commandLineInput *common.CopyCmdArgsAndFlags,
	jobPartOrderToFill *common.CopyJobPartOrderRequest) bool {

	// set the source and destination type
	jobPartOrderToFill.SourceType = common.Local
	jobPartOrderToFill.DestinationType = common.Blob

	// attempt to parse the destination url
	destinationUrl, err := url.Parse(commandLineInput.Destination)
	if err != nil {
		// the destination should have already been validated, it would be surprising if it cannot be parsed at this point
		panic(err)
	}

	// list the source files and directories
	matches, err := filepath.Glob(commandLineInput.Source)
	if err != nil || len(matches) == 0 {
		fmt.Println("Cannot find source to upload.")
		return false
	}

	enumerator := newUploadTaskEnumerator(jobPartOrderToFill)
	err = enumerator.enumerate(matches, commandLineInput.Recursive, destinationUrl)

	if err != nil {
		fmt.Printf("Cannot start job due to error: %s.\n", err)
		return false
	} else {
		return true
	}
}

func handleDownloadFromBlobStorageToLocal(commandLineInput *common.CopyCmdArgsAndFlags,
	jobPartOrderToFill *common.CopyJobPartOrderRequest) bool {
	util := copyHandlerUtil{}

	// set the source and destination type
	jobPartOrderToFill.SourceType = common.Blob
	jobPartOrderToFill.DestinationType = common.Local

	// attempt to parse the container/blob url
	sourceUrl, err := url.Parse(commandLineInput.Source)
	if err != nil {
		panic(err)
	}
	sourcePathParts := strings.Split(sourceUrl.Path[1:], "/")

	destinationFileInfo, err := os.Stat(commandLineInput.Destination)
	// something is wrong with the destination, handle if it does not exist, else throw
	if err != nil {

		// create the destination if it does not exist
		if os.IsNotExist(err) {
			if len(sourcePathParts) < 2 { // create the directory if the source is a container
				err = os.MkdirAll(commandLineInput.Destination, os.ModePerm)
				if err != nil {
					panic("failed to create the destination on the local file system")
				}
			}

			destinationFileInfo, err = os.Stat(commandLineInput.Destination)
		} else {
			panic("cannot access destination, not a valid local file system path")
		}
	}

	// source is a single blob
	if len(sourcePathParts) > 1 {
		p := azblob.NewPipeline(azblob.NewAnonymousCredential(), azblob.PipelineOptions{})
		blobUrl := azblob.NewBlobURL(*sourceUrl, p)
		blobProperties, err := blobUrl.GetPropertiesAndMetadata(context.Background(), azblob.BlobAccessConditions{})
		if err != nil {
			panic("Cannot get blob properties")
		}

		//TODO figure out what to do when destination is dir for a single blob download
		//unless file info tells us, it is impossible to know whether the destination is a dir
		//if destinationFileInfo.IsDir() { // destination is dir, therefore the file name needs to be generated
		//	blobName := sourcePathParts[1]
		//	commandLineInput.Destination = path.Join(commandLineInput.Destination, blobName)
		//}

		singleTask := common.CopyTransfer{
			Source:           sourceUrl.String(),
			Destination:      commandLineInput.Destination,
			LastModifiedTime: blobProperties.LastModified(),
			SourceSize:       blobProperties.ContentLength(),
		}
		jobPartOrderToFill.Transfers = []common.CopyTransfer{singleTask}
		jobStarted, errorMsg := util.sendJobPartOrderToSTE(jobPartOrderToFill, 0, true)
		if !jobStarted {
			fmt.Println(fmt.Sprintf("copy job part order with JobId %s and part number %d failed because %s", jobPartOrderToFill.ID, jobPartOrderToFill.PartNum, errorMsg))
			return jobStarted
		}
	} else { // source is a container
		if !destinationFileInfo.IsDir() {
			panic("destination should be a directory")
		}

		p := azblob.NewPipeline(azblob.NewAnonymousCredential(), azblob.PipelineOptions{})
		containerUrl := azblob.NewContainerURL(*sourceUrl, p)
		// temporarily save the path of the container
		cleanContainerPath := sourceUrl.Path
		var Transfers []common.CopyTransfer
		partNumber := 0

		// iterate over the container
		for marker := (azblob.Marker{}); marker.NotDone(); {
			// Get a result segment starting with the blob indicated by the current Marker.
			listBlob, err := containerUrl.ListBlobs(context.Background(), marker, azblob.ListBlobsOptions{})
			if err != nil {
				log.Fatal(err)
			}
			marker = listBlob.NextMarker

			// Process the blobs returned in this result segment (if the segment is empty, the loop body won't execute)
			for _, blobInfo := range listBlob.Blobs.Blob {
				sourceUrl.Path = cleanContainerPath + "/" + blobInfo.Name
				Transfers = append(Transfers, common.CopyTransfer{Source: sourceUrl.String(), Destination: path.Join(commandLineInput.Destination, blobInfo.Name), LastModifiedTime: blobInfo.Properties.LastModified, SourceSize: *blobInfo.Properties.ContentLength})
			}
			jobPartOrderToFill.Transfers = Transfers
			jobStarted, errorMsg := util.sendJobPartOrderToSTE(jobPartOrderToFill, common.PartNumber(partNumber), !marker.NotDone())

			if !jobStarted {
				fmt.Println(fmt.Sprintf("copy job part order with JobId %s and part number %d failed because %s", jobPartOrderToFill.ID, jobPartOrderToFill.PartNum, errorMsg))
				return jobStarted
			}

			partNumber += 1
		}
	}
	// erase the blob type, as it does not matter
	commandLineInput.BlobType = ""
	return true
}
