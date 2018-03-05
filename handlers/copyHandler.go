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
	"encoding/json"
	"fmt"
	"github.com/Azure/azure-storage-azcopy/common"
	"github.com/Azure/azure-storage-blob-go/2016-05-31/azblob"
	tm "github.com/buger/goterm"
	"io/ioutil"
	"log"
	"net/url"
	"os"
	"os/signal"
	"path"
	"strings"
	"time"
)

const (
	NumOfFilesPerUploadJobPart = 1000
)

// handles the copy command
// dispatches the job order (in parts) to the storage engine
func HandleCopyCommand(commandLineInput common.CopyCmdArgsAndFlags) {
	jobPartOrder := common.CopyJobPartOrderRequest{}
	ApplyFlags(&commandLineInput, &jobPartOrder)

	// generate job id
	jobId := common.JobID(common.NewUUID())
	jobStarted := true

	if jobPartOrder.OptionalAttributes.BlobType == common.InvalidBlob {
		fmt.Println("invalid blob type passed. Please enter the valid blob type - BlockBlob, AppendBlob, PageBlob")
		return
	}
	jobPartOrder.ID = jobId

	if commandLineInput.SourceType == common.Local && commandLineInput.DestinationType == common.Blob {
		jobStarted = HandleUploadFromLocalToBlobStorage(&commandLineInput, &jobPartOrder)
	} else if commandLineInput.SourceType == common.Blob && commandLineInput.DestinationType == common.Local {
		jobStarted = HandleDownloadFromBlobStorageToLocal(&commandLineInput, &jobPartOrder)
	}
	if !jobStarted {
		fmt.Println("Job with id", jobId, "was not able to start. Please try again.")
		return
	}

	fmt.Println("Job with id", jobId, "has started.")
	if commandLineInput.IsaBackgroundOp {
		return
	}

	// created a signal channel to receive the Interrupt and Kill signal send to OS
	cancelChannel := make(chan os.Signal, 1)
	// cancelChannel will be notified when os receives os.Interrupt and os.Kill signals
	signal.Notify(cancelChannel, os.Interrupt, os.Kill)

	// timeOut channel will receive a message after every 2 seconds
	//timeOut := time.After(2 * time.Second)

	// Waiting for signals from either cancelChannel or timeOut Channel. If no signal received, will sleep for 100 milliseconds
	for {
		select {
		case <-cancelChannel:
			fmt.Println("Cancelling Job")
			HandleCancelCommand(jobId.String())
			os.Exit(1)
		default:
			jobStatus := fetchJobStatus(jobId)
			if jobStatus == "JobCompleted" {
				os.Exit(1)
			}
			//time.Sleep(1 * time.Second)
			time.Sleep(500 * time.Millisecond)
		}
	}
	return
}

func HandleUploadFromLocalToBlobStorage(commandLineInput *common.CopyCmdArgsAndFlags,
	jobPartOrderToFill *common.CopyJobPartOrderRequest) bool {

	fmt.Println("HandleUploadFromLocalToWastore startTime ", time.Now())
	// set the source and destination type
	jobPartOrderToFill.SourceType = common.Local
	jobPartOrderToFill.DestinationType = common.Blob

	sourceFileInfo, err := os.Stat(commandLineInput.Source)

	// since source was already validated, it would be surprising if file/directory cannot be accessed at this point
	if err != nil {
		panic("cannot access source, not a valid local file system path")
	}

	// attempt to parse the destination url
	destinationUrl, err := url.Parse(commandLineInput.Destination)
	if err != nil {
		panic(err)
	}

	// TODO add source id = last modified time
	// uploading entire directory to Azure Storage
	// listing needs to be performed
	if sourceFileInfo.IsDir() {
		files, err := ioutil.ReadDir(commandLineInput.Source)

		// since source was already validated, it would be surprising if file/directory cannot be accessed at this point
		if err != nil {
			panic("cannot access source, not a valid local file system path")
		}

		// make sure this is a container url
		//TODO root container handling
		if strings.Contains(destinationUrl.Path[1:], "/") {
			panic("destination is not a valid container url")
		}

		// temporarily save the path of the container
		cleanContainerPath := destinationUrl.Path
		var Transfers []common.CopyTransfer
		numInTransfers := 0
		partNumber := 0

		for _, f := range files {
			if !f.IsDir() {
				destinationUrl.Path = fmt.Sprintf("%s/%s", cleanContainerPath, f.Name())
				Transfers = append(Transfers, common.CopyTransfer{
					Source:           path.Join(commandLineInput.Source, f.Name()),
					Destination:      destinationUrl.String(),
					LastModifiedTime: f.ModTime(),
					SourceSize:       f.Size(),
				})
				numInTransfers += 1

				if numInTransfers == NumOfFilesPerUploadJobPart {
					jobPartOrderToFill.Transfers = Transfers //TODO make truth, more defensive, consider channel
					jobPartOrderToFill.PartNum = common.PartNumber(partNumber)
					partNumber += 1
					jobStarted, errorMsg := sendJobPartOrderToSTE(jobPartOrderToFill)
					if !jobStarted {
						fmt.Println(fmt.Sprintf("copy job part order with JobId %s and part number %d failed because %s", jobPartOrderToFill.ID, jobPartOrderToFill.PartNum, errorMsg))
						return jobStarted
					}
					Transfers = []common.CopyTransfer{}
					numInTransfers = 0
				}
			}
		}

		if numInTransfers != 0 {
			jobPartOrderToFill.Transfers = Transfers
		} else {
			jobPartOrderToFill.Transfers = []common.CopyTransfer{}
		}
		jobPartOrderToFill.PartNum = common.PartNumber(partNumber)
		jobPartOrderToFill.IsFinalPart = true
		jobStarted, errorMsg := sendJobPartOrderToSTE(jobPartOrderToFill)
		if !jobStarted {
			fmt.Println(fmt.Sprintf("copy job part order with JobId %s and part number %d failed because %s", jobPartOrderToFill.ID, jobPartOrderToFill.PartNum, errorMsg))
			return jobStarted
		}

	} else { // upload single file

		// if a container url is given, must append file name to it
		if !strings.Contains(destinationUrl.Path[1:], "/") {
			destinationUrl.Path = fmt.Sprintf("%s/%s", destinationUrl.Path, sourceFileInfo.Name())
		}
		//fmt.Println("Upload", path.Join(commandLineInput.Source), "to", destinationUrl.String(), "with size", sourceFileInfo.Size())
		singleTask := common.CopyTransfer{
			Source:           commandLineInput.Source,
			Destination:      destinationUrl.String(),
			LastModifiedTime: sourceFileInfo.ModTime(),
			SourceSize:       sourceFileInfo.Size(),
		}
		jobPartOrderToFill.Transfers = []common.CopyTransfer{singleTask}
		jobPartOrderToFill.PartNum = 0
		jobPartOrderToFill.IsFinalPart = true
		jobStarted, errorMsg := sendJobPartOrderToSTE(jobPartOrderToFill)
		if !jobStarted {
			fmt.Println(fmt.Sprintf("copy job part order with JobId %s and part number %d failed because %s", jobPartOrderToFill.ID, jobPartOrderToFill.PartNum, errorMsg))
			return jobStarted
		}
	}
	return true
}

func HandleDownloadFromBlobStorageToLocal(commandLineInput *common.CopyCmdArgsAndFlags,
	jobPartOrderToFill *common.CopyJobPartOrderRequest) bool {
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
		jobPartOrderToFill.IsFinalPart = true
		jobPartOrderToFill.PartNum = 0

		jobStarted, errorMsg := sendJobPartOrderToSTE(jobPartOrderToFill)
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
			jobPartOrderToFill.PartNum = common.PartNumber(partNumber)
			partNumber += 1
			if !marker.NotDone() { // if there is no more segment
				jobPartOrderToFill.IsFinalPart = true
			}
			jobStarted, errorMsg := sendJobPartOrderToSTE(jobPartOrderToFill)
			if !jobStarted {
				fmt.Println(fmt.Sprintf("copy job part order with JobId %s and part number %d failed because %s", jobPartOrderToFill.ID, jobPartOrderToFill.PartNum, errorMsg))
				return jobStarted
			}
		}
	}
	// erase the blob type, as it does not matter
	commandLineInput.BlobType = ""
	return true
}

func ApplyFlags(commandLineInput *common.CopyCmdArgsAndFlags, jobPartOrderToFill *common.CopyJobPartOrderRequest) {
	optionalAttributes := common.BlobTransferAttributes{
		BlobType:                 common.BlobTypeStringToBlobType(commandLineInput.BlobType),
		BlockSizeinBytes:         commandLineInput.BlockSize,
		ContentType:              commandLineInput.ContentType,
		ContentEncoding:          commandLineInput.ContentEncoding,
		Metadata:                 commandLineInput.Metadata,
		NoGuessMimeType:          commandLineInput.NoGuessMimeType,
		PreserveLastModifiedTime: commandLineInput.PreserveLastModifiedTime,
	}

	jobPartOrderToFill.OptionalAttributes = optionalAttributes
	jobPartOrderToFill.LogVerbosity = common.LogLevel(commandLineInput.LogVerbosity)
	jobPartOrderToFill.IsaBackgroundOp = commandLineInput.IsaBackgroundOp
}

func sendJobPartOrderToSTE(jobOrder *common.CopyJobPartOrderRequest) (bool, string) {

	for tryCount := 0; tryCount < 3; tryCount++ {
		resp, err := common.Rpc("copy", jobOrder)
		if err == nil {
			return parseCopyJobPartResponse(resp)
		} else {
			// in case the transfer engine has not finished booting up, we must wait
			time.Sleep(time.Duration(tryCount) * time.Second)
		}
	}
	return false, ""
}

func fetchJobStatus(jobId common.JobID) string {
	lsCommand := common.ListRequest{JobId: jobId}

	responseBytes, _ := common.Rpc("listJobProgressSummary", lsCommand)

	if len(responseBytes) == 0 {
		return ""
	}
	var summary common.ListJobSummaryResponse
	json.Unmarshal(responseBytes, &summary)

	tm.Clear()
	tm.MoveCursor(1, 1)

	fmt.Println("----------------- Progress Summary for JobId ", jobId, "------------------")
	tm.Println("Total Number of Transfers: ", summary.TotalNumberOfTransfers)
	tm.Println("Total Number of Transfers Completed: ", summary.TotalNumberofTransferCompleted)
	tm.Println("Total Number of Transfers Failed: ", summary.TotalNumberofFailedTransfer)
	tm.Println("Job order fully received: ", summary.CompleteJobOrdered)

	//tm.Println(tm.Background(tm.Color(tm.Bold(fmt.Sprintf("Job Progress: %d %%", summary.PercentageProgress)), tm.WHITE), tm.GREEN))
	//tm.Println(tm.Background(tm.Color(tm.Bold(fmt.Sprintf("Realtime Throughput: %f MB/s", summary.ThroughputInBytesPerSeconds/1024/1024)), tm.WHITE), tm.BLUE))

	tm.Println(fmt.Sprintf("Job Progress: %d %%", summary.PercentageProgress))
	tm.Println(fmt.Sprintf("Realtime Throughput: %f MB/s", summary.ThroughputInBytesPerSeconds/1024/1024))

	for index := 0; index < len(summary.FailedTransfers); index++ {
		message := fmt.Sprintf("transfer-%d	source: %s	destination: %s", index, summary.FailedTransfers[index].Src, summary.FailedTransfers[index].Dst)
		fmt.Println(message)
	}
	tm.Flush()

	return summary.JobStatus
}

func parseCopyJobPartResponse(data []byte) (bool, string) {
	var copyJobPartResponse common.CopyJobPartOrderResponse
	err := json.Unmarshal(data, &copyJobPartResponse)
	if err != nil {
		panic(err)
	}
	return copyJobPartResponse.JobStarted, copyJobPartResponse.ErrorMsg
}
