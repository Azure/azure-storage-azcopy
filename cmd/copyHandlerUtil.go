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

package cmd

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/Azure/azure-storage-azcopy/common"
	"github.com/Azure/azure-storage-blob-go/2016-05-31/azblob"
	tm "github.com/buger/goterm"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"time"
	"encoding/binary"
	"encoding/base64"
)

const (
	NumOfFilesPerUploadJobPart = 1000
)

type copyHandlerUtil struct{}

// apply the flags from command line input to the job part order
func (copyHandlerUtil) applyFlags(copyArgs *common.CopyCmdArgsAndFlags, jobPartOrderToFill *common.CopyJobPartOrderRequest) {
	bt, err:= (common.BlobType{}).Parse(copyArgs.BlobType)
	if err != nil { return }	// TODO: Fix
	optionalAttributes := common.BlobTransferAttributes{
		BlobType:                 bt,
		BlockSizeinBytes:         copyArgs.BlockSize,
		ContentType:              copyArgs.ContentType,
		ContentEncoding:          copyArgs.ContentEncoding,
		Metadata:                 copyArgs.Metadata,
		NoGuessMimeType:          copyArgs.NoGuessMimeType,
		PreserveLastModifiedTime: copyArgs.PreserveLastModifiedTime,
	}

	jobPartOrderToFill.OptionalAttributes = optionalAttributes
	jobPartOrderToFill.LogVerbosity = common.LogLevel{ copyArgs.LogVerbosity}	// TODO: Fix via parsing
	jobPartOrderToFill.IsaBackgroundOp = copyArgs.IsaBackgroundOp
}

// checks whether a given url contains a prefix pattern
func (copyHandlerUtil) numOfStarInUrl(url string) int {
	return strings.Count(url, "*")
}

// checks if a given url points to a container, as opposed to a blob or prefix match
func (copyHandlerUtil) urlIsContainer(url *url.URL) bool {
	// if the path contains more than one "/", then it means it points to a blob, and not a container
	numOfSlashes := strings.Count(url.Path[1:], "/")

	if numOfSlashes == 0 {
		return true
	} else if numOfSlashes == 1 && url.Path[len(url.Path)-1:] == "/" { // this checks if container_name/ was given
		return true
	}
	return false
}

// append a file name to the container path to generate a blob path
func (copyHandlerUtil) generateBlobPath(destinationPath, fileName string) string {
	return fmt.Sprintf("%s/%s", destinationPath, fileName)
}

// get relative path given a root path
func (copyHandlerUtil) getRelativePath(rootPath, filePath string) string {
	// root path contains the entire absolute path to the root directory, so we need to take away everything except the root directory from filePath
	// example: rootPath = "/dir1/dir2/dir3" filePath = "/dir1/dir2/dir3/file1.txt" result = "dir3/file1.txt" scrubAway="/dir1/dir2/"

	var scrubAway string
	// test if root path finishes with a /, if yes, ignore it
	if rootPath[len(rootPath)-1:] == string(os.PathSeparator) {
		scrubAway = rootPath[:strings.LastIndex(rootPath[:len(rootPath)-1], string(os.PathSeparator))+1]
	} else {
		// +1 because we want to include the / at the end of the dir
		scrubAway = rootPath[:strings.LastIndex(rootPath, string(os.PathSeparator))+1]
	}

	result := strings.Replace(filePath, scrubAway, "", 1)

	// the back slashes need to be replaced with forward ones
	if os.PathSeparator == '\\' {
		result = strings.Replace(result, "\\", "/", -1)
	}
	return result
}

// this function can tell if a path represents a directory (must exist)
func (util copyHandlerUtil) isPathDirectory(pathString string) bool {
	// check if path exists
	destinationInfo, err := os.Stat(pathString)

	if err == nil && destinationInfo.IsDir() {
		return true
	}

	return false
}

func (util copyHandlerUtil) generateLocalPath(directoryPath, fileName string) string {
	var result string

	// check if the directory path ends with the path separator
	if strings.LastIndex(directoryPath, string(os.PathSeparator)) == len(directoryPath)-1 {
		result = fmt.Sprintf("%s%s", directoryPath, fileName)
	} else {
		result = fmt.Sprintf("%s%s%s", directoryPath, string(os.PathSeparator), fileName)
	}

	if os.PathSeparator == '\\' {
		return strings.Replace(result, "/", "\\", -1)
	}
	return result
}

func (util copyHandlerUtil) getBlobNameFromURL(path string) string {
	// return everything after the second /
	return strings.SplitAfterN(path[1:], "/", 2)[1]
}

func (util copyHandlerUtil) getContainerURLFromString(url url.URL) url.URL {
	containerName := strings.SplitAfterN(url.Path[1:], "/", 2)[0]
	url.Path = "/" + containerName
	return url
}

func (util copyHandlerUtil) generateBlobUrl(containerUrl url.URL, blobName string) string {
	containerUrl.Path = containerUrl.Path + blobName
	return containerUrl.String()
}

// for a given virtual directory, find the directory directly above the virtual file
func (util copyHandlerUtil) getLastVirtualDirectoryFromPath(path string) string {
	if path == "" {
		return ""
	}

	lastSlashIndex := strings.LastIndex(path, "/")
	if lastSlashIndex == -1 {
		return ""
	}

	return path[0:lastSlashIndex]
}

func (util copyHandlerUtil) blockIDIntToBase64 (blockID int) string {
	blockIDBinaryToBase64 := func(blockID []byte) string { return base64.StdEncoding.EncodeToString(blockID) }

	binaryBlockID := (&[4]byte{})[:] // All block IDs are 4 bytes long
	binary.LittleEndian.PutUint32(binaryBlockID, uint32(blockID))
	return blockIDBinaryToBase64(binaryBlockID)
}

func (util copyHandlerUtil) sendJobPartOrderToSTE(jobOrder *common.CopyJobPartOrderRequest, partNum common.PartNumber, isFinalPart bool) (bool, string) {
	jobOrder.PartNum = partNum
	jobOrder.IsFinalPart = isFinalPart

	for tryCount := 0; tryCount < 3; tryCount++ {
		resp, err := common.Rpc("copy", jobOrder)
		if err == nil {
			return util.parseCopyJobPartResponse(resp)
		} else {
			// in case the transfer engine has not finished booting up, we must wait
			time.Sleep(time.Duration(tryCount) * time.Second)
		}
	}
	return false, ""
}

func (copyHandlerUtil) fetchJobStatus(jobId common.JobID) string {
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

	tm.Println(fmt.Sprintf("Job Progress: %d %%", summary.PercentageProgress))
	tm.Println(fmt.Sprintf("Realtime Throughput: %f MB/s", summary.ThroughputInBytesPerSeconds/1024/1024))

	for index := 0; index < len(summary.FailedTransfers); index++ {
		message := fmt.Sprintf("transfer-%d	source: %s	destination: %s", index, summary.FailedTransfers[index].Src, summary.FailedTransfers[index].Dst)
		fmt.Println(message)
	}
	tm.Flush()

	return summary.JobStatus
}

func (copyHandlerUtil) parseCopyJobPartResponse(data []byte) (bool, string) {
	var copyJobPartResponse common.CopyJobPartOrderResponse
	err := json.Unmarshal(data, &copyJobPartResponse)
	if err != nil {
		panic(err)
	}
	return copyJobPartResponse.JobStarted, copyJobPartResponse.ErrorMsg
}

type uploadTaskEnumerator struct {
	jobPartOrderToFill *common.CopyJobPartOrderRequest
	transfers          []common.CopyTransfer
	partNumber         int
}

// return an upload task enumerator with a given job part order template
// uploadTaskEnumerator can walk through a list of files/directories and dispatch the job part orders using the template
func newUploadTaskEnumerator(jobPartOrderToFill *common.CopyJobPartOrderRequest) *uploadTaskEnumerator {
	enumerator := uploadTaskEnumerator{}
	enumerator.jobPartOrderToFill = jobPartOrderToFill
	return &enumerator
}

// accept a new transfer, if the threshold is reached, dispatch a job part order
func (enumerator *uploadTaskEnumerator) addTransfer(transfer common.CopyTransfer) error {
	enumerator.transfers = append(enumerator.transfers, transfer)

	// if the transfer to be added is a page blob, we need to validate its file size
	if enumerator.jobPartOrderToFill.OptionalAttributes.BlobType == common.PageBlob && transfer.SourceSize%512 != 0 {
		return errors.New(fmt.Sprintf("cannot perform upload for %s as a page blob because its size is not an exact multiple 512 bytes", transfer.Source))
	}

	// dispatch the transfers once the number reaches NumOfFilesPerUploadJobPart
	// we do this so that in the case of large uploads, the transfer engine can get started
	// while the frontend is still gathering more transfers
	if len(enumerator.transfers) == NumOfFilesPerUploadJobPart {
		enumerator.jobPartOrderToFill.Transfers = enumerator.transfers // use the template, replace the list of transfers with current list
		jobStarted, errorMsg := copyHandlerUtil{}.sendJobPartOrderToSTE(enumerator.jobPartOrderToFill, common.PartNumber(enumerator.partNumber), false)
		if !jobStarted {
			return errors.New(fmt.Sprintf("copy job part order with JobId %s and part number %d failed because %s", enumerator.jobPartOrderToFill.ID, enumerator.jobPartOrderToFill.PartNum, errorMsg))
		}
		enumerator.transfers = []common.CopyTransfer{}
		enumerator.partNumber += 1
	}

	return nil
}

// we need to send a last part with isFinalPart set to true, along with whatever transfers that still haven't been sent
func (enumerator *uploadTaskEnumerator) dispatchFinalPart() error {
	if len(enumerator.transfers) != 0 {
		enumerator.jobPartOrderToFill.Transfers = enumerator.transfers
	} else {
		enumerator.jobPartOrderToFill.Transfers = []common.CopyTransfer{}
	}
	jobStarted, errorMsg := copyHandlerUtil{}.sendJobPartOrderToSTE(enumerator.jobPartOrderToFill, common.PartNumber(enumerator.partNumber), true)
	if !jobStarted {
		return errors.New(fmt.Sprintf("copy job part order with JobId %s and part number %d failed because %s", enumerator.jobPartOrderToFill.ID, enumerator.jobPartOrderToFill.PartNum, errorMsg))
	}

	return nil
}

// this function accepts the list of files/directories to transfer and processes them
func (enumerator *uploadTaskEnumerator) enumerate(listOfFilesAndDirectories []string, isRecursiveOn bool, destinationUrl *url.URL) error {
	util := copyHandlerUtil{}

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

			err = enumerator.addTransfer(common.CopyTransfer{
				Source:           listOfFilesAndDirectories[0],
				Destination:      destinationUrl.String(),
				LastModifiedTime: f.ModTime(),
				SourceSize:       f.Size(),
			})

			if err != nil {
				return err
			}
			return enumerator.dispatchFinalPart()
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
						destinationUrl.Path = util.generateBlobPath(cleanContainerPath, util.getRelativePath(fileOrDirectoryPath, pathToFile))
						err = enumerator.addTransfer(common.CopyTransfer{
							Source:           pathToFile,
							Destination:      destinationUrl.String(),
							LastModifiedTime: f.ModTime(),
							SourceSize:       f.Size(),
						})
						if err != nil {
							return err
						}
					}
					return nil
				})
			} else if !f.IsDir() {
				// files are uploaded using their file name as blob name
				destinationUrl.Path = util.generateBlobPath(cleanContainerPath, f.Name())
				err = enumerator.addTransfer(common.CopyTransfer{
					Source:           fileOrDirectoryPath,
					Destination:      destinationUrl.String(),
					LastModifiedTime: f.ModTime(),
					SourceSize:       f.Size(),
				})
				if err != nil {
					return err
				}
			}
		}
	}

	if enumerator.partNumber == 0 && len(enumerator.transfers) == 0 {
		return errors.New("nothing can be uploaded, please use --recursive to upload directories")
	}
	return enumerator.dispatchFinalPart()
}

type downloadTaskEnumerator struct {
	jobPartOrderToFill *common.CopyJobPartOrderRequest
	transfers          []common.CopyTransfer
	partNumber         int
}

// return a download task enumerator with a given job part order template
// downloadTaskEnumerator can walk through the list of blobs requested and dispatch the job part orders using the template
func newDownloadTaskEnumerator(jobPartOrderToFill *common.CopyJobPartOrderRequest) *downloadTaskEnumerator {
	enumerator := downloadTaskEnumerator{}
	enumerator.jobPartOrderToFill = jobPartOrderToFill
	return &enumerator
}

// this function accepts a url (with or without *) to blobs for download and processes them
func (enumerator *downloadTaskEnumerator) enumerate(sourceUrlString string, isRecursiveOn bool, destinationPath string) error {
	util := copyHandlerUtil{}
	p := azblob.NewPipeline(azblob.NewAnonymousCredential(), azblob.PipelineOptions{})

	// attempt to parse the source url
	sourceUrl, err := url.Parse(sourceUrlString)
	if err != nil {
		return errors.New("cannot parse source URL")
	}

	// get the container url to be used later for listing
	literalContainerUrl := util.getContainerURLFromString(*sourceUrl)
	containerUrl := azblob.NewContainerURL(literalContainerUrl, p)

	// check if the given url is a container
	if util.urlIsContainer(sourceUrl) {
		return errors.New("cannot download an entire container, use prefix match with a * at the end of path instead")
	}

	numOfStarInUrlPath := util.numOfStarInUrl(sourceUrl.Path)
	if numOfStarInUrlPath == 1 { // prefix search

		// the * must be at the end of the path
		if strings.LastIndex(sourceUrl.Path, "*") != len(sourceUrl.Path)-1 {
			return errors.New("the * in the source URL must be at the end of the path")
		}

		// the destination must be a directory, otherwise we don't know where to put the files
		if !util.isPathDirectory(destinationPath) {
			return errors.New("the destination must be an existing directory in this download scenario")
		}

		// get the search prefix to query the service
		searchPrefix := util.getBlobNameFromURL(sourceUrl.Path)
		searchPrefix = searchPrefix[:len(searchPrefix)-1] // strip away the * at the end

		closestVirtualDirectory := util.getLastVirtualDirectoryFromPath(searchPrefix)

		// strip away the leading / in the closest virtual directory
		if len(closestVirtualDirectory) > 0 && closestVirtualDirectory[0:1] == "/" {
			closestVirtualDirectory = closestVirtualDirectory[1:]
		}

		// perform a list blob
		for marker := (azblob.Marker{}); marker.NotDone(); {
			// look for all blobs that start with the prefix
			listBlob, err := containerUrl.ListBlobs(context.Background(), marker, azblob.ListBlobsOptions{Prefix: searchPrefix})
			if err != nil {
				return errors.New("cannot list blobs for download")
			}

			// Process the blobs returned in this result segment (if the segment is empty, the loop body won't execute)
			for _, blobInfo := range listBlob.Blobs.Blob {
				blobNameAfterPrefix := blobInfo.Name[len(closestVirtualDirectory):]
				if !isRecursiveOn && strings.Contains(blobNameAfterPrefix, "/") {
					continue
				}

				enumerator.addTransfer(common.CopyTransfer{
					Source:           util.generateBlobUrl(literalContainerUrl, blobInfo.Name),
					Destination:      util.generateLocalPath(destinationPath, blobNameAfterPrefix),
					LastModifiedTime: blobInfo.Properties.LastModified,
					SourceSize:       *blobInfo.Properties.ContentLength})
			}

			marker = listBlob.NextMarker
			err = enumerator.dispatchPart(false)
			if err != nil {
				return err
			}
		}

		err = enumerator.dispatchPart(true)
		if err != nil {
			return err
		}

	} else if numOfStarInUrlPath == 0 { // no prefix search

		// see if source blob exists
		blobUrl := azblob.NewBlobURL(*sourceUrl, p)
		blobProperties, err := blobUrl.GetPropertiesAndMetadata(context.Background(), azblob.BlobAccessConditions{})

		// for a single blob, the destination can either be a file or a directory
		var singleBlobDestinationPath string
		if util.isPathDirectory(destinationPath) {
			singleBlobDestinationPath = util.generateLocalPath(destinationPath, util.getBlobNameFromURL(sourceUrl.Path))
		} else {
			singleBlobDestinationPath = destinationPath
		}

		// if the single blob exists, upload it
		if err == nil {
			enumerator.addTransfer(common.CopyTransfer{
				Source:           sourceUrl.String(),
				Destination:      singleBlobDestinationPath,
				LastModifiedTime: blobProperties.LastModified(),
				SourceSize:       blobProperties.ContentLength(),
			})

			err = enumerator.dispatchPart(false)
			if err != nil {
				return err
			}
		} else if err != nil && !isRecursiveOn {
			return errors.New("cannot get source blob properties, make sure it exists, for virtual directory download please use --recursive")
		}

		// if recursive happens to be turned on, then we will attempt to download a virtual directory
		if isRecursiveOn {
			// recursively download everything that is under the given path, that is a virtual directory
			searchPrefix := util.getBlobNameFromURL(sourceUrl.Path)

			// if the user did not specify / at the end of the virtual directory, add it before doing the prefix search
			if strings.LastIndex(searchPrefix, "/") != len(searchPrefix) - 1 {
				searchPrefix += "/"
			}

			// the destination must be a directory, otherwise we don't know where to put the files
			if !util.isPathDirectory(destinationPath) {
				return errors.New("the destination must be an existing directory in this download scenario")
			}

			// perform a list blob
			for marker := (azblob.Marker{}); marker.NotDone(); {
				// look for all blobs that start with the prefix, so that if a blob is under the virtual directory, it will show up
				listBlob, err := containerUrl.ListBlobs(context.Background(), marker, azblob.ListBlobsOptions{Prefix: searchPrefix})
				if err != nil {
					return errors.New("cannot list blobs for download")
				}

				// Process the blobs returned in this result segment (if the segment is empty, the loop body won't execute)
				for _, blobInfo := range listBlob.Blobs.Blob {
					enumerator.addTransfer(common.CopyTransfer{
						Source:           util.generateBlobUrl(literalContainerUrl, blobInfo.Name),
						Destination:      util.generateLocalPath(destinationPath, util.getRelativePath(searchPrefix, blobInfo.Name)),
						LastModifiedTime: blobInfo.Properties.LastModified,
						SourceSize:       *blobInfo.Properties.ContentLength})
				}

				marker = listBlob.NextMarker
				err = enumerator.dispatchPart(false)
				if err != nil {
					return err
				}
			}
		}

		err = enumerator.dispatchPart(true)
		if err != nil {
			return err
		}

	} else { // more than one * is not supported
		return errors.New("only one * is allowed in the source URL")
	}
	return nil
}

// accept a new transfer, simply add to the list of transfers and wait for the dispatch call to send the order
func (enumerator *downloadTaskEnumerator) addTransfer(transfer common.CopyTransfer) {
	enumerator.transfers = append(enumerator.transfers, transfer)
}

// send the current list of transfer to the STE
func (enumerator *downloadTaskEnumerator) dispatchPart(isFinalPart bool) error {
	// if the job is empty, throw an error
	if !isFinalPart && len(enumerator.transfers) == 0 {
		return errors.New("cannot initiate empty job, please make sure source is not empty")
	}

	// add the transfers and part number to template
	enumerator.jobPartOrderToFill.Transfers = enumerator.transfers
	enumerator.jobPartOrderToFill.PartNum = common.PartNumber(enumerator.partNumber)

	jobStarted, errorMsg := copyHandlerUtil{}.sendJobPartOrderToSTE(enumerator.jobPartOrderToFill, common.PartNumber(enumerator.partNumber), isFinalPart)
	if !jobStarted {
		return errors.New(fmt.Sprintf("copy job part order with JobId %s and part number %d failed because %s", enumerator.jobPartOrderToFill.ID, enumerator.jobPartOrderToFill.PartNum, errorMsg))
	}

	// empty the transfers and increment part number count
	enumerator.transfers = []common.CopyTransfer{}
	enumerator.partNumber += 1
	return nil
}
