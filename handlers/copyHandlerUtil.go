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
	"encoding/json"
	"errors"
	"fmt"
	"github.com/Azure/azure-storage-azcopy/common"
	tm "github.com/buger/goterm"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"time"
)

const (
	NumOfFilesPerUploadJobPart = 1000
)

type copyHandlerUtil struct{}

// apply the flags from command line input to the job part order
func (copyHandlerUtil) applyFlags(commandLineInput *common.CopyCmdArgsAndFlags, jobPartOrderToFill *common.CopyJobPartOrderRequest) {
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

// checks whether a given url contains a prefix pattern
func (copyHandlerUtil) urlContainsMagic(url string) bool {
	return strings.ContainsAny(url, `*`)
}

// checks if a given url points to a container, as opposed to a blob
func (copyHandlerUtil) urlIsContainer(url *url.URL) bool {
	// if the path contains more than one "/", then it means it points to a blob, and not a container
	return !strings.Contains(url.Path[1:], "/")
}

// append a file name to the container path to generate a blob path
func (copyHandlerUtil) generateBlobPath(destinationPath, fileName string) string {
	return fmt.Sprintf("%s/%s", destinationPath, fileName)
}

// get relative path given a root path
func (copyHandlerUtil) getRelativePath(rootPath, filePath string) string {
	// root path contains the entire absolute path to the root directory, so we need to take away everything except the root directory from filePath
	// example: rootPath = "/dir1/dir2/dir3" filePath = "/dir1/dir2/dir3/file1.txt" result = "dir3/file1.txt" scrubAway="/dir1/dir2/"

	// +1 because we want to include the / at the end of the dir
	scrubAway := rootPath[:strings.LastIndex(rootPath, string(os.PathSeparator))+1]

	result := strings.Replace(filePath, scrubAway, "", 1)

	// the back slashes need to be replaced with forward ones
	if os.PathSeparator == '\\' {
		result = strings.Replace(result, "\\", "/", -1)
	}
	return result
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
	if enumerator.jobPartOrderToFill.OptionalAttributes.BlobType == common.PageBlob && transfer.SourceSize % 512 != 0 {
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

// this function accepts the list of files/directories to transfer and process them
func (enumerator *uploadTaskEnumerator) enumerate(listOfFilesAndDirectories []string, isRecursiveOn bool, destinationUrl *url.URL) error {
	util := copyHandlerUtil{}

	// when a single file is being uploaded, we need to treat this case differently, as the destinationUrl might be a blob
	if len(listOfFilesAndDirectories) == 1 {
		f, err := os.Stat(listOfFilesAndDirectories[0])
		if err != nil {
			return errors.New("cannot find source to upload")
		}

		if !f.IsDir() {
			// append file name as blob name in case the given URL is a blob
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
