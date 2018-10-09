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

package ste

import (
	"bytes"
	"context"
	"fmt"
	"net/http"
	"net/url"
	"os"
	"strings"
	"unsafe"

	"github.com/Azure/azure-pipeline-go/pipeline"
	"github.com/Azure/azure-storage-azcopy/common"
	"github.com/Azure/azure-storage-file-go/2017-07-29/azfile"
)

func LocalToFile(jptm IJobPartTransferMgr, p pipeline.Pipeline, pacer *pacer) {

	// step 1: Get info from transfer.
	info := jptm.Info()

	u, _ := url.Parse(info.Destination)

	fileURL := azfile.NewFileURL(*u, p)

	fileSize := int64(info.SourceSize)

	chunkSize := int64(info.BlockSize)
	// If the given chunk Size for the Job is greater than maximum file chunk size i.e 4 MB
	// then chunk size will be 4 MB.
	if chunkSize > common.DefaultAzureFileChunkSize {
		chunkSize = common.DefaultAzureFileChunkSize
		if jptm.ShouldLog(pipeline.LogWarning) {
			jptm.Log(pipeline.LogWarning,
				fmt.Sprintf("Block size %d larger than maximum file chunk size, 4 MB chunk size used", info.BlockSize))
		}
	}

	if jptm.ShouldLog(pipeline.LogInfo) {
		jptm.LogTransferStart(info.Source, info.Destination, fmt.Sprintf("Chunk size %d", chunkSize))
	}

	// If the transfer was cancelled, then reporting transfer as done and increasing the bytestransferred by the size of the source.
	if jptm.WasCanceled() {
		jptm.ReportTransferDone()
		return
	}

	// If the force Write flags is set to false
	// then check the file exists or not.
	// If it does, mark transfer as failed.
	if !jptm.IsForceWriteTrue() {
		_, err := fileURL.GetProperties(jptm.Context())
		if err == nil {
			// If the error is nil, then blob exists and it doesn't needs to be uploaded.
			jptm.LogUploadError(info.Source, info.Destination, "File already exists ", 0)
			// Mark the transfer as failed with FileAlreadyExistsFailure
			jptm.SetStatus(common.ETransferStatus.FileAlreadyExistsFailure())
			jptm.ReportTransferDone()
			return
		}
	}

	// step 2: Map file upload before transferring chunks and get info from map file.
	srcFile, err := os.Open(info.Source)
	if err != nil {
		jptm.LogUploadError(info.Source, info.Destination, "File Open Error "+err.Error(), 0)
		jptm.SetStatus(common.ETransferStatus.Failed())
		jptm.ReportTransferDone()
		return
	}
	srcFileInfo, err := srcFile.Stat()
	if err != nil {
		jptm.LogUploadError(info.Source, info.Destination, "File Stat Error "+err.Error(), 0)
		jptm.SetStatus(common.ETransferStatus.Failed())
		jptm.ReportTransferDone()
		return
	}

	byteLength := common.Iffint64(srcFileInfo.Size() > 512, 512, srcFileInfo.Size())
	byteBuffer := make([]byte, byteLength)
	_, err = srcFile.Read(byteBuffer)

	// Get http headers and meta data of file.
	fileHTTPHeaders, metaData := jptm.FileDstData(byteBuffer)

	// step 3: Create parent directories and file.
	// 3a: Create the parent directories of the file. Note share must be existed, as the files are listed from share or directory.
	err = createParentDirToRoot(jptm.Context(), fileURL, p)
	if err != nil {
		jptm.LogUploadError(info.Source, info.Destination, "Parent Directory Create Error "+err.Error(), 0)
		jptm.Cancel()
		jptm.SetStatus(common.ETransferStatus.Failed())
		jptm.ReportTransferDone()
		srcFile.Close()
		return
	}

	// 3b: Create Azure file with the source size.
	_, err = fileURL.Create(jptm.Context(), fileSize, fileHTTPHeaders, metaData)
	if err != nil {
		status, msg := ErrorEx{err}.ErrorCodeAndString()
		jptm.LogUploadError(info.Source, info.Destination, "File Create Error "+msg, status)
		jptm.Cancel()
		jptm.SetStatus(common.ETransferStatus.Failed())
		jptm.SetErrorCode(int32(status))
		jptm.ReportTransferDone()
		srcFile.Close()
		return
	}

	// If the file size is 0, scheduling chunk msgs for UploadRange is not required
	if info.SourceSize == 0 {
		// mark the transfer as successful
		jptm.SetStatus(common.ETransferStatus.Success())
		jptm.ReportTransferDone()
		return
	}

	numChunks := uint32(0)
	if rem := fileSize % chunkSize; rem == 0 {
		numChunks = uint32(fileSize / chunkSize)
	} else {
		numChunks = uint32(fileSize/chunkSize) + 1
	}

	jptm.SetNumberOfChunks(numChunks)
	// step 4: Scheduling range update to the object created in Step 3
	for startIndex := int64(0); startIndex < fileSize; startIndex += chunkSize {
		adjustedChunkSize := chunkSize

		// compute actual size of the chunk
		if startIndex+chunkSize > fileSize {
			adjustedChunkSize = fileSize - startIndex
		}

		// schedule the chunk job/msg
		jptm.ScheduleChunks(fileUploadFunc(jptm, srcFile, fileURL, pacer, startIndex, adjustedChunkSize))
	}
}

func fileUploadFunc(jptm IJobPartTransferMgr, srcFile *os.File, fileURL azfile.FileURL, pacer *pacer, startRange int64, pageSize int64) chunkFunc {
	info := jptm.Info()
	return func(workerId int) {
		// rangeDone is the function called after success / failure of each range.
		// If the calling range is the last range of transfer, then it updates the transfer status,
		// mark transfer done, unmap the source memory map and close the source file descriptor.
		rangeDone := func() {
			if lastPage, _ := jptm.ReportChunkDone(); lastPage {
				if jptm.ShouldLog(pipeline.LogDebug) {
					jptm.Log(pipeline.LogDebug, "Finalizing transfer")
				}
				jptm.SetStatus(common.ETransferStatus.Success())
				err := srcFile.Close()
				if err != nil {
					jptm.LogError(info.Source, "File Close Error ", err)
				}
				// If the transfer status is less than or equal to 0
				// then transfer was either failed or cancelled
				// the file created in share needs to be deleted
				if jptm.TransferStatus() <= 0 {
					_, err = fileURL.Delete(context.TODO())
					if err != nil {
						if jptm.ShouldLog(pipeline.LogError) {
							jptm.Log(pipeline.LogInfo, fmt.Sprintf("error deleting the file %s. Failed with error %s", fileURL.String(), err.Error()))
						}
					}
				}
				jptm.ReportTransferDone()
			}
		}

		srcMMF, err := common.NewMMF(srcFile, false, startRange, pageSize)
		if err != nil {
			if err != nil {
				if jptm.WasCanceled() {
					if jptm.ShouldLog(pipeline.LogDebug) {
						jptm.Log(pipeline.LogDebug,
							fmt.Sprintf("Failed to UploadRange from %d to %d, transfer was cancelled", startRange, startRange+pageSize))
					}
				} else {
					status, msg := ErrorEx{err}.ErrorCodeAndString()
					jptm.LogUploadError(info.Source, info.Destination, "Upload Range Error "+msg, status)
					// cancelling the transfer
					jptm.Cancel()
					jptm.SetStatus(common.ETransferStatus.Failed())
					jptm.SetErrorCode(int32(status))
				}
				rangeDone()
				return
			}
		}
		if jptm.WasCanceled() {
			if jptm.ShouldLog(pipeline.LogDebug) {
				jptm.Log(pipeline.LogDebug,
					fmt.Sprintf("Range %d not picked, transfer is cancelled", startRange))
			}
			rangeDone()
		} else {
			// rangeBytes is the byte slice of Page for the given range range
			rangeBytes := srcMMF.Slice()
			// converted the bytes slice to int64 array.
			// converting each of 8 bytes of byteSlice to an integer.
			int64Slice := (*(*[]int64)(unsafe.Pointer(&rangeBytes)))[:len(rangeBytes)/8]

			allBytesZero := true
			// Iterating though each integer of in64 array to check if any of the number is greater than 0 or not.
			// If any no is greater than 0, it means that the 8 bytes slice represented by that integer has atleast one byte greater than 0
			// If all integers are 0, it means that the 8 bytes slice represented by each integer has no byte greater than 0
			for index := 0; index < len(int64Slice); index++ {
				if int64Slice[index] != 0 {
					// If one number is greater than 0, then we need to perform the PutPage update.
					allBytesZero = false
					break
				}
			}

			// If all the bytes in the rangeBytes is 0, then we do not need to perform the PutPage
			// Updating number of chunks done.
			if allBytesZero {
				if jptm.ShouldLog(pipeline.LogDebug) {
					jptm.Log(pipeline.LogDebug,
						fmt.Sprintf("Not uploading range from %d to %d,  all bytes are zero", startRange, startRange+pageSize))
				}
				rangeDone()
				return
			}

			body := newRequestBodyPacer(bytes.NewReader(rangeBytes), pacer, srcMMF)
			_, err := fileURL.UploadRange(jptm.Context(), startRange, body)
			if err != nil {
				if jptm.WasCanceled() {
					if jptm.ShouldLog(pipeline.LogDebug) {
						jptm.Log(pipeline.LogDebug,
							fmt.Sprintf("Failed to UploadRange from %d to %d, transfer was cancelled", startRange, startRange+pageSize))
					}
				} else {
					status, msg := ErrorEx{err}.ErrorCodeAndString()
					jptm.LogUploadError(info.Source, info.Destination, "Upload Range Error "+msg, status)
					// cancelling the transfer
					jptm.Cancel()
					jptm.SetStatus(common.ETransferStatus.Failed())
					jptm.SetErrorCode(int32(status))
				}
				rangeDone()
				return
			}
			if jptm.ShouldLog(pipeline.LogInfo) {
				jptm.Log(pipeline.LogInfo, "UPLOAD SUCCESSFUL")
			}
			rangeDone()
		}
	}
}

// getParentDirectoryURL gets parent directory URL of an Azure FileURL.
func getParentDirectoryURL(fileURL azfile.FileURL, p pipeline.Pipeline) azfile.DirectoryURL {
	u := fileURL.URL()
	u.Path = u.Path[:strings.LastIndex(u.Path, "/")]
	return azfile.NewDirectoryURL(u, p)
}

// verifyAndHandleCreateErrors handles create errors, StatusConflict is ignored, as specific level directory could be existing.
// Report http.StatusForbidden, as user should at least have read and write permission of the destination,
// and there is no permission on directory level, i.e. create directory is a general permission for each level diretories for Azure file.
func verifyAndHandleCreateErrors(err error) error {
	if err != nil {
		sErr := err.(azfile.StorageError)
		if sErr != nil && sErr.Response() != nil &&
			(sErr.Response().StatusCode == http.StatusConflict) { // Note the ServiceCode actually be AuthenticationFailure when share failed to be created, if want to create share as well.
			return nil
		}
		return err
	}

	return nil
}

// splitWithoutToken splits string with a given token, and returns splitted results without token.
func splitWithoutToken(str string, token rune) []string {
	return strings.FieldsFunc(str, func(c rune) bool {
		return c == token
	})
}

// createParentDirToRoot creates parent directories of the Azure file if file's parent directory doesn't exist.
func createParentDirToRoot(ctx context.Context, fileURL azfile.FileURL, p pipeline.Pipeline) error {
	dirURL := getParentDirectoryURL(fileURL, p)
	dirURLExtension := common.FileURLPartsExtension{FileURLParts: azfile.NewFileURLParts(dirURL.URL())}
	// Check whether parent dir of the file exists.
	if _, err := dirURL.GetProperties(ctx); err != nil {
		if err.(azfile.StorageError) != nil && (err.(azfile.StorageError)).Response() != nil &&
			(err.(azfile.StorageError).Response().StatusCode == http.StatusNotFound) { // At least need read and write permisson for destination
			// File's parent directory doesn't exist, try to create the parent directories.
			// Split directories as segments.
			segments := splitWithoutToken(dirURLExtension.DirectoryOrFilePath, '/')

			shareURL := azfile.NewShareURL(dirURLExtension.GetShareURL(), p)
			curDirURL := shareURL.NewRootDirectoryURL() // Share directory should already exist, doesn't support creating share
			// Try to create the directories
			for i := 0; i < len(segments); i++ {
				curDirURL = curDirURL.NewDirectoryURL(segments[i])
				_, err := curDirURL.Create(ctx, azfile.Metadata{})
				if verifiedErr := verifyAndHandleCreateErrors(err); verifiedErr != nil {
					return verifiedErr
				}
			}
		} else {
			return err
		}
	}

	// Directly return if parent directory exists.
	return nil
}
