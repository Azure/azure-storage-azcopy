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
	"net"
	"net/http"
	"net/url"
	"os"
	"strconv"
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
		if jptm.ShouldLog(pipeline.LogInfo) {
			jptm.Log(pipeline.LogInfo,
				fmt.Sprintf("specified block size %d is larger than maximum file chunk size, use 4MB as chunk size.", info.BlockSize))
		}
	}

	// If the transfer was cancelled, then reporting transfer as done and increasing the bytestransferred by the size of the source.
	if jptm.WasCanceled() {
		jptm.AddToBytesDone(info.SourceSize)
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
			if jptm.ShouldLog(pipeline.LogInfo) {
				jptm.Log(pipeline.LogInfo, fmt.Sprintf("skipping the transfer since blob already exists"))
			}
			// Mark the transfer as failed with FileAlreadyExistsFailure
			jptm.SetStatus(common.ETransferStatus.FileAlreadyExistsFailure())
			jptm.AddToBytesDone(info.SourceSize)
			jptm.ReportTransferDone()
			return
		}
	}

	// step 2: Map file upload before transferring chunks and get info from map file.
	srcFile, err := os.Open(info.Source)
	if err != nil {
		if jptm.ShouldLog(pipeline.LogInfo) {
			jptm.Log(pipeline.LogInfo, fmt.Sprintf("error opening the source file %d", info.SourceSize))
		}
		jptm.SetStatus(common.ETransferStatus.Failed())
		jptm.AddToBytesDone(info.SourceSize)
		jptm.ReportTransferDone()
		return
	}
	srcFileInfo, err := srcFile.Stat()
	if err != nil {
		if jptm.ShouldLog(pipeline.LogInfo) {
			jptm.Log(pipeline.LogInfo, fmt.Sprintf("error getting the source file Info of file %d", info.SourceSize))
		}
		jptm.SetStatus(common.ETransferStatus.Failed())
		jptm.AddToBytesDone(info.SourceSize)
		jptm.ReportTransferDone()
		return
	}

	var srcMmf *common.MMF
	if srcFileInfo.Size() > 0 {
		// file needs to be memory mapped only when the file size is greater than 0.
		srcMmf, err = common.NewMMF(srcFile, false, 0, srcFileInfo.Size())
		if err != nil {
			if jptm.ShouldLog(pipeline.LogInfo) {
				jptm.Log(pipeline.LogInfo, fmt.Sprintf("error memory mapping the source file %s. Failed with error %s", srcFile.Name(), err.Error()))
			}
			srcFile.Close()
			jptm.SetStatus(common.ETransferStatus.Failed())
			jptm.AddToBytesDone(info.SourceSize)
			jptm.ReportTransferDone()
			return
		}
	}

	// Get http headers and meta data of file.
	fileHTTPHeaders, metaData := jptm.FileDstData(srcMmf)

	// step 3: Create parent directories and file.
	// 3a: Create the parent directories of the file. Note share must be existed, as the files are listed from share or directory.
	err = createParentDirToRoot(jptm.Context(), fileURL, p)
	if err != nil {
		if jptm.ShouldLog(pipeline.LogInfo) {
			jptm.Log(pipeline.LogInfo,
				fmt.Sprintf("failed since Create parent directory for file failed due to %s", err.Error()))
		}
		jptm.Cancel()
		jptm.SetStatus(common.ETransferStatus.Failed())
		jptm.ReportTransferDone()
		// Unmap only if the source size is > 0
		if info.SourceSize > 0 {
			srcMmf.Unmap()
		}
		err = srcFile.Close()
		if err != nil {
			if jptm.ShouldLog(pipeline.LogInfo) {
				jptm.Log(pipeline.LogInfo,
					fmt.Sprintf("got an error while closing file %s because of %s", srcFile.Name(), err.Error()))
			}
		}
		return
	}

	// 3b: Create Azure file with the source size.
	_, err = fileURL.Create(jptm.Context(), fileSize, fileHTTPHeaders, metaData)
	if err != nil {
		if jptm.ShouldLog(pipeline.LogInfo) {
			jptm.Log(pipeline.LogInfo,
				fmt.Sprintf("failed since Create failed due to %s", err.Error()))
		}
		jptm.Cancel()
		jptm.SetStatus(common.ETransferStatus.Failed())
		jptm.ReportTransferDone()
		// Unmap only if the source size > 0
		if info.SourceSize > 0 {
			srcMmf.Unmap()
		}
		err = srcFile.Close()
		if err != nil {
			if jptm.ShouldLog(pipeline.LogInfo) {
				jptm.Log(pipeline.LogInfo,
					fmt.Sprintf("got an error while closing file %s because of %s", srcFile.Name(), err.Error()))
			}
		}
		return
	}

	// If the file size is 0, scheduling chunk msgs for UploadRange is not required
	if info.SourceSize == 0 {
		// mark the transfer as successful
		jptm.SetStatus(common.ETransferStatus.Success())
		jptm.ReportTransferDone()
		err = srcFile.Close()
		if err != nil {
			if jptm.ShouldLog(pipeline.LogInfo) {
				jptm.Log(pipeline.LogInfo,
					fmt.Sprintf("got an error while closing file %s because of %s", srcFile.Name(), err.Error()))
			}
		}
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
		jptm.ScheduleChunks(fileUploadFunc(jptm, srcFile, srcMmf, fileURL, pacer, startIndex, adjustedChunkSize))
	}
}

func fileUploadFunc(jptm IJobPartTransferMgr, srcFile *os.File, srcMmf *common.MMF, fileURL azfile.FileURL, pacer *pacer, startRange int64, pageSize int64) chunkFunc {
	return func(workerId int) {
		// rangeDone is the function called after success / failure of each range.
		// If the calling range is the last range of transfer, then it updates the transfer status,
		// mark transfer done, unmap the source memory map and close the source file descriptor.
		rangeDone := func() {
			// adding the range size to the bytes transferred.
			jptm.AddToBytesDone(pageSize)
			if lastPage, _ := jptm.ReportChunkDone(); lastPage {
				if jptm.ShouldLog(pipeline.LogInfo) {
					jptm.Log(pipeline.LogInfo,
						fmt.Sprintf("has worker %d which is finalizing transfer", workerId))
				}
				jptm.SetStatus(common.ETransferStatus.Success())
				srcMmf.Unmap()
				err := srcFile.Close()
				if err != nil {
					if jptm.ShouldLog(pipeline.LogInfo) {
						jptm.Log(pipeline.LogInfo,
							fmt.Sprintf("got an error while closing file %s because of %s", srcFile.Name(), err.Error()))
					}
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

		if jptm.WasCanceled() {
			if jptm.ShouldLog(pipeline.LogInfo) {
				jptm.Log(pipeline.LogInfo,
					fmt.Sprintf("is cancelled. Hence not picking up range %d", startRange))
			}
			rangeDone()
		} else {
			// rangeBytes is the byte slice of Page for the given range range
			rangeBytes := srcMmf.Slice()[startRange : startRange+pageSize]
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
				if jptm.ShouldLog(pipeline.LogInfo) {
					jptm.Log(pipeline.LogInfo,
						fmt.Sprintf("has worker %d which is not performing UploadRange for range from %d to %d since all the bytes are zero", workerId, startRange, startRange+pageSize))
				}
				rangeDone()
				return
			}

			body := newRequestBodyPacer(bytes.NewReader(rangeBytes), pacer, srcMmf)
			_, err := fileURL.UploadRange(jptm.Context(), startRange, body)
			if err != nil {
				if jptm.WasCanceled() {
					if jptm.ShouldLog(pipeline.LogInfo) {
						jptm.Log(pipeline.LogInfo,
							fmt.Sprintf("has worker %d which failed to UploadRange from %d to %d because transfer was cancelled", workerId, startRange, startRange+pageSize))
					}
				} else {
					if jptm.ShouldLog(pipeline.LogInfo) {
						jptm.Log(pipeline.LogInfo,
							fmt.Sprintf("has worker %d which failed to UploadRange from %d to %d because of following error %s", workerId, startRange, startRange+pageSize, err.Error()))
					}
					// cancelling the transfer
					jptm.Cancel()
					jptm.SetStatus(common.ETransferStatus.Failed())
				}
				rangeDone()
				return
			}
			if jptm.ShouldLog(pipeline.LogInfo) {
				jptm.Log(pipeline.LogInfo,
					fmt.Sprintf("has workedId %d which successfully complete PUT range request from range %d to %d", workerId, startRange, startRange+pageSize))
			}
			rangeDone()
		}
	}
}

// isUserEndpointStyle verfies if a given URL is pointing to Azure storage dev fabric's or emulator's resource.
func isUserEndpointStyle(url url.URL) bool {
	pathStylePorts := map[int16]struct{}{10000: struct{}{}, 10001: struct{}{}, 10002: struct{}{}, 10003: struct{}{}, 10004: struct{}{}, 10100: struct{}{}, 10101: struct{}{}, 10102: struct{}{}, 10103: struct{}{}, 10104: struct{}{}, 11000: struct{}{}, 11001: struct{}{}, 11002: struct{}{}, 11003: struct{}{}, 11004: struct{}{}, 11100: struct{}{}, 11101: struct{}{}, 11102: struct{}{}, 11103: struct{}{}, 11104: struct{}{}}

	// Decides whether it's user endpoint style, and compose the new path.
	if net.ParseIP(url.Host) != nil {
		return true
	}

	if url.Port() != "" {
		port, err := strconv.Atoi(url.Port())
		if err != nil {
			return false
		}
		if _, ok := pathStylePorts[int16(port)]; ok {
			return true
		}
	}

	return false
}

// getServiceURL gets service URL from an Azure file resource URL.
func getServiceURL(u url.URL, p pipeline.Pipeline) azfile.ServiceURL {
	path := u.Path

	if path != "" {
		if path[0] == '/' {
			path = path[1:]
		}
		if isUserEndpointStyle(u) {
			panic(fmt.Errorf("doesn't support user endpoint style currently"))
		} else {
			u.Path = ""
		}
	}

	return azfile.NewServiceURL(u, p)
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

	segments := splitWithoutToken(dirURL.URL().Path, '/')

	if isUserEndpointStyle(dirURL.URL()) {
		panic(fmt.Errorf("doesn't support user endpoint style currently"))
	}

	_, err := dirURL.GetProperties(ctx)
	if err != nil {
		if err.(azfile.StorageError) != nil && (err.(azfile.StorageError)).Response() != nil &&
			(err.(azfile.StorageError).Response().StatusCode == http.StatusNotFound) { // At least need read and write permisson for destination
			// fileParentDirURL doesn't exist, try to create the directories to the root.
			serviceURL := getServiceURL(dirURL.URL(), p)
			curDirURL := serviceURL.NewShareURL(segments[0]).NewRootDirectoryURL() // Share directory should already exist, otherwise invalid state
			// try to create the directories
			for i := 1; i < len(segments); i++ {
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
