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
	"fmt"
	"os"

	"github.com/Azure/azure-pipeline-go/pipeline"
	"github.com/Azure/azure-storage-azcopy/common"
)

// general-purpose "any remote persistence location" to local
func RemoteToLocal(jptm IJobPartTransferMgr, p pipeline.Pipeline, pacer *pacer, df DownloaderFactory) {
	// step 1: create downloader instance for this transfer
	// We are using a separate instance per transfer, in case some implementations need to hold per-transfer state
	dl := df()

	// step 2: get the source, destination info for the transfer.
	info := jptm.Info()
	fileSize := int64(info.SourceSize)
	downloadChunkSize := int64(info.BlockSize) // TODO: is this available for non-Blob cases?

        // TODO: we are not logging chunk size here (as was done for some remotes in previous code, notably Azure files.  Should we?)
	
	// step 3: Perform initial checks
	// If the transfer was cancelled, then report transfer as done
	// TODO the above comment had this text too. What does it mean: "and increasing the bytestransferred by the size of the source."
	if jptm.WasCanceled() {
		jptm.ReportTransferDone()
		return
	}
	// If the force Write flags is set to false
	// then check the file exists locally or not.
	// If it does, mark transfer as failed.
	if !jptm.IsForceWriteTrue() {
		_, err := os.Stat(info.Destination)
		if err == nil {
			// TODO: Confirm if this is an error condition or not
			// If the error is nil, then file exists locally and it doesn't need to be downloaded.
			jptm.LogDownloadError(info.Source, info.Destination, "File already exists", 0)
			// Mark the transfer as failed
			jptm.SetStatus(common.ETransferStatus.FileAlreadyExistsFailure()) // TODO: this was BlobAlreadyExists, but its local, so IMHO its a file. And "file" makes this code here re-usable for multiple remotes - JR.
			jptm.ReportTransferDone()
			return
		}
	}

	// step 4a: special handling for empty files
	if fileSize == 0 {
		err := createEmptyFile(info.Destination)
		if err != nil {
			jptm.LogDownloadError(info.Source, info.Destination, "Empty File Creation error "+err.Error(), 0)
			jptm.SetStatus(common.ETransferStatus.Failed())
		}
		epilogueWithCleanup(jptm, nil, nil) // need standard epilogue, rather than a quick exit, so we can preserve modification dates
		return
	}

	// step 4b: normal file creation when source has content
	writeThrough := true     // makes sense for bulk ingest, because OS-level caching can't possibly help there, and really only adds overhead
	if fileSize < 512 {
		writeThrough = false // but, for very small files, we do  need it. TODO: double-check with more testing, do we really need this
	}
	dstFile, err := common.CreateFileOfSizeWithWriteThroughOption(info.Destination, fileSize, writeThrough)
	if err != nil {
		jptm.LogDownloadError(info.Source, info.Destination, "File Creation Error "+err.Error(), 0)
		jptm.SetStatus(common.ETransferStatus.Failed())
		epilogueWithCleanup(jptm, nil, nil)
		return
	}
	dstWriter := common.NewChunkedFileWriter(jptm.Context(), jptm.CacheLimiter(), dstFile, 1024 * 1024) // TODO: parameterize write size?
	// TODO: why do we need to Stat the file, to check its size, after explicitly making it with the desired size?
	// I've commented it out to be more concise, but we'll put it back if someone knows why it needs to be here
	/*
	dstFileInfo, err := dstFile.Stat()
	if err != nil || (dstFileInfo.Size() != blobSize) {
		jptm.LogDownloadError(info.Source, info.Destination, "File Creation Error "+err.Error(), 0)
		jptm.SetStatus(common.ETransferStatus.Failed())
		// Since the transfer failed, the file created above should be deleted
		// If there was an error while opening / creating the file, delete will fail.
		// But delete is required when error occurred while truncating the file and
		// in this case file should be deleted.
		tryDeleteFile(info, jptm)
		jptm.ReportTransferDone()
		return
	}*/

	// step 5: tell jptm what to expect, and how to clean up at the end
	numChunks := uint32(0)
	if rem := fileSize % downloadChunkSize; rem == 0 {
		numChunks = uint32(fileSize / downloadChunkSize)
	} else {
		numChunks = uint32(fileSize/downloadChunkSize + 1)
	}
	jptm.SetNumberOfChunks(numChunks)
	jptm.SetActionAfterLastChunk(func(){ epilogueWithCleanup(jptm, dstFile, dstWriter)})

	// step 6: go through the blob range and schedule download chunk jobs
	// TODO: currently, the epilogue will only run if the number of completed chunks = numChunks.
	// TODO: ...which means that we can't exit this loop early, if there is a cancellation or failure (because we
	// TODO: ...must schedule the expected number of chunks, so that the last of them will trigger the epilogue).
	// TODO: ...To decide: is that OK?
	//blockIdCount := int32(0)
	for startIndex := int64(0); startIndex < fileSize; startIndex += downloadChunkSize {
		id := common.ChunkID{Name: info.Destination, OffsetInFile: startIndex}
		adjustedChunkSize := downloadChunkSize

		// compute exact size of the chunk
		if startIndex+downloadChunkSize > fileSize {
			adjustedChunkSize = fileSize - startIndex
		}

		// Wait until its OK to schedule it
		// To prevent excessive RAM consumption, we have a limit on the amount of scheduled-but-not-yet-saved data
		// TODO: as per comment above, currently, if there's an error here we must continue because we must schedule all chunks
		// Can we refactor/improve that?
		_ = dstWriter.WaitToScheduleChunk(jptm.Context(), id, adjustedChunkSize)

		// create download func that is a appropriate to the remote data source
		downloadFunc := dl.GenerateDownloadFunc(jptm, p, dstWriter, id, adjustedChunkSize, pacer)

		// schedule the download chunk job
		jptm.ScheduleChunks(downloadFunc)
		//blockIdCount++  TODO: why was this originally used?  What should be done with it now

		common.LogChunkWaitReason(id, common.EWaitReason.WorkerGR())
	}
}

// complete epilogue. Handles both success and failure
func epilogueWithCleanup(jptm IJobPartTransferMgr, activeDstFile *os.File, cw common.ChunkedFileWriter){
	info := jptm.Info()

	if activeDstFile != nil {
		// wait until all received chunks are flushed out
		_, flushError := cw.Flush(jptm.Context())    // todo: use, and check the MD5 hash returned here

		// Close file
		fileCloseErr := activeDstFile.Close()  // always try to close if, even if flush failed
		if flushError != nil && fileCloseErr != nil && !jptm.TransferStatus().DidFail() {
			// it WAS successful up to now, but the file flush/closing failed
			message := "File Closure Error "+fileCloseErr.Error()
			if flushError != nil {
				message = "File Flush Error " + flushError.Error()
			}
			jptm.LogDownloadError(info.Source, info.Destination, message, 0)
			jptm.SetStatus(common.ETransferStatus.Failed())
		}
	}

	// Preserve modified time
	if !jptm.TransferStatus().DidFail(){
		// TODO: the old version of this code did NOT consider it an error to be unable to set the modification date/time
		// So I have preserved that behavior here.  But is that correct? (see the code for zero-size files, which does consdier a failure here to be failing the transfer)
		lastModifiedTime, preserveLastModifiedTime := jptm.PreserveLastModifiedTime()
		if preserveLastModifiedTime {
			err := os.Chtimes(jptm.Info().Destination, lastModifiedTime, lastModifiedTime)
			if err != nil {
				jptm.LogError(info.Destination, "Changing Modified Time ", err)
				return
			}
			if jptm.ShouldLog(pipeline.LogInfo) {
				jptm.Log(pipeline.LogInfo, fmt.Sprintf(" Preserved Modified Time for %s", info.Destination))
			}
		}
	}

	if jptm.TransferStatus() <= 0 {
		// If failed, log and delete the "bad" local file
		// If the current transfer status value is less than or equal to 0
		// then transfer either failed or was cancelled
		// TODO: is it right that 0 (not started) is _included_ here?  And, related, why is there no Cancelled Status?
		// .. or at least a IsFailedOrUnstarted method?
		if jptm.ShouldLog(pipeline.LogDebug) {
			jptm.Log(pipeline.LogDebug, " Finalizing Transfer Cancellation")
		}
		// the file created locally should be deleted
		tryDeleteFile(info, jptm)
	} else {
		// We know all chunks are done (because this routine was called)
		// and we know the transfer didn't fail (because just checked its status above),
		// so it must have succeeded. So make sure its not left "in progress" state
		jptm.SetStatus(common.ETransferStatus.Success())

		// Final logging
		if jptm.ShouldLog(pipeline.LogInfo) { // TODO: can we remove these ShouldLogs?  Aren't they inside Log?
			jptm.Log(pipeline.LogInfo, "DOWNLOAD SUCCESSFUL")
		}
		if jptm.ShouldLog(pipeline.LogDebug) {
			jptm.Log(pipeline.LogDebug, "Finalizing Transfer")
		}
	}

	// successful or unsuccessful, it's definitely over
	jptm.ReportTransferDone()
}

// create an empty file and its parent directories, without any content
func createEmptyFile(destinationPath string) error {
	err := common.CreateParentDirectoryIfNotExist(destinationPath)
	if err != nil {
		return err
	}
	f, err := os.OpenFile(destinationPath, os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0644)
	if err != nil {
		return err
	}
	_ = f.Close()
	return nil
}

// deletes the file
func deleteFile(destinationPath string) error {
	return os.Remove(destinationPath)
}

// tries to delete file, but if that fails just logs and returns
func tryDeleteFile(info TransferInfo, jptm IJobPartTransferMgr) {
	err := deleteFile(info.Destination)
	if err != nil {
		// If there was an error deleting the file, log the error
		jptm.LogError(info.Destination, "Delete File Error ", err)
	}
}

