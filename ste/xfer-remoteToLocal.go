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
	"io"
	"os"
	"strings"

	"github.com/Azure/azure-pipeline-go/pipeline"
	"github.com/Azure/azure-storage-azcopy/common"
)

// general-purpose "any remote persistence location" to local
func remoteToLocal(jptm IJobPartTransferMgr, p pipeline.Pipeline, pacer pacer, df downloaderFactory) {
	// step 1: create downloader instance for this transfer
	// We are using a separate instance per transfer, in case some implementations need to hold per-transfer state
	dl := df()

	// step 2: get the source, destination info for the transfer.
	info := jptm.Info()
	fileSize := int64(info.SourceSize)
	downloadChunkSize := int64(info.BlockSize)

	// step 3: Perform initial checks
	// If the transfer was cancelled, then report transfer as done
	// TODO Question: the above comment had this following text too: "and increasing the bytestransferred by the size of the source." what does it mean?
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
			// If the error is nil, then file exists locally and it doesn't need to be downloaded.
			jptm.LogDownloadError(info.Source, info.Destination, "File already exists", 0)
			// Mark the transfer as failed
			jptm.SetStatus(common.ETransferStatus.FileAlreadyExistsFailure()) // Deliberately not using BlobAlreadyExists, as was previously done in the old xfer-blobToLocal, since this is not blob-specific code, and its the local file we are talking about
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
		epilogueWithCleanupDownload(jptm, dl, nil, false, nil) // need standard epilogue, rather than a quick exit, so we can preserve modification dates
		return
	}

	// step 4b: normal file creation when source has content
	writeThrough := false
	// TODO: consider cases where we might set it to true. It might give more predictable and understandable disk throughput.
	//    But can't be used in the cases shown in the if statement below (one of which is only pseudocode, at this stage)
	//      if fileSize <= 1*1024*1024 || jptm.JobHasLowFileCount() || <is a short-running job> {
	//        // but, for very small files, testing indicates that we can need it in at least some cases. (Presumably just can't get enough queue depth to physical disk without it.)
	//        // And also, for very low file counts, we also need it. Presumably for same reasons of queue depth (given our sequential write strategy as at March 2019)
	//        // And for very short-running jobs, it looks and feels faster for the user to just let the OS cache flush out after the job appears to have finished.
	//        writeThrough = false
	//    }

	failFileCreation := func(err error, forceReleaseFileCount bool) {
		jptm.LogDownloadError(info.Source, info.Destination, "File Creation Error "+err.Error(), 0)
		jptm.SetStatus(common.ETransferStatus.Failed())
		// use standard epilogue for consistency, but force release of file count (without an actual file) if necessary
		epilogueWithCleanupDownload(jptm, dl, nil, forceReleaseFileCount, nil)
	}
	// block until we can safely use a file handle	// TODO: it might be nice if this happened inside chunkedFileWriter, when first chunk needs to be saved,
	err := jptm.FileCountLimiter().WaitUntilAdd(jptm.Context(), 1, func() bool { return true })
	if err != nil {
		failFileCreation(err, false)
		return
	}

	var dstFile io.WriteCloser
	if strings.EqualFold(info.Destination, common.Dev_Null) {
		// the user wants to discard the downloaded data
		dstFile = devNullWriter{}
	} else {
		// normal scenario, create the destination file as expected
		dstFile, err = common.CreateFileOfSizeWithWriteThroughOption(info.Destination, fileSize, writeThrough)
		if err != nil {
			failFileCreation(err, true)
			return
		}
	}

	// TODO: Question: do we need to Stat the file, to check its size, after explicitly making it with the desired size?
	// That was what the old xfer-blobToLocal code used to do
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

	// step 5a: compute num chunks
	numChunks := uint32(0)
	if rem := fileSize % downloadChunkSize; rem == 0 {
		numChunks = uint32(fileSize / downloadChunkSize)
	} else {
		numChunks = uint32(fileSize/downloadChunkSize + 1)
	}

	// step 5b: create destination writer
	chunkLogger := jptm.ChunkStatusLogger()
	sourceMd5Exists := len(info.SrcHTTPHeaders.ContentMD5) > 0
	dstWriter := common.NewChunkedFileWriter(
		jptm.Context(),
		jptm.SlicePool(),
		jptm.CacheLimiter(),
		chunkLogger,
		dstFile,
		numChunks,
		MaxRetryPerDownloadBody,
		jptm.MD5ValidationOption(),
		sourceMd5Exists)

	// step 5c: run prologue in downloader (here it can, for example, create things that will require cleanup in the epilogue)
	dl.Prologue(jptm)

	// step 5d: tell jptm what to expect, and how to clean up at the end
	jptm.SetNumberOfChunks(numChunks)
	jptm.SetActionAfterLastChunk(func() { epilogueWithCleanupDownload(jptm, dl, dstFile, false, dstWriter) })

	// step 6: go through the blob range and schedule download chunk jobs
	// TODO: currently, the epilogue will only run if the number of completed chunks = numChunks.
	//     ...which means that we can't exit this loop early, if there is a cancellation or failure. Instead we
	//     ...must schedule the expected number of chunks, i.e. schedule all of them even if the transfer is already failed,
	//     ...so that the last of them will trigger the epilogue.
	//     ...Question: is that OK?
	// DECISION: 16 Jan, 2019: for now, we are leaving in place the above rule than number of of completed chunks must
	// eventually reach numChunks, since we have no better short-term alternative.

	chunkCount := uint32(0)
	for startIndex := int64(0); startIndex < fileSize; startIndex += downloadChunkSize {
		id := common.NewChunkID(info.Destination, startIndex)
		adjustedChunkSize := downloadChunkSize

		// compute exact size of the chunk
		if startIndex+downloadChunkSize > fileSize {
			adjustedChunkSize = fileSize - startIndex
		}

		// Wait until its OK to schedule it
		// To prevent excessive RAM consumption, we have a limit on the amount of scheduled-but-not-yet-saved data
		// TODO: as per comment above, currently, if there's an error here we must continue because we must schedule all chunks
		// TODO: ... Can we refactor/improve that?
		_ = dstWriter.WaitToScheduleChunk(jptm.Context(), id, adjustedChunkSize)

		// create download func that is a appropriate to the remote data source
		downloadFunc := dl.GenerateDownloadFunc(jptm, p, dstWriter, id, adjustedChunkSize, pacer)

		// schedule the download chunk job
		jptm.ScheduleChunks(downloadFunc)
		chunkCount++

		jptm.LogChunkStatus(id, common.EWaitReason.WorkerGR())
	}

	// sanity check to verify the number of chunks scheduled
	if chunkCount != numChunks {
		panic(fmt.Errorf("difference in the number of chunk calculated %v and actual chunks scheduled %v for src %s of size %v", numChunks, chunkCount, info.Source, fileSize))
	}

}

// complete epilogue. Handles both success and failure
func epilogueWithCleanupDownload(jptm IJobPartTransferMgr, dl downloader, activeDstFile io.WriteCloser, forceReleaseFileCount bool, cw common.ChunkedFileWriter) {
	info := jptm.Info()

	// allow our usual state tracking mechanism to keep count of how many epilogues are running at any given instant, for perf diagnostics
	pseudoId := common.NewPseudoChunkIDForWholeFile(info.Source)
	jptm.LogChunkStatus(pseudoId, common.EWaitReason.Epilogue())
	defer jptm.LogChunkStatus(pseudoId, common.EWaitReason.ChunkDone()) // normal setting to done doesn't apply to these pseudo ids

	haveNonEmptyFile := activeDstFile != nil
	if haveNonEmptyFile {

		// wait until all received chunks are flushed out
		md5OfFileAsWritten, flushError := cw.Flush(jptm.Context())
		closeErr := activeDstFile.Close() // always try to close if, even if flush failed
		jptm.FileCountLimiter().Remove(1) // always release it from our count, no matter what happened
		if flushError != nil {
			jptm.FailActiveDownload("Flushing file", flushError)
		}
		if closeErr != nil {
			jptm.FailActiveDownload("Closing file", closeErr)
		}

		// Check MD5 (but only if file was fully flushed and saved - else no point and may not have actualAsSaved hash anyway)
		if !jptm.TransferStatus().DidFail() {
			comparison := md5Comparer{
				expected:         info.SrcHTTPHeaders.ContentMD5, // the MD5 that came back from Service when we enumerated the source
				actualAsSaved:    md5OfFileAsWritten,
				validationOption: jptm.MD5ValidationOption(),
				logger:           jptm}
			err := comparison.Check()
			if err != nil {
				jptm.FailActiveDownload("Checking MD5 hash", err)
			}
		}
	} else {
		if forceReleaseFileCount {
			jptm.FileCountLimiter().Remove(1) // special case, for we we failed after adding it to count, but before making an actual file
		}
	}

	if dl != nil {
		dl.Epilogue() // it can release resources here
	}

	// Preserve modified time
	if !jptm.TransferStatus().DidFail() {
		// TODO: the old version of this code did NOT consider it an error to be unable to set the modification date/time
		// TODO: ...So I have preserved that behavior here.
		// TODO: question: But is that correct?
		lastModifiedTime, preserveLastModifiedTime := jptm.PreserveLastModifiedTime()
		if preserveLastModifiedTime {
			err := os.Chtimes(jptm.Info().Destination, lastModifiedTime, lastModifiedTime)
			if err != nil {
				jptm.LogError(info.Destination, "Changing Modified Time ", err)
				// do NOT return, since final status and cleanup logging still to come
			} else {
				jptm.Log(pipeline.LogInfo, fmt.Sprintf(" Preserved Modified Time for %s", info.Destination))
			}
		}
	}

	// note that we do not really know whether the context was canceled because of an error, or because the user asked for it
	// if was an intentional cancel, the status is still "in progress", so we are still counting it as pending
	// we leave these transfer status alone
	// in case of errors, the status was already set, so we don't need to do anything here either
	if jptm.TransferStatus() <= 0 || jptm.WasCanceled() {
		// If failed, log and delete the "bad" local file
		// If the current transfer status value is less than or equal to 0
		// then transfer either failed or was cancelled
		// TODO: question: is it right that 0 (not started) is _included_ here? It was included in the previous version of this code.
		if jptm.ShouldLog(pipeline.LogDebug) {
			jptm.Log(pipeline.LogDebug, " Finalizing Transfer Cancellation/Failure")
		}
		// the file created locally should be deleted
		tryDeleteFile(info, jptm)
	} else {
		// We know all chunks are done (because this routine was called)
		// and we know the transfer didn't fail (because just checked its status above),
		// so it must have succeeded. So make sure its not left "in progress" state
		jptm.SetStatus(common.ETransferStatus.Success())

		// Final logging
		if jptm.ShouldLog(pipeline.LogInfo) { // TODO: question: can we remove these ShouldLogs?  Aren't they inside Log?
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
	f, err := os.OpenFile(destinationPath, os.O_RDWR|os.O_CREATE|os.O_TRUNC, common.DEFAULT_FILE_PERM)
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
	// skip deleting if we are targeting dev null and throwing away the data
	if strings.EqualFold(common.Dev_Null, info.Destination) {
		return
	}

	err := deleteFile(info.Destination)
	if err != nil {
		// If there was an error deleting the file, log the error
		jptm.LogError(info.Destination, "Delete File Error ", err)
	}
}

// conforms to io.Writer and io.Closer
// does absolutely nothing to discard the given data
type devNullWriter struct{}

func (devNullWriter) Write(p []byte) (n int, err error) {
	return len(p), nil
}

func (devNullWriter) Close() error {
	return nil
}
