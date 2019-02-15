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
	"crypto/md5"
	"errors"
	"fmt"
	"os"

	"github.com/Azure/azure-pipeline-go/pipeline"
	"github.com/Azure/azure-storage-azcopy/common"
)

// general-purpose local to "any remote persistence location"
func localToRemote(jptm IJobPartTransferMgr, p pipeline.Pipeline, pacer *pacer, uf uploaderFactory) {

	info := jptm.Info()
	fileSize := info.SourceSize

	// step 1. perform initial checks
	if jptm.WasCanceled() {
		jptm.ReportTransferDone()
		return
	}

	// step 2a. Create uploader
	ul, err := uf(jptm, info.Destination, p, pacer)
	if err != nil {
		jptm.LogUploadError(info.Source, info.Destination, err.Error(), 0)
		jptm.SetStatus(common.ETransferStatus.Failed())
		jptm.ReportTransferDone()
		return
	}
	md5Channel := ul.Md5Channel()
	defer close(md5Channel) // never leave receiver hanging, waiting for a result, even if we fail here

	// step 2b. Check chunk size and count from the uploader (it may have applied its own defaults and/or calculations to produce these values
	if jptm.ShouldLog(pipeline.LogInfo) {
		jptm.LogTransferStart(info.Source, info.Destination, fmt.Sprintf("Specified chunk size %d", ul.ChunkSize()))
	}
	if ul.NumChunks() == 0 {
		panic("must always schedule one chunk, even if file is empty") // this keeps our code structure simpler, by using a dummy chunk for empty files
	}

	// step 3: Check overwrite
	// If the force Write flags is set to false
	// then check the file exists at the remote location
	// If it does, mark transfer as failed.
	if !jptm.IsForceWriteTrue() {
		exists, existenceErr := ul.RemoteFileExists()
		if existenceErr != nil {
			jptm.LogUploadError(info.Source, info.Destination, "Could not check file existence. "+existenceErr.Error(), 0)
			jptm.SetStatus(common.ETransferStatus.Failed()) // is a real failure, not just a FileAlreadyExists, in this case
			jptm.ReportTransferDone()
			return
		}
		if exists {
			jptm.LogUploadError(info.Source, info.Destination, "File already exists", 0)
			jptm.SetStatus(common.ETransferStatus.FileAlreadyExistsFailure()) // TODO: question: is it OK to always use FileAlreadyExists here, instead of BlobAlreadyExists, even when saving to blob storage?  I.e. do we really need a different error for blobs?
			jptm.ReportTransferDone()
			return
		}
	}

	// step 4: Open the Source File.
	// Declare factory func, because we need it later too
	sourceFileFactory := func() (common.CloseableReaderAt, error) {
		return os.Open(info.Source)
	}
	srcFile, err := sourceFileFactory()
	if err != nil {
		jptm.LogUploadError(info.Source, info.Destination, "Couldn't open source-"+err.Error(), 0)
		jptm.SetStatus(common.ETransferStatus.Failed())
		jptm.ReportTransferDone()
		return
	}
	defer srcFile.Close() // we read all the chunks in this routine, so can close the file at the end

	i, err := os.Stat(info.Source)
	if err != nil {
		jptm.LogUploadError(info.Source, info.Destination, "Couldn't stat source-"+err.Error(), 0)
		jptm.SetStatus(common.ETransferStatus.Failed())
		jptm.ReportTransferDone()
		return
	}
	if i.ModTime() != jptm.LastModifiedTime() {
		jptm.LogUploadError(info.Source, info.Destination, "File modified since transfer scheduled", 0)
		jptm.SetStatus(common.ETransferStatus.Failed())
		jptm.ReportTransferDone()
		return
	}

	// *****
	// Error-handling rules change here.
	// ABOVE this point, we end the transfer using the code as shown above
	// BELOW this point, this routine always schedules the expected number
	// of chunks, even if it has seen a failure, and the
	// workers (the chunkfunc implementations) must use
	// jptm.FailActiveUpload when there's an error)
	// ******

	// step 5: tell jptm what to expect, and how to clean up at the end
	jptm.SetNumberOfChunks(ul.NumChunks())
	jptm.SetActionAfterLastChunk(func() { epilogueWithCleanupUpload(jptm, ul) })

	// TODO: currently, the epilogue will only run if the number of completed chunks = numChunks.
	//     which means that we can't exit this loop early, if there is a cancellation or failure. Instead we
	//     ...must schedule the expected number of chunks, i.e. schedule all of them even if the transfer is already failed,
	//     ...so that the last of them will trigger the epilogue.
	//     ...Question: is that OK? (Same question as we have for downloads)
	// DECISION: 16 Jan, 2019: for now, we are leaving in place the above rule than number of of completed chunks must
	// eventually reach numChunks, since we have no better short-term alternative.

	// Step 5: Go through the file and schedule chunk messages to upload each chunk
	scheduleUploadChunks(jptm, info.Source, srcFile, fileSize, ul, sourceFileFactory, md5Channel)
}

// Schedule all the upload chunks.
// As we do this, we force preload of each chunk to memory, and we wait (block)
// here if the amount of preloaded data gets excessive. That's OK to do,
// because if we already have that much data preloaded (and scheduled for sending in
// chunks) then we don't need to schedule any more chunks right now, so the blocking
// is harmless (and a good thing, to avoid excessive RAM usage).
// To take advantage of the good sequential read performance provided by many file systems,
// and to be able to compute an MD5 hash for the file, we work sequentially through the file here.
func scheduleUploadChunks(jptm IJobPartTransferMgr, srcName string, srcFile common.CloseableReaderAt, fileSize int64, ul uploader, sourceFileFactory common.ChunkReaderSourceFactory, md5Channel chan<- []byte) {
	chunkSize := ul.ChunkSize()
	numChunks := ul.NumChunks()
	context := jptm.Context()
	slicePool := jptm.SlicePool()
	cacheLimiter := jptm.CacheLimiter()

	chunkCount := int32(0)
	md5Hasher := md5.New()
	safeToUseHash := true
	for startIndex := int64(0); startIndex < fileSize || isDummyChunkInEmptyFile(startIndex, fileSize); startIndex += int64(chunkSize) {

		id := common.ChunkID{Name: srcName, OffsetInFile: startIndex}
		adjustedChunkSize := int64(chunkSize)

		// compute actual size of the chunk
		if startIndex+int64(chunkSize) > fileSize {
			adjustedChunkSize = fileSize - startIndex
		}

		// Make reader for this chunk.
		// Each chunk reader also gets a factory to make a reader for the file, in case it needs to repeat its part
		// of the file read later (when doing a retry)
		// BTW, the reader we create here just works with a single chuck. (That's in contrast with downloads, where we have
		// to use an object that encompasses the whole file, so that it can put the chunks back into order. We don't have that requirement here.)
		chunkReader := common.NewSingleChunkReader(context, sourceFileFactory, id, adjustedChunkSize, jptm, jptm, slicePool, cacheLimiter)

		// Wait until we have enough RAM, and when we do, prefetch the data for this chunk.
		chunkDataError := chunkReader.BlockingPrefetch(srcFile, false)

		// Add the bytes to the hash
		// NOTE: if there is a retry on this chunk later (a 503 from Service) our current implementation of singleChunkReader
		// (as at Jan 2019) will re-read from the disk.  If that part of the file has been updated by another process,
		// that means it will not longer match the hash we set here. That would be bad. So we rely on logic
		// elsewhere in our upload code to avoid/fail or retry such transfers.
		// TODO: move the above note to the place where we implement the avoid/fail/retry and refer to that in a comment
		//      on the retry file-re-read logic
		if chunkDataError == nil {
			chunkReader.WriteBufferTo(md5Hasher)
		} else {
			safeToUseHash = false // because we've missed a chunk
		}

		// If this is the the very first chunk, do special init steps
		if startIndex == 0 {
			// Capture the leading bytes of the file for mime-type detection
			// We do this here, to avoid needing any separate disk read elsewhere in the code (i.e. we just prefetched what we need for this, so use it)
			leadingBytes := chunkReader.CaptureLeadingBytes()
			// Run prologue before first chunk is scheduled
			// There is deliberately no error return value from the Prologue.
			// If it failed, the Prologue itself must call jptm.FailActiveUpload.
			ul.Prologue(leadingBytes)
		}

		// schedule the chunk job/msg
		jptm.LogChunkStatus(id, common.EWaitReason.WorkerGR())
		var cf chunkFunc
		if chunkDataError == nil {
			isWholeFile := numChunks == 1
			cf = ul.GenerateUploadFunc(id, chunkCount, chunkReader, isWholeFile)
		} else {
			_ = chunkReader.Close()
			// Our jptm logic currently requires us to schedule every chunk, even if we know there's an error,
			// so we schedule a func that will just fail with the given error
			cf = createUploadChunkFunc(jptm, id, func() { jptm.FailActiveUpload("chunk data read", chunkDataError) })
		}
		jptm.ScheduleChunks(cf)

		chunkCount += 1
	}
	// sanity check to verify the number of chunks scheduled
	if chunkCount != int32(numChunks) {
		panic(fmt.Errorf("difference in the number of chunk calculated %v and actual chunks scheduled %v for src %s of size %v", numChunks, chunkCount, srcName, fileSize))
	}
	// provide the hash that we computed
	if safeToUseHash {
		md5Channel <- md5Hasher.Sum(nil)
	}
}

func isDummyChunkInEmptyFile(startIndex int64, fileSize int64) bool {
	return startIndex == 0 && fileSize == 0
}

// Complete epilogue. Handles both success and failure.
// Most of the processing is delegated to the uploader object, since details will
// depend on the destination type
func epilogueWithCleanupUpload(jptm IJobPartTransferMgr, ul uploader) {

	if jptm.TransferStatus() > 0 {
		// Stat the file again to see if it was changed during transfer. If it was, mark the transfer as failed.
		i, err := os.Stat(jptm.Info().Source)
		if err != nil {
			jptm.FailActiveUpload("epilogueWithCleanupUpload", err)
		}
		if i.ModTime() != jptm.LastModifiedTime() {
			jptm.FailActiveUpload("epilogueWithCleanupUpload", errors.New("source modified during transfer"))
		}
	}

	ul.Epilogue()

	// TODO: finalize and wrap in functions whether 0 is included or excluded in status comparisons
	if jptm.TransferStatus() == 0 {
		panic("think we're finished but status is notStarted")
	}

	if jptm.TransferStatus() > 0 {
		// We know all chunks are done (because this routine was called)
		// and we know the transfer didn't fail (because just checked its status above),
		// so it must have succeeded. So make sure its not left "in progress" state
		jptm.SetStatus(common.ETransferStatus.Success())

		// Final logging
		if jptm.ShouldLog(pipeline.LogInfo) { // TODO: question: can we remove these ShouldLogs?  Aren't they inside Log?
			jptm.Log(pipeline.LogInfo, "UPLOAD SUCCESSFUL")
		}
		if jptm.ShouldLog(pipeline.LogDebug) {
			jptm.Log(pipeline.LogDebug, "Finalizing Transfer")
		}
	} else {
		if jptm.ShouldLog(pipeline.LogDebug) {
			jptm.Log(pipeline.LogDebug, "Finalizing Transfer Cancellation/Failure")
		}
	}

	// successful or unsuccessful, it's definitely over
	jptm.ReportTransferDone()
}
