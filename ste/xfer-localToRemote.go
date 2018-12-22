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

// general-purpose local to "any remote persistence location"
func LocalToRemote(jptm IJobPartTransferMgr, p pipeline.Pipeline, pacer *pacer, uf UploaderFactory) {

	// step 1a. get the source, destination info for the transfer.
	info := jptm.Info()
	fileSize := int64(info.SourceSize)
	chunkSize := int64(info.BlockSize)

	// step 1b. perform initial checks
	if jptm.WasCanceled() {
		jptm.ReportTransferDone()
		return
	}
	if jptm.ShouldLog(pipeline.LogInfo) {
		jptm.LogTransferStart(info.Source, info.Destination, fmt.Sprintf("Specified chunk size %d", chunkSize))
	}

	// step 1c: compute num chunks
	// We map zero size files to ONE chunk (not zero chunks)
	numChunks := uint32(1)
	if fileSize > 0  {
		numChunks = common.Iffuint32(
			fileSize%chunkSize == 0,
			uint32(fileSize/chunkSize),
			uint32(fileSize/chunkSize)+1)
	}

	// step 2. Create uploader
	proposedStats := ChunkStats{
		FileSize: fileSize,
		ChunkSize: chunkSize,
		NumChunks: numChunks,
	}
	ul, err := uf(jptm, info.Destination, p, proposedStats, pacer)
	if err != nil {
		jptm.LogUploadError(info.Source, info.Destination, err.Error(), 0)
		jptm.Cancel()
		jptm.SetStatus(common.ETransferStatus.Failed())
		jptm.ReportTransferDone()
		return
	}

	// step 3: Check overwrite
	// If the force Write flags is set to false
	// then check the file exists at the remote location
	// If it does, mark transfer as failed.
	if !jptm.IsForceWriteTrue() {
		exists, err := ul.RemoteFileExists()
		if exists || err != nil {
			// TODO: Confirm if this is an error condition or not
			message := err.Error()
			if exists {
				message = "File already exists"
			}
			jptm.LogUploadError(info.Source, info.Destination, message, 0)
			// Mark the transfer as failed
			jptm.SetStatus(common.ETransferStatus.FileAlreadyExistsFailure())  // TODO: question: is it OK to use FileAlreadyExists here, instead of BlobAlreadyExists, even when saving to blob storage?  I.e. do we really need a different error for blobs?
			jptm.ReportTransferDone()
			return
		}
	}

	// step 4: Open the Source File.
	// Declare factory func, because we need it later too
	sourceFileFactory := func()(common.CloseableReaderAt, error) {
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


	// step 5: tell jptm what to expect, and how to clean up at the end
	jptm.SetNumberOfChunks(numChunks)
	jptm.SetActionAfterLastChunk(func(){ epilogueWithCleanupUpload(jptm, ul)})

	// step 6: go through the blob range and schedule download chunk jobs
	// TODO: currently, the epilogue will only run if the number of completed chunks = numChunks.
	// TODO: ...which means that we can't exit this loop early, if there is a cancellation or failure. Instead we
	// TODO: ...must schedule the expected number of chunks, i.e. schedule all of them even if the transfer is already failed,
	// TODO: ...so that the last of them will trigger the epilogue.
	// TODO: ...Question: is that OK? (Same question as we have for downloads)

	// TODO: for at least some destinations (e.g. BlockBlobs) you can do a single chunk PUT (of the whole file)
	//    for sizes that are larger than the normal single-chunk limit.  Do we want to support that?  What would the advantages be?

	// Step 6: Go through the file and schedule chunk messages to upload each chunk
	// As we do this, we force preload of each chunk to memory, and we wait (block)
	// here if the amount of preloaded data gets excessive. That's OK to do,
	// because if we already have that much data preloaded (and scheduled for sending in
	// chunks) then we don't need to schedule any more chunks right now, so the blocking
	// is harmless (and a good thing, to avoid excessive RAM usage).
	// To take advantage of the good sequential read performance provided by many file systems,
	// we work sequentially through the file here.
	context := jptm.Context()
	slicePool := jptm.SlicePool()
	cacheLimiter := jptm.CacheLimiter()
	blockIdCount := int32(0)
	for startIndex := int64(0); startIndex < fileSize; startIndex += chunkSize {

		id := common.ChunkID{Name: info.Source, OffsetInFile: startIndex}
		adjustedChunkSize := chunkSize

		// compute actual size of the chunk
		if startIndex+chunkSize > fileSize {
			adjustedChunkSize = fileSize - startIndex
		}

		// Make reader for this chunk.
		// Each chunk reader also gets a factory to make a reader for the file, in case it needs to repeat it's part
		// of the file read later (when doing a retry)
		// BTW, the reader we create here just works with a single chuck. (That's in contrast with downloads, where we have
		// to use an object that encompasses the whole file, so that it can but the chunks back into order. We don't have that requirement here.)
		chunkReader := common.NewSingleChunkReader(context, sourceFileFactory, id, adjustedChunkSize, jptm, slicePool, cacheLimiter)

		// Wait until we have enough RAM, and when we do, prefetch the data for this chunk.
		chunkReader.TryBlockingPrefetch(srcFile)

		// schedule the chunk job/msg
		jptm.LogChunkStatus(id, common.EWaitReason.WorkerGR())
		isWholeFile := numChunks == 1
		jptm.ScheduleChunks(ul.GenerateUploadFunc(id, blockIdCount, chunkReader, isWholeFile))

		blockIdCount += 1
	}
	// sanity check to verify the number of chunks scheduled
	if blockIdCount != int32(numChunks) {
		jptm.Panic(fmt.Errorf("difference in the number of chunk calculated %v and actual chunks scheduled %v for src %s of size %v", numChunks, blockIdCount, info.Source, fileSize))
	}
}

// Complete epilogue. Handles both success and failure.
// Most of the processing is delegated to the uploader object, since details will
// depend on the destination type
func epilogueWithCleanupUpload(jptm IJobPartTransferMgr, ul Uploader) {

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
			jptm.Log(pipeline.LogInfo, "DOWNLOAD SUCCESSFUL")
		}
		if jptm.ShouldLog(pipeline.LogDebug) {
			jptm.Log(pipeline.LogDebug, "Finalizing Transfer")
		}
	} else {
		if jptm.ShouldLog(pipeline.LogDebug) {
			jptm.Log(pipeline.LogDebug, " Finalizing Transfer Cancellation")
		}
	}

	// successful or unsuccessful, it's definitely over
	jptm.ReportTransferDone()
}




