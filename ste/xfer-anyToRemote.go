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
	"hash"
	"os"

	"github.com/Azure/azure-pipeline-go/pipeline"
	"github.com/Azure/azure-storage-azcopy/common"
)

// anyToRemote handles all kinds of sender operations - both uploads from local files, and S2S copies
func anyToRemote(jptm IJobPartTransferMgr, p pipeline.Pipeline, pacer *pacer, senderFactory senderFactory, sipf sourceInfoProviderFactory) {

	info := jptm.Info()
	srcSize := info.SourceSize

	// step 1. perform initial checks
	if jptm.WasCanceled() {
		jptm.ReportTransferDone()
		return
	}

	// step 2a. Create sender
	srcInfoProvider, err := sipf(jptm)
	if err != nil {
		jptm.LogSendError(info.Source, info.Destination, err.Error(), 0)
		jptm.SetStatus(common.ETransferStatus.Failed())
		jptm.ReportTransferDone()
		return
	}

	s, err := senderFactory(jptm, info.Destination, p, pacer, srcInfoProvider)
	if err != nil {
		jptm.LogSendError(info.Source, info.Destination, err.Error(), 0)
		jptm.SetStatus(common.ETransferStatus.Failed())
		jptm.ReportTransferDone()
		return
	}
	// step 2b. Read chunk size and count from the sender (since it may have applied its own defaults and/or calculations to produce these values
	numChunks := s.NumChunks()
	if jptm.ShouldLog(pipeline.LogInfo) {
		jptm.LogTransferStart(info.Source, info.Destination, fmt.Sprintf("Specified chunk size %d", s.ChunkSize()))
	}
	if s.NumChunks() == 0 {
		panic("must always schedule one chunk, even if file is empty") // this keeps our code structure simpler, by using a dummy chunk for empty files
	}

	// step 3: Check overwrite
	// If the force Write flags is set to false
	// then check the file exists at the remote location
	// If it does, mark transfer as failed.
	if !jptm.IsForceWriteTrue() {
		exists, existenceErr := s.RemoteFileExists()
		if existenceErr != nil {
			jptm.LogSendError(info.Source, info.Destination, "Could not check file existence. "+existenceErr.Error(), 0)
			jptm.SetStatus(common.ETransferStatus.Failed()) // is a real failure, not just a FileAlreadyExists, in this case
			jptm.ReportTransferDone()
			return
		}
		if exists {
			jptm.LogSendError(info.Source, info.Destination, "File already exists", 0)
			jptm.SetStatus(common.ETransferStatus.FileAlreadyExistsFailure()) // TODO: question: is it OK to always use FileAlreadyExists here, instead of BlobAlreadyExists, even when saving to blob storage?  I.e. do we really need a different error for blobs?
			jptm.ReportTransferDone()
			return
		}
	}

	// step 4: Open the local Source File (if any)
	var sourceFileFactory func() (common.CloseableReaderAt, error)
	srcFile := (common.CloseableReaderAt)(nil)
	if srcInfoProvider.IsLocal() {
		sourceFileFactory = func() (common.CloseableReaderAt, error) {
			return openSourceFile(info)
		}
		srcFile, err = sourceFileFactory()
		if err != nil {
			jptm.LogSendError(info.Source, info.Destination, "Couldn't open source-"+err.Error(), 0)
			jptm.SetStatus(common.ETransferStatus.Failed())
			jptm.ReportTransferDone()
			return
		}
		defer srcFile.Close() // we read all the chunks in this routine, so can close the file at the end
	}

	// Do LMT verfication before transfer, when:
	// 1) Source is local, so get source file's LMT is free.
	// 2) Source is remote, i.e. S2S copy case. And source's size is larger than one chunk. So verification can possibly save transfer's cost.
	if copier, isS2SCopier := s.(s2sCopier); srcInfoProvider.IsLocal() ||
		(isS2SCopier && info.S2SSourceChangeValidation && srcSize > int64(copier.ChunkSize())) {
		lmt, err := srcInfoProvider.GetLastModifiedTime()
		if err != nil {
			jptm.LogSendError(info.Source, info.Destination, "Couldn't get source's last modified time-"+err.Error(), 0)
			jptm.SetStatus(common.ETransferStatus.Failed())
			jptm.ReportTransferDone()
			return
		}
		if lmt.UTC() != jptm.LastModifiedTime().UTC() {
			jptm.LogSendError(info.Source, info.Destination, "File modified since transfer scheduled", 0)
			jptm.SetStatus(common.ETransferStatus.Failed())
			jptm.ReportTransferDone()
			return
		}
	}

	// *****
	// Error-handling rules change here.
	// ABOVE this point, we end the transfer using the code as shown above
	// BELOW this point, this routine always schedules the expected number
	// of chunks, even if it has seen a failure, and the
	// workers (the chunkfunc implementations) must use
	// jptm.FailActiveSend when there's an error)
	// TODO: are we comfortable with this approach?
	//   DECISION: 16 Jan, 2019: for now, we are leaving in place the above rule than number of of completed chunks must
	//   eventually reach numChunks, since we have no better short-term alternative.
	// ******

	// step 5: tell jptm what to expect, and how to clean up at the end
	jptm.SetNumberOfChunks(numChunks)
	jptm.SetActionAfterLastChunk(func() { epilogueWithCleanupSendToRemote(jptm, s, srcInfoProvider) })

	// Step 6: Go through the file and schedule chunk messages to send each chunk
	scheduleSendChunks(jptm, info.Source, srcFile, srcSize, s, sourceFileFactory, srcInfoProvider)
}

func openSourceFile(info TransferInfo) (common.CloseableReaderAt, error) {
	if common.IsPlaceholderForRandomDataGenerator(info.Source) {
		// Generate a "file" of random data. Useful for testing when you want really big files, but don't want
		// to make them yourself
		return common.NewRandomDataGenerator(info.SourceSize), nil
	} else {
		return os.Open(info.Source)
	}
}

// Schedule all the send chunks.
// For upload, we force preload of each chunk to memory, and we wait (block)
// here if the amount of preloaded data gets excessive. That's OK to do,
// because if we already have that much data preloaded (and scheduled for sending in
// chunks) then we don't need to schedule any more chunks right now, so the blocking
// is harmless (and a good thing, to avoid excessive RAM usage).
// To take advantage of the good sequential read performance provided by many file systems,
// and to be able to compute an MD5 hash for the file, we work sequentially through the file here.
func scheduleSendChunks(jptm IJobPartTransferMgr, srcPath string, srcFile common.CloseableReaderAt, srcSize int64, s ISenderBase, sourceFileFactory common.ChunkReaderSourceFactory, srcInfoProvider ISourceInfoProvider) {
	// For generic send
	chunkSize := s.ChunkSize()
	numChunks := s.NumChunks()

	// For upload
	var md5Channel chan<- []byte
	var prefetchErr error
	var chunkReader common.SingleChunkReader
	ps := common.PrologueState{}

	var md5Hasher hash.Hash
	if jptm.ShouldPutMd5() {
		md5Hasher = md5.New()
	} else {
		md5Hasher = common.NewNullHasher()
	}
	safeToUseHash := true

	if srcInfoProvider.IsLocal() {
		md5Channel = s.(uploader).Md5Channel()
		defer close(md5Channel)
	}

	chunkIDCount := int32(0)
	for startIndex := int64(0); startIndex < srcSize || isDummyChunkInEmptyFile(startIndex, srcSize); startIndex += int64(chunkSize) {

		id := common.NewChunkID(srcPath, startIndex)
		adjustedChunkSize := int64(chunkSize)

		// compute actual size of the chunk
		if startIndex+int64(chunkSize) > srcSize {
			adjustedChunkSize = srcSize - startIndex
		}

		if srcInfoProvider.IsLocal() {
			// create reader and prefetch the data into it
			chunkReader = createPopulatedChunkReader(jptm, sourceFileFactory, id, adjustedChunkSize, srcFile)

			// Wait until we have enough RAM, and when we do, prefetch the data for this chunk.
			prefetchErr = chunkReader.BlockingPrefetch(srcFile, false)
			if prefetchErr == nil {
				chunkReader.WriteBufferTo(md5Hasher)
				ps = chunkReader.GetPrologueState()
			} else {
				safeToUseHash = false // because we've missed a chunk
			}
		}

		// If this is the the very first chunk, do special init steps
		if startIndex == 0 {
			// Run prologue before first chunk is scheduled.
			// If file is not local, we'll get no leading bytes, but we still run the prologue in case
			// there's other initialization to do in the sender.
			s.Prologue(ps)
		}

		// schedule the chunk job/msg
		jptm.LogChunkStatus(id, common.EWaitReason.WorkerGR())
		isWholeFile := numChunks == 1
		var cf chunkFunc
		if srcInfoProvider.IsLocal() {
			if prefetchErr == nil {
				cf = s.(uploader).GenerateUploadFunc(id, chunkIDCount, chunkReader, isWholeFile)
			} else {
				_ = chunkReader.Close()
				// Our jptm logic currently requires us to schedule every chunk, even if we know there's an error,
				// so we schedule a func that will just fail with the given error
				cf = createSendToRemoteChunkFunc(jptm, id, func() { jptm.FailActiveSend("chunk data read", prefetchErr) })
			}
		} else {
			cf = s.(s2sCopier).GenerateCopyFunc(id, chunkIDCount, adjustedChunkSize, isWholeFile)
		}
		jptm.ScheduleChunks(cf)

		chunkIDCount++
	}

	// sanity check to verify the number of chunks scheduled
	if chunkIDCount != int32(numChunks) {
		panic(fmt.Errorf("difference in the number of chunk calculated %v and actual chunks scheduled %v for src %s of size %v", numChunks, chunkIDCount, srcPath, srcSize))
	}

	if srcInfoProvider.IsLocal() && safeToUseHash {
		md5Channel <- md5Hasher.Sum(nil)
	}
}

// Make reader for this chunk.
// Each chunk reader also gets a factory to make a reader for the file, in case it needs to repeat its part
// of the file read later (when doing a retry)
// BTW, the reader we create here just works with a single chuck. (That's in contrast with downloads, where we have
// to use an object that encompasses the whole file, so that it can put the chunks back into order. We don't have that requirement here.)
func createPopulatedChunkReader(jptm IJobPartTransferMgr, sourceFileFactory common.ChunkReaderSourceFactory, id common.ChunkID, adjustedChunkSize int64, srcFile common.CloseableReaderAt) common.SingleChunkReader {
	chunkReader := common.NewSingleChunkReader(jptm.Context(),
		sourceFileFactory,
		id,
		adjustedChunkSize,
		jptm.ChunkStatusLogger(),
		jptm,
		jptm.SlicePool(),
		jptm.CacheLimiter())

	return chunkReader
}

func isDummyChunkInEmptyFile(startIndex int64, fileSize int64) bool {
	return startIndex == 0 && fileSize == 0
}

// Complete epilogue. Handles both success and failure.
func epilogueWithCleanupSendToRemote(jptm IJobPartTransferMgr, s ISenderBase, sip ISourceInfoProvider) {
	if jptm.TransferStatus() > 0 {
		if _, isS2SCopier := s.(s2sCopier); sip.IsLocal() || (isS2SCopier && jptm.Info().S2SSourceChangeValidation) {
			// Check the source to see if it was changed during transfer. If it was, mark the transfer as failed.
			lmt, err := sip.GetLastModifiedTime()
			if err != nil {
				jptm.FailActiveSend("epilogueWithCleanupSendToRemote", err)
			}
			if lmt.UTC() != jptm.LastModifiedTime().UTC() {
				jptm.FailActiveSend("epilogueWithCleanupSendToRemote", errors.New("source modified during transfer"))
			}
		}
	}

	s.Epilogue()

	// TODO: finalize and wrap in functions whether 0 is included or excluded in status comparisons
	if jptm.TransferStatus() == 0 {
		panic("think we're finished but status is notStarted")
	}

	// note that we do not really know whether the context was canceled because of an error, or because the user asked for it
	// if was an intentional cancel, the status is still "in progress", so we are still counting it as pending
	// we leave these transfer status alone
	// in case of errors, the status was already set, so we don't need to do anything here either
	//
	// it is entirely possible that all the chunks were finished, but then by the time we get to this line
	// the context is canceled. In this case, a completely transferred file would not be marked "completed".
	// it's definitely a case that we should be aware of, but given how rare it is, and how low the impact (the user can just resume), we don't have to do anything more to it atm.
	if jptm.TransferStatus() > 0 && !jptm.WasCanceled() {
		// We know all chunks are done (because this routine was called)
		// and we know the transfer didn't fail (because just checked its status above and made sure the context was not canceled),
		// so it must have succeeded. So make sure its not left "in progress" state
		jptm.SetStatus(common.ETransferStatus.Success())

		// Final logging
		if jptm.ShouldLog(pipeline.LogInfo) { // TODO: question: can we remove these ShouldLogs?  Aren't they inside Log?
			if _, ok := s.(s2sCopier); ok {
				jptm.Log(pipeline.LogInfo, "COPY SUCCESSFUL")
			} else if _, ok := s.(uploader); ok {
				jptm.Log(pipeline.LogInfo, "UPLOAD SUCCESSFUL")
			} else {
				panic("invalid state: epilogueWithCleanupSendToRemote should be used by COPY and UPLOAD")
			}
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
