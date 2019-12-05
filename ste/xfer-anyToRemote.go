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
	"net/url"
	"strings"
	"sync"

	"github.com/Azure/azure-pipeline-go/pipeline"
	"github.com/Azure/azure-storage-azcopy/common"
)

// This sync.Once is present to ensure we output information about a S2S access tier preservation failure to stdout once
var s2sAccessTierFailureLogStdout sync.Once

// anyToRemote handles all kinds of sender operations - both uploads from local files, and S2S copies
func anyToRemote(jptm IJobPartTransferMgr, p pipeline.Pipeline, pacer pacer, senderFactory senderFactory, sipf sourceInfoProviderFactory) {

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

	// step 3: check overwrite option
	// if the force Write flags is set to false or prompt
	// then check the file exists at the remote location
	// if it does, react accordingly
	if jptm.GetOverwriteOption() != common.EOverwriteOption.True() {
		exists, existenceErr := s.RemoteFileExists()
		if existenceErr != nil {
			jptm.LogSendError(info.Source, info.Destination, "Could not check file existence. "+existenceErr.Error(), 0)
			jptm.SetStatus(common.ETransferStatus.Failed()) // is a real failure, not just a SkippedFileAlreadyExists, in this case
			jptm.ReportTransferDone()
			return
		}
		if exists {
			shouldOverwrite := false

			// if necessary, prompt to confirm user's intent
			if jptm.GetOverwriteOption() == common.EOverwriteOption.Prompt() {
				// remove the SAS before prompting the user
				parsed, _ := url.Parse(info.Destination)
				parsed.RawQuery = ""
				shouldOverwrite = jptm.GetOverwritePrompter().shouldOverwrite(parsed.String())
			}

			if !shouldOverwrite {
				// logging as Warning so that it turns up even in compact logs, and because previously we use Error here
				jptm.LogAtLevelForCurrentTransfer(pipeline.LogWarning, "File already exists, so will be skipped")
				jptm.SetStatus(common.ETransferStatus.SkippedFileAlreadyExists())
				jptm.ReportTransferDone()
				return
			}
		}
	}

	// step 4: Open the local Source File (if any)
	var sourceFileFactory func() (common.CloseableReaderAt, error)
	srcFile := (common.CloseableReaderAt)(nil)
	if srcInfoProvider.IsLocal() {
		sourceFileFactory = srcInfoProvider.(ILocalSourceInfoProvider).OpenSourceFile // all local providers must implement this interface
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

	// step 5a: lock the destination
	// (is safe to do it relatively early here, before we run the prologue, because its just a internal lock, within the app)
	// But must be after all of the early returns that are above here (since
	// if we succeed here, we need to know the epilogue will definitely run to unlock later)
	err = jptm.WaitUntilLockDestination(jptm.Context())
	if err != nil {
		jptm.LogSendError(info.Source, info.Destination, err.Error(), 0)
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
	// jptm.FailActiveSend when there's an error)
	// TODO: are we comfortable with this approach?
	//   DECISION: 16 Jan, 2019: for now, we are leaving in place the above rule than number of of completed chunks must
	//   eventually reach numChunks, since we have no better short-term alternative.
	// ******

	// step 5b: tell jptm what to expect, and how to clean up at the end
	jptm.SetNumberOfChunks(numChunks)
	jptm.SetActionAfterLastChunk(func() { epilogueWithCleanupSendToRemote(jptm, s, srcInfoProvider) })

	// Step 6: Go through the file and schedule chunk messages to send each chunk
	scheduleSendChunks(jptm, info.Source, srcFile, srcSize, s, sourceFileFactory, srcInfoProvider)
}

var jobCancelledLocalPrefetchErr = errors.New("job was cancelled; Pre-fetching stopped")

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

		adjustedChunkSize := int64(chunkSize)

		// compute actual size of the chunk
		if startIndex+int64(chunkSize) > srcSize {
			adjustedChunkSize = srcSize - startIndex
		}

		id := common.NewChunkID(srcPath, startIndex, adjustedChunkSize) // TODO: stop using adjustedChunkSize, below, and use the size that's in the ID

		if srcInfoProvider.IsLocal() {
			if jptm.WasCanceled() {
				prefetchErr = jobCancelledLocalPrefetchErr
			} else {
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
		}

		// If this is the the very first chunk, do special init steps
		if startIndex == 0 {
			// Run prologue before first chunk is scheduled.
			// If file is not local, we'll get no leading bytes, but we still run the prologue in case
			// there's other initialization to do in the sender.
			modified := s.Prologue(ps)
			if modified {
				jptm.SetDestinationIsModified()
			}
		}

		// schedule the chunk job/msg
		jptm.LogChunkStatus(id, common.EWaitReason.WorkerGR())
		isWholeFile := numChunks == 1
		var cf chunkFunc
		if srcInfoProvider.IsLocal() {
			if prefetchErr == nil {
				cf = s.(uploader).GenerateUploadFunc(id, chunkIDCount, chunkReader, isWholeFile)
			} else {
				if chunkReader != nil {
					_ = chunkReader.Close()
				}
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
	info := jptm.Info()
	// allow our usual state tracking mechanism to keep count of how many epilogues are running at any given instant, for perf diagnostics
	pseudoId := common.NewPseudoChunkIDForWholeFile(info.Source)
	jptm.LogChunkStatus(pseudoId, common.EWaitReason.Epilogue())
	defer jptm.LogChunkStatus(pseudoId, common.EWaitReason.ChunkDone()) // normal setting to done doesn't apply to these pseudo ids

	if jptm.IsLive() {
		if _, isS2SCopier := s.(s2sCopier); sip.IsLocal() || (isS2SCopier && info.S2SSourceChangeValidation) {
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

	s.Epilogue() // Perform service-specific cleanup before jptm cleanup. Some services may actually require setup to make the file actually appear.

	if jptm.IsLive() && info.DestLengthValidation {
		_, isS2SCopier := s.(s2sCopier)
		destLength, err := s.GetDestinationLength()

		if err != nil {
			jptm.FailActiveSend(common.IffString(isS2SCopier, "S2S ", "Upload ")+"Length check: Get destination length", err)
		}

		if destLength != jptm.Info().SourceSize {
			jptm.FailActiveSend(common.IffString(isS2SCopier, "S2S ", "Upload ")+"Length check", errors.New("destination length does not match source length"))
		}
	}

	if jptm.HoldsDestinationLock() { // TODO consider add test of jptm.IsDeadInflight here, so we can remove that from inside all the cleanup methods
		s.Cleanup() // Perform jptm cleanup, if THIS jptm has the lock on the destination
	}

	jptm.UnlockDestination()

	if jptm.TransferStatusIgnoringCancellation() == 0 {
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
	if jptm.IsLive() {
		// We know all chunks are done (because this routine was called)
		// and we know the transfer didn't fail (because just checked its status above and made sure the context was not canceled),
		// so it must have succeeded. So make sure its not left "in progress" state
		jptm.SetStatus(common.ETransferStatus.Success())

		// Final logging
		if jptm.ShouldLog(pipeline.LogInfo) { // TODO: question: can we remove these ShouldLogs?  Aren't they inside Log?
			if _, ok := s.(s2sCopier); ok {
				jptm.Log(pipeline.LogInfo, fmt.Sprintf("COPYSUCCESSFUL: %s", strings.Split(info.Destination, "?")[0]))
			} else if _, ok := s.(uploader); ok {
				// Output relative path of file, includes file name.
				jptm.Log(pipeline.LogInfo, fmt.Sprintf("UPLOADSUCCESSFUL: %s", strings.Split(info.Destination, "?")[0]))
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
