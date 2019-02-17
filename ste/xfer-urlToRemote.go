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

	"github.com/Azure/azure-pipeline-go/pipeline"
	"github.com/Azure/azure-storage-azcopy/common"
)

// urlToRemote copies resource through URL to other remote persistence location.
func urlToRemote(jptm IJobPartTransferMgr, p pipeline.Pipeline, pacer *pacer, cpf s2sCopierFactory, sipf s2sSourceInfoProviderFactory) {

	info := jptm.Info()
	srcSize := info.SourceSize

	// step 1. perform initial checks
	if jptm.WasCanceled() {
		jptm.ReportTransferDone()
		return
	}

	srcInfoProvider, err := sipf(jptm)
	if err != nil {
		jptm.LogS2SCopyError(info.Source, info.Destination, err.Error(), 0)
		jptm.SetStatus(common.ETransferStatus.Failed())
		jptm.ReportTransferDone()
		return
	}

	// step 2a. Create s2s copier
	s2sCopier, err := cpf(jptm, srcInfoProvider, info.Destination, p, pacer)
	if err != nil {
		jptm.LogS2SCopyError(info.Source, info.Destination, err.Error(), 0)
		jptm.SetStatus(common.ETransferStatus.Failed())
		jptm.ReportTransferDone()
		return
	}

	// step 2b. Read chunk size and count from the copier (since it may have applied its own defaults and/or calculations to produce these values
	chunkSize := s2sCopier.ChunkSize()
	numChunks := s2sCopier.NumChunks()
	if jptm.ShouldLog(pipeline.LogInfo) {
		jptm.LogTransferStart(info.Source, info.Destination, fmt.Sprintf("Specified chunk size %d", chunkSize))
	}
	if numChunks == 0 {
		panic("must always schedule one chunk, even if file is empty") // this keeps our code structure simpler, by using a dummy chunk for empty files
	}

	// step 3: Check overwrite
	// If the force Write flags is set to false
	// then check the file exists at the remote location
	// If it does, mark transfer as failed.
	if !jptm.IsForceWriteTrue() {
		exists, existenceErr := s2sCopier.RemoteFileExists()
		if existenceErr != nil {
			jptm.LogS2SCopyError(info.Source, info.Destination, "Could not check file existence. "+existenceErr.Error(), 0)
			jptm.SetStatus(common.ETransferStatus.Failed()) // is a real failure, not just a FileAlreadyExists, in this case
			jptm.ReportTransferDone()
			return
		}
		if exists {
			jptm.LogS2SCopyError(info.Source, info.Destination, "File already exists", 0)
			jptm.SetStatus(common.ETransferStatus.FileAlreadyExistsFailure()) // TODO: question: is it OK to always use FileAlreadyExists here, instead of BlobAlreadyExists, even when saving to blob storage?  I.e. do we really need a different error for blobs?
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
	// jptm.FailActiveS2SCopy when there's an error)
	// ******

	// step 4: tell jptm what to expect, and how to clean up at the end
	jptm.SetNumberOfChunks(numChunks)
	jptm.SetActionAfterLastChunk(func() { epilogueWithCleanupSendToRemote(jptm, s2sCopier) })

	// Step 5: Schedule to copy each chunk.
	chunkIDCount := int32(0)
	for startIndex := int64(0); startIndex < srcSize || isDummyChunkInEmptyFile(srcSize, startIndex); startIndex += int64(chunkSize) {

		id := common.ChunkID{Name: info.Source, OffsetInFile: startIndex}
		adjustedChunkSize := int64(chunkSize)

		// compute actual size of the chunk
		if startIndex+int64(chunkSize) > srcSize {
			adjustedChunkSize = srcSize - startIndex
		}

		// If this is the the very first chunk, do special init steps
		if startIndex == 0 {
			// Run prologue before first chunk is scheduled
			// There is deliberately no error return value from the Prologue.
			// If it failed, the Prologue itself must call jptm.FailActiveS2SCopy.
			s2sCopier.Prologue(PrologueState{})
		}

		// schedule the chunk job/msg
		jptm.LogChunkStatus(id, common.EWaitReason.WorkerGR())
		isWholeFile := numChunks == 1
		jptm.ScheduleChunks(s2sCopier.GenerateCopyFunc(id, chunkIDCount, adjustedChunkSize, isWholeFile))

		chunkIDCount++
	}

	// sanity check to verify the number of chunks scheduled
	if chunkIDCount != int32(numChunks) {
		jptm.Panic(fmt.Errorf("difference in the number of chunk calculated %v and actual chunks scheduled %v for src %s of size %v", numChunks, chunkIDCount, info.Source, srcSize))
	}
}
