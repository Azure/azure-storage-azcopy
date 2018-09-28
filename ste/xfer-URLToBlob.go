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
	"encoding/base64"
	"fmt"
	"net/http"
	"net/url"

	"github.com/Azure/azure-pipeline-go/pipeline"
	"github.com/Azure/azure-storage-azcopy/common"
	"github.com/Azure/azure-storage-blob-go/2018-03-28/azblob"
)

type blockBlobCopy struct {
	jptm           IJobPartTransferMgr
	srcURL         url.URL
	destBlobURL    azblob.BlobURL
	pacer          *pacer
	blockIDs       []string
	srcHTTPHeaders azblob.BlobHTTPHeaders
	srcMetadata    azblob.Metadata
}

func URLToBlob(jptm IJobPartTransferMgr, p pipeline.Pipeline, pacer *pacer) {

	// step 1: Get the source, destination info for the transfer.
	info := jptm.Info()
	srcURL, _ := url.Parse(info.Source)
	destURL, _ := url.Parse(info.Destination)

	destBlobURL := azblob.NewBlobURL(*destURL, p)

	// step 2: Get size info for the copy.
	srcSize := int64(info.SourceSize)
	chunkSize := int64(info.BlockSize)

	// TODO: Validate Block Size, ensure it's validate for both source and destination
	if jptm.ShouldLog(pipeline.LogInfo) {
		jptm.LogTransferStart(info.Source, info.Destination, fmt.Sprintf("Chunk size %d", chunkSize))
	}

	// If the transfer was cancelled, then reporting transfer as done and increasing the bytestransferred by the size of the source.
	if jptm.WasCanceled() {
		jptm.ReportTransferDone()
		return
	}

	// If the force Write flags is set to false
	// then check the blob exists or not.
	// If it does, mark transfer as failed.
	if !jptm.IsForceWriteTrue() {
		_, err := destBlobURL.GetProperties(jptm.Context(), azblob.BlobAccessConditions{})
		if err == nil {
			// If the error is nil, then blob exists and doesn't needs to be copied.
			jptm.LogS2SCopyError(info.Source, info.Destination, "Blob Already Exists ", 0)
			// Mark the transfer as failed with BlobAlreadyExistsFailure
			jptm.SetStatus(common.ETransferStatus.BlobAlreadyExistsFailure())
			jptm.ReportTransferDone()
			return
		}
	}

	// validate blob type and fail for page/append blob temporarily.
	// TODO: support page/append blob when service side is ready.
	if info.SrcBlobType != azblob.BlobNone && info.SrcBlobType != azblob.BlobBlockBlob {
		err := fmt.Errorf("skipping %v. This version of AzCopy only supports BlockBlob transfer", info.SrcBlobType)
		status, msg := ErrorEx{err}.ErrorCodeAndString()
		jptm.LogS2SCopyError(info.Source, info.Destination, msg, status)
		jptm.SetStatus(common.ETransferStatus.Failed())
		jptm.SetErrorCode(int32(status))
		jptm.ReportTransferDone()
		return
	}

	var azblobMetadata azblob.Metadata
	if info.SrcMetadata != nil {
		azblobMetadata = info.SrcMetadata.ToAzBlobMetadata()
	}

	// step 3: copy file to blob
	// Currently only support block blob.
	if srcSize == 0 {
		// Create blob and finish.
		_, err := destBlobURL.ToBlockBlobURL().Upload(jptm.Context(), bytes.NewReader(nil), info.SrcHTTPHeaders, azblobMetadata, azblob.BlobAccessConditions{})
		// if the create blob failed, updating the transfer status to failed
		if err != nil {
			status, msg := ErrorEx{err}.ErrorCodeAndString()
			jptm.LogS2SCopyError(info.Source, info.Destination, msg, status)
			if !jptm.WasCanceled() {
				jptm.SetStatus(common.ETransferStatus.Failed())
				jptm.SetErrorCode(int32(status))
			}
		} else {
			// if the create blob is a success, updating the transfer status to success
			if jptm.ShouldLog(pipeline.LogInfo) {
				jptm.Log(pipeline.LogInfo, "COPY SUCCESSFUL")
			}

			// TODO: set blob tier
			jptm.SetStatus(common.ETransferStatus.Success())
		}

		// updating number of transfers done for job part order
		jptm.ReportTransferDone()
	} else {
		// step 3: go through the source range and schedule copy chunk jobs
		numChunks := common.Iffuint32(
			srcSize%chunkSize == 0,
			uint32(srcSize/chunkSize),
			uint32(srcSize/chunkSize)+1)

		jptm.SetNumberOfChunks(numChunks)

		// If the number of chunks exceeds MaxNumberOfBlocksPerBlob, then copying blob with
		// given blockSize will fail.
		if numChunks > common.MaxNumberOfBlocksPerBlob {
			jptm.LogS2SCopyError(info.Source, info.Destination,
				fmt.Sprintf("BlockSize %d for copying source of size %d is not correct. Number of blocks will exceed the limit", chunkSize, srcSize),
				0)
			jptm.Cancel()
			jptm.SetStatus(common.ETransferStatus.Failed())
			jptm.ReportTransferDone()
			return
		}

		// TODO: Dispatch according to blob types

		// creating a slice to contain the block IDs
		blockIDs := make([]string, numChunks)
		blockIDCount := int32(0)
		adjustedChunkSize := chunkSize

		bbc := &blockBlobCopy{
			jptm:           jptm,
			srcURL:         *srcURL,
			destBlobURL:    destBlobURL,
			pacer:          pacer,
			blockIDs:       blockIDs,
			srcHTTPHeaders: info.SrcHTTPHeaders,
			srcMetadata:    azblobMetadata}

		for startRange := int64(0); startRange < srcSize; startRange += chunkSize {
			// compute exact size of the chunk
			// startRange also equals to overall scheduled size
			if startRange+chunkSize > srcSize {
				adjustedChunkSize = srcSize - startRange
			}

			// schedule the download chunk job
			jptm.ScheduleChunks(
				bbc.generateCopyURLToBlockBlobFunc(blockIDCount, startRange, adjustedChunkSize))
			blockIDCount++
		}
	}
}

func (bbc *blockBlobCopy) generateCopyURLToBlockBlobFunc(chunkId int32, startIndex int64, adjustedChunkSize int64) chunkFunc {
	return func(workerId int) {

		// TODO: added the two operations for debugging purpose. remove later
		// Increment a number of goroutine performing the transfer / acting on chunks msg by 1
		bbc.jptm.OccupyAConnection()
		// defer the decrement in the number of goroutine performing the transfer / acting on chunks msg by 1
		defer bbc.jptm.ReleaseAConnection()

		// This function allows routine to manage behavior of unexpected panics.
		// The panic error along with transfer details are logged.
		// The transfer is marked as failed and is reported as done.
		//defer func(jptm IJobPartTransferMgr) {
		//	r := recover()
		//	if r != nil {
		//		info := jptm.Info()
		//		if jptm.ShouldLog(pipeline.LogError) {
		//			jptm.Log(pipeline.LogError, fmt.Sprintf(" recovered from unexpected crash %s. Transfer Src %s Dst %s SrcSize %v startIndex %v chunkSize %v",
		//				r, info.Source, info.Destination, info.SourceSize, startIndex, adjustedChunkSize))
		//		}
		//		jptm.SetStatus(common.ETransferStatus.Failed())
		//		jptm.ReportTransferDone()
		//	}
		//}(bbc.jptm)

		// and the chunkFunc has been changed to the version without param workId
		// transfer done is internal function which marks the transfer done.
		transferDone := func() {
			bbc.jptm.Log(pipeline.LogInfo, "Transfer Done")
			// Get the Status of the transfer
			// If the transfer status value < 0, then transfer failed with some failure
			// there is a possibility that some uncommitted blocks will be there
			// Delete the uncommitted blobs
			if bbc.jptm.TransferStatus() <= 0 {
				_, err := bbc.destBlobURL.ToBlockBlobURL().Delete(context.TODO(), azblob.DeleteSnapshotsOptionNone, azblob.BlobAccessConditions{})
				if stErr, ok := err.(azblob.StorageError); ok && stErr.Response().StatusCode != http.StatusNotFound {
					// If the delete failed with Status Not Found, then it means there were no uncommitted blocks.
					// Other errors report that uncommitted blocks are there
					bbc.jptm.LogError(bbc.destBlobURL.String(), "Delete Uncommitted blocks ", err)
				}
			}
			bbc.jptm.ReportTransferDone()
		}

		if bbc.jptm.WasCanceled() {
			if bbc.jptm.ShouldLog(pipeline.LogDebug) {
				bbc.jptm.Log(pipeline.LogDebug, fmt.Sprintf("Transfer cancelled. not picking up chunk %d", chunkId))
			}
			if lastChunk, _ := bbc.jptm.ReportChunkDone(); lastChunk {
				if bbc.jptm.ShouldLog(pipeline.LogDebug) {
					bbc.jptm.Log(pipeline.LogDebug, "Finalizing transfer cancellation")
				}
				transferDone()
			}
			return
		}
		// step 1: generate block ID
		blockID := common.NewUUID().String()
		encodedBlockID := base64.StdEncoding.EncodeToString([]byte(blockID))

		// step 2: save the block ID into the list of block IDs
		(bbc.blockIDs)[chunkId] = encodedBlockID

		// step 3: perform put block
		destBlockBlobURL := bbc.destBlobURL.ToBlockBlobURL()
		_, err := destBlockBlobURL.StageBlockFromURL(bbc.jptm.Context(), encodedBlockID, bbc.srcURL, startIndex, adjustedChunkSize, azblob.LeaseAccessConditions{})
		if err != nil {
			// check if the transfer was cancelled while Stage Block was in process.
			if bbc.jptm.WasCanceled() {
				bbc.jptm.LogError(bbc.destBlobURL.String(), "Chunk copy from URL failed ", err)
			} else {
				// cancel entire transfer because this chunk has failed
				bbc.jptm.Cancel()
				status, msg := ErrorEx{err}.ErrorCodeAndString()
				bbc.jptm.LogS2SCopyError(bbc.srcURL.String(), bbc.destBlobURL.String(), msg, status)
				//updateChunkInfo(jobId, partNum, transferId, uint16(chunkId), ChunkTransferStatusFailed, jobsInfoMap)
				bbc.jptm.SetStatus(common.ETransferStatus.Failed())
				bbc.jptm.SetErrorCode(int32(status))
			}

			if lastChunk, _ := bbc.jptm.ReportChunkDone(); lastChunk {
				if bbc.jptm.ShouldLog(pipeline.LogDebug) {
					bbc.jptm.Log(pipeline.LogDebug, "Finalizing transfer cancellation")
				}
				transferDone()
			}
			return
		}

		// step 4: check if this is the last chunk
		if lastChunk, _ := bbc.jptm.ReportChunkDone(); lastChunk {
			// If the transfer gets cancelled before the putblock list
			if bbc.jptm.WasCanceled() {
				transferDone()
				return
			}
			// step 5: this is the last block, perform EPILOGUE
			if bbc.jptm.ShouldLog(pipeline.LogDebug) {
				bbc.jptm.Log(pipeline.LogDebug, "Concluding transfer")
			}

			// commit the blocks.
			_, err := destBlockBlobURL.CommitBlockList(bbc.jptm.Context(), bbc.blockIDs, bbc.srcHTTPHeaders, bbc.srcMetadata, azblob.BlobAccessConditions{})
			if err != nil {
				status, msg := ErrorEx{err}.ErrorCodeAndString()
				bbc.jptm.LogS2SCopyError(bbc.srcURL.String(), bbc.destBlobURL.String(), "Commit block list"+msg, status)
				bbc.jptm.SetStatus(common.ETransferStatus.Failed())
				bbc.jptm.SetErrorCode(int32(status))
				bbc.jptm.SetErrorCode(int32(status))
				transferDone()
				return
			}

			if bbc.jptm.ShouldLog(pipeline.LogInfo) {
				bbc.jptm.Log(pipeline.LogInfo, "COPY SUCCESSFUL")
			}

			// TODO: get and set blob tier correctly
			// blockBlobTier, _ := bbc.jptm.BlobTiers()
			// if blockBlobTier != common.EBlockBlobTier.None() {
			// 	// for blob tier, set the latest service version from sdk as service version in the context.
			// 	ctxWithValue := context.WithValue(bbc.jptm.Context(), ServiceAPIVersionOverride, azblob.ServiceVersion)
			// 	_, err := destBlockBlobURL.SetTier(ctxWithValue, blockBlobTier.ToAccessTierType())
			// 	if err != nil {
			// 		if bbc.jptm.ShouldLog(pipeline.LogError) {
			// 			bbc.jptm.Log(pipeline.LogError,
			// 				fmt.Sprintf("copy from URL to block blob failed, worker %d failed to set tier %s on blob and failed with error %s",
			// 					workerId, blockBlobTier, string(err.Error())))
			// 		}
			// 		bbc.jptm.SetStatus(common.ETransferStatus.BlobTierFailure())
			// 	}
			// }
			bbc.jptm.SetStatus(common.ETransferStatus.Success())
			transferDone()
		}
	}
}
