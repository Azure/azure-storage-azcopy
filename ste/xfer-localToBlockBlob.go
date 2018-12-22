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

// TODO: remove this file
/* replaced by uploader-blockBlob

import (
	"bytes"
	"context"
	"encoding/base64"
	"fmt"
	"net/http"
	"net/url"
	"os"

	"github.com/Azure/azure-pipeline-go/pipeline"
	"github.com/Azure/azure-storage-azcopy/common"
	"github.com/Azure/azure-storage-blob-go/azblob"
)

type blockBlobUpload struct {
	jptm        IJobPartTransferMgr
	srcFile     *os.File
	source      string
	destination string
	blobURL     azblob.BlobURL
	pacer       *pacer
	blockIds    []string
}

func LocalToBlockBlob(jptm IJobPartTransferMgr, p pipeline.Pipeline, pacer *pacer) {
	// step 1. Get the transfer Information which include source, destination string, source size and other information.
	info := jptm.Info()
	blobSize := int64(info.SourceSize)
	chunkSize := int64(info.BlockSize)

	destURL, _ := url.Parse(info.Destination)

	blobUrl := azblob.NewBlobURL(*destURL, p)

	if jptm.ShouldLog(pipeline.LogInfo) {
		jptm.LogTransferStart(info.Source, info.Destination, fmt.Sprintf("Specified chunk size %d", chunkSize))
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
		_, err := blobUrl.GetProperties(jptm.Context(), azblob.BlobAccessConditions{})
		if err == nil {
			// TODO: Confirm if this is an error condition or not
			// If the error is nil, then blob exists and it doesn't need to be uploaded.
			jptm.LogUploadError(info.Source, info.Destination, "Blob already exists", 0)
			// Mark the transfer as failed with BlobAlreadyExistsFailure
			jptm.SetStatus(common.ETransferStatus.BlobAlreadyExistsFailure())
			jptm.ReportTransferDone()
			return
		}
	}

	// step 2a: Open the Source File.
	srcFile, err := os.Open(info.Source)
	if err != nil {
		jptm.LogUploadError(info.Source, info.Destination, "Couldn't open source-"+err.Error(), 0)
		jptm.SetStatus(common.ETransferStatus.Failed())
		jptm.ReportTransferDone()
		return
	}
	if blobSize == 0 || blobSize <= chunkSize {
		// step 3.b: if blob size is smaller than chunk size and it is not a vhd file
		// we should do a put blob instead of chunk up the file
		PutBlobUploadFunc(jptm, srcFile, blobUrl.ToBlockBlobURL(), pacer)
		return
	} else {
		// step 3.c: If the source is not a vhd and size is greater than chunk Size,
		// then uploading the source as block Blob.
		// calculating num of chunks using the source size and chunkSize.
		numChunks := common.Iffuint32(
			blobSize%chunkSize == 0,
			uint32(blobSize/chunkSize),
			uint32(blobSize/chunkSize)+1)

		// Set the number of chunk for the current transfer.
		jptm.SetNumberOfChunks(numChunks)

		// If the number of chunks exceeds MaxNumberOfBlocksPerBlob, then uploading blob with
		// given blockSize will fail.
		if numChunks > common.MaxNumberOfBlocksPerBlob {
			jptm.LogUploadError(info.Source, info.Destination,
				fmt.Sprintf("BlockSize %d for uploading source of size %d is not correct. Number of blocks will exceed the limit", chunkSize, blobSize),
				0)
			jptm.Cancel()
			jptm.SetStatus(common.ETransferStatus.Failed())
			jptm.ReportTransferDone()
			return
		}
		// creating a slice to contain the blockIds
		blockIds := make([]string, numChunks)
		blockIdCount := int32(0)

		// creating block Blob struct which holds the srcFile, srcMmf memory map byte slice, pacer instance and blockId list.
		// Each chunk uses these details which uploading the block.
		bbu := &blockBlobUpload{
			jptm:        jptm,
			srcFile:     srcFile,
			source:      info.Source,
			destination: info.Destination,
			blobURL:     blobUrl,
			pacer:       pacer,
			blockIds:    blockIds}

		// go through the file and schedule chunk messages to upload each chunk
		for startIndex := int64(0); startIndex < blobSize; startIndex += chunkSize {
			adjustedChunkSize := chunkSize

			// compute actual size of the chunk
			if startIndex+chunkSize > blobSize {
				adjustedChunkSize = blobSize - startIndex
			}

			// schedule the chunk job/msg
			jptm.ScheduleChunks(bbu.blockBlobUploadFunc(blockIdCount, startIndex, adjustedChunkSize))

			blockIdCount += 1
		}
		// sanity check to verify the number of chunks scheduled
		if blockIdCount != int32(numChunks) {
			jptm.Panic(fmt.Errorf("difference in the number of chunk calculated %v and actual chunks scheduled %v for src %s of size %v", numChunks, blockIdCount, info.Source, blobSize))
		}
	}
}

// This method blockBlobUploadFunc uploads the block of src data from given startIndex till the given chunkSize.
func (bbu *blockBlobUpload) blockBlobUploadFunc(chunkId int32, startIndex int64, adjustedChunkSize int64) chunkFunc {
	return func(workerId int) {

		// TODO: added the two operations for debugging purpose. remove later
		// Increment a number of goroutine performing the transfer / acting on chunks msg by 1
		bbu.jptm.OccupyAConnection()
		// defer the decrement in the number of goroutine performing the transfer / acting on chunks msg by 1
		defer bbu.jptm.ReleaseAConnection()

		// and the chunkFunc has been changed to the version without param workId
		// transfer done is internal function which marks the transfer done, unmaps the src file and close the  source file.
		transferDone := func() {
			bbu.jptm.Log(pipeline.LogInfo, "Transfer done")
			// Get the Status of the transfer
			// If the transfer status value < 0, then transfer failed with some failure
			// there is a possibility that some uncommitted blocks will be there
			// Delete the uncommitted blobs
			if bbu.jptm.TransferStatus() <= 0 {
				_, err := bbu.blobURL.ToBlockBlobURL().Delete(context.TODO(), azblob.DeleteSnapshotsOptionNone, azblob.BlobAccessConditions{})
				if stErr, ok := err.(azblob.StorageError); ok && stErr.Response().StatusCode != http.StatusNotFound {
					// If the delete failed with Status Not Found, then it means there were no uncommitted blocks.
					// Other errors report that uncommitted blocks are there
					bbu.jptm.LogError(bbu.blobURL.String(), "Deleting uncommitted blocks", err)
				}
			}
			bbu.srcFile.Close()

			bbu.jptm.ReportTransferDone()
		}

		if bbu.jptm.WasCanceled() {
			if bbu.jptm.ShouldLog(pipeline.LogDebug) {
				bbu.jptm.Log(pipeline.LogDebug, fmt.Sprintf("Transfer cancelled; skipping chunk %d", chunkId))
			}
			if lastChunk, _ := bbu.jptm.ReportChunkDone(); lastChunk {
				if bbu.jptm.ShouldLog(pipeline.LogDebug) {
					bbu.jptm.Log(pipeline.LogDebug,
						fmt.Sprintf("Finalizing transfer cancellation"))
				}
				transferDone()
			}
			return
		}
		// step 1: generate block ID
		blockId := common.NewUUID().String()
		encodedBlockId := base64.StdEncoding.EncodeToString([]byte(blockId))

		// step 2: save the block ID into the list of block IDs
		(bbu.blockIds)[chunkId] = encodedBlockId

		srcMMF, err := common.NewMMF(bbu.srcFile, false, startIndex, adjustedChunkSize)
		if err != nil {
			if bbu.jptm.WasCanceled() {
				if bbu.jptm.ShouldLog(pipeline.LogDebug) {
					bbu.jptm.Log(pipeline.LogDebug,
						fmt.Sprintf("Chunk %d upload failed because transfer was cancelled", chunkId))
				}
			} else {
				// cancel entire transfer because this chunk has failed
				bbu.jptm.Cancel()
				status, msg := ErrorEx{err}.ErrorCodeAndString()
				bbu.jptm.LogUploadError(bbu.source, bbu.destination, "Chunk Upload Failed "+msg, status)
				bbu.jptm.SetStatus(common.ETransferStatus.Failed())
				bbu.jptm.SetErrorCode(int32(status))
			}

			if lastChunk, _ := bbu.jptm.ReportChunkDone(); lastChunk {
				if bbu.jptm.ShouldLog(pipeline.LogDebug) {
					bbu.jptm.Log(pipeline.LogDebug,
						fmt.Sprintf(" Finalizing transfer cancellation"))
				}
				transferDone()
			}
			return
		}

		defer srcMMF.Unmap()
		// step 3: perform put block
		blockBlobUrl := bbu.blobURL.ToBlockBlobURL()
		body := newRequestBodyPacer(bytes.NewReader(srcMMF.Slice()), bbu.pacer, srcMMF)
		_, err = blockBlobUrl.StageBlock(bbu.jptm.Context(), encodedBlockId, body, azblob.LeaseAccessConditions{}, nil)
		if err != nil {
			// check if the transfer was cancelled while Stage Block was in process.
			if bbu.jptm.WasCanceled() {
				if bbu.jptm.ShouldLog(pipeline.LogDebug) {
					bbu.jptm.Log(pipeline.LogDebug,
						fmt.Sprintf("Chunk %d upload failed because transfer was cancelled", chunkId))
				}
			} else {
				// cancel entire transfer because this chunk has failed
				bbu.jptm.Cancel()
				status, msg := ErrorEx{err}.ErrorCodeAndString()
				bbu.jptm.LogUploadError(bbu.source, bbu.destination, "Chunk Upload Failed "+msg, status)
				//updateChunkInfo(jobId, partNum, transferId, uint16(chunkId), ChunkTransferStatusFailed, jobsInfoMap)
				bbu.jptm.SetStatus(common.ETransferStatus.Failed())
				bbu.jptm.SetErrorCode(int32(status))
				// If the status code was 403, it means there was an authentication error and we exit.
				// User can resume the job if completely ordered with a new sas.
				if status == http.StatusForbidden {
					common.GetLifecycleMgr().Exit(fmt.Sprintf("Authentication Failed. The SAS is not correct or expired or does not have the correct permission %s", err.Error()), 1)
				}
			}

			if lastChunk, _ := bbu.jptm.ReportChunkDone(); lastChunk {
				if bbu.jptm.ShouldLog(pipeline.LogDebug) {
					bbu.jptm.Log(pipeline.LogDebug,
						fmt.Sprintf(" Finalizing transfer cancellation"))
				}
				transferDone()
			}
			return
		}

		// step 4: check if this is the last chunk
		if lastChunk, _ := bbu.jptm.ReportChunkDone(); lastChunk {
			// If the transfer gets cancelled before the putblock list
			if bbu.jptm.WasCanceled() {
				transferDone()
				return
			}
			// step 5: this is the last block, perform EPILOGUE
			if bbu.jptm.ShouldLog(pipeline.LogDebug) {
				bbu.jptm.Log(pipeline.LogDebug,
					fmt.Sprintf("Conclude Transfer with BlockList %s", bbu.blockIds))
			}

			// fetching the blob http headers with content-type, content-encoding attributes
			// fetching the metadata passed with the JobPartOrder
			blobHttpHeader, metaData := bbu.jptm.BlobDstData(srcMMF.Slice())

			// commit the blocks.
			_, err := blockBlobUrl.CommitBlockList(bbu.jptm.Context(), bbu.blockIds, blobHttpHeader, metaData, azblob.BlobAccessConditions{})
			if err != nil {
				status, msg := ErrorEx{err}.ErrorCodeAndString()
				bbu.jptm.LogUploadError(bbu.source, bbu.destination, "Commit block list failed "+msg, status)
				bbu.jptm.SetStatus(common.ETransferStatus.Failed())
				bbu.jptm.SetErrorCode(int32(status))
				transferDone()
				// If the status code was 403, it means there was an authentication error and we exit.
				// User can resume the job if completely ordered with a new sas.
				if status == http.StatusForbidden {
					common.GetLifecycleMgr().Exit(fmt.Sprintf("Authentication Failed. The SAS is not correct or expired or does not have the correct permission %s", err.Error()), 1)
				}
				return
			}

			if bbu.jptm.ShouldLog(pipeline.LogInfo) {
				bbu.jptm.Log(pipeline.LogInfo, "UPLOAD SUCCESSFUL ")
			}

			blockBlobTier, _ := bbu.jptm.BlobTiers()
			if blockBlobTier != common.EBlockBlobTier.None() {
				// for blob tier, set the latest service version from sdk as service version in the context.
				ctxWithValue := context.WithValue(bbu.jptm.Context(), ServiceAPIVersionOverride, azblob.ServiceVersion)
				_, err := blockBlobUrl.SetTier(ctxWithValue, blockBlobTier.ToAccessTierType(), azblob.LeaseAccessConditions{})
				if err != nil {
					status, msg := ErrorEx{err}.ErrorCodeAndString()
					bbu.jptm.LogUploadError(bbu.source, bbu.destination, "BlockBlob SetTier "+msg, status)
					bbu.jptm.SetStatus(common.ETransferStatus.BlobTierFailure())
					bbu.jptm.SetErrorCode(int32(status))
					// If the status code was 403, it means there was an authentication error and we exit.
					// User can resume the job if completely ordered with a new sas.
					if status == http.StatusForbidden {
						common.GetLifecycleMgr().Exit(fmt.Sprintf("Authentication Failed. The SAS is not correct or expired or does not have the correct permission %s", err.Error()), 1)
					}
				}
			}
			bbu.jptm.SetStatus(common.ETransferStatus.Success())
			transferDone()
		}
	}
}

func PutBlobUploadFunc(jptm IJobPartTransferMgr, srcFile *os.File, blockBlobUrl azblob.BlockBlobURL, pacer *pacer) {

	// TODO: added the two operations for debugging purpose. remove later
	// Increment a number of goroutine performing the transfer / acting on chunks msg by 1
	jptm.OccupyAConnection()
	// defer the decrement in the number of goroutine performing the transfer / acting on chunks msg by 1
	defer jptm.ReleaseAConnection()

	defer srcFile.Close()

	tInfo := jptm.Info()

	var err error
	srcMMF := &common.MMF{}
	if tInfo.SourceSize > 0 {
		srcMMF, err = common.NewMMF(srcFile, false, 0, tInfo.SourceSize)
		if err != nil {
			status, msg := ErrorEx{err}.ErrorCodeAndString()
			jptm.LogUploadError(tInfo.Source, tInfo.Destination, "PutBlob Failed "+msg, status)
			if !jptm.WasCanceled() {
				jptm.SetStatus(common.ETransferStatus.Failed())
				jptm.SetErrorCode(int32(status))
			}
		}
		defer srcMMF.Unmap()
	}

	// Get blob http headers and metadata.
	blobHttpHeader, metaData := jptm.BlobDstData(srcMMF.Slice())

	// take care of empty blobs
	if tInfo.SourceSize == 0 {
		_, err = blockBlobUrl.Upload(jptm.Context(), bytes.NewReader(nil), blobHttpHeader, metaData, azblob.BlobAccessConditions{})
	} else {
		body := newRequestBodyPacer(bytes.NewReader(srcMMF.Slice()), pacer, srcMMF)
		_, err = blockBlobUrl.Upload(jptm.Context(), body, blobHttpHeader, metaData, azblob.BlobAccessConditions{})
	}

	// if the put blob is a failure, updating the transfer status to failed
	if err != nil {
		status, msg := ErrorEx{err}.ErrorCodeAndString()
		jptm.LogUploadError(tInfo.Source, tInfo.Destination, "PutBlob Failed "+msg, status)
		if !jptm.WasCanceled() {
			jptm.SetStatus(common.ETransferStatus.Failed())
			jptm.SetErrorCode(int32(status))
			// If the status code was 403, it means there was an authentication error and we exit.
			// User can resume the job if completely ordered with a new sas.
			if status == http.StatusForbidden {
				common.GetLifecycleMgr().Exit(fmt.Sprintf("Authentication Failed. The SAS is not correct or expired or does not have the correct permission %s", err.Error()), 1)
			}
		}
	} else {
		// if the put blob is a success, updating the transfer status to success
		if jptm.ShouldLog(pipeline.LogInfo) {
			jptm.Log(pipeline.LogInfo, "UPLOAD SUCCESSFUL")
		}

		blockBlobTier, _ := jptm.BlobTiers()
		if blockBlobTier != common.EBlockBlobTier.None() {
			// for blob tier, set the latest service version from sdk as service version in the context.
			ctxWithValue := context.WithValue(jptm.Context(), ServiceAPIVersionOverride, azblob.ServiceVersion)
			_, err := blockBlobUrl.SetTier(ctxWithValue, blockBlobTier.ToAccessTierType(), azblob.LeaseAccessConditions{})
			if err != nil {
				status, msg := ErrorEx{err}.ErrorCodeAndString()
				jptm.LogUploadError(tInfo.Source, tInfo.Destination, "BlockBlob SetTier "+msg, status)
				jptm.SetStatus(common.ETransferStatus.BlobTierFailure())
				jptm.SetErrorCode(int32(status))
				// If the status code was 403, it means there was an authentication error and we exit.
				// User can resume the job if completely ordered with a new sas.
				if status == http.StatusForbidden {
					common.GetLifecycleMgr().Exit(fmt.Sprintf("Authentication Failed. The SAS is not correct or expired or does not have the correct permission %s", err.Error()), 1)
				}
				// since blob tier failed, the transfer failed
				// the blob created should be deleted
				_, err := blockBlobUrl.Delete(context.TODO(), azblob.DeleteSnapshotsOptionNone, azblob.BlobAccessConditions{})
				if err != nil {
					jptm.LogError(blockBlobUrl.String(), "DeleteBlobFailed", err)
				}
			}
		}
		jptm.SetStatus(common.ETransferStatus.Success())
	}

	// updating number of transfers done for job part order
	jptm.ReportTransferDone()
} */
