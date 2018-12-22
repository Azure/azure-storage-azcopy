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
	"errors"
	"fmt"
	"github.com/Azure/azure-pipeline-go/pipeline"
	"github.com/Azure/azure-storage-azcopy/common"
	"github.com/Azure/azure-storage-blob-go/azblob"
	"net/url"
	"sync"
	"sync/atomic"
	"time"
)

type blockBlobUploader struct {
	jptm IJobPartTransferMgr
	blobURL azblob.BlobURL
	pipeline pipeline.Pipeline
	pacer *pacer

	needEpilogueIndicator int32  // accessed via sync.atomic
	mu *sync.Mutex               // protects the fields below
	blockIds []string
	leadingBytes []byte
}

func newBlockBlobUploader(jptm IJobPartTransferMgr, destination string, p pipeline.Pipeline, proposedStats ChunkStats, pacer *pacer) (Uploader, error) {
	// check chunk count
	if proposedStats.NumChunks > common.MaxNumberOfBlocksPerBlob {
		return nil, errors.New(
			fmt.Sprintf("BlockSize %d for uploading source of size %d is not correct. Number of blocks will exceed the limit",
				proposedStats.ChunkSize,
				proposedStats.FileSize))
	}

	// make sure URL is parsable
	destURL, err := url.Parse(destination)
	if err != nil {
		return nil, err
	}

	return &blockBlobUploader{
		jptm: jptm,
		blobURL: azblob.NewBlobURL(*destURL, p),
		pipeline: p,
		pacer: pacer,
		mu: &sync.Mutex{},
		blockIds: make([]string, proposedStats.NumChunks),
	}, nil
}


func (bu *blockBlobUploader) RemoteFileExists() (bool, error) {
	_, err := bu.blobURL.GetProperties(bu.jptm.Context(), azblob.BlobAccessConditions{})
	return err != nil, nil  // TODO: is there a better, more robust way to do this check, rather than just taking ANY error as evidence of non-existence?
}

// Returns a chunk-func for blob uploads
func(bu *blockBlobUploader) GenerateUploadFunc(id common.ChunkID, blockIndex int32, reader common.SingleChunkReader, chunkIsWholeFile bool) chunkFunc {

	if blockIndex == 0 {
		bu.mu.Lock()
		bu.leadingBytes = reader.CaptureLeadingBytes()
		bu.mu.Unlock()
	}

	if chunkIsWholeFile {
		if blockIndex > 0 {
			panic("chunk cannot be whole file where there is more than one chunk")
		}
		bu.setEpilogueNeed(epilogueNotNeeded)
		return bu.generatePutWholeBlob(id, blockIndex, reader)
	} else {
		bu.setEpilogueNeed(epilogueNeeded)
		return bu.generatePutBlock(id, blockIndex, reader)
	}
}

// generatePutBlock generates a func to uploads the block of src data from given startIndex till the given chunkSize.
func (bu *blockBlobUploader) generatePutBlock(id common.ChunkID, blockIndex int32, reader common.SingleChunkReader) chunkFunc {
	return func(workerId int) {
		jptm := bu.jptm

		defer jptm.ReportChunkDone() // whether successful or failed, it's always "done" and we must always tell the jptm

		// TODO: added the two operations for debugging purpose. remove later
		jptm.OccupyAConnection()        // Increment a number of goroutine performing the transfer / acting on chunks msg by 1
		defer jptm.ReleaseAConnection() // defer the decrement in the number of goroutine performing the transfer / acting on chunks msg by 1

		if jptm.WasCanceled() {
			jptm.LogChunkStatus(id, common.EWaitReason.Cancelled())
			return
		}

		// step 1: generate block ID
		blockId := common.NewUUID().String()
		encodedBlockId := base64.StdEncoding.EncodeToString([]byte(blockId))

		// step 2: save the block ID into the list of block IDs
		bu.setBlockId(blockIndex, encodedBlockId)

		// step 3: perform put block
		bu.jptm.LogChunkStatus(id, common.EWaitReason.BodyResponse())
		blockBlobUrl := bu.blobURL.ToBlockBlobURL()
		body := newLiteRequestBodyPacer(reader, bu.pacer)
		_, err := blockBlobUrl.StageBlock(bu.jptm.Context(), encodedBlockId, body, azblob.LeaseAccessConditions{}, nil)
		if err != nil {
			jptm.FailActiveUpload(err)
			return
		}

		// step 5: update chunk status logging to done
		jptm.LogChunkStatus(id, common.EWaitReason.ChunkDone())
	}
}

// generates PUT Blob (for a blob that fits in a single put request)
func (bu *blockBlobUploader) generatePutWholeBlob(id common.ChunkID, blockIndex int32, reader common.SingleChunkReader) chunkFunc {
	return func(workerId int) {
		jptm := bu.jptm
		info := jptm.Info()

		defer jptm.ReportChunkDone() // whether successful or failed, it's always "done" and we must always tell the jptm

		// TODO: added the two operations for debugging purpose. remove later
		jptm.OccupyAConnection()        // Increment a number of goroutine performing the transfer / acting on chunks msg by 1
		defer jptm.ReleaseAConnection() // defer the decrement in the number of goroutine performing the transfer / acting on chunks msg by 1

		if jptm.WasCanceled() {
			jptm.LogChunkStatus(id, common.EWaitReason.Cancelled())
			return
		}

		// Get blob http headers and metadata.
		blobHttpHeader, metaData := jptm.BlobDstData(bu.leadingBytes)

		// Upload the blob
		var err error
		blockBlobUrl := bu.blobURL.ToBlockBlobURL()
		if info.SourceSize == 0 {
			_, err = blockBlobUrl.Upload(jptm.Context(), bytes.NewReader(nil), blobHttpHeader, metaData, azblob.BlobAccessConditions{})
		} else {
			body := newLiteRequestBodyPacer(reader, bu.pacer)
			_, err = blockBlobUrl.Upload(jptm.Context(), body, blobHttpHeader, metaData, azblob.BlobAccessConditions{})
		}

		// if the put blob is a failure, updating the transfer status to failed
		if err != nil {
			jptm.FailActiveUpload(err)
		}
	}
}


func(bu *blockBlobUploader) Epilogue() {
	bu.mu.Lock()
	needed := bu.needEpilogueIndicator
	blockIds := bu.blockIds
	bu.mu.Unlock()
	if needed == epilogueNotNeeded {
		return   // nothing to do
	} else if needed == epilogueNeedUnknown {
		panic("epilogue need flag was never set")
	}

	jptm := bu.jptm

	// TODO: finalize and wrap in functions whether 0 is included or excluded in status comparisons

	// commit the blocks
	if jptm.TransferStatus() > 0 {
		jptm.Log(pipeline.LogDebug,	fmt.Sprintf("Conclude Transfer with BlockList %s", bu.blockIds))

		// fetching the blob http headers with content-type, content-encoding attributes
		// fetching the metadata passed with the JobPartOrder
		blobHttpHeader, metaData := jptm.BlobDstData(bu.leadingBytes)

		_, err := bu.blobURL.ToBlockBlobURL().CommitBlockList(jptm.Context(), blockIds, blobHttpHeader, metaData, azblob.BlobAccessConditions{})
		if err != nil {
			jptm.FailActiveUploadWithDetails(err, "Commit block list failed ", common.ETransferStatus.Failed())
		} else {
			jptm.Log(pipeline.LogInfo, "UPLOAD SUCCESSFUL ")
		}
	}

	// set tier
	if jptm.TransferStatus() > 0 {
		blockBlobTier, _ := jptm.BlobTiers()
		if blockBlobTier != common.EBlockBlobTier.None() {
			// for blob tier, set the latest service version from sdk as service version in the context.
			ctxWithValue := context.WithValue(jptm.Context(), ServiceAPIVersionOverride, azblob.ServiceVersion)
			_, err := bu.blobURL.ToBlockBlobURL().SetTier(ctxWithValue, blockBlobTier.ToAccessTierType(), azblob.LeaseAccessConditions{})
			if err != nil {
				jptm.FailActiveUploadWithDetails(err, "BlockBlob SetTier ", common.ETransferStatus.BlobTierFailure())
			}
		}
	}

	// Cleanup
	if jptm.TransferStatus() <= 0 {   // TODO: <=0 or <0?
		// If the transfer status value < 0, then transfer failed with some failure
		// there is a possibility that some uncommitted blocks will be there
		// Delete the uncommitted blobs
		// TODO: should we really do this deletion?  What if we are in an overwrite-existing-blob
		//    situation. Deletion has very different semantics then, compared to not deleting.
		deletionContext, _ := context.WithTimeout(context.Background(), 30 * time.Second)
		_, _ = bu.blobURL.ToBlockBlobURL().Delete(deletionContext, azblob.DeleteSnapshotsOptionNone, azblob.BlobAccessConditions{})
		// TODO: question, is it OK to remoe this logging of failures (since there's no adverse effect of failure)
		//  if stErr, ok := err.(azblob.StorageError); ok && stErr.Response().StatusCode != http.StatusNotFound {
			// If the delete failed with Status Not Found, then it means there were no uncommitted blocks.
			// Other errors report that uncommitted blocks are there
			// bbu.jptm.LogError(bbu.blobURL.String(), "Deleting uncommitted blocks", err)
		//  }

	}

}


func (bu *blockBlobUploader) setEpilogueNeed(value int32){
	// atomic because uploaders are used by multiple threads at the same time
	previous := atomic.SwapInt32(&bu.needEpilogueIndicator, value)
	if previous != epilogueNeedUnknown  && previous != value {
		panic("epilogue need cannot be set twice")
	}
}

func (bu *blockBlobUploader) setBlockId(index int32, value string){
	bu.mu.Lock()
	defer bu.mu.Unlock()
	if len(bu.blockIds[index]) > 0 {
		panic("block id set twice for one block")
	}
	bu.blockIds[index] = value
}




