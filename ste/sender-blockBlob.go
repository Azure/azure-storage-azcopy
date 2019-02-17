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
	"context"
	"encoding/base64"
	"errors"
	"fmt"
	"net/url"
	"sync"
	"time"

	"github.com/Azure/azure-pipeline-go/pipeline"
	"github.com/Azure/azure-storage-azcopy/common"
	"github.com/jiacfan/azure-storage-blob-go/azblob"
)

type blockBlobSenderBase struct {
	jptm             IJobPartTransferMgr
	destBlockBlobURL azblob.BlockBlobURL
	chunkSize        uint32
	numChunks        uint32
	pacer            *pacer
	blockIDs         []string

	putListIndicator int32       // accessed via sync.atomic
	mu               *sync.Mutex // protects the fields below
}

type putBlockFunc = func(encodedBlockID string)

func newBlockBlobSenderBase(jptm IJobPartTransferMgr, destination string, p pipeline.Pipeline, pacer *pacer) (*blockBlobSenderBase, error) {
	transferInfo := jptm.Info()

	// compute chunk count
	chunkSize := transferInfo.BlockSize
	srcSize := transferInfo.SourceSize
	numChunks := getNumChunks(srcSize, chunkSize)
	if numChunks > common.MaxNumberOfBlocksPerBlob {
		return nil, fmt.Errorf("BlockSize %d for source of size %d is not correct. Number of blocks will exceed the limit", chunkSize, srcSize)
	}

	destURL, err := url.Parse(destination)
	if err != nil {
		return nil, err
	}

	destBlockBlobURL := azblob.NewBlockBlobURL(*destURL, p)

	return &blockBlobSenderBase{
		jptm:             jptm,
		destBlockBlobURL: destBlockBlobURL,
		chunkSize:        chunkSize,
		numChunks:        numChunks,
		pacer:            pacer,
		mu:               &sync.Mutex{}}, nil
}

func (s *blockBlobSenderBase) ChunkSize() uint32 {
	return s.chunkSize
}

func (s *blockBlobSenderBase) NumChunks() uint32 {
	return s.numChunks
}

func (s *blockBlobSenderBase) RemoteFileExists() (bool, error) {
	return remoteObjectExists(s.destBlockBlobURL.GetProperties(s.jptm.Context(), azblob.BlobAccessConditions{}))
}

// generatePutBlockFromURL generates a func to copy the block of src data from given startIndex till the given chunkSize.
func (s *blockBlobSenderBase) generatePutBlockToRemoteFunc(id common.ChunkID, blockIndex int32, putBlock putBlockFunc) chunkFunc {

	return createSendToRemoteChunkFunc(s.jptm, id, func() {
		// step 1: generate block ID
		blockID := common.NewUUID().String()
		encodedBlockID := base64.StdEncoding.EncodeToString([]byte(blockID))

		// step 2: save the block ID into the list of block IDs
		s.setBlockID(blockIndex, encodedBlockID)

		putBlock(encodedBlockID)
	})
}

func (s *blockBlobSenderBase) epilogue(httpHeader azblob.BlobHTTPHeaders, metadata azblob.Metadata, accessTier azblob.AccessTierType, logger ISenderLogger) {
	s.mu.Lock()
	blockIDs := s.blockIDs
	s.mu.Unlock()
	shouldPutBlockList := getPutListNeed(&s.putListIndicator)
	if shouldPutBlockList == putListNeedUnknown {
		s.jptm.Panic(errors.New("'put list' need flag was never set"))
	}

	jptm := s.jptm
	// TODO: finalize and wrap in functions whether 0 is included or excluded in status comparisons

	// commit block list if necessary
	if jptm.TransferStatus() > 0 && shouldPutBlockList == putListNeeded {
		jptm.Log(pipeline.LogDebug, fmt.Sprintf("Conclude Transfer with BlockList %s", blockIDs))

		// commit the blocks.
		if _, err := s.destBlockBlobURL.CommitBlockList(jptm.Context(), blockIDs, httpHeader, metadata, azblob.BlobAccessConditions{}); err != nil {
			logger.FailActiveSend("Committing block list", err)
			// don't return, since need cleanup below
		}
	}

	// Set tier
	// GPv2 or Blob Storage is supported, GPv1 is not supported, can only set to blob without snapshot in active status.
	// https://docs.microsoft.com/en-us/azure/storage/blobs/storage-blob-storage-tiers
	if jptm.TransferStatus() > 0 && accessTier != azblob.AccessTierNone {
		// Set the latest service version from sdk as service version in the context.
		ctxWithLatestServiceVersion := context.WithValue(jptm.Context(), ServiceAPIVersionOverride, azblob.ServiceVersion)
		_, err := s.destBlockBlobURL.SetTier(ctxWithLatestServiceVersion, accessTier, azblob.LeaseAccessConditions{})
		if err != nil {
			logger.FailActiveSendWithStatus("Setting BlockBlob tier", err, common.ETransferStatus.BlobTierFailure())
			// don't return, because need cleanup below
		}
	}

	// Cleanup
	if jptm.TransferStatus() <= 0 { // TODO: <=0 or <0?
		// If the transfer status value < 0, then transfer failed with some failure
		// there is a possibility that some uncommitted blocks will be there
		// Delete the uncommitted blobs
		// TODO: should we really do this deletion?  What if we are in an overwrite-existing-blob
		//    situation. Deletion has very different semantics then, compared to not deleting.
		deletionContext, cancelFn := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancelFn()
		_, _ = s.destBlockBlobURL.Delete(deletionContext, azblob.DeleteSnapshotsOptionNone, azblob.BlobAccessConditions{})
		// TODO: question, is it OK to remoe this logging of failures (since there's no adverse effect of failure)
		//  if stErr, ok := err.(azblob.StorageError); ok && stErr.Response().StatusCode != http.StatusNotFound {
		// If the delete failed with Status Not Found, then it means there were no uncommitted blocks.
		// Other errors report that uncommitted blocks are there
		// bbu.jptm.LogError(bbu.blobURL.String(), "Deleting uncommitted blocks", err)
		//  }

	}
}

func (s *blockBlobSenderBase) setBlockID(index int32, value string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if len(s.blockIDs[index]) > 0 {
		s.jptm.Panic(errors.New("block id set twice for one block"))
	}
	s.blockIDs[index] = value
}
