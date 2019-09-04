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
	"github.com/Azure/azure-storage-blob-go/azblob"

	"github.com/Azure/azure-storage-azcopy/common"
)

type blockBlobSenderBase struct {
	jptm             IJobPartTransferMgr
	destBlockBlobURL azblob.BlockBlobURL
	chunkSize        uint32
	numChunks        uint32
	pacer            pacer
	blockIDs         []string
	destBlobTier     azblob.AccessTierType

	// Headers and other info that we will apply to the destination
	// object. For S2S, these come from the source service.
	// When sending local data, they are computed based on
	// the properties of the local file
	headersToApply  azblob.BlobHTTPHeaders
	metadataToApply azblob.Metadata

	atomicPutListIndicator int32
	muBlockIDs             *sync.Mutex
}

func newBlockBlobSenderBase(jptm IJobPartTransferMgr, destination string, p pipeline.Pipeline, pacer pacer, srcInfoProvider ISourceInfoProvider, inferredAccessTierType azblob.AccessTierType) (*blockBlobSenderBase, error) {
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

	props, err := srcInfoProvider.Properties()
	if err != nil {
		return nil, err
	}

	// If user set blob tier explicitly, override any value that our caller
	// may have guessed.
	destBlobTier := inferredAccessTierType
	blockBlobTierOverride, _ := jptm.BlobTiers()
	if blockBlobTierOverride != common.EBlockBlobTier.None() {
		destBlobTier = blockBlobTierOverride.ToAccessTierType()
	}

	return &blockBlobSenderBase{
		jptm:             jptm,
		destBlockBlobURL: destBlockBlobURL,
		chunkSize:        chunkSize,
		numChunks:        numChunks,
		pacer:            pacer,
		blockIDs:         make([]string, numChunks),
		headersToApply:   props.SrcHTTPHeaders.ToAzBlobHTTPHeaders(),
		metadataToApply:  props.SrcMetadata.ToAzBlobMetadata(),
		destBlobTier:     destBlobTier,
		muBlockIDs:       &sync.Mutex{}}, nil
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

func (s *blockBlobSenderBase) Prologue(ps common.PrologueState) {
	if ps.CanInferContentType() {
		// sometimes, specifically when reading local files, we have more info
		// about the file type at this time than what we had before
		s.headersToApply.ContentType = ps.GetInferredContentType(s.jptm)
	}
}

func (s *blockBlobSenderBase) Epilogue() {
	jptm := s.jptm

	s.muBlockIDs.Lock()
	blockIDs := s.blockIDs
	s.muBlockIDs.Unlock()
	shouldPutBlockList := getPutListNeed(&s.atomicPutListIndicator)
	if shouldPutBlockList == putListNeedUnknown && !jptm.WasCanceled() {
		panic(errors.New("'put list' need flag was never set"))
	}
	// TODO: finalize and wrap in functions whether 0 is included or excluded in status comparisons

	// commit block list if necessary
	if jptm.TransferStatus() > 0 && shouldPutBlockList == putListNeeded {
		jptm.Log(pipeline.LogDebug, fmt.Sprintf("Conclude Transfer with BlockList %s", blockIDs))

		// commit the blocks.
		if _, err := s.destBlockBlobURL.CommitBlockList(jptm.Context(), blockIDs, s.headersToApply, s.metadataToApply, azblob.BlobAccessConditions{}); err != nil {
			jptm.FailActiveSend("Committing block list", err)
			return
		}
	}

	// Set tier
	// GPv2 or Blob Storage is supported, GPv1 is not supported, can only set to blob without snapshot in active status.
	// https://docs.microsoft.com/en-us/azure/storage/blobs/storage-blob-storage-tiers
	if jptm.TransferStatus() > 0 && s.destBlobTier != azblob.AccessTierNone {
		// Set the latest service version from sdk as service version in the context.
		ctxWithLatestServiceVersion := context.WithValue(jptm.Context(), ServiceAPIVersionOverride, azblob.ServiceVersion)
		_, err := s.destBlockBlobURL.SetTier(ctxWithLatestServiceVersion, s.destBlobTier, azblob.LeaseAccessConditions{})
		if err != nil {
			if s.jptm.Info().S2SSrcBlobTier != azblob.AccessTierNone {
				s.jptm.LogTransferInfo(pipeline.LogError, s.jptm.Info().Source, s.jptm.Info().Destination, "Failed to replicate blob tier at destination. Try transferring with the flag --s2s-preserve-access-tier=false")
				s2sAccessTierFailureLogStdout.Do(func() {
					glcm := common.GetLifecycleMgr()
					glcm.Error("One or more blobs have failed blob tier replication at the destination. Try transferring with the flag --s2s-preserve-access-tier=false")
				})
			}

			jptm.FailActiveSendWithStatus("Setting BlockBlob tier", err, common.ETransferStatus.BlobTierFailure())
			return
		}
	}
}

func (s *blockBlobSenderBase) Cleanup() {
	jptm := s.jptm

	// Cleanup
	if jptm.TransferStatus() <= 0 { // TODO: <=0 or <0?
		// If the transfer status value < 0, then transfer failed with some failure
		// there is a possibility that some uncommitted blocks will be there
		// Delete the uncommitted blobs
		// TODO: should we really do this deletion?  What if we are in an overwrite-existing-blob
		//    situation. Deletion has very different semantics then, compared to not deleting.
		deletionContext, cancelFn := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancelFn()
		if jptm.WasCanceled() {
			_, err := s.destBlockBlobURL.GetProperties(deletionContext, azblob.BlobAccessConditions{})

			if stgErr, ok := err.(azblob.StorageError); ok && stgErr.ServiceCode() == azblob.ServiceCodeBlobNotFound {
				_, _ = s.destBlockBlobURL.Delete(deletionContext, azblob.DeleteSnapshotsOptionNone, azblob.BlobAccessConditions{})
			}
		} else {
			_, _ = s.destBlockBlobURL.Delete(deletionContext, azblob.DeleteSnapshotsOptionNone, azblob.BlobAccessConditions{})
		}

		// TODO: question, is it OK to remoe this logging of failures (since there's no adverse effect of failure)
		//  if stErr, ok := err.(azblob.StorageError); ok && stErr.Response().StatusCode != http.StatusNotFound {
		// If the delete failed with Status Not Found, then it means there were no uncommitted blocks.
		// Other errors report that uncommitted blocks are there
		// bbu.jptm.LogError(bbu.blobURL.String(), "Deleting uncommitted blocks", err)
		//  }

	}
}

func (s *blockBlobSenderBase) setBlockID(index int32, value string) {
	s.muBlockIDs.Lock()
	defer s.muBlockIDs.Unlock()
	if len(s.blockIDs[index]) > 0 {
		panic(errors.New("block id set twice for one block"))
	}
	s.blockIDs[index] = value
}

func (s *blockBlobSenderBase) generateEncodedBlockID() string {
	blockID := common.NewUUID().String()
	return base64.StdEncoding.EncodeToString([]byte(blockID))
}
