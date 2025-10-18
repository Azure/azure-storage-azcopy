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
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"
	"unsafe"

	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/bloberror"

	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/blob"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/blockblob"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azdatalake/file"

	"github.com/Azure/azure-storage-azcopy/v10/common"
)

var lowMemoryLimitAdvice sync.Once

type blockBlobSenderBase struct {
	jptm                IJobPartTransferMgr
	sip                 ISourceInfoProvider
	destBlockBlobClient *blockblob.Client
	chunkSize           int64
	numChunks           uint32
	pacer               pacer
	blockIDs            []string
	destBlobTier        *blob.AccessTier

	// Headers and other info that we will apply to the destination object.
	// 1. For S2S, these come from the source service.
	// 2. When sending local data, they are computed based on the properties of the local file
	headersToApply  blob.HTTPHeaders
	metadataToApply common.Metadata
	blobTagsToApply common.BlobTags

	atomicChunksWritten    int32
	atomicPutListIndicator int32
	muBlockIDs             *sync.Mutex
	blockNamePrefix        string
	completedBlockList     map[int]string
}

func getVerifiedChunkParams(transferInfo *TransferInfo, memLimit int64, strictMemLimit int64) (chunkSize int64, numChunks uint32, err error) {
	chunkSize = transferInfo.BlockSize
	putBlobSize := transferInfo.PutBlobSize
	srcSize := transferInfo.SourceSize
	numChunks = getNumChunks(srcSize, chunkSize, putBlobSize)

	maxSize := int64(common.MaxBlockBlobBlockSize)
	if srcSize < putBlobSize {
		chunkSize = putBlobSize
		maxSize = common.MaxPutBlobSize
	}

	toGiB := func(bytes int64) float64 {
		return float64(bytes) / float64(1024*1024*1024)
	}

	if common.MinParallelChunkCountThreshold >= memLimit/chunkSize {
		glcm := common.GetLifecycleMgr()
		msg := fmt.Sprintf("Using a blocksize of %.2fGiB for file %s. AzCopy is limited to use %.2fGiB of memory."+
			"Consider providing at least %.2fGiB to AzCopy, using environment variable %s.",
			toGiB(chunkSize), transferInfo.Source, toGiB(memLimit),
			toGiB(common.MinParallelChunkCountThreshold*chunkSize),
			common.EEnvironmentVariable.BufferGB().Name)

		lowMemoryLimitAdvice.Do(func() { glcm.Info(msg) })
	}

	if chunkSize >= memLimit {
		err = fmt.Errorf("Cannot use a block size of %.2fGiB. AzCopy is limited to use only %.2fGiB of memory",
			toGiB(chunkSize), toGiB(memLimit))
		return
	}

	if chunkSize >= strictMemLimit {
		err = fmt.Errorf("Cannot use a block size of %.2fGiB. AzCopy is limited to use only %.2fGiB of memory, and only %.2fGiB of these are available for chunks.",
			toGiB(chunkSize), toGiB(memLimit), toGiB(strictMemLimit))
		return
	}

	// sanity check
	if chunkSize > maxSize { // Use relevant PutBlob or Block size max
		// mercy, please
		err = fmt.Errorf("block size of %.2fGiB for file %s of size %.2fGiB exceeds maximum allowed %.2fMiB block size for a BlockBlob",
			toGiB(chunkSize), transferInfo.Source, toGiB(transferInfo.SourceSize), float64(maxSize/(1024*1024)))
		return
	}

	if putBlobSize >= memLimit {
		err = fmt.Errorf("Cannot use a put blob size of %.2fGiB. AzCopy is limited to use only %.2fGiB of memory",
			toGiB(putBlobSize), toGiB(memLimit))
		return
	}

	if putBlobSize >= strictMemLimit {
		err = fmt.Errorf("Cannot use a put blob size of %.2fGiB. AzCopy is limited to use only %.2fGiB of memory, and only %.2fGiB of these are available for chunks.",
			toGiB(putBlobSize), toGiB(memLimit), toGiB(strictMemLimit))
		return
	}

	// sanity check
	if putBlobSize > common.MaxPutBlobSize {
		// mercy, please
		err = fmt.Errorf("put blob size of %.2fGiB for file %s of size %.2fGiB exceeds maximum allowed put blob size for a BlockBlob",
			toGiB(putBlobSize), transferInfo.Source, toGiB(transferInfo.SourceSize))
		return
	}

	if numChunks > common.MaxNumberOfBlocksPerBlob {
		err = fmt.Errorf("Block size %d for source of size %d is not correct. Number of blocks will exceed the limit", chunkSize, srcSize)
		return
	}

	return
}

// Current size of block names in AzCopy is 48B. To be consistent with this,
// we have to generate a 36B string and then base64-encode this to conform
// to the same size. We generate prefix here.
// Block Names of blobs are of format noted below.
// <5B empty placeholder><16B GUID of AzCopy re-interpreted as string><5B PartNum><5B Index in the jobPart><5B blockNum>
func getBlockNamePrefix(jobID common.JobID, partNum uint32, transferIndex uint32) string {
	jobIdStr := string((*[16]byte)(unsafe.Pointer(&jobID))[:])
	placeHolderPrefix := "00000"
	return fmt.Sprintf("%s%s%05d%05d", placeHolderPrefix, jobIdStr, partNum, transferIndex)
}

func newBlockBlobSenderBase(jptm IJobPartTransferMgr, pacer pacer, srcInfoProvider ISourceInfoProvider, inferredAccessTierType *blob.AccessTier) (*blockBlobSenderBase, error) {
	// compute chunk count
	chunkSize, numChunks, err := getVerifiedChunkParams(jptm.Info(), jptm.CacheLimiter().Limit(), jptm.CacheLimiter().StrictLimit())
	if err != nil {
		return nil, err
	}

	c, err := jptm.DstServiceClient().BlobServiceClient()
	if err != nil {
		return nil, err
	}
	destBlockBlobClient := c.NewContainerClient(jptm.Info().DstContainer).NewBlockBlobClient(jptm.Info().DstFilePath)

	props, err := srcInfoProvider.Properties()
	if err != nil {
		return nil, err
	}

	// If user set blob tier explicitly, override any value that our caller
	// may have guessed.
	destBlobTier := inferredAccessTierType
	blockBlobTierOverride, _ := jptm.BlobTiers()
	if blockBlobTierOverride != common.EBlockBlobTier.None() {
		t := blockBlobTierOverride.ToAccessTierType()
		destBlobTier = &t
	}

	if (props.SrcMetadata["hdi_isfolder"] != nil && *props.SrcMetadata["hdi_isfolder"] == "true") ||
		(props.SrcMetadata["Hdi_isfolder"] != nil && *props.SrcMetadata["Hdi_isfolder"] == "true") {
		destBlobTier = nil
	}

	partNum, transferIndex := jptm.TransferIndex()

	return &blockBlobSenderBase{
		jptm:                jptm,
		sip:                 srcInfoProvider,
		destBlockBlobClient: destBlockBlobClient,
		chunkSize:           chunkSize,
		numChunks:           numChunks,
		pacer:               pacer,
		blockIDs:            make([]string, numChunks),
		headersToApply:      props.SrcHTTPHeaders.ToBlobHTTPHeaders(),
		metadataToApply:     props.SrcMetadata,
		blobTagsToApply:     props.SrcBlobTags,
		destBlobTier:        destBlobTier,
		muBlockIDs:          &sync.Mutex{},
		blockNamePrefix:     getBlockNamePrefix(jptm.Info().JobID, partNum, transferIndex),
	}, nil
}

func (s *blockBlobSenderBase) SendableEntityType() common.EntityType {
	return common.EEntityType.File()
}

func (s *blockBlobSenderBase) ChunkSize() int64 {
	return s.chunkSize
}

func (s *blockBlobSenderBase) NumChunks() uint32 {
	return s.numChunks
}

func (s *blockBlobSenderBase) RemoteFileExists() (bool, time.Time, error) {
	properties, err := s.destBlockBlobClient.GetProperties(s.jptm.Context(), &blob.GetPropertiesOptions{CPKInfo: s.jptm.CpkInfo()})
	return remoteObjectExists(blobPropertiesResponseAdapter{properties}, err)
}

func (s *blockBlobSenderBase) Prologue(ps common.PrologueState) (destinationModified bool) {
	if s.jptm.RestartedTransfer() {
		s.buildCommittedBlockMap()
	}
	if s.jptm.ShouldInferContentType() {
		s.headersToApply.BlobContentType = ps.GetInferredContentType(s.jptm)
	}
	if s.jptm.DeleteDestinationFileIfNecessary() {
		s.DeleteDstBlob()
	}

	return false
}

func (s *blockBlobSenderBase) Epilogue() {
	jptm := s.jptm

	s.muBlockIDs.Lock()
	blockIDs := s.blockIDs
	s.blockIDs = nil // so we know for sure that only this routine has access after we release the lock (nothing else should need it now, since we're in the epilogue. Nil-ing here is just being defensive)
	s.muBlockIDs.Unlock()
	shouldPutBlockList := getPutListNeed(&s.atomicPutListIndicator)
	if shouldPutBlockList == putListNeedUnknown && !jptm.WasCanceled() {
		panic(errors.New("'put list' need flag was never set"))
	}
	// TODO: finalize and wrap in functions whether 0 is included or excluded in status comparisons

	// commit block list if necessary
	if jptm.IsLive() && shouldPutBlockList == putListNeeded {
		// commit the blocks.
		if !ValidateTier(jptm, s.destBlobTier, s.destBlockBlobClient.BlobClient(), s.jptm.Context(), false) {
			s.destBlobTier = nil
		}

		blobTags := s.blobTagsToApply
		setTags := separateSetTagsRequired(blobTags)
		if setTags || len(blobTags) == 0 {
			blobTags = nil
		}

		// TODO: Remove this snippet once service starts supporting CPK with blob tier
		destBlobTier := s.destBlobTier
		if s.jptm.IsSourceEncrypted() {
			destBlobTier = nil
		}

		_, err := s.destBlockBlobClient.CommitBlockList(jptm.Context(), blockIDs,
			&blockblob.CommitBlockListOptions{
				HTTPHeaders:  &s.headersToApply,
				Metadata:     s.metadataToApply,
				Tier:         destBlobTier,
				Tags:         blobTags,
				CPKInfo:      s.jptm.CpkInfo(),
				CPKScopeInfo: s.jptm.CpkScopeInfo(),
			})
		if err != nil {
			jptm.FailActiveSend(common.Iff(blobTags != nil, "Committing block list (with tags)", "Committing block list"), err)

			/*
				If we get an invalid block list, it's likely one of our blocks was deleted, or GC'd mid-job or something.
				Knowing which blocks are missing is useful, as up to 50k blocks can exist in a single object.

				This info could *potentially* be used to just re-upload the missing blocks later, but for now we want to discover the why.
			*/
			if bloberror.HasCode(err, bloberror.InvalidBlockList) {
				blockList, err := s.destBlockBlobClient.GetBlockList(jptm.Context(), blockblob.BlockListTypeAll, nil)
				if err != nil {
					jptm.Log(common.LogWarning, fmt.Sprintf("Failed to get block list to provide delta: %v", err))
				} else {
					blockSet := map[string]bool{}
					extraBlocks := map[string]bool{} // any blocks the service has but we don't
					for _, v := range blockIDs {
						blockSet[v] = true
					}

					recordBlock := func(blockId string) { // Subtract blocks we know of, but keep track of the ones we don't
						if blockSet[blockId] {
							delete(blockSet, blockId)
						} else {
							extraBlocks[blockId] = true
						}
					}

					for _, v := range blockList.UncommittedBlocks { // Uncommitted is primarily what we're looking for, BUT:
						recordBlock(*v.Name)
					}
					for _, v := range blockList.CommittedBlocks { // For the sake of thoroughness, we'll include blocks that already were present.
						recordBlock(*v.Name)
					}

					formatBlocklist := func(dict map[string]bool) string { // Format things pretty
						out := ""

						out += fmt.Sprintf("%d blocks: ", len(dict))
						out += "["
						for k := range dict {
							out += k + ", "
						}
						out = out[:len(out)-2] + "]"

						return out
					}

					// And do our due diligence in the logs.
					jptm.Log(common.LogError, fmt.Sprintf("Total blocks: %d blocks: %v", len(blockIDs), blockIDs))
					jptm.Log(common.LogError, fmt.Sprintf("Missing blocks: %s", formatBlocklist(blockSet)))
					jptm.Log(common.LogError, fmt.Sprintf("Unrecognized blocks: %s", formatBlocklist(extraBlocks)))
				}
			}

			return
		}

		if setTags {
			if _, err := s.destBlockBlobClient.SetTags(jptm.Context(), s.blobTagsToApply, nil); err != nil {
				jptm.FailActiveSend("Setting tags", err)
			}
		}
	}

	// Upload ADLS Gen 2 ACLs
	fromTo := jptm.FromTo()
	if fromTo.From().SupportsHnsACLs() && fromTo.To().SupportsHnsACLs() && jptm.Info().PreservePermissions.IsTruthy() {
		// We know for a fact our source is a "blob".
		acl, err := s.sip.(*blobSourceInfoProvider).AccessControl()
		if err != nil {
			jptm.FailActiveSend("Grabbing source ACLs", err)
			return
		}

		dsc, err := jptm.DstServiceClient().DatalakeServiceClient()
		if err != nil {
			jptm.FailActiveSend("Getting source client", err)
			return
		}
		dstDatalakeClient := dsc.NewFileSystemClient(jptm.Info().DstContainer).NewFileClient(jptm.Info().DstFilePath)

		_, err = dstDatalakeClient.SetAccessControl(jptm.Context(), &file.SetAccessControlOptions{ACL: acl})
		if err != nil {
			jptm.FailActiveSend("Putting ACLs", err)
			return
		}
	}
}

func (s *blockBlobSenderBase) Cleanup() {
	jptm := s.jptm

	// Cleanup
	if jptm.IsDeadInflight() && atomic.LoadInt32(&s.atomicChunksWritten) != 0 {
		// there is a possibility that some uncommitted blocks will be there
		// Delete the uncommitted blobs
		deletionContext, cancelFn := context.WithTimeout(context.WithValue(context.Background(), ServiceAPIVersionOverride, DefaultServiceApiVersion), 30*time.Second)
		defer cancelFn()
		if jptm.WasCanceled() {
			// If we cancelled, and the only blocks that exist are uncommitted, then clean them up.
			// This prevents customer paying for their storage for a week until they get garbage collected, and it
			// also prevents any issues with "too many uncommitted blocks" if user tries to upload the blob again in future.
			// But if there are committed blocks, leave them there (since they still safely represent the state before our job even started)
			blockList, err := s.destBlockBlobClient.GetBlockList(deletionContext, blockblob.BlockListTypeAll, nil)
			hasUncommittedOnly := err == nil && len(blockList.CommittedBlocks) == 0 && len(blockList.UncommittedBlocks) > 0
			if hasUncommittedOnly {
				jptm.LogAtLevelForCurrentTransfer(common.LogDebug, "Deleting uncommitted destination blob due to cancellation")
				// Delete can delete uncommitted blobs.
				_, _ = s.destBlockBlobClient.Delete(deletionContext, nil)
			}
		} else {
			// TODO: review (one last time) should we really do this?  Or should we just give better error messages on "too many uncommitted blocks" errors
			jptm.LogAtLevelForCurrentTransfer(common.LogDebug, "Deleting destination blob due to failure")
			_, _ = s.destBlockBlobClient.Delete(deletionContext, nil)
		}
	}
}

// Currently we've common Metadata Copier across all senders for block blob.
func (s *blockBlobSenderBase) GenerateCopyMetadata(id common.ChunkID) chunkFunc {
	return createChunkFunc(true, s.jptm, id, func() {
		if unixSIP, ok := s.sip.(IUNIXPropertyBearingSourceInfoProvider); ok {
			// Clone the metadata before we write to it, we shouldn't be writing to the same metadata as every other blob.
			s.metadataToApply = s.metadataToApply.Clone()

			statAdapter, err := unixSIP.GetUNIXProperties()
			if err != nil {
				s.jptm.FailActiveSend("GetUNIXProperties", err)
			}

			common.AddStatToBlobMetadata(statAdapter, s.metadataToApply)
		}
		_, err := s.destBlockBlobClient.SetMetadata(s.jptm.Context(), s.metadataToApply,
			&blob.SetMetadataOptions{
				CPKInfo:      s.jptm.CpkInfo(),
				CPKScopeInfo: s.jptm.CpkScopeInfo(),
			})
		if err != nil {
			s.jptm.FailActiveSend("Setting Metadata", err)
			return
		}
	})
}

func (s *blockBlobSenderBase) setBlockID(index int32, value string) {
	s.muBlockIDs.Lock()
	defer s.muBlockIDs.Unlock()
	if len(s.blockIDs[index]) > 0 {
		panic(errors.New("block id set twice for one block"))
	}
	s.blockIDs[index] = value
}

func (s *blockBlobSenderBase) generateEncodedBlockID(index int32) string {
	return common.GenerateBlockBlobBlockID(s.blockNamePrefix, index)
}

func (s *blockBlobSenderBase) buildCommittedBlockMap() {
	invalidAzCopyBlockNameMsg := "buildCommittedBlockMap: Found blocks which are not committed by AzCopy. Restarting whole file"
	changedChunkSize := "buildCommittedBlockMap: Chunksize mismatch on uncommitted blocks"
	list := make(map[int]string)

	if common.GetEnvironmentVariable(common.EEnvironmentVariable.DisableBlobTransferResume()) == "true" {
		return
	}

	blockList, err := s.destBlockBlobClient.GetBlockList(s.jptm.Context(), blockblob.BlockListTypeUncommitted, nil)
	if err != nil {
		s.jptm.LogAtLevelForCurrentTransfer(common.LogError, "Failed to get blocklist. Restarting whole file.")
		return
	}

	if len(blockList.UncommittedBlocks) == 0 {
		s.jptm.LogAtLevelForCurrentTransfer(common.LogDebug, "No uncommitted chunks found.")
		return
	}

	// We return empty list if
	// 1. We find chunks by a different actor
	// 2. Chunk size differs
	for _, block := range blockList.UncommittedBlocks {
		name := common.IffNotNil(block.Name, "")
		size := common.IffNotNil(block.Size, 0)
		if len(name) != common.AZCOPY_BLOCKNAME_LENGTH {
			s.jptm.LogAtLevelForCurrentTransfer(common.LogDebug, invalidAzCopyBlockNameMsg)
			return
		}

		tmp, err := base64.StdEncoding.DecodeString(name)
		decodedBlockName := string(tmp)
		if err != nil || !strings.HasPrefix(decodedBlockName, s.blockNamePrefix) {
			s.jptm.LogAtLevelForCurrentTransfer(common.LogDebug, invalidAzCopyBlockNameMsg)
			return
		}

		index, err := strconv.Atoi(decodedBlockName[len(s.blockNamePrefix):])
		if err != nil || index < 0 || index > int(s.numChunks) {
			s.jptm.LogAtLevelForCurrentTransfer(common.LogDebug, invalidAzCopyBlockNameMsg)
			return
		}

		// Last chunk may have different blockSize
		if size != s.ChunkSize() && index != int(s.numChunks) {
			s.jptm.LogAtLevelForCurrentTransfer(common.LogDebug, changedChunkSize)
			return
		}

		list[index] = decodedBlockName
	}

	// We are here only if all the uncommitted blocks are uploaded by this job with same blockSize
	s.completedBlockList = list
}

func (s *blockBlobSenderBase) ChunkAlreadyTransferred(index int32) bool {
	if s.completedBlockList != nil {
		return false
	}
	_, ok := s.completedBlockList[int(index)]
	return ok
}

// GetDestinationLength gets the destination length.
func (s *blockBlobSenderBase) GetDestinationLength() (int64, error) {
	prop, err := s.destBlockBlobClient.GetProperties(s.jptm.Context(), &blob.GetPropertiesOptions{CPKInfo: s.jptm.CpkInfo()})

	if err != nil {
		return -1, err
	}

	if prop.ContentLength == nil {
		return -1, fmt.Errorf("destination content length not returned")
	}
	return *prop.ContentLength, nil
}

func (s *blockBlobSenderBase) DeleteDstBlob() {
	// Delete destination blob with uncommitted blocks, called in Prologue
	resp, err := s.destBlockBlobClient.GetBlockList(s.jptm.Context(), blockblob.BlockListTypeUncommitted, nil)
	if err != nil {
		s.jptm.LogError(s.destBlockBlobClient.URL(), "GetBlockList with Uncommitted BlockListType failed ", err)
	}
	if len(resp.UncommittedBlocks) > 0 {
		_, err := s.destBlockBlobClient.Delete(s.jptm.Context(), nil)
		if err != nil {
			s.jptm.LogError(s.destBlockBlobClient.URL(), "Deleting destination blob with uncommitted blocks failed ", err)
		}
	}
}
