package ste

import (
	"bytes"
	"context"
	"encoding/base64"
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"sync"
	"time"

	"github.com/Azure/azure-pipeline-go/pipeline"
	"github.com/Azure/azure-storage-azcopy/common"
	"github.com/Azure/azure-storage-blob-go/azblob"
)

type urlToBlockBlobCopier struct {
	jptm             IJobPartTransferMgr
	srcURL           url.URL
	destBlockBlobURL azblob.BlockBlobURL
	chunkSize        uint32
	numChunks        uint32
	pacer            *pacer
	blockIDs         []string
	srcHTTPHeaders   azblob.BlobHTTPHeaders
	srcMetadata      azblob.Metadata

	putListIndicator int32       // accessed via sync.atomic
	mu               *sync.Mutex // protects the fields below
}

func newURLToBlockBlobCopier(jptm IJobPartTransferMgr, source string, destination string, p pipeline.Pipeline, pacer *pacer) (s2sCopier, error) {
	// compute chunk count
	info := jptm.Info()
	srcSize := info.SourceSize
	chunkSize := info.BlockSize

	numChunks := getNumCopyChunks(srcSize, chunkSize)
	if numChunks > common.MaxNumberOfBlocksPerBlob {
		return nil, fmt.Errorf("BlockSize %d for copying source of size %d is not correct. Number of blocks will exceed the limit", chunkSize, srcSize)
	}

	srcURL, err := url.Parse(info.Source)
	if err != nil {
		return nil, err
	}
	destURL, err := url.Parse(info.Destination)
	if err != nil {
		return nil, err
	}

	destBlockBlobURL := azblob.NewBlockBlobURL(*destURL, p)

	var azblobMetadata azblob.Metadata
	if info.SrcMetadata != nil {
		azblobMetadata = info.SrcMetadata.ToAzBlobMetadata()
	}

	return &urlToBlockBlobCopier{
		jptm:             jptm,
		srcURL:           *srcURL,
		destBlockBlobURL: destBlockBlobURL,
		chunkSize:        chunkSize,
		numChunks:        numChunks,
		pacer:            pacer,
		blockIDs:         make([]string, numChunks),
		srcHTTPHeaders:   info.SrcHTTPHeaders,
		srcMetadata:      azblobMetadata,
		mu:               &sync.Mutex{}}, nil
}

func (c *urlToBlockBlobCopier) ChunkSize() uint32 {
	return c.chunkSize
}

func (c *urlToBlockBlobCopier) NumChunks() uint32 {
	return c.numChunks
}

func (c *urlToBlockBlobCopier) RemoteFileExists() (bool, error) {
	if _, err := c.destBlockBlobURL.GetProperties(c.jptm.Context(), azblob.BlobAccessConditions{}); err != nil {
		if stgErr, ok := err.(azblob.StorageError); ok && stgErr.Response().StatusCode == http.StatusNotFound {
			// If status code is not found, then the file doesn't exist
			return false, nil
		}
		return false, err
	} else {
		// If err equals nil, the file exists
		return true, nil
	}
}

func (c *urlToBlockBlobCopier) Prologue() {
	// block blobs don't need any work done at this stage
}

// Returns a chunk-func for blob copies
func (c *urlToBlockBlobCopier) GenerateCopyFunc(id common.ChunkID, blockIndex int32, adjustedChunkSize int64, chunkIsWholeFile bool) chunkFunc {
	// TODO: use sync version of copy blob from URL, when the blob size is small enough.
	if blockIndex == 0 && adjustedChunkSize == 0 {
		setPutListNeed(&c.putListIndicator, putListNotNeeded)
		return c.generateCreateEmptyBlob(id)
	}

	setPutListNeed(&c.putListIndicator, putListNeeded)
	return c.generatePutBlockFromURL(id, blockIndex, adjustedChunkSize)
}

func (c *urlToBlockBlobCopier) Epilogue() {
	c.mu.Lock()
	blockIDs := c.blockIDs
	c.mu.Unlock()
	shouldPutBlockList := getPutListNeed(&c.putListIndicator)
	if shouldPutBlockList == putListNeedUnknown {
		c.jptm.Panic(errors.New("'put list' need flag was never set"))
	}

	jptm := c.jptm
	// TODO: keep align with upload, finalize and wrap in functions whether 0 is included or excluded in status comparisons

	// commit block list if necessary
	if jptm.TransferStatus() > 0 && shouldPutBlockList == putListNeeded {
		jptm.Log(pipeline.LogDebug, fmt.Sprintf("Conclude Transfer with BlockList %s", blockIDs))

		// commit the blocks.
		if _, err := c.destBlockBlobURL.CommitBlockList(jptm.Context(), blockIDs, c.srcHTTPHeaders, c.srcMetadata, azblob.BlobAccessConditions{}); err != nil {
			jptm.FailActiveS2SCopy("Committing block list", err)
			// don't return, since need cleanup below
		} else {
			jptm.Log(pipeline.LogInfo, "Blob committed.")
		}
	}

	// TODO: set blob tier

	// Cleanup
	if jptm.TransferStatus() <= 0 {
		// TODO: Whether to delete the blob in destination is under discussion, optimize the cleanup after the decision made.
		deletionContext, cancleFunc := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancleFunc()
		_, err := c.destBlockBlobURL.Delete(deletionContext, azblob.DeleteSnapshotsOptionNone, azblob.BlobAccessConditions{})
		if stErr, ok := err.(azblob.StorageError); ok && stErr.Response().StatusCode != http.StatusNotFound {
			// If the delete failed with Status Not Found, then it means there were no uncommitted blocks.
			// Other errors report that uncommitted blocks are there
			jptm.LogError(c.destBlockBlobURL.String(), "Delete uncommitted blocks ", err)
		}
	}
}

// generateCreateEmptyBlob generates a func to create empty blob in destination.
// This could be replaced by sync version of copy blob from URL.
func (c *urlToBlockBlobCopier) generateCreateEmptyBlob(id common.ChunkID) chunkFunc {
	return createCopyChunkFunc(c.jptm, id, func() {
		jptm := c.jptm

		jptm.LogChunkStatus(id, common.EWaitReason.S2SCopyOnWire())
		// Create blob and finish.
		if _, err := c.destBlockBlobURL.Upload(c.jptm.Context(), bytes.NewReader(nil), c.srcHTTPHeaders, c.srcMetadata, azblob.BlobAccessConditions{}); err != nil {
			jptm.FailActiveS2SCopy("Creating empty blob", err)
			return
		}
	})
}

// generatePutBlockFromURL generates a func to copy the block of src data from given startIndex till the given chunkSize.
func (c *urlToBlockBlobCopier) generatePutBlockFromURL(id common.ChunkID, blockIndex int32, adjustedChunkSize int64) chunkFunc {

	return createCopyChunkFunc(c.jptm, id, func() {
		jptm := c.jptm

		// step 1: generate block ID
		blockID := common.NewUUID().String()
		encodedBlockID := base64.StdEncoding.EncodeToString([]byte(blockID))

		// step 2: save the block ID into the list of block IDs
		c.setBlockId(blockIndex, encodedBlockID)
		jptm.LogChunkStatus(id, common.EWaitReason.S2SCopyOnWire())
		_, err := c.destBlockBlobURL.StageBlockFromURL(c.jptm.Context(), encodedBlockID, c.srcURL, id.OffsetInFile, adjustedChunkSize, azblob.LeaseAccessConditions{})
		if err != nil {
			jptm.FailActiveS2SCopy("Staging block", err)
			return
		}
	})
}

func (c *urlToBlockBlobCopier) setBlockId(index int32, value string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if len(c.blockIDs[index]) > 0 {
		c.jptm.Panic(errors.New("block id set twice for one block"))
	}
	c.blockIDs[index] = value
}
