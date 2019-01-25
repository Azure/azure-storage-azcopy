package ste

import (
	"context"
	"net/url"
	"time"

	"github.com/Azure/azure-pipeline-go/pipeline"
	"github.com/Azure/azure-storage-azcopy/common"
	"github.com/jiacfan/azure-storage-blob-go/azblob"
	"golang.org/x/sync/semaphore"
)

type urlToAppendBlobCopier struct {
	jptm                   IJobPartTransferMgr
	srcURL                 url.URL
	destAppendBlobURL      azblob.AppendBlobURL
	chunkSize              uint32
	numChunks              uint32
	pacer                  *pacer
	srcHTTPHeaders         azblob.BlobHTTPHeaders
	srcMetadata            azblob.Metadata
	soleChunkFuncSemaphore *semaphore.Weighted
}

func newURLToAppendBlobCopier(jptm IJobPartTransferMgr, source string, destination string, p pipeline.Pipeline, pacer *pacer) (s2sCopier, error) {
	// compute chunk count
	info := jptm.Info()
	srcSize := info.SourceSize
	chunkSize := info.BlockSize

	// If the given chunk Size for the Job is greater than maximum append blob block size i.e 4 MB,
	// then set chunkSize as 4 MB.
	chunkSize = common.Iffuint32(
		chunkSize > common.MaxAppendBlobBlockSize,
		common.MaxAppendBlobBlockSize,
		chunkSize)

	numChunks := getNumCopyChunks(srcSize, chunkSize)

	srcURL, err := url.Parse(info.Source)
	if err != nil {
		return nil, err
	}
	destURL, err := url.Parse(info.Destination)
	if err != nil {
		return nil, err
	}

	destAppendBlobURL := azblob.NewAppendBlobURL(*destURL, p)

	var azblobMetadata azblob.Metadata
	if info.SrcMetadata != nil {
		azblobMetadata = info.SrcMetadata.ToAzBlobMetadata()
	}

	return &urlToAppendBlobCopier{
		jptm:                   jptm,
		srcURL:                 *srcURL,
		destAppendBlobURL:      destAppendBlobURL,
		chunkSize:              chunkSize,
		numChunks:              numChunks,
		pacer:                  pacer,
		srcHTTPHeaders:         info.SrcHTTPHeaders,
		srcMetadata:            azblobMetadata,
		soleChunkFuncSemaphore: semaphore.NewWeighted(1)}, nil
}

func (c *urlToAppendBlobCopier) ChunkSize() uint32 {
	return c.chunkSize
}

func (c *urlToAppendBlobCopier) NumChunks() uint32 {
	return c.numChunks
}

func (c *urlToAppendBlobCopier) RemoteFileExists() (bool, error) {
	return remoteObjectExists(c.destAppendBlobURL.GetProperties(c.jptm.Context(), azblob.BlobAccessConditions{}))
}

func (c *urlToAppendBlobCopier) Prologue() {
	jptm := c.jptm

	if _, err := c.destAppendBlobURL.Create(jptm.Context(), c.srcHTTPHeaders, c.srcMetadata, azblob.BlobAccessConditions{}); err != nil {
		jptm.FailActiveS2SCopy("Creating blob", err)
		return
	}
}

// Returns a chunk-func for blob copies
func (c *urlToAppendBlobCopier) GenerateCopyFunc(id common.ChunkID, blockIndex int32, adjustedChunkSize int64, chunkIsWholeFile bool) chunkFunc {
	// Copy must be totally sequential for append blobs
	// The way we enforce that is simple: we won't even CREATE
	// a chunk func, until all previously-scheduled chunk funcs have completed
	// Here we block until there are no other chunkfuncs in existence for this blob
	err := c.soleChunkFuncSemaphore.Acquire(c.jptm.Context(), 1)
	if err != nil {
		// Must have been cancelled
		// We must still return a chunk func, so return a no-op one
		return createCopyChunkFunc(c.jptm, id, func() {})
	}

	return createCopyChunkFunc(c.jptm, id, func() {

		// Here, INSIDE the chunkfunc, we release the semaphore when we have finished running
		defer c.soleChunkFuncSemaphore.Release(1)

		jptm := c.jptm

		if jptm.Info().SourceSize == 0 {
			// nothing to do, since this is a dummy chunk in a zero-size file, and the prologue will have done all the real work
			return
		}

		jptm.LogChunkStatus(id, common.EWaitReason.S2SCopyOnWire())
		_, err = c.destAppendBlobURL.AppendBlockFromURL(jptm.Context(), c.srcURL, id.OffsetInFile, adjustedChunkSize, azblob.AppendBlobAccessConditions{}, nil)
		if err != nil {
			jptm.FailActiveS2SCopy("Appending block from URL", err)
			return
		}
	})
}

func (c *urlToAppendBlobCopier) Epilogue() {
	jptm := c.jptm
	// Cleanup
	if jptm.TransferStatus() <= 0 { // TODO: <=0 or <0?
		// If the transfer status value < 0, then transfer failed with some failure
		// there is a possibility that some uncommitted blocks will be there
		// Delete the uncommitted blobs
		// TODO: should we really do this deletion?  What if we are in an overwrite-existing-blob
		//    situation. Deletion has very different semantics then, compared to not deleting.
		deletionContext, cancelFunc := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancelFunc()
		_, err := c.destAppendBlobURL.Delete(deletionContext, azblob.DeleteSnapshotsOptionNone, azblob.BlobAccessConditions{})
		if err != nil {
			jptm.LogError(c.destAppendBlobURL.String(), "Delete (incomplete) Append Blob ", err)
		}
	}
}
