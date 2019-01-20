package ste

import (
	"context"
	"net/http"
	"net/url"
	"time"

	"github.com/Azure/azure-pipeline-go/pipeline"
	"github.com/Azure/azure-storage-azcopy/common"
	"github.com/Azure/azure-storage-blob-go/azblob"
)

type urlToPageBlobCopier struct {
	jptm            IJobPartTransferMgr
	srcURL          url.URL
	destPageBlobURL azblob.PageBlobURL
	srcSize         int64
	chunkSize       uint32
	numChunks       uint32
	pacer           *pacer
	srcHTTPHeaders  azblob.BlobHTTPHeaders
	srcMetadata     azblob.Metadata
}

func newURLToPageBlobCopier(jptm IJobPartTransferMgr, source string, destination string, p pipeline.Pipeline, pacer *pacer) (s2sCopier, error) {
	// compute chunk count
	info := jptm.Info()
	srcSize := info.SourceSize
	chunkSize := info.BlockSize

	// If the given chunk Size for the Job is invalild for page blob or greater than maximum page size,
	// then set chunkSize as maximum pageSize.
	chunkSize = common.Iffuint32(
		chunkSize > common.DefaultPageBlobChunkSize || (chunkSize%azblob.PageBlobPageBytes != 0),
		common.DefaultPageBlobChunkSize,
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

	destPageBlobURL := azblob.NewPageBlobURL(*destURL, p)

	var azblobMetadata azblob.Metadata
	if info.SrcMetadata != nil {
		azblobMetadata = info.SrcMetadata.ToAzBlobMetadata()
	}

	return &urlToPageBlobCopier{
		jptm:            jptm,
		srcURL:          *srcURL,
		destPageBlobURL: destPageBlobURL,
		srcSize:         info.SourceSize,
		chunkSize:       chunkSize,
		numChunks:       numChunks,
		pacer:           pacer,
		srcHTTPHeaders:  info.SrcHTTPHeaders,
		srcMetadata:     azblobMetadata}, nil
}

func (c *urlToPageBlobCopier) ChunkSize() uint32 {
	return c.chunkSize
}

func (c *urlToPageBlobCopier) NumChunks() uint32 {
	return c.numChunks
}

func (c *urlToPageBlobCopier) RemoteFileExists() (bool, error) {
	if _, err := c.destPageBlobURL.GetProperties(c.jptm.Context(), azblob.BlobAccessConditions{}); err != nil {
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

func (c *urlToPageBlobCopier) Prologue() {
	jptm := c.jptm

	if _, err := c.destPageBlobURL.Create(jptm.Context(), c.srcSize, 0, c.srcHTTPHeaders, c.srcMetadata, azblob.BlobAccessConditions{}); err != nil {
		jptm.FailActiveS2SCopy("Creating blob", err)
		return
	}

	// TODO: set blob tier
}

// Returns a chunk-func for blob copies
func (c *urlToPageBlobCopier) GenerateCopyFunc(id common.ChunkID, blockIndex int32, adjustedChunkSize int64, chunkIsWholeFile bool) chunkFunc {
	return createCopyChunkFunc(c.jptm, id, func() {
		jptm := c.jptm

		if jptm.Info().SourceSize == 0 {
			// nothing to do, since this is a dummy chunk in a zero-size file, and the prologue will have done all the real work
			return
		}

		jptm.LogChunkStatus(id, common.EWaitReason.S2SCopyOnWire())
		// TODO: Using PutPageFromURL to fulfill the copy
	})
}

func (c *urlToPageBlobCopier) Epilogue() {
	jptm := c.jptm

	// Cleanup
	if jptm.TransferStatus() <= 0 { // TODO: <=0 or <0?
		deletionContext, cancelFunc := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancelFunc()
		_, err := c.destPageBlobURL.Delete(deletionContext, azblob.DeleteSnapshotsOptionNone, azblob.BlobAccessConditions{})
		if err != nil {
			jptm.LogError(c.destPageBlobURL.String(), "Delete (incomplete) Page Blob ", err)
		}
	}
}
