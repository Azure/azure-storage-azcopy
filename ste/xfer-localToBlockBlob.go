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
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"strings"

	"github.com/Azure/azure-pipeline-go/pipeline"
	"github.com/Azure/azure-storage-azcopy/common"
	"github.com/Azure/azure-storage-blob-go/2018-03-28/azblob"
)

type blockBlobUpload struct {
	jptm         IJobPartTransferMgr
	leadingBytes []byte // for Mime type recognition
	source       string
	destination  string
	blobURL      azblob.BlobURL
	pacer        *pacer
	blockIds     []string
}

/* TODO Uncomment and re-enable withOUT MMF
type pageBlobUpload struct {
	jptm        IJobPartTransferMgr
	srcMmf      *common.MMF
	source      string
	destination string
	blobUrl     azblob.BlobURL
	pacer       *pacer
}*/

func LocalToBlockBlob(jptm IJobPartTransferMgr, p pipeline.Pipeline, pacer *pacer) {

	EndsWith := func(s string, t string) bool {
		return len(s) >= len(t) && strings.EqualFold(s[len(s)-len(t):], t)
	}

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
	// Declare factory func, because we need it later too
	sourceFileFactory := func()(common.CloseableReaderAt, error) {
		return os.Open(info.Source)
	}
	sourceFile, err := sourceFileFactory()
	if err != nil {
		jptm.LogUploadError(info.Source, info.Destination, "Couldn't open source-"+err.Error(), 0)
		jptm.SetStatus(common.ETransferStatus.Failed())
		jptm.ReportTransferDone()
		return
	}
	defer sourceFile.Close() // we read all the chunks in this routine, so can close the file at the end



	if EndsWith(info.Source, ".vhd") && (blobSize%azblob.PageBlobPageBytes == 0) {
		panic("page blobs disabled in this test")
		/*
			// step 3.b: If the Source is vhd file and its size is multiple of 512,
			// then upload the blob as a pageBlob.
			pageBlobUrl := blobUrl.ToPageBlobURL()

			// If the given chunk Size for the Job is greater than maximum page size i.e 4 MB
			// then set maximum pageSize will be 4 MB.
			chunkSize = common.Iffint64(
				chunkSize > common.DefaultPageBlobChunkSize || (chunkSize%azblob.PageBlobPageBytes != 0),
				common.DefaultPageBlobChunkSize,
				chunkSize)

			// Get http headers and meta data of page.
			blobHttpHeaders, metaData := jptm.BlobDstData(srcMmf)

			// Create Page Blob of the source size
			_, err := pageBlobUrl.Create(jptm.Context(), blobSize,
				0, blobHttpHeaders, metaData, azblob.BlobAccessConditions{})
			if err != nil {
				status, msg := ErrorEx{err}.ErrorCodeAndString()
				jptm.LogUploadError(info.Source, info.Destination, "PageBlob Create-"+msg, status)
				jptm.Cancel()
				jptm.SetStatus(common.ETransferStatus.Failed())
				jptm.ReportTransferDone()
				srcMmf.Unmap()
				return
			}

			//set tier on pageBlob.
			//If set tier fails, then cancelling the job.
			_, pageBlobTier := jptm.BlobTiers()
			if pageBlobTier != common.EPageBlobTier.None() {
				// for blob tier, set the latest service version from sdk as service version in the context.
				ctxWithValue := context.WithValue(jptm.Context(), ServiceAPIVersionOverride, azblob.ServiceVersion)
				_, err := pageBlobUrl.SetTier(ctxWithValue, pageBlobTier.ToAccessTierType())
				if err != nil {
					status, msg := ErrorEx{err}.ErrorCodeAndString()
					jptm.LogUploadError(info.Source, info.Destination, "PageBlob SetTier-"+msg, status)
					jptm.Cancel()
					jptm.SetStatus(common.ETransferStatus.BlobTierFailure())
					// Since transfer failed while setting the page blob tier
					// Deleting the created page blob
					_, err := pageBlobUrl.Delete(context.TODO(), azblob.DeleteSnapshotsOptionNone, azblob.BlobAccessConditions{})
					if err != nil {
						// Log the error if deleting the page blob failed.
						jptm.LogError(pageBlobUrl.String(), "Deleting page blob", err)

					}
					jptm.ReportTransferDone()
					srcMmf.Unmap()
					return
				}
			}

			// Calculate the number of Page Ranges for the given PageSize.
			numPages := common.Iffuint32(blobSize%chunkSize == 0,
				uint32(blobSize/chunkSize),
				uint32(blobSize/chunkSize)+1)

			jptm.SetNumberOfChunks(numPages)

			pbu := &pageBlobUpload{
				jptm:        jptm,
				srcMmf:      srcMmf,
				source:      info.Source,
				destination: info.Destination,
				blobUrl:     blobUrl,
				pacer:       pacer}

			// Scheduling page range update to the Page Blob created above.
			for startIndex := int64(0); startIndex < blobSize; startIndex += chunkSize {
				adjustedPageSize := chunkSize
				// compute actual size of the chunk
				if startIndex+chunkSize > blobSize {
					adjustedPageSize = blobSize - startIndex
				}
				// schedule the chunk job/msg
				jptm.ScheduleChunks(pbu.pageBlobUploadFunc(startIndex, adjustedPageSize))
			}*/
	} else {
		// BEGIN Removed step 3.b
		// TODO: review. Remove for now, using putBlock, because making the choice here, as we used to, relied on older
		// file size measurement, made during scan phase of job.  And that figure might now be out of date

		//if blobSize == 0 || blobSize <= chunkSize {
		/*  step 3.b: if blob size is smaller than chunk size and it is not a vhd file
		// we should do a put blob instead of chunk up the file
		PutBlobUploadFunc(jptm, srcMmf, blobUrl.ToBlockBlobURL(), pacer)
		return */
		// END removed step 3.b

		// TODO: address the issue where file size may have change since the scan happened

		// step 3.c: If the source is not a vhd uploading the source as block Blob.
		// calculating num of chunks using the source size and chunkSize.
		numChunks := common.Iffuint32(
			blobSize%chunkSize == 0,
			uint32(blobSize/chunkSize),
			uint32(blobSize/chunkSize)+1)

		// TODO: remove this if we re-enable step 3.b, above
		// Force a zero-size blob to contain 1 chuck (of zero size), rather than zero chunks
		if numChunks == 0 {
			numChunks = 1
		}

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
			jptm: jptm,
			source:      info.Source,
			destination: info.Destination,
			blobURL:     blobUrl,
			pacer:       pacer,
			blockIds:    blockIds}

		context := jptm.Context()
		sendLimiter := jptm.GetSendLimiter()
		prefetchedByteCounter := jptm.GetPrefetchedByteCounter()

		// Go through the file and schedule chunk messages to upload each chunk
		// As we do this, we force preload of each chunk to memory, and we wait (block)
		// here if the amount of preloaded data gets excessive. That's OK to do,
		// because if we already have that much data preloaded (and scheduled for sending in
		// chunks) then we don't need to schedule any more chunks right now, so the blocking
		// is harmless (and a good thing, to avoid excessive RAM usage)
		for startIndex := int64(0); startIndex < blobSize; startIndex += chunkSize {
			adjustedChunkSize := chunkSize

			// compute actual size of the chunk
			if startIndex+chunkSize > blobSize {
				adjustedChunkSize = blobSize - startIndex
			}

			// block if we already have too much prefetch data in in RAM
			prefetchedByteCounter.WaitUntilBelowLimit()

			// Make reader for this chunk, and prefetch its contents right now.
			// To take advantage of the good sequential read performance provided by many file systems,
			// we work sequentially through the file here.
			// Each chunk reader also gets a factory to make a reader for the file, in case it needs to repeat it's part
			// of the file read later (when doing a retry)
			chunkReader := common.NewSimpleFileChunkReader(context, sourceFileFactory, startIndex, adjustedChunkSize, sendLimiter, prefetchedByteCounter)
			// There is no error returned by PerfectIfPossible
			// We need to schedule the chunks, even if the data is not fetchable, because all our error handing is in the chunkFunc
			fetched := chunkReader.TryPrefetch(sourceFile)  // use the file handle we have already opened, instead of getting each chunk reader to open its own here

			if startIndex == 0 && fetched {
				bbu.leadingBytes = captureLeadingBytes(chunkReader)
			}

			// schedule the chunk job/msg
			jptm.ScheduleChunks(bbu.blockBlobUploadFunc(blockIdCount, chunkReader))

			blockIdCount += 1

		}
		// sanity check to verify the number of chunks scheduled
		if blockIdCount != int32(numChunks) {
			jptm.Panic(fmt.Errorf("difference in the number of chunk calculated %v and actual chunks scheduled %v for src %s of size %v", numChunks, blockIdCount, info.Source, blobSize))
		}
	}
}

// Grab the leading bytes, for later MIME type recognition
// (else we would have to re-read the start of the file later, and that breaks our rule to use sequential
// reads as much as possible)
func captureLeadingBytes(chunkReader common.FileChunkReader) []byte {
	const mimeRecgonitionLen = 512
	leadingBytes := make([]byte, mimeRecgonitionLen)
	_, err := chunkReader.Read(leadingBytes)
	if err != nil {
		return nil // we just can't sniff the mime type
	}
	// MUST re-wind, so that the bytes we read will get transferred too!
	chunkReader.Seek(0, io.SeekStart)
	return leadingBytes
}

// This method blockBlobUploadFunc uploads the block of src data
func (bbu *blockBlobUpload) blockBlobUploadFunc(chunkId int32, chunkReader common.FileChunkReader) chunkFunc {
	return func(workerId int) {

		defer chunkReader.Close()

		// TODO: added the two operations for debugging purpose. remove later
		// Increment a number of goroutine performing the transfer / acting on chunks msg by 1
		bbu.jptm.OccupyAConnection()
		// defer the decrement in the number of goroutine performing the transfer / acting on chunks msg by 1
		defer bbu.jptm.ReleaseAConnection()

		// This function allows routine to manage behavior of unexpected panics.
		// The panic error along with transfer details are logged.
		// The transfer is marked as failed and is reported as done.
		//defer func(jptm IJobPartTransferMgr) {
		//	r := recover()
		//	if r != nil {
		//		info := jptm.Info()
		//		if jptm.ShouldLog(pipeline.LogError) {
		//			jptm.Log(pipeline.LogError, fmt.Sprintf(" recovered from unexpected crash %s. Transfer Src %s Dst %s SrcSize %v startIndex %v chunkSize %v sourceMMF size %v",
		//				r, info.Source, info.Destination, info.SourceSize, startIndex, adjustedChunkSize, len(bbu.srcMmf.Slice())))
		//		}
		//		jptm.SetStatus(common.ETransferStatus.Failed())
		//		jptm.ReportTransferDone()
		//	}
		//}(bbu.jptm)

		// and the chunkFunc has been changed to the version without param workId
		// transfer done is internal function which marks the transfer done
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

		// step 3: perform put block
		blockBlobUrl := bbu.blobURL.ToBlockBlobURL()
		body := newLiteRequestBodyPacer(chunkReader, bbu.pacer)
		_, err := blockBlobUrl.StageBlock(bbu.jptm.Context(), encodedBlockId, body, azblob.LeaseAccessConditions{})
		if err != nil {
			// check if the transfer was cancelled while Stage Block was in process.
			if bbu.jptm.WasCanceled() {
				if bbu.jptm.ShouldLog(pipeline.LogDebug) {
					bbu.jptm.Log(pipeline.LogDebug,
						fmt.Sprintf("Chunk %d upload failed because transfer was cancelled",
							workerId, chunkId))
				}
			} else {
				// cancel entire transfer because this chunk has failed
				bbu.jptm.Cancel()
				status, msg := ErrorEx{err}.ErrorCodeAndString()
				bbu.jptm.LogUploadError(bbu.source, bbu.destination, "Chunk Upload Failed "+msg, status)
				//updateChunkInfo(jobId, partNum, transferId, uint16(chunkId), ChunkTransferStatusFailed, jobsInfoMap)
				bbu.jptm.SetStatus(common.ETransferStatus.Failed())
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
			blobHttpHeader, metaData := bbu.jptm.BlobDstData(bbu.leadingBytes)

			// commit the blocks.
			// TODO: we don't have any way to hook this into sendLimiter at present, because it doesn't read from a Reader...
			// ... so review whether we need to find a way to hook it in, or if its OK to leave it as-is
			_, err := blockBlobUrl.CommitBlockList(bbu.jptm.Context(), bbu.blockIds, blobHttpHeader, metaData, azblob.BlobAccessConditions{})
			if err != nil {
				status, msg := ErrorEx{err}.ErrorCodeAndString()
				bbu.jptm.LogUploadError(bbu.source, bbu.destination, "Commit block list failed "+msg, status)
				bbu.jptm.SetStatus(common.ETransferStatus.Failed())
				transferDone()
				return
			}

			if bbu.jptm.ShouldLog(pipeline.LogInfo) {
				bbu.jptm.Log(pipeline.LogInfo, "UPLOAD SUCCESSFUL ")
			}

			blockBlobTier, _ := bbu.jptm.BlobTiers()
			if blockBlobTier != common.EBlockBlobTier.None() {
				// for blob tier, set the latest service version from sdk as service version in the context.
				ctxWithValue := context.WithValue(bbu.jptm.Context(), ServiceAPIVersionOverride, azblob.ServiceVersion)
				_, err := blockBlobUrl.SetTier(ctxWithValue, blockBlobTier.ToAccessTierType())
				if err != nil {
					status, msg := ErrorEx{err}.ErrorCodeAndString()
					bbu.jptm.LogUploadError(bbu.source, bbu.destination, "BlockBlob SetTier "+msg, status)
					bbu.jptm.SetStatus(common.ETransferStatus.BlobTierFailure())
				}
			}
			bbu.jptm.SetStatus(common.ETransferStatus.Success())
			transferDone()
		}
	}
}

/*
func PutBlobUploadFunc(jptm IJobPartTransferMgr, srcMmf *common.MMF, blockBlobUrl azblob.BlockBlobURL, pacer *pacer) {

	// TODO: added the two operations for debugging purpose. remove later
	// Increment a number of goroutine performing the transfer / acting on chunks msg by 1
	jptm.OccupyAConnection()
	// defer the decrement in the number of goroutine performing the transfer / acting on chunks msg by 1
	defer jptm.ReleaseAConnection()

	// This function allows routine to manage behavior of unexpected panics.
	// The panic error along with transfer details are logged.
	// The transfer is marked as failed and is reported as done.
	//defer func(jptm IJobPartTransferMgr) {
	//	r := recover()
	//	if r != nil {
	//		info := jptm.Info()
	//		lenSrcMmf := 0
	//		if info.SourceSize != 0 {
	//			lenSrcMmf = len(srcMmf.Slice())
	//		}
	//		if jptm.ShouldLog(pipeline.LogError) {
	//			jptm.Log(pipeline.LogError, fmt.Sprintf(" recovered from unexpected crash %s. Transfer Src %s Dst %s SrcSize %v sourceMMF size %v",
	//				r, info.Source, info.Destination, info.SourceSize, lenSrcMmf))
	//		}
	//		jptm.SetStatus(common.ETransferStatus.Failed())
	//		jptm.ReportTransferDone()
	//	}
	//}(jptm)

	// Get blob http headers and metadata.
	blobHttpHeader, metaData := jptm.BlobDstData(srcMmf)

	var err error

	tInfo := jptm.Info()
	// take care of empty blobs
	if tInfo.SourceSize == 0 {
		_, err = blockBlobUrl.Upload(jptm.Context(), bytes.NewReader(nil), blobHttpHeader, metaData, azblob.BlobAccessConditions{})
	} else {
		body := newRequestBodyPacer(bytes.NewReader(srcMmf.Slice()), pacer, srcMmf)
		_, err = blockBlobUrl.Upload(jptm.Context(), body, blobHttpHeader, metaData, azblob.BlobAccessConditions{})
	}

	// if the put blob is a failure, updating the transfer status to failed
	if err != nil {
		status, msg := ErrorEx{err}.ErrorCodeAndString()
		jptm.LogUploadError(tInfo.Source, tInfo.Destination, "PutBlob Failed "+msg, status)
		if !jptm.WasCanceled() {
			jptm.SetStatus(common.ETransferStatus.Failed())
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
			_, err := blockBlobUrl.SetTier(ctxWithValue, blockBlobTier.ToAccessTierType())
			if err != nil {
				status, msg := ErrorEx{err}.ErrorCodeAndString()
				jptm.LogUploadError(tInfo.Source, tInfo.Destination, "BlockBlob SetTier "+msg, status)
				jptm.SetStatus(common.ETransferStatus.BlobTierFailure())
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

	// close the memory map
	if jptm.Info().SourceSize != 0 {
		srcMmf.Unmap()
	}
}

func (pbu *pageBlobUpload) pageBlobUploadFunc(startPage int64, calculatedPageSize int64) chunkFunc {
	return func(workerId int) {
		// TODO: added the two operations for debugging purpose. remove later
		// Increment a number of goroutine performing the transfer / acting on chunks msg by 1
		pbu.jptm.OccupyAConnection()
		// defer the decrement in the number of goroutine performing the transfer / acting on chunks msg by 1
		defer pbu.jptm.ReleaseAConnection()

		// This function allows routine to manage behavior of unexpected panics.
		// The panic error along with transfer details are logged.
		// The transfer is marked as failed and is reported as done.
		//defer func(jptm IJobPartTransferMgr) {
		//	r := recover()
		//	if r != nil {
		//		info := jptm.Info()
		//		if jptm.ShouldLog(pipeline.LogError) {
		//			jptm.Log(pipeline.LogError, fmt.Sprintf(" recovered from unexpected crash %s. Transfer Src %s Dst %s SrcSize %v startPage %v calculatedPageSize %v sourceMMF size %v",
		//				r, info.Source, info.Destination, info.SourceSize, startPage, calculatedPageSize, len(pbu.srcMmf.Slice())))
		//		}
		//		jptm.SetStatus(common.ETransferStatus.Failed())
		//		jptm.ReportTransferDone()
		//	}
		//}(pbu.jptm)

		// pageDone is the function called after success / failure of each page.
		// If the calling page is the last page of transfer, then it updates the transfer status,
		// mark transfer done, unmap the source memory map and close the source file descriptor.
		pageDone := func() {
			if lastPage, _ := pbu.jptm.ReportChunkDone(); lastPage {
				if pbu.jptm.ShouldLog(pipeline.LogDebug) {
					pbu.jptm.Log(pipeline.LogDebug,
						fmt.Sprintf("Finalizing transfer"))
				}
				pbu.jptm.SetStatus(common.ETransferStatus.Success())
				pbu.srcMmf.Unmap()
				// If the value of transfer Status is less than 0
				// transfer failed. Delete the page blob created
				if pbu.jptm.TransferStatus() <= 0 {
					// Deleting the created page blob
					_, err := pbu.blobUrl.ToPageBlobURL().Delete(context.TODO(), azblob.DeleteSnapshotsOptionNone, azblob.BlobAccessConditions{})
					if err != nil {
						// Log the error if deleting the page blob failed.
						pbu.jptm.LogError(pbu.blobUrl.String(), "DeletePageBlob ", err)
					}
				}
				pbu.jptm.ReportTransferDone()
			}
		}

		if pbu.jptm.WasCanceled() {
			if pbu.jptm.ShouldLog(pipeline.LogInfo) {
				pbu.jptm.Log(pipeline.LogInfo, "Transfer Not Started since it is cancelled")
			}
			pageDone()
		} else {
			// pageBytes is the byte slice of Page for the given page range
			pageBytes := pbu.srcMmf.Slice()[startPage : startPage+calculatedPageSize]
			// converted the bytes slice to int64 array.
			// converting each of 8 bytes of byteSlice to an integer.
			int64Slice := (*(*[]int64)(unsafe.Pointer(&pageBytes)))[:len(pageBytes)/8]

			allBytesZero := true
			// Iterating though each integer of in64 array to check if any of the number is greater than 0 or not.
			// If any no is greater than 0, it means that the 8 bytes slice represented by that integer has atleast one byte greater than 0
			// If all integers are 0, it means that the 8 bytes slice represented by each integer has no byte greater than 0
			for index := 0; index < len(int64Slice); index++ {
				if int64Slice[index] != 0 {
					// If one number is greater than 0, then we need to perform the PutPage update.
					allBytesZero = false
					break
				}
			}

			// If all the bytes in the pageBytes is 0, then we do not need to perform the PutPage
			// Updating number of chunks done.
			if allBytesZero {
				if pbu.jptm.ShouldLog(pipeline.LogDebug) {
					pbu.jptm.Log(pipeline.LogDebug,
						fmt.Sprintf("All zero bytes. No Page Upload for range from %d to %d", startPage, startPage+calculatedPageSize))
				}
				pageDone()
				return
			}

			body := newRequestBodyPacer(bytes.NewReader(pageBytes), pbu.pacer, pbu.srcMmf)
			pageBlobUrl := pbu.blobUrl.ToPageBlobURL()
			_, err := pageBlobUrl.UploadPages(pbu.jptm.Context(), startPage, body, azblob.BlobAccessConditions{})
			if err != nil {
				if pbu.jptm.WasCanceled() {
					pbu.jptm.LogError(pageBlobUrl.String(), "PutPageFailed ", err)
				} else {
					status, msg := ErrorEx{err}.ErrorCodeAndString()
					pbu.jptm.LogUploadError(pbu.source, pbu.destination, "UploadPages "+msg, status)
					// cancelling the transfer
					pbu.jptm.Cancel()
					pbu.jptm.SetStatus(common.ETransferStatus.Failed())
				}
				pageDone()
				return
			}
			if pbu.jptm.ShouldLog(pipeline.LogDebug) {
				pbu.jptm.Log(pipeline.LogDebug,
					fmt.Sprintf("PUT page request successful: range %d to %d", startPage, startPage+calculatedPageSize))
			}
			pageDone()
		}
	}
}
*/
