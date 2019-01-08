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
	"fmt"
	"github.com/Azure/azure-pipeline-go/pipeline"
	"github.com/Azure/azure-storage-azcopy/azbfs"
	"github.com/Azure/azure-storage-azcopy/common"
	"net/url"
	"os"
	"sync"
	"time"
)

type blobFSUploader struct {
	jptm         IJobPartTransferMgr
	fileURL      azbfs.FileURL
	chunkSize    uint32
	numChunks    uint32
	pipeline     pipeline.Pipeline
	pacer        *pacer
	prologueOnce *sync.Once
}

func newBlobFSUploader(jptm IJobPartTransferMgr, destination string, p pipeline.Pipeline, pacer *pacer) (uploader, error) {

	info := jptm.Info()

	// make sure URL is parsable
	destURL, err := url.Parse(destination)
	if err != nil {
		return nil, err
	}

	// Get the file/dir Info to determine whether source is a file or directory
	// since url to upload files and directories is different
	fInfo, err := os.Stat(info.Source)
	if err != nil {
		return nil, err
	}
	if fInfo.IsDir() {
		panic("directory transfers not yet supported")
		// TODO perhaps implement this by returning a different uploader type...
		//      Note that when doing so, remember  our rule that all uploaders process 1 chunk
		//      The returned type will just do one pseudo chunk, in which it creates the directory
	}

	// compute chunk size and number of chunks
	chunkSize := info.BlockSize
	numChunks := getNumUploadChunks(info.SourceSize, chunkSize)

	return &blobFSUploader{
		jptm:         jptm,
		fileURL:      azbfs.NewFileURL(*destURL, p),
		chunkSize:    chunkSize,
		numChunks:    numChunks,
		pipeline:     p,
		pacer:        pacer,
		prologueOnce: &sync.Once{},
	}, nil
}

func (bu *blobFSUploader) ChunkSize() uint32 {
	return bu.chunkSize
}

func (bu *blobFSUploader) NumChunks() uint32 {
	return bu.numChunks
}

func (bu *blobFSUploader) RemoteFileExists() (bool, error) {
	_, err := bu.fileURL.GetProperties(bu.jptm.Context())
	return err != nil, nil // TODO: is there a better, more robust way to do this check, rather than just taking ANY error as evidence of non-existence?
}

// see comments in uploader-azureFiles for why this is done like this
func (bu *blobFSUploader) runPrologueOnce() {
	bu.prologueOnce.Do(func() {
		// Create file with the source size
		jptm := bu.jptm
		_, err := bu.fileURL.Create(jptm.Context()) // note that "create" actually calls "create path"
		if err != nil {
			jptm.FailActiveUploadWithDetails(err, "File Create Error ", common.ETransferStatus.Failed())
			return
		}
	})
}

func (bu *blobFSUploader) GenerateUploadFunc(id common.ChunkID, blockIndex int32, reader common.SingleChunkReader, chunkIsWholeFile bool) chunkFunc {

	return func(workerId int) {

		jptm := bu.jptm

		defer jptm.ReportChunkDone() // whether successful or failed, it's always "done" and we must always tell the jptm

		jptm.OccupyAConnection() // TODO: added the two operations for debugging purpose. remove later
		defer jptm.ReleaseAConnection()

		if jptm.WasCanceled() {
			jptm.LogChunkStatus(id, common.EWaitReason.Cancelled())
			return
		} else {
			defer jptm.LogChunkStatus(id, common.EWaitReason.ChunkDone())
		}
		// TODO: above this point, is

		// Ensure prologue has been run exactly once, before we do anything else
		bu.runPrologueOnce()

		if bu.jptm.Info().SourceSize == 0 {
			// nothing to do, since this is a dummy chunk in a zero-size file, and the prologue will have done all the real work
			return
		}

		// upload the byte range represented by this chunk
		jptm.LogChunkStatus(id, common.EWaitReason.Body())
		body := newLiteRequestBodyPacer(reader, bu.pacer)
		_, err := bu.fileURL.AppendData(jptm.Context(), id.OffsetInFile, body) // note: AppendData is really UpdatePath with "append" action
		if err != nil {
			jptm.FailActiveUploadWithDetails(err, "Upload range error", common.ETransferStatus.Failed())
			return
		}
	}
}

func (bu *blobFSUploader) Epilogue() {
	jptm := bu.jptm

	// flush
	if jptm.TransferStatus() > 0 {
		_, err := bu.fileURL.FlushData(jptm.Context(), jptm.Info().SourceSize)
		if err != nil {
			jptm.FailActiveUpload(err)
		}
	}

	// Cleanup if status is now failed
	if jptm.TransferStatus() <= 0 {
		// If the transfer status is less than or equal to 0
		// then transfer was either failed or cancelled
		// the file created in share needs to be deleted, since it's
		// contents will be at an unknown stage of partial completeness
		deletionContext, _ := context.WithTimeout(context.Background(), 2*time.Minute)
		_, err := bu.fileURL.Delete(deletionContext)
		if err != nil {
			jptm.Log(pipeline.LogError, fmt.Sprintf("error deleting the file %s. Failed with error %s", bu.fileURL.String(), err.Error()))
		}
	}

}
