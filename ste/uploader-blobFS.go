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
	"time"
)

type blobFSUploader struct {
	jptm      IJobPartTransferMgr
	fileURL   azbfs.FileURL
	chunkSize uint32
	numChunks uint32
	pipeline  pipeline.Pipeline
	pacer     *pacer
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
		/* for the record, here is what the chunkFunc used to do, in the directory case - even though that code was never actually called in the current release,
		   because, as at 1 Jan 2019, we don't actually pass in directories here.  But if we do, this code below could be repacked into an uploader

			if fInfo.IsDir() {
				dirUrl := azbfs.NewDirectoryURL(*dUrl, p)
				_, err := dirUrl.Create(jptm.Context())
				if err != nil {
					// Note: As description in document https://docs.microsoft.com/en-us/rest/api/storageservices/datalakestoragegen2/path/create,
					// the default behavior of creating directory is overwrite, unless there is lease, or destination exists, and there is If-None-Match:"*".
					// Check for overwrite flag correspondingly, if overwrite is true, and fail to recreate directory, report error.
					// If overwrite is false, and fail to recreate directoroy, report directory already exists.
					if !jptm.IsForceWriteTrue() {
						if stgErr, ok := err.(azbfs.StorageError); ok && stgErr.Response().StatusCode == http.StatusConflict {
							jptm.LogUploadError(info.Source, info.Destination, "Directory already exists ", 0)
							// Mark the transfer as failed with ADLSGen2PathAlreadyExistsFailure
							jptm.SetStatus(common.ETransferStatus.ADLSGen2PathAlreadyExistsFailure())
							jptm.ReportTransferDone()
							return
						}
					}

					status, msg := ErrorEx{err}.ErrorCodeAndString()
					jptm.LogUploadError(info.Source, info.Destination, "Directory creation error "+msg, status)
					if jptm.WasCanceled() {
						transferDone(jptm.TransferStatus())
					} else {
						transferDone(common.ETransferStatus.Failed())
					}
					return
				}
				if jptm.ShouldLog(pipeline.LogInfo) {
					jptm.Log(pipeline.LogInfo, "UPLOAD SUCCESSFUL")
				}
				transferDone(common.ETransferStatus.Success())
				return
			}
		*/
	}

	// compute chunk size and number of chunks
	chunkSize := info.BlockSize
	numChunks := getNumUploadChunks(info.SourceSize, chunkSize)

	return &blobFSUploader{
		jptm:      jptm,
		fileURL:   azbfs.NewFileURL(*destURL, p),
		chunkSize: chunkSize,
		numChunks: numChunks,
		pipeline:  p,
		pacer:     pacer,
	}, nil
}

func (u *blobFSUploader) ChunkSize() uint32 {
	return u.chunkSize
}

func (u *blobFSUploader) NumChunks() uint32 {
	return u.numChunks
}

func (u *blobFSUploader) RemoteFileExists() (bool, error) {
	_, err := u.fileURL.GetProperties(u.jptm.Context())
	return err == nil, nil // TODO: is there a better, more robust way to do this check, rather than just taking ANY error as evidence of non-existence?
}

func (u *blobFSUploader) Prologue(leadingBytes []byte) {
	// Create file with the source size
	_, err := u.fileURL.Create(u.jptm.Context()) // note that "create" actually calls "create path"
	if err != nil {
		u.jptm.FailActiveUploadWithDetails(err, "File Create Error ", common.ETransferStatus.Failed())
		return
	}
}

func (u *blobFSUploader) GenerateUploadFunc(id common.ChunkID, blockIndex int32, reader common.SingleChunkReader, chunkIsWholeFile bool) chunkFunc {

	return createUploadChunkFunc(u.jptm, id, func() {
		jptm := u.jptm

		if jptm.Info().SourceSize == 0 {
			// nothing to do, since this is a dummy chunk in a zero-size file, and the prologue will have done all the real work
			return
		}

		// upload the byte range represented by this chunk
		jptm.LogChunkStatus(id, common.EWaitReason.Body())
		body := newLiteRequestBodyPacer(reader, u.pacer)
		_, err := u.fileURL.AppendData(jptm.Context(), id.OffsetInFile, body) // note: AppendData is really UpdatePath with "append" action
		if err != nil {
			jptm.FailActiveUploadWithDetails(err, "Upload range error", common.ETransferStatus.Failed())
			return
		}
	})
}

func (u *blobFSUploader) Epilogue() {
	jptm := u.jptm

	// flush
	if jptm.TransferStatus() > 0 {
		_, err := u.fileURL.FlushData(jptm.Context(), jptm.Info().SourceSize)
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
		_, err := u.fileURL.Delete(deletionContext)
		if err != nil {
			jptm.Log(pipeline.LogError, fmt.Sprintf("error deleting the (incomplete) file %s. Failed with error %s", u.fileURL.String(), err.Error()))
		}
	}
}
