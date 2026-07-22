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
	"errors"
	"os"
	"time"

	"github.com/Azure/azure-sdk-for-go/sdk/storage/azdatalake/file"
	"github.com/Azure/azure-storage-azcopy/v10/common"
)

type blobFSDownloader struct {
	jptm          IJobPartTransferMgr
	txInfo        *TransferInfo
	srcFileClient *file.Client
}

func newBlobFSDownloader(jptm IJobPartTransferMgr) (downloader, error) {
	s, err := jptm.SrcServiceClient().DatalakeServiceClient()
	if err != nil {
		return nil, err
	}

	srcFileClient := s.NewFileSystemClient(jptm.Info().SrcContainer).NewFileClient(jptm.Info().SrcFilePath)

	return &blobFSDownloader{srcFileClient: srcFileClient}, nil
}

func (bd *blobFSDownloader) Prologue(jptm IJobPartTransferMgr) {
	bd.jptm = jptm
	bd.txInfo = jptm.Info() // Inform the downloader
}

func (bd *blobFSDownloader) Epilogue() {
	if bd.jptm != nil {
		if bd.jptm.IsLive() && bd.jptm.Info().PreservePOSIXProperties {
			bsip, err := newBlobSourceInfoProvider(bd.jptm)
			if err != nil {
				bd.jptm.FailActiveDownload("get blob source info provider", err)
			}
			unixstat, _ := bsip.(IUNIXPropertyBearingSourceInfoProvider)
			if ubd, ok := (interface{})(bd).(unixPropertyAwareDownloader); ok && unixstat.HasUNIXProperties() {
				adapter, err := unixstat.GetUNIXProperties()
				if err != nil {
					bd.jptm.FailActiveDownload("get unix properties", err)
				}

				stage, err := ubd.ApplyUnixProperties(adapter)
				if err != nil {
					bd.jptm.FailActiveDownload("set unix properties: "+stage, err)
				}
			}
		}
	}
}

// Returns a chunk-func for ADLS gen2 downloads

func (bd *blobFSDownloader) GenerateDownloadFunc(jptm IJobPartTransferMgr, destWriter common.ChunkedFileWriter, id common.ChunkID, length int64, pacer pacer) chunkFunc {
	return createDownloadChunkFunc(jptm, id, func() {

		srcFileClient := bd.srcFileClient

		// At this point we create an HTTP(S) request for the desired portion of the file, and
		// wait until we get the headers back... but we have not yet read its whole body.
		// The Download method encapsulates any retries that may be necessary to get to the point of receiving response headers.
		jptm.LogChunkStatus(id, common.EWaitReason.HeaderResponse())
		get, err := srcFileClient.DownloadStream(jptm.Context(), &file.DownloadStreamOptions{Range: &file.HTTPRange{Offset: id.OffsetInFile(), Count: length}})
		if err != nil {
			jptm.FailActiveDownload("Downloading response body", err) // cancel entire transfer because this chunk has failed
			return
		}

		// parse the remote lmt, there shouldn't be any error, unless the service returned a new format
		getLMT := get.LastModified.In(time.FixedZone("GMT", 0))
		if !getLMT.Equal(jptm.LastModifiedTime().In(time.FixedZone("GMT", 0))) {
			jptm.FailActiveDownload("BFS File modified during transfer",
				errors.New("BFS File modified during transfer"))
		}

		// step 2: Enqueue the response body to be written out to disk
		// The retryReader encapsulates any retries that may be necessary while downloading the body
		jptm.LogChunkStatus(id, common.EWaitReason.Body())
		retryReader := get.NewRetryReader(jptm.Context(), &file.RetryReaderOptions{
			MaxRetries:   MaxRetryPerDownloadBody,
			OnFailedRead: common.NewDatalakeReadLogFunc(jptm, srcFileClient.DFSURL()),
		})
		defer retryReader.Close()
		err = destWriter.EnqueueChunk(jptm.Context(), id, length, newPacedResponseBody(jptm.Context(), retryReader, pacer), true)
		if err != nil {
			jptm.FailActiveDownload("Enqueuing chunk", err)
			return
		}
	})
}

func (bd *blobFSDownloader) CreateSymlink(jptm IJobPartTransferMgr) error {
	sip, err := newBlobSourceInfoProvider(jptm)
	if err != nil {
		return err
	}
	symsip := sip.(ISymlinkBearingSourceInfoProvider) // blob always implements this
	symlinkInfo, _ := symsip.ReadLink()

	// create the link
	err = os.Symlink(symlinkInfo, jptm.Info().Destination)

	return err
}
