// Copyright © Microsoft <wastore@microsoft.com>
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
	"github.com/Azure/azure-sdk-for-go/sdk/azcore/to"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azdatalake/file"
	"github.com/Azure/azure-storage-azcopy/v10/common"
	"github.com/Azure/azure-storage-azcopy/v10/pacer"

	"math"
)

type blobFSUploader struct {
	blobFSSenderBase
	md5Channel chan []byte
}

func newBlobFSUploader(jptm IJobPartTransferMgr, destination string, pacer pacer.Interface, sip ISourceInfoProvider) (sender, error) {
	senderBase, err := newBlobFSSenderBase(jptm, destination, pacer, sip)
	if err != nil {
		return nil, err
	}

	return &blobFSUploader{blobFSSenderBase: *senderBase, md5Channel: newMd5Channel()}, nil

}

func (u *blobFSUploader) Md5Channel() chan<- []byte {
	return u.md5Channel
}

func (u *blobFSUploader) GenerateUploadFunc(id common.ChunkID, blockIndex int32, reader common.SingleChunkReader, chunkIsWholeFile bool) chunkFunc {

	return createSendToRemoteChunkFunc(u.jptm, id, func() {
		jptm := u.jptm

		if jptm.Info().SourceSize == 0 {
			// nothing to do, since this is a dummy chunk in a zero-size file, and the prologue will have done all the real work
			return
		}

		// upload the byte range represented by this chunk
		jptm.LogChunkStatus(id, common.EWaitReason.Body())

		// inject our pacer so our policy picks it up
		pacerCtx, err := u.pacer.InjectPacer(reader.Length(), u.jptm.FromTo(), u.jptm.Context())
		if err != nil {
			u.jptm.FailActiveDownload("Injecting pacer into context", err)
			return
		}

		_, err = u.getFileClient().AppendData(pacerCtx, id.OffsetInFile(), reader, nil) // note: AppendData is really UpdatePath with "append" action
		if err != nil {
			jptm.FailActiveUpload("Uploading range", err)
			return
		}
	})
}

func (u *blobFSUploader) Epilogue() {
	jptm := u.jptm

	// flush
	if jptm.IsLive() {
		ss := jptm.Info().SourceSize
		md5Hash, ok := <-u.md5Channel
		if ok {
			// Flush incrementally to avoid timeouts on a full flush
			for i := int64(math.Min(float64(ss), float64(u.flushThreshold))); ; i = int64(math.Min(float64(ss), float64(i+u.flushThreshold))) {
				// Close only at the end of the file, keep all uncommitted data before then.
				httpHeaders := u.creationTimeHeaders
				httpHeaders.ContentMD5 = md5Hash
				_, err := u.getFileClient().FlushData(jptm.Context(), i, &file.FlushDataOptions{HTTPHeaders: u.creationTimeHeaders, RetainUncommittedData: to.Ptr(i != ss), Close: to.Ptr(i == ss)})
				if err != nil {
					jptm.FailActiveUpload("Flushing data", err)
					break // don't return, since need cleanup below
				}

				if i == ss {
					break
				}
			}
		} else {
			jptm.FailActiveUpload("Getting hash", errNoHash) // don't return, since need cleanup below
		}
	}

	// Write POSIX data
	if jptm.IsLive() {
		if jptm.Info().PreservePOSIXProperties { // metadata would be set here
			err := u.SetPOSIXProperties()
			if err != nil {
				jptm.FailActiveUpload("Setting POSIX Properties", err)
			}
		} else if len(u.metadataToSet) > 0 { // but if we aren't writing POSIX properties, let's set metadata to be consistent.
			_, err := u.blobClient.SetMetadata(u.jptm.Context(), u.metadataToSet, nil)
			if err != nil {
				jptm.FailActiveUpload("Setting blob metadata", err)
			}
		}
	}
}
