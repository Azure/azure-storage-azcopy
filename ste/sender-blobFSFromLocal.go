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
	"errors"
	"fmt"
	"github.com/Azure/azure-pipeline-go/pipeline"
	"github.com/Azure/azure-storage-azcopy/v10/common"
	"github.com/Azure/azure-storage-blob-go/azblob"
	"math"
	"strings"
)

type blobFSUploader struct {
	blobFSSenderBase
	md5Channel chan []byte
}

func newBlobFSUploader(jptm IJobPartTransferMgr, destination string, p pipeline.Pipeline, pacer pacer, sip ISourceInfoProvider) (sender, error) {
	senderBase, err := newBlobFSSenderBase(jptm, destination, p, pacer, sip)
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
		body := newPacedRequestBody(jptm.Context(), reader, u.pacer)
		_, err := u.fileURL().AppendData(jptm.Context(), id.OffsetInFile(), body) // note: AppendData is really UpdatePath with "append" action
		if err != nil {
			jptm.FailActiveUpload("Uploading range", err)
			return
		}
	})
}

func (u *blobFSUploader) GetBlobURL() azblob.BlobURL{
	blobPipeline := u.jptm.(*jobPartTransferMgr).jobPartMgr.(*jobPartMgr).secondaryPipeline // pull the secondary (blob) pipeline
	bURLParts := azblob.NewBlobURLParts(u.fileOrDirURL.URL())
	bURLParts.Host = strings.ReplaceAll(bURLParts.Host, ".dfs", ".blob") // switch back to blob

	return azblob.NewBlobURL(bURLParts.URL(), blobPipeline)
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
				_, err := u.fileURL().FlushData(jptm.Context(), i, md5Hash, *u.creationTimeHeaders, i != ss, i == ss)
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
		if jptm.Info().PreservePOSIXProperties {
			sip, err := newLocalSourceInfoProvider(jptm) // never returns an error (as of yet)
			if err != nil {
				jptm.FailActiveUpload("Creating local source info provider for POSIX properties", err)
				return // Defensively handle the error just in case
			}

			if unixSIP, ok := sip.(IUNIXPropertyBearingSourceInfoProvider); ok {
				stat, err := unixSIP.GetUNIXProperties()
				if err != nil {
					jptm.FailActiveUpload("Getting POSIX properties from source", err)
					return
				}

				blobURL := u.GetBlobURL()

				meta := azblob.Metadata{}
				common.AddStatToBlobMetadata(stat, meta)
				delete(meta, common.POSIXFolderMeta) // hdi_isfolder is illegal to set on HNS accounts

				_, err = blobURL.SetMetadata(
					jptm.Context(),
					meta,
					azblob.BlobAccessConditions{},
					azblob.ClientProvidedKeyOptions{}) // cpk isn't used for dfs
				if err != nil {
					jptm.FailActiveSend("Putting POSIX properties in blob metadata", err)
				}
			}
		}
	}
}

func (u *blobFSUploader) SendSymlink(linkData string) error {
	sip, err := newLocalSourceInfoProvider(u.jptm)
	if err != nil {
		return fmt.Errorf("when creating local source info provider: %w", err)
	}

	meta := azblob.Metadata{} // meta isn't traditionally supported for dfs, but still exists

	if u.jptm.Info().PreservePOSIXProperties {
		if unixSIP, ok := sip.(IUNIXPropertyBearingSourceInfoProvider); ok {
			statAdapter, err := unixSIP.GetUNIXProperties()
			if err != nil {
				return err
			}

			if !(statAdapter.FileMode()&common.S_IFLNK == common.S_IFLNK) { // sanity check this is actually targeting the symlink
				return errors.New("sanity check: GetUNIXProperties did not return symlink properties")
			}

			common.AddStatToBlobMetadata(statAdapter, meta)
		}
	}

	meta["is_symlink"] = "true"
	blobHeaders := azblob.BlobHTTPHeaders{ // translate headers, since those still apply
		ContentType: u.creationTimeHeaders.ContentType,
		ContentEncoding: u.creationTimeHeaders.ContentEncoding,
		ContentLanguage: u.creationTimeHeaders.ContentLanguage,
		ContentDisposition: u.creationTimeHeaders.ContentDisposition,
		CacheControl: u.creationTimeHeaders.CacheControl,
	}

	u.GetBlobURL().ToBlockBlobURL().Upload(
		u.jptm.Context(),
		strings.NewReader(linkData),
		blobHeaders,
		meta,
		azblob.BlobAccessConditions{},
		azblob.AccessTierNone, // dfs uses default tier
		nil, // dfs doesn't support tags
		azblob.ClientProvidedKeyOptions{}, // cpk isn't used for dfs
		azblob.ImmutabilityPolicyOptions{}) // dfs doesn't support immutability policy

	//_, err = s.destBlockBlobURL.Upload(s.jptm.Context(), strings.NewReader(linkData), s.headersToApply, s.metadataToApply, azblob.BlobAccessConditions{}, s.destBlobTier, s.blobTagsToApply, s.cpkToApply, azblob.ImmutabilityPolicyOptions{})
	return err
}
