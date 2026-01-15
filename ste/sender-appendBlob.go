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
	"bytes"
	"context"
	"crypto/md5"
	"fmt"
	"io"
	"time"

	"github.com/Azure/azure-sdk-for-go/sdk/azcore/to"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/appendblob"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/blob"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/bloberror"

	"golang.org/x/sync/semaphore"

	"github.com/Azure/azure-storage-azcopy/v10/common"
)

type appendBlobSenderBase struct {
	jptm                 IJobPartTransferMgr
	destAppendBlobClient *appendblob.Client
	chunkSize            int64
	numChunks            uint32
	pacer                pacer
	// Headers and other info that we will apply to the destination
	// object. For S2S, these come from the source service.
	// When sending local data, they are computed based on
	// the properties of the local file
	headersToApply  blob.HTTPHeaders
	metadataToApply common.SafeMetadata
	blobTagsToApply common.BlobTags

	sip ISourceInfoProvider

	soleChunkFuncSemaphore *semaphore.Weighted
}

type appendBlockFunc = func()

func newAppendBlobSenderBase(jptm IJobPartTransferMgr, destination string, pacer pacer, srcInfoProvider ISourceInfoProvider) (*appendBlobSenderBase, error) {
	transferInfo := jptm.Info()

	// compute chunk count
	chunkSize := transferInfo.BlockSize
	// If the given chunk Size for the Job is greater than maximum append blob block size i.e common.MaxAppendBlobBlockSize,
	// then set chunkSize as common.MaxAppendBlobBlockSize.
	chunkSize = common.Iff(
		chunkSize > common.MaxAppendBlobBlockSize,
		common.MaxAppendBlobBlockSize,
		chunkSize)

	srcSize := transferInfo.SourceSize
	numChunks := getNumChunks(srcSize, chunkSize, chunkSize)

	bsc, err := jptm.DstServiceClient().BlobServiceClient()
	if err != nil {
		return nil, err
	}
	destAppendBlobClient := bsc.NewContainerClient(transferInfo.DstContainer).NewAppendBlobClient(transferInfo.DstFilePath)

	props, err := srcInfoProvider.Properties()
	if err != nil {
		return nil, err
	}

	return &appendBlobSenderBase{
		jptm:                 jptm,
		destAppendBlobClient: destAppendBlobClient,
		chunkSize:            chunkSize,
		numChunks:            numChunks,
		pacer:                pacer,
		headersToApply:       props.SrcHTTPHeaders.ToBlobHTTPHeaders(),
		metadataToApply: common.SafeMetadata{
			Metadata: props.SrcMetadata,
		},
		blobTagsToApply:        props.SrcBlobTags,
		sip:                    srcInfoProvider,
		soleChunkFuncSemaphore: semaphore.NewWeighted(1)}, nil
}

func (s *appendBlobSenderBase) SendableEntityType() common.EntityType {
	return common.EEntityType.File()
}

func (s *appendBlobSenderBase) ChunkSize() int64 {
	return s.chunkSize
}

func (s *appendBlobSenderBase) NumChunks() uint32 {
	return s.numChunks
}

func (s *appendBlobSenderBase) RemoteFileExists() (bool, time.Time, error) {
	properties, err := s.destAppendBlobClient.GetProperties(s.jptm.Context(), &blob.GetPropertiesOptions{CPKInfo: s.jptm.CpkInfo()})
	return remoteObjectExists(blobPropertiesResponseAdapter{properties}, err)
}

// Returns a chunk-func for sending append blob to remote
func (s *appendBlobSenderBase) generateAppendBlockToRemoteFunc(id common.ChunkID, appendBlock appendBlockFunc) chunkFunc {
	// Copy must be totally sequential for append blobs
	// The way we enforce that is simple: we won't even CREATE
	// a chunk func, until all previously-scheduled chunk funcs have completed
	// Here we block until there are no other chunkfuncs in existence for this blob
	err := s.soleChunkFuncSemaphore.Acquire(s.jptm.Context(), 1)
	if err != nil {
		// Must have been cancelled
		// We must still return a chunk func, so return a no-op one
		return createSendToRemoteChunkFunc(s.jptm, id, func() {})
	}

	return createSendToRemoteChunkFunc(s.jptm, id, func() {

		// Here, INSIDE the chunkfunc, we release the semaphore when we have finished running
		defer s.soleChunkFuncSemaphore.Release(1)

		jptm := s.jptm

		if jptm.Info().SourceSize == 0 {
			// nothing to do, since this is a dummy chunk in a zero-size file, and the prologue will have done all the real work
			return
		}

		appendBlock()
	})
}

func (s *appendBlobSenderBase) Prologue(ps common.PrologueState) (destinationModified bool) {
	if s.jptm.ShouldInferContentType() {
		// sometimes, specifically when reading local files, we have more info
		// about the file type at this time than what we had before
		s.headersToApply.BlobContentType = ps.GetInferredContentType(s.jptm)
	}

	blobTags := s.blobTagsToApply
	setTags := separateSetTagsRequired(blobTags)
	if setTags || len(blobTags) == 0 {
		blobTags = nil
	}
	_, err := s.destAppendBlobClient.Create(s.jptm.Context(), &appendblob.CreateOptions{
		HTTPHeaders:  &s.headersToApply,
		Metadata:     s.metadataToApply.Metadata,
		Tags:         blobTags,
		CPKInfo:      s.jptm.CpkInfo(),
		CPKScopeInfo: s.jptm.CpkScopeInfo(),
	})
	if err != nil {
		s.jptm.FailActiveSend(common.Iff(len(blobTags) > 0, "Creating blob (with tags)", "Creating blob"), err)
		return
	}
	destinationModified = true

	if setTags {
		_, err = s.destAppendBlobClient.SetTags(s.jptm.Context(), s.blobTagsToApply, nil)
		if err != nil {
			s.jptm.FailActiveSend("Set blob tags", err)
		}
	}
	return
}

func (s *appendBlobSenderBase) Epilogue() {
	// Empty function because you don't have to commit on an append blob
}

func (s *appendBlobSenderBase) Cleanup() {
	jptm := s.jptm
	// Cleanup
	if jptm.IsDeadInflight() {
		// There is a possibility that some uncommitted blocks will be there
		// Delete the uncommitted blobs
		// TODO: particularly, given that this is an APPEND blob, do we really need to delete it?  But if we don't delete it,
		//   it will still be in an ambiguous situation with regard to how much has been added to it.  Probably best to delete
		//   to be consistent with other
		deletionContext, cancelFunc := context.WithTimeout(context.WithValue(context.Background(), ServiceAPIVersionOverride, DefaultServiceApiVersion), 30*time.Second)
		defer cancelFunc()
		_, err := s.destAppendBlobClient.Delete(deletionContext, nil)
		if err != nil {
			jptm.LogError(s.destAppendBlobClient.URL(), "Delete (incomplete) Append Blob ", err)
		}
	}
}

// GetDestinationLength gets the destination length.
func (s *appendBlobSenderBase) GetDestinationLength() (int64, error) {
	prop, err := s.destAppendBlobClient.GetProperties(s.jptm.Context(), &blob.GetPropertiesOptions{CPKInfo: s.jptm.CpkInfo()})

	if err != nil {
		return -1, err
	}

	if prop.ContentLength == nil {
		return -1, fmt.Errorf("destination content length not returned")
	}
	return *prop.ContentLength, nil
}

func (s *appendBlobSenderBase) GetMD5(offset, count int64) ([]byte, error) {
	var rangeGetContentMD5 *bool
	if count <= common.MaxRangeGetSize {
		rangeGetContentMD5 = to.Ptr(true)
	}
	response, err := s.destAppendBlobClient.DownloadStream(s.jptm.Context(),
		&blob.DownloadStreamOptions{
			Range:              blob.HTTPRange{Offset: offset, Count: count},
			RangeGetContentMD5: rangeGetContentMD5,
			CPKInfo:            s.jptm.CpkInfo(),
			CPKScopeInfo:       s.jptm.CpkScopeInfo(),
		})
	if err != nil {
		return nil, err
	}
	if len(response.ContentMD5) > 0 {
		return response.ContentMD5, nil
	} else {
		// compute md5
		body := response.NewRetryReader(s.jptm.Context(), &blob.RetryReaderOptions{MaxRetries: MaxRetryPerDownloadBody})
		defer body.Close()
		h := md5.New()
		if _, err = io.Copy(h, body); err != nil {
			return nil, err
		}
		return h.Sum(nil), nil
	}
}

func (s *appendBlobSenderBase) transformAppendConditionMismatchError(timeoutFromCtx bool, offset, count int64, err error) (string, error) {
	if err != nil && bloberror.HasCode(err, bloberror.AppendPositionConditionNotMet) && timeoutFromCtx {
		if _, ok := s.sip.(benchmarkSourceInfoProvider); ok {
			// If the source is a benchmark, then we don't need to check MD5 since the data is constantly changing.  This is OK.
			return "", nil
		}
		// Download Range of last append
		destMD5, destErr := s.GetMD5(offset, count)
		if destErr != nil {
			return ", get destination md5 after timeout", destErr
		}
		sourceMD5, sourceErr := s.sip.GetMD5(offset, count)
		if sourceErr != nil {
			return ", get source md5 after timeout", sourceErr
		}
		if destMD5 != nil && sourceMD5 != nil && len(destMD5) > 0 && len(sourceMD5) > 0 {
			// Compare MD5
			if bytes.Equal(destMD5, sourceMD5) {
				return "", nil
			}
		}
	}
	return "", err
}
