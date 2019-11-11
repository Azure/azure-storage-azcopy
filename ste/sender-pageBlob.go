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
	"errors"
	"fmt"
	"net/url"
	"strings"
	"time"

	"github.com/Azure/azure-pipeline-go/pipeline"
	"github.com/Azure/azure-storage-blob-go/azblob"

	"github.com/Azure/azure-storage-azcopy/common"
)

type pageBlobSenderBase struct {
	jptm            IJobPartTransferMgr
	destPageBlobURL azblob.PageBlobURL
	srcSize         int64
	chunkSize       uint32
	numChunks       uint32
	pacer           pacer
	// Headers and other info that we will apply to the destination
	// object. For S2S, these come from the source service.
	// When sending local data, they are computed based on
	// the properties of the local file
	headersToApply  azblob.BlobHTTPHeaders
	metadataToApply azblob.Metadata
	destBlobTier    azblob.AccessTierType
	// filePacer is necessary because page blobs have per-blob throughput limits. The limits depend on
	// what type of page blob it is (e.g. premium) and can be significantly lower than the blob account limit.
	// Using a automatic pacer here lets us find the right rate for this particular page blob, at which
	// we won't be trying to move the faster than the Service wants us to.
	filePacer autopacer
}

const (
	managedDiskImportExportAccountPrefix = "md-impexp-"
	legacyDiskExportPrefix               = "md-" // these don't have the impepx bit that follows

	// Start high(ish), because it auto-tunes downwards faster than it auto-tunes upwards
	pageBlobInitialBytesPerSecond = (4 * 1000 * 1000 * 1000) / 8
)

var (
	md5NotSupportedInManagedDiskError = errors.New("the Content-MD5 hash is not supported for managed disk uploads")
)

func newPageBlobSenderBase(jptm IJobPartTransferMgr, destination string, p pipeline.Pipeline, pacer pacer, srcInfoProvider ISourceInfoProvider, inferredAccessTierType azblob.AccessTierType) (*pageBlobSenderBase, error) {
	transferInfo := jptm.Info()

	// compute chunk count
	chunkSize := transferInfo.BlockSize
	// If the given chunk Size for the Job is invalid for page blob or greater than maximum page size,
	// then set chunkSize as maximum pageSize.
	chunkSize = common.Iffuint32(
		chunkSize > common.DefaultPageBlobChunkSize || (chunkSize%azblob.PageBlobPageBytes != 0),
		common.DefaultPageBlobChunkSize,
		chunkSize)

	srcSize := transferInfo.SourceSize
	numChunks := getNumChunks(srcSize, chunkSize)

	destURL, err := url.Parse(destination)
	if err != nil {
		return nil, err
	}

	destPageBlobURL := azblob.NewPageBlobURL(*destURL, p)

	props, err := srcInfoProvider.Properties()
	if err != nil {
		return nil, err
	}

	// If user set blob tier explicitly, override any value that our caller
	// may have guessed.
	destBlobTier := inferredAccessTierType
	_, pageBlobTierOverride := jptm.BlobTiers()
	if pageBlobTierOverride != common.EPageBlobTier.None() {
		destBlobTier = pageBlobTierOverride.ToAccessTierType()
	}

	s := &pageBlobSenderBase{
		jptm:            jptm,
		destPageBlobURL: destPageBlobURL,
		srcSize:         srcSize,
		chunkSize:       chunkSize,
		numChunks:       numChunks,
		pacer:           pacer,
		headersToApply:  props.SrcHTTPHeaders.ToAzBlobHTTPHeaders(),
		metadataToApply: props.SrcMetadata.ToAzBlobMetadata(),
		destBlobTier:    destBlobTier,
		filePacer:       newNullAutoPacer(), // defer creation of real one to Prologue
	}

	if s.isInManagedDiskImportExportAccount() && jptm.ShouldPutMd5() {
		return nil, md5NotSupportedInManagedDiskError
	}

	return s, nil
}

// these accounts have special restrictions of which APIs operations they support
func isInManagedDiskImportExportAccount(u url.URL) bool {
	return strings.HasPrefix(u.Host, managedDiskImportExportAccountPrefix)
}

// "legacy" is perhaps not the best name, since these remain in use for (all?) EXports of managed disks.
// These "legacy" ones an older mechanism than the new md-impexp path that is used for IMports as of mid 2019.
func isInLegacyDiskExportAccount(u url.URL) bool {
	if isInManagedDiskImportExportAccount(u) {
		return false // it's the new-style md-impexp
	}
	return strings.HasPrefix(u.Host, legacyDiskExportPrefix) // md-....
}

func (s *pageBlobSenderBase) isInManagedDiskImportExportAccount() bool {
	return isInManagedDiskImportExportAccount(s.destPageBlobURL.URL())
}

func (s *pageBlobSenderBase) ChunkSize() uint32 {
	return s.chunkSize
}

func (s *pageBlobSenderBase) NumChunks() uint32 {
	return s.numChunks
}

func (s *pageBlobSenderBase) RemoteFileExists() (bool, error) {
	return remoteObjectExists(s.destPageBlobURL.GetProperties(s.jptm.Context(), azblob.BlobAccessConditions{}))
}

func (s *pageBlobSenderBase) Prologue(ps common.PrologueState) (destinationModified bool) {

	// Create file pacer now.  Safe to create now, because we know that if Prologue is called the Epilogue will be to
	// so we know that the pacer will be closed.  // TODO: consider re-factor xfer-anyToRemote so that epilogue is always called if uploader is constructed, and move this to constructor
	s.filePacer = newPageBlobAutoPacer(pageBlobInitialBytesPerSecond, s.ChunkSize(), false, s.jptm.(common.ILogger))

	if s.isInManagedDiskImportExportAccount() {
		// Target will already exist (and CANNOT be created through the REST API, because
		// managed-disk import-export accounts have restricted API surface)

		// Check its length, since it already has a size, and the upload will fail at the end if you what
		// upload to it is bigger than its existing size. (And, for big files, it may be hours until you discover that
		// difference if we don't check here).
		p, err := s.destPageBlobURL.GetProperties(s.jptm.Context(), azblob.BlobAccessConditions{})
		if err != nil {
			s.jptm.FailActiveSend("Checking size of managed disk blob", err)
			return
		}
		if s.srcSize > p.ContentLength() {
			sizeErr := errors.New(fmt.Sprintf("source file is too big for the destination page blob. Source size is %d bytes but destination size is %d bytes",
				s.srcSize, p.ContentLength()))
			s.jptm.FailActiveSend("Checking size of managed disk blob", sizeErr)
			return
		}

		s.jptm.Log(pipeline.LogInfo, "Blob is managed disk import/export blob, so no Create call is required") // the blob always already exists
		return
	} else {
		destinationModified = true
	}

	if ps.CanInferContentType() {
		// sometimes, specifically when reading local files, we have more info
		// about the file type at this time than what we had before
		s.headersToApply.ContentType = ps.GetInferredContentType(s.jptm)
	}

	if _, err := s.destPageBlobURL.Create(s.jptm.Context(),
		s.srcSize,
		0,
		s.headersToApply,
		s.metadataToApply,
		azblob.BlobAccessConditions{}); err != nil {
		s.jptm.FailActiveSend("Creating blob", err)
		return
	}

	// Set tier, https://docs.microsoft.com/en-us/azure/storage/blobs/storage-blob-storage-tiers
	if s.destBlobTier != azblob.AccessTierNone {
		// Ensure destBlobTier is not block blob tier, i.e. not Hot, Cool and Archive.
		// Note: When copying from page blob source, the inferred blob tier could be Hot.
		var blockBlobTier common.BlockBlobTier
		if err := blockBlobTier.Parse(string(s.destBlobTier)); err != nil { // i.e it's not block blob tier
			// Set the latest service version from sdk as service version in the context.
			ctxWithLatestServiceVersion := context.WithValue(s.jptm.Context(), ServiceAPIVersionOverride, azblob.ServiceVersion)
			if _, err := s.destPageBlobURL.SetTier(ctxWithLatestServiceVersion, s.destBlobTier, azblob.LeaseAccessConditions{}); err != nil {
				if s.jptm.Info().S2SSrcBlobTier != azblob.AccessTierNone {
					s.jptm.LogTransferInfo(pipeline.LogError, s.jptm.Info().Source, s.jptm.Info().Destination, "Failed to replicate blob tier at destination. Try transferring with the flag --s2s-preserve-access-tier=false")
					s2sAccessTierFailureLogStdout.Do(func() {
						glcm := common.GetLifecycleMgr()
						glcm.Error("One or more blobs have failed blob tier replication at the destination. Try transferring with the flag --s2s-preserve-access-tier=false")
					})
				}

				s.jptm.FailActiveSendWithStatus("Setting PageBlob tier ", err, common.ETransferStatus.BlobTierFailure())
				return
			}
		}
	}

	return
}

func (s *pageBlobSenderBase) Epilogue() {
	_ = s.filePacer.Close() // release resources
}

func (s *pageBlobSenderBase) Cleanup() {
	jptm := s.jptm

	// Cleanup
	if jptm.IsDeadInflight() {
		if s.isInManagedDiskImportExportAccount() {
			// no deletion is possible. User just has to upload it again.
		} else {
			deletionContext, cancelFunc := context.WithTimeout(context.Background(), 30*time.Second)
			defer cancelFunc()
			_, err := s.destPageBlobURL.Delete(deletionContext, azblob.DeleteSnapshotsOptionNone, azblob.BlobAccessConditions{})
			if err != nil {
				jptm.LogError(s.destPageBlobURL.String(), "Delete (incomplete) Page Blob ", err)
			}
		}
	}
}
