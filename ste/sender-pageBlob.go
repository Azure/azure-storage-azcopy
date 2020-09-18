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
	"regexp"
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
	chunkSize       int64
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

	// destPageRangeOptimizer is necessary for managed disk imports,
	// as it helps us identify where we actually need to write all zeroes to.
	// Previously, if a page prefetched all zeroes, we'd ignore it.
	// In a edge-case scenario (where two different VHDs had been uploaded to the same md impexp URL),
	// there was a potential for us to not zero out 512b segments that we'd prefetched all zeroes for.
	// This only posed danger when there was already data in one of these segments.
	destPageRangeOptimizer *pageRangeOptimizer
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
	chunkSize = common.Iffint64(
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

	// This is only necessary if our destination is a managed disk impexp account.
	// Read the in struct explanation if necessary.
	var destRangeOptimizer *pageRangeOptimizer
	if isInManagedDiskImportExportAccount(*destURL) {
		destRangeOptimizer = newPageRangeOptimizer(destPageBlobURL,
			context.WithValue(jptm.Context(), ServiceAPIVersionOverride, azblob.ServiceVersion))
	}

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
		jptm:                   jptm,
		destPageBlobURL:        destPageBlobURL,
		srcSize:                srcSize,
		chunkSize:              chunkSize,
		numChunks:              numChunks,
		pacer:                  pacer,
		headersToApply:         props.SrcHTTPHeaders.ToAzBlobHTTPHeaders(),
		metadataToApply:        props.SrcMetadata.ToAzBlobMetadata(),
		destBlobTier:           destBlobTier,
		filePacer:              newNullAutoPacer(), // defer creation of real one to Prologue
		destPageRangeOptimizer: destRangeOptimizer,
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

func (s *pageBlobSenderBase) SendableEntityType() common.EntityType {
	return common.EEntityType.File()
}

func (s *pageBlobSenderBase) ChunkSize() int64 {
	return s.chunkSize
}

func (s *pageBlobSenderBase) NumChunks() uint32 {
	return s.numChunks
}

func (s *pageBlobSenderBase) RemoteFileExists() (bool, time.Time, error) {
	return remoteObjectExists(s.destPageBlobURL.GetProperties(s.jptm.Context(), azblob.BlobAccessConditions{}))
}

var premiumPageBlobTierRegex = regexp.MustCompile(`P\d+`)

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
		//
		// We use an equality check (rather than ensuring sourceSize <= dest), because customer should have declared the correct exact size when
		// making the disk in Azure. (And if we don't check equality here, by default we do check it after upload for all blobs, as of version 10.3)
		//
		// Note re types and sizes:
		// Currently (2019) only VHDs are supported for Azure managed disk upload. VHDXs (which have a different footer size, are not).
		// Azure requires VHD size to be a multiple of 1MB plus 512 bytes for the VHD footer. And the VHD must be fixed size.
		// E.g. these are the values reported by PowerShell's Get-VHD for a valid 1 GB VHD:
		// VhdFormat               : VHD
		// VhdType                 : Fixed
		// FileSize                : 1073742336  (equals our s.srcSize, i.e. the size of the disk file)
		// Size                    : 1073741824

		p, err := s.destPageBlobURL.GetProperties(s.jptm.Context(), azblob.BlobAccessConditions{})
		if err != nil {
			s.jptm.FailActiveSend("Checking size of managed disk blob", err)
			return
		}
		if s.srcSize != p.ContentLength() {
			sizeErr := errors.New(fmt.Sprintf("source file is not same size as the destination page blob. Source size is %d bytes but destination size is %d bytes. Re-create the destination with exactly the right size. E.g. see parameter UploadSizeInBytes in PowerShell's New-AzDiskConfig. Ensure the source is a fixed-size VHD",
				s.srcSize, p.ContentLength()))
			s.jptm.FailActiveSend("Checking size of managed disk blob", sizeErr)
			return
		}

		// Next, grab the page ranges on the destination.
		s.destPageRangeOptimizer.fetchPages()

		s.jptm.Log(pipeline.LogInfo, "Blob is managed disk import/export blob, so no Create call is required") // the blob always already exists
		return
	} else {
		destinationModified = true
	}

	// sometimes, specifically when reading local files, we have more info
	// about the file type at this time than what we had before
	s.headersToApply.ContentType = ps.GetInferredContentType(s.jptm)

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
	AttemptSetBlobTier(s.jptm, s.destBlobTier, s.destPageBlobURL.BlobURL, s.jptm.Context())

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
