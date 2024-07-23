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

	"github.com/Azure/azure-sdk-for-go/sdk/azcore/to"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/blob"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/pageblob"

	"github.com/Azure/azure-storage-azcopy/v10/common"
)

type pageBlobSenderBase struct {
	jptm               IJobPartTransferMgr
	destPageBlobClient *pageblob.Client
	srcSize            int64
	chunkSize          int64
	numChunks          uint32
	pacer              pacer

	// Headers and other info that we will apply to the destination
	// object. For S2S, these come from the source service.
	// When sending local data, they are computed based on
	// the properties of the local file
	headersToApply  blob.HTTPHeaders
	metadataToApply common.Metadata
	blobTagsToApply common.BlobTags

	destBlobTier *blob.AccessTier
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
	managedDiskImportExportAccountPrefix = "md-"

	// Start high(ish), because it auto-tunes downwards faster than it auto-tunes upwards
	pageBlobInitialBytesPerSecond = (4 * 1000 * 1000 * 1000) / 8
)

var (
	md5NotSupportedInManagedDiskError = errors.New("the Content-MD5 hash is not supported for managed disk uploads")
)

func newPageBlobSenderBase(jptm IJobPartTransferMgr, destination string, pacer pacer, srcInfoProvider ISourceInfoProvider, inferredAccessTierType *blob.AccessTier) (*pageBlobSenderBase, error) {
	transferInfo := jptm.Info()

	// compute chunk count
	chunkSize := transferInfo.BlockSize
	// If the given chunk Size for the Job is invalid for page blob or greater than maximum page size,
	// then set chunkSize as maximum pageSize.
	chunkSize = common.Iff(
		chunkSize > common.DefaultPageBlobChunkSize || (chunkSize%pageblob.PageBytes != 0),
		common.DefaultPageBlobChunkSize,
		chunkSize)

	srcSize := transferInfo.SourceSize
	numChunks := getNumChunks(srcSize, chunkSize, chunkSize)

	bsc, err := jptm.DstServiceClient().BlobServiceClient()
	if err != nil {
		return nil, err
	}

	destPageBlobClient := bsc.NewContainerClient(jptm.Info().DstContainer).NewPageBlobClient(jptm.Info().DstFilePath)

	// This is only necessary if our destination is a managed disk impexp account.
	// Read the in struct explanation if necessary.
	var destRangeOptimizer *pageRangeOptimizer
	if isInManagedDiskImportExportAccount(destination) {
		destRangeOptimizer = newPageRangeOptimizer(destPageBlobClient, jptm.Context())
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
		t := pageBlobTierOverride.ToAccessTierType()
		destBlobTier = &t
	}

	s := &pageBlobSenderBase{
		jptm:                   jptm,
		destPageBlobClient:     destPageBlobClient,
		srcSize:                srcSize,
		chunkSize:              chunkSize,
		numChunks:              numChunks,
		pacer:                  pacer,
		headersToApply:         props.SrcHTTPHeaders.ToBlobHTTPHeaders(),
		metadataToApply:        FixBustedMetadata(props.SrcMetadata),
		blobTagsToApply:        props.SrcBlobTags,
		destBlobTier:           destBlobTier,
		filePacer:              NewNullAutoPacer(), // defer creation of real one to Prologue
		destPageRangeOptimizer: destRangeOptimizer,
	}

	if s.isInManagedDiskImportExportAccount() && jptm.ShouldPutMd5() {
		return nil, md5NotSupportedInManagedDiskError
	}

	return s, nil
}

// these accounts have special restrictions of which APIs operations they support
func isInManagedDiskImportExportAccount(rawURL string) bool {
	u, err := url.Parse(rawURL)
	if err != nil {
		return false
	}
	return strings.HasPrefix(u.Host, managedDiskImportExportAccountPrefix)
}

func (s *pageBlobSenderBase) isInManagedDiskImportExportAccount() bool {
	return isInManagedDiskImportExportAccount(s.destPageBlobClient.URL())
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
	properties, err := s.destPageBlobClient.GetProperties(s.jptm.Context(), &blob.GetPropertiesOptions{CPKInfo: s.jptm.CpkInfo()})
	return remoteObjectExists(blobPropertiesResponseAdapter{properties}, err)
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

		p, err := s.destPageBlobClient.GetProperties(s.jptm.Context(), &blob.GetPropertiesOptions{CPKInfo: s.jptm.CpkInfo()})
		if err != nil {
			s.jptm.FailActiveSend("Checking size of managed disk blob", err)
			return
		}
		if p.ContentLength == nil {
			sizeErr := fmt.Errorf("destination content length not returned")
			s.jptm.FailActiveSend("Checking size of managed disk blob", sizeErr)
		}
		if s.srcSize != *p.ContentLength {
			sizeErr := fmt.Errorf("source file is not same size as the destination page blob. Source size is %d bytes but destination size is %d bytes. Re-create the destination with exactly the right size. E.g. see parameter UploadSizeInBytes in PowerShell's New-AzDiskConfig. Ensure the source is a fixed-size VHD",
				s.srcSize, *p.ContentLength)
			s.jptm.FailActiveSend("Checking size of managed disk blob", sizeErr)
			return
		}

		// Next, grab the page ranges on the destination.
		s.destPageRangeOptimizer.fetchPages()

		s.jptm.Log(common.LogInfo, "Blob is managed disk import/export blob, so no Create call is required") // the blob always already exists
		return
	}

	if s.jptm.ShouldInferContentType() {
		// sometimes, specifically when reading local files, we have more info
		// about the file type at this time than what we had before
		s.headersToApply.BlobContentType = ps.GetInferredContentType(s.jptm)
	}

	var destBlobTier *pageblob.PremiumPageBlobAccessTier
	if s.destBlobTier != nil {
		destBlobTier = to.Ptr(pageblob.PremiumPageBlobAccessTier(*s.destBlobTier))
	}
	if !ValidateTier(s.jptm, s.destBlobTier, s.destPageBlobClient, s.jptm.Context(), false) {
		destBlobTier = nil
	}
	// TODO: Remove this snippet once service starts supporting CPK with blob tier
	if s.jptm.IsSourceEncrypted() {
		destBlobTier = nil
	}

	blobTags := s.blobTagsToApply
	setTags := separateSetTagsRequired(blobTags)
	if setTags || len(blobTags) == 0 {
		blobTags = nil
	}

	_, err := s.destPageBlobClient.Create(s.jptm.Context(), s.srcSize,
		&pageblob.CreateOptions{
			SequenceNumber: to.Ptr(int64(0)),
			HTTPHeaders:    &s.headersToApply,
			Metadata:       s.metadataToApply,
			Tier:           destBlobTier,
			Tags:           blobTags,
			CPKInfo:        s.jptm.CpkInfo(),
			CPKScopeInfo:   s.jptm.CpkScopeInfo(),
		})
	if err != nil {
		s.jptm.FailActiveSend(common.Iff(len(blobTags) > 0, "Creating blob (with tags)", "Creating blob"), err)
		return
	}

	destinationModified = true

	if setTags {
		if _, err := s.destPageBlobClient.SetTags(s.jptm.Context(), s.blobTagsToApply, nil); err != nil {
			s.jptm.FailActiveSend("Set blob tags", err)
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
			deletionContext, cancelFunc := context.WithTimeout(context.WithValue(context.Background(), ServiceAPIVersionOverride, DefaultServiceApiVersion), 30*time.Second)
			defer cancelFunc()
			_, err := s.destPageBlobClient.Delete(deletionContext, nil)
			if err != nil {
				jptm.LogError(s.destPageBlobClient.URL(), "Delete (incomplete) Page Blob ", err)
			}
		}
	}
}

// GetDestinationLength gets the destination length.
func (s *pageBlobSenderBase) GetDestinationLength() (int64, error) {
	prop, err := s.destPageBlobClient.GetProperties(s.jptm.Context(), &blob.GetPropertiesOptions{CPKInfo: s.jptm.CpkInfo()})

	if err != nil {
		return -1, err
	}

	if prop.ContentLength == nil {
		return -1, fmt.Errorf("destination content length not returned")
	}
	return *prop.ContentLength, nil
}
