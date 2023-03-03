package ste

import (
	"fmt"
	"github.com/Azure/azure-pipeline-go/pipeline"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/blob"
	"github.com/Azure/azure-storage-azcopy/v10/common"
	"github.com/Azure/azure-storage-blob-go/azblob"
	"net/url"
	"strings"
	"time"
)

type blobSymlinkSender struct {
	destBlockBlobURL azblob.BlockBlobURL
	jptm             IJobPartTransferMgr
	sip              ISourceInfoProvider
	headersToApply   blob.HTTPHeaders
	metadataToApply  azblob.Metadata
	destBlobTier     blob.AccessTier
	blobTagsToApply  azblob.BlobTagsMap
	cpkToApply       azblob.ClientProvidedKeyOptions
}

func newBlobSymlinkSender(jptm IJobPartTransferMgr, destination string, p pipeline.Pipeline, pacer pacer, sip ISourceInfoProvider) (sender, error) {
	destURL, err := url.Parse(destination)
	if err != nil {
		return nil, err
	}

	destBlockBlobURL := azblob.NewBlockBlobURL(*destURL, p)

	props, err := sip.Properties()
	if err != nil {
		return nil, err
	}

	var destBlobTier blob.AccessTier
	blockBlobTierOverride, _ := jptm.BlobTiers()
	if blockBlobTierOverride != common.EBlockBlobTier.None() {
		destBlobTier = blockBlobTierOverride.ToAccessTierType()
	}

	var out sender
	ssend := blobSymlinkSender{
		jptm:             jptm,
		sip:              sip,
		destBlockBlobURL: destBlockBlobURL,
		metadataToApply:  props.SrcMetadata.Clone().ToAzBlobMetadata(), // We're going to modify it, so we should clone it.
		headersToApply:   props.SrcHTTPHeaders.ToBlobHTTPHeaders(),
		blobTagsToApply:  props.SrcBlobTags.ToAzBlobTagsMap(),
		cpkToApply:       common.ToClientProvidedKeyOptions(jptm.CpkInfo(), jptm.CpkScopeInfo()),
		destBlobTier:     destBlobTier,
	}
	fromTo := jptm.FromTo()
	if fromTo.IsUpload() {
		out = &dummySymlinkUploader{ssend}
	} else {
		out = &dummySymlinkS2SCopier{ssend}
	}

	return out, nil
}

func (s *blobSymlinkSender) SendSymlink(linkData string) error {
	err := s.getExtraProperties()
	if err != nil {
		return fmt.Errorf("when getting additional folder properties: %w", err)
	}
	s.metadataToApply["is_symlink"] = "true"

	_, err = s.destBlockBlobURL.Upload(s.jptm.Context(), strings.NewReader(linkData), common.ToAzBlobHTTPHeaders(s.headersToApply), s.metadataToApply, azblob.BlobAccessConditions{}, azblob.AccessTierType(s.destBlobTier), s.blobTagsToApply, s.cpkToApply, azblob.ImmutabilityPolicyOptions{})
	return err
}

// ===== Implement sender so that it can be returned in newBlobUploader. =====
/*
	It's OK to just panic all of these out, as they will never get called in a symlink transfer.
*/

func (s *blobSymlinkSender) ChunkSize() int64 {
	panic("this sender only sends symlinks.")
}

func (s *blobSymlinkSender) NumChunks() uint32 {
	panic("this sender only sends symlinks.")
}

func (s *blobSymlinkSender) RemoteFileExists() (bool, time.Time, error) {
	panic("this sender only sends symlinks.")
}

func (s *blobSymlinkSender) Prologue(state common.PrologueState) (destinationModified bool) {
	panic("this sender only sends symlinks.")
}

func (s *blobSymlinkSender) Epilogue() {
	panic("this sender only sends symlinks.")
}

func (s *blobSymlinkSender) Cleanup() {
	panic("this sender only sends symlinks.")
}

func (s *blobSymlinkSender) GetDestinationLength() (int64, error) {
	panic("this sender only sends symlinks.")
}

// implement uploader to handle commonSenderCompletion

type dummySymlinkUploader struct {
	blobSymlinkSender
}

func (d dummySymlinkUploader) GenerateUploadFunc(chunkID common.ChunkID, blockIndex int32, reader common.SingleChunkReader, chunkIsWholeFile bool) chunkFunc {
	panic("this sender only sends folders.")
}

func (d dummySymlinkUploader) Md5Channel() chan<- []byte {
	panic("this sender only sends folders.")
}

// ditto for s2sCopier

type dummySymlinkS2SCopier struct {
	blobSymlinkSender
}

func (d dummySymlinkS2SCopier) GenerateCopyFunc(chunkID common.ChunkID, blockIndex int32, adjustedChunkSize int64, chunkIsWholeFile bool) chunkFunc {
	panic("this sender only sends folders.")
}
