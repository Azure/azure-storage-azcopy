package ste

import (
	"fmt"
	"strings"
	"time"

	"github.com/Azure/azure-sdk-for-go/sdk/azcore/streaming"
	"github.com/Azure/azure-sdk-for-go/sdk/azcore/to"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/blob"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/blockblob"
	"github.com/Azure/azure-storage-azcopy/v10/common"
)

type blobSymlinkSender struct {
	destinationClient *blockblob.Client
	jptm              IJobPartTransferMgr
	sip               ISourceInfoProvider
	headersToApply    blob.HTTPHeaders
	metadataToApply   *common.SafeMetadata
	destBlobTier      *blob.AccessTier
	blobTagsToApply   common.BlobTags
}

func newBlobSymlinkSender(jptm IJobPartTransferMgr, destination string, sip ISourceInfoProvider) (sender, error) {
	s, err := jptm.DstServiceClient().BlobServiceClient()
	if err != nil {
		return nil, nil
	}
	destinationClient := s.NewContainerClient(jptm.Info().DstContainer).NewBlockBlobClient(jptm.Info().DstFilePath)

	props, err := sip.Properties()
	if err != nil {
		return nil, err
	}

	var destBlobTier *blob.AccessTier
	blockBlobTierOverride, _ := jptm.BlobTiers()
	if blockBlobTierOverride != common.EBlockBlobTier.None() {
		t := blob.AccessTier(blockBlobTierOverride.ToAccessTierType())
		destBlobTier = &t
	}

	var out sender
	ssend := blobSymlinkSender{
		jptm:              jptm,
		sip:               sip,
		destinationClient: destinationClient,
		metadataToApply:   &common.SafeMetadata{Metadata: props.SrcMetadata.Clone()}, // We're going to modify it, so we should clone it.
		headersToApply:    props.SrcHTTPHeaders.ToBlobHTTPHeaders(),
		blobTagsToApply:   props.SrcBlobTags,
		destBlobTier:      destBlobTier,
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
	s.metadataToApply.Metadata["is_symlink"] = to.Ptr("true")

	blobTags := s.blobTagsToApply
	setTags := separateSetTagsRequired(blobTags)
	if setTags || len(blobTags) == 0 {
		blobTags = nil
	}

	_, err = s.destinationClient.Upload(s.jptm.Context(), streaming.NopCloser(strings.NewReader(linkData)),
		&blockblob.UploadOptions{
			HTTPHeaders:  &s.headersToApply,
			Metadata:     s.metadataToApply.Metadata,
			Tier:         s.destBlobTier,
			Tags:         blobTags,
			CPKInfo:      s.jptm.CpkInfo(),
			CPKScopeInfo: s.jptm.CpkScopeInfo(),
		})
	if err != nil {
		s.jptm.FailActiveSend(common.Iff(len(blobTags) > 0, "Upload symlink (with tags)", "Upload symlink"), err)
		return nil
	}

	if setTags {
		if _, err := s.destinationClient.SetTags(s.jptm.Context(), s.blobTagsToApply, nil); err != nil {
			s.jptm.FailActiveSend("Set tags", err)
			return nil
		}
	}
	return nil
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
