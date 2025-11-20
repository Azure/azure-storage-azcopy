package cmd

import (
	"encoding/json"
	"fmt"
	"net/url"

	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/blob"
	"github.com/Azure/azure-storage-azcopy/v10/common"
	"github.com/Azure/azure-storage-azcopy/v10/traverser"
)

type dryrunDeleter struct {
	objectTypeToDisplay     string
	objectLocationToDisplay string
}

func (d *dryrunDeleter) dryrunDelete(object traverser.StoredObject) error {
	glcm.Dryrun(func(format OutputFormat) string {
		if format == EOutputFormat.Json() {
			deleteTarget := common.ELocation.Local()
			if d.objectTypeToDisplay != LocalFileObjectType {
				_ = deleteTarget.Parse(d.objectTypeToDisplay)
			}

			tx := DryrunTransfer{
				Source:     common.GenerateFullPath(d.objectLocationToDisplay, object.RelativePath),
				BlobType:   common.FromBlobType(object.BlobType),
				EntityType: object.EntityType,
				FromTo:     common.FromToValue(deleteTarget, common.ELocation.Unknown()),
			}

			jsonOutput, err := json.Marshal(tx)
			common.PanicIfErr(err)
			return string(jsonOutput)
		} else { // remove for sync
			return fmt.Sprintf("DRYRUN: remove %v",
				common.GenerateFullPath(d.objectLocationToDisplay, object.RelativePath))
		}
	})
	return nil
}

func newSyncDryRunDeleteProcessor(cca *cookedSyncCmdArgs, objectTypeToDisplay string) *interactiveDeleteProcessor {
	deleter := dryrunDeleter{objectTypeToDisplay: objectTypeToDisplay, objectLocationToDisplay: cca.destination.Value}
	return newInteractiveDeleteProcessor(deleter.dryrunDelete, cca.deleteDestination, objectTypeToDisplay, cca.destination, cca.incrementDeletionCount)
}

func getDryrunNewCopyJobPartOrder(sourceRoot, destinationRoot string, fromTo common.FromTo) func(common.CopyJobPartOrderRequest) common.CopyJobPartOrderResponse {
	return func(order common.CopyJobPartOrderRequest) common.CopyJobPartOrderResponse {
		for _, transfer := range order.Transfers.List {
			glcm.Dryrun(func(format OutputFormat) string {
				var err error
				prettySrcRelativePath, prettyDstRelativePath := transfer.Source, transfer.Destination

				if fromTo.From().IsRemote() {
					prettySrcRelativePath, err = url.PathUnescape(prettySrcRelativePath)
					if err != nil {
						prettySrcRelativePath = transfer.Source // Fall back, because it's better than failing.
					}
				}

				if fromTo.To().IsRemote() {
					prettyDstRelativePath, err = url.PathUnescape(prettyDstRelativePath)
					if err != nil {
						prettyDstRelativePath = transfer.Destination // Fall back, because it's better than failing.
					}
				}

				if format == EOutputFormat.Json() {
					tx := DryrunTransfer{
						EntityType:  transfer.EntityType,
						BlobType:    common.FromBlobType(transfer.BlobType),
						FromTo:      fromTo,
						Source:      common.GenerateFullPath(sourceRoot, prettySrcRelativePath),
						Destination: "",
						SourceSize:  &transfer.SourceSize,
						HttpHeaders: blob.HTTPHeaders{
							BlobCacheControl:       &transfer.CacheControl,
							BlobContentDisposition: &transfer.ContentDisposition,
							BlobContentEncoding:    &transfer.ContentEncoding,
							BlobContentLanguage:    &transfer.ContentLanguage,
							BlobContentMD5:         transfer.ContentMD5,
							BlobContentType:        &transfer.ContentType,
						},
						Metadata:     transfer.Metadata,
						BlobTier:     &transfer.BlobTier,
						BlobVersion:  &transfer.BlobVersionID,
						BlobTags:     transfer.BlobTags,
						BlobSnapshot: &transfer.BlobSnapshotID,
					}

					if fromTo.To() != common.ELocation.None() && fromTo.To() != common.ELocation.Unknown() {
						tx.Destination = common.GenerateFullPath(destinationRoot, prettyDstRelativePath)
					}

					jsonOutput, err := json.Marshal(tx)
					common.PanicIfErr(err)
					return string(jsonOutput)
				} else {
					// if remove then To() will equal to common.ELocation.Unknown()
					if fromTo.To() == common.ELocation.Unknown() { // remove
						return fmt.Sprintf("DRYRUN: remove %v",
							common.GenerateFullPath(sourceRoot, prettySrcRelativePath))
					}
					if fromTo.To() == common.ELocation.None() { // set-properties
						return fmt.Sprintf("DRYRUN: set-properties %v",
							common.GenerateFullPath(sourceRoot, prettySrcRelativePath))
					} else { // copy for sync
						return fmt.Sprintf("DRYRUN: copy %v to %v",
							common.GenerateFullPath(sourceRoot, prettySrcRelativePath),
							common.GenerateFullPath(destinationRoot, prettyDstRelativePath))
					}
				}
			})
		}

		return common.CopyJobPartOrderResponse{
			JobStarted: true,
		}
	}
}

type DryrunTransfer struct {
	EntityType   common.EntityType
	BlobType     common.BlobType
	FromTo       common.FromTo
	Source       string
	Destination  string
	SourceSize   *int64
	HttpHeaders  blob.HTTPHeaders
	Metadata     common.Metadata
	BlobTier     *blob.AccessTier
	BlobVersion  *string
	BlobTags     common.BlobTags
	BlobSnapshot *string
}

type dryrunTransferSurrogate struct {
	EntityType         string
	BlobType           string
	FromTo             string
	Source             string
	Destination        string
	SourceSize         int64           `json:"SourceSize,omitempty"`
	ContentType        string          `json:"ContentType,omitempty"`
	ContentEncoding    string          `json:"ContentEncoding,omitempty"`
	ContentDisposition string          `json:"ContentDisposition,omitempty"`
	ContentLanguage    string          `json:"ContentLanguage,omitempty"`
	CacheControl       string          `json:"CacheControl,omitempty"`
	ContentMD5         []byte          `json:"ContentMD5,omitempty"`
	BlobTags           common.BlobTags `json:"BlobTags,omitempty"`
	Metadata           common.Metadata `json:"Metadata,omitempty"`
	BlobTier           blob.AccessTier `json:"BlobTier,omitempty"`
	BlobVersion        string          `json:"BlobVersion,omitempty"`
	BlobSnapshotID     string          `json:"BlobSnapshotID,omitempty"`
}

func (d *DryrunTransfer) UnmarshalJSON(bytes []byte) error {
	var surrogate dryrunTransferSurrogate

	err := json.Unmarshal(bytes, &surrogate)
	if err != nil {
		return fmt.Errorf("failed to parse dryrun transfer: %w", err)
	}

	err = d.FromTo.Parse(surrogate.FromTo)
	if err != nil {
		return fmt.Errorf("failed to parse fromto: %w", err)
	}

	err = d.EntityType.Parse(surrogate.EntityType)
	if err != nil {
		return fmt.Errorf("failed to parse entity type: %w", err)
	}

	err = d.BlobType.Parse(surrogate.BlobType)
	if err != nil {
		return fmt.Errorf("failed to parse entity type: %w", err)
	}

	d.Source = surrogate.Source
	d.Destination = surrogate.Destination

	d.SourceSize = &surrogate.SourceSize
	d.HttpHeaders.BlobContentType = &surrogate.ContentType
	d.HttpHeaders.BlobContentEncoding = &surrogate.ContentEncoding
	d.HttpHeaders.BlobCacheControl = &surrogate.CacheControl
	d.HttpHeaders.BlobContentDisposition = &surrogate.ContentDisposition
	d.HttpHeaders.BlobContentLanguage = &surrogate.ContentLanguage
	d.HttpHeaders.BlobContentMD5 = surrogate.ContentMD5
	d.BlobTags = surrogate.BlobTags
	d.Metadata = surrogate.Metadata
	d.BlobTier = &surrogate.BlobTier
	d.BlobVersion = &surrogate.BlobVersion
	d.BlobSnapshot = &surrogate.BlobSnapshotID

	return nil
}

func (d DryrunTransfer) MarshalJSON() ([]byte, error) {
	surrogate := dryrunTransferSurrogate{
		d.EntityType.String(),
		d.BlobType.String(),
		d.FromTo.String(),
		d.Source,
		d.Destination,
		common.IffNotNil(d.SourceSize, 0),
		common.IffNotNil(d.HttpHeaders.BlobContentType, ""),
		common.IffNotNil(d.HttpHeaders.BlobContentEncoding, ""),
		common.IffNotNil(d.HttpHeaders.BlobContentDisposition, ""),
		common.IffNotNil(d.HttpHeaders.BlobContentLanguage, ""),
		common.IffNotNil(d.HttpHeaders.BlobCacheControl, ""),
		d.HttpHeaders.BlobContentMD5,
		d.BlobTags,
		d.Metadata,
		common.IffNotNil(d.BlobTier, ""),
		common.IffNotNil(d.BlobVersion, ""),
		common.IffNotNil(d.BlobSnapshot, ""),
	}

	return json.Marshal(surrogate)
}
