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
	glcm.Dryrun(func(format common.OutputFormat) string {
		if format == common.EOutputFormat.Json() {
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
			glcm.Dryrun(func(format common.OutputFormat) string {
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

				if format == common.EOutputFormat.Json() {
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
							common.GenerateFullPath(sourceRoot, prettyDstRelativePath))
					}
				}
			})
		}

		return common.CopyJobPartOrderResponse{
			JobStarted: true,
		}
	}
}
