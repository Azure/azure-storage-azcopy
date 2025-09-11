package cmd

import (
	"encoding/json"
	"fmt"

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
