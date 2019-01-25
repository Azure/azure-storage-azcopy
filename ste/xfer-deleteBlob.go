package ste

import (
	"net/http"
	"net/url"

	"fmt"

	"github.com/Azure/azure-pipeline-go/pipeline"
	"github.com/Azure/azure-storage-azcopy/common"
	"github.com/jiacfan/azure-storage-blob-go/azblob"
)

func DeleteBlobPrologue(jptm IJobPartTransferMgr, p pipeline.Pipeline, pacer *pacer) {

	info := jptm.Info()
	// Get the source blob url of blob to delete
	u, _ := url.Parse(info.Source)

	srcBlobURL := azblob.NewBlobURL(*u, p)

	// If the transfer was cancelled, then reporting transfer as done and increasing the bytestransferred by the size of the source.
	if jptm.WasCanceled() {
		jptm.ReportTransferDone()
		return
	}

	// Internal function which checks the transfer status and logs the msg respectively.
	// Sets the transfer status and Report Transfer as Done.
	// Internal function is created to avoid redundancy of the above steps from several places in the api.
	transferDone := func(status common.TransferStatus, err error) {
		if jptm.ShouldLog(pipeline.LogInfo) {
			if status == common.ETransferStatus.Failed() {
				jptm.LogError(info.Source, "DELETE ERROR ", err)
			} else {
				if jptm.ShouldLog(pipeline.LogInfo) {
					jptm.Log(pipeline.LogInfo, "DELETE SUCCESSFUL")
				}
			}
		}
		jptm.SetStatus(status)
		jptm.ReportTransferDone()
	}

	_, err := srcBlobURL.Delete(jptm.Context(), azblob.DeleteSnapshotsOptionNone, azblob.BlobAccessConditions{})
	if err != nil {
		// If the delete failed with err 404, i.e resource not found, then mark the transfer as success.
		if strErr, ok := err.(azblob.StorageError); ok {
			if strErr.Response().StatusCode == http.StatusNotFound {
				transferDone(common.ETransferStatus.Success(), nil)
			}
			// If the status code was 403, it means there was an authentication error and we exit.
			// User can resume the job if completely ordered with a new sas.
			if strErr.Response().StatusCode == http.StatusForbidden {
				common.GetLifecycleMgr().Exit(fmt.Sprintf("Authentication Failed. The SAS is not correct or expired or does not have the correct permission %s", err.Error()), 1)
			}
		} else {
			transferDone(common.ETransferStatus.Failed(), err)
		}
	} else {
		transferDone(common.ETransferStatus.Success(), nil)
	}
}
