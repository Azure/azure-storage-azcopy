package ste

import (
	"fmt"
	"net/http"
	"net/url"

	"github.com/Azure/azure-pipeline-go/pipeline"
	"github.com/Azure/azure-storage-azcopy/common"
	"github.com/Azure/azure-storage-blob-go/2018-03-28/azblob"
)

func DeleteBlobPrologue(jptm IJobPartTransferMgr, p pipeline.Pipeline, pacer *pacer) {

	info := jptm.Info()
	// Get the source blob url of blob to delete
	u, _ := url.Parse(info.Source)
	if len(u.RawQuery) > 0 {
		u.RawQuery += "&" + info.SourceSAS
	}else{
		u.RawQuery = info.SourceSAS
	}
	srcBlobURL := azblob.NewBlobURL(*u, p)

	// If the transfer was cancelled, then reporting transfer as done and increasing the bytestransferred by the size of the source.
	if jptm.WasCanceled() {
		jptm.AddToBytesDone(info.SourceSize)
		jptm.ReportTransferDone()
		return
	}

	// Internal function which checks the transfer status and logs the msg respectively.
	// Sets the transfer status and Report Transfer as Done.
	// Internal function is created to avoid redundancy of the above steps from several places in the api.
	transferDone := func(status common.TransferStatus, err error) {
		if jptm.ShouldLog(pipeline.LogInfo) {
			var msg = ""
			if status == common.ETransferStatus.Failed() {
				msg = fmt.Sprintf("delete blob failed. Failed with error %s", err.Error())
			} else {
				msg = "blob delete successful"
			}
			jptm.Log(pipeline.LogInfo, msg)
		}
		jptm.SetStatus(status)
		jptm.AddToBytesDone(info.SourceSize)
		jptm.ReportTransferDone()
	}

	_, err := srcBlobURL.Delete(jptm.Context(), azblob.DeleteSnapshotsOptionNone, azblob.BlobAccessConditions{})
	if err != nil {
		// If the delete failed with err 404, i.e resource not found, then mark the transfer as success.
		if err.(azblob.StorageError) != nil {
			if err.(azblob.StorageError).Response().StatusCode == http.StatusNotFound {
				transferDone(common.ETransferStatus.Success(), nil)
			}
		} else {
			transferDone(common.ETransferStatus.Failed(), err)
		}
	} else {
		transferDone(common.ETransferStatus.Success(), nil)
	}
}
