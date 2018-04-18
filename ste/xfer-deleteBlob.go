package ste

import (
	"net/url"
	"github.com/Azure/azure-storage-azcopy/common"
	"fmt"
	"github.com/Azure/azure-pipeline-go/pipeline"
	"github.com/Azure/azure-storage-blob-go/2017-07-29/azblob"
	"io"
	"io/ioutil"
)

func DeleteBlobPrologue(jptm IJobPartTransferMgr, p pipeline.Pipeline, pacer *pacer) {

	info := jptm.Info()
	// Get the source blob url of blob to dele
	u, _ := url.Parse(info.Source)
	srcBlobURL := azblob.NewBlobURL(*u, p)

	// If the transfer was cancelled, then reporting transfer as done and increasing the bytestransferred by the size of the source.
	if jptm.WasCanceled(){
		jptm.AddToBytesTransferred(info.SourceSize)
		jptm.ReportTransferDone()
		return
	}
	deleteResp, err := srcBlobURL.Delete(jptm.Context(), azblob.DeleteSnapshotsOptionNone, azblob.BlobAccessConditions{})
	if err != nil{
		// If there was error deleting the blob, mark the transfer as failed.
		if jptm.ShouldLog(pipeline.LogInfo) {
			jptm.Log(pipeline.LogInfo, fmt.Sprintf("error deleting the blob. Failed with err %s", err.Error()))
		}
		jptm.SetStatus(common.ETransferStatus.Failed())
	}else{
		// If the delete was successful, close the resp body and mark transfer as success.
		if jptm.ShouldLog(pipeline.LogInfo) {
			jptm.Log(pipeline.LogInfo, "successfully deleted the blob.")
		}
		if deleteResp.Response() != nil{
			io.Copy(ioutil.Discard, deleteResp.Response().Body)
			deleteResp.Response().Body.Close()
		}
		jptm.SetStatus(common.ETransferStatus.Success())
	}
	jptm.AddToBytesTransferred(info.SourceSize)
	jptm.ReportTransferDone()
}