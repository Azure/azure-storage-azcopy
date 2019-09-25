package ste

import (
	"fmt"
	"net/http"
	"net/url"
	"strings"

	"github.com/Azure/azure-pipeline-go/pipeline"
	"github.com/Azure/azure-storage-azcopy/common"
	"github.com/Azure/azure-storage-file-go/azfile"
)

func DeleteFilePrologue(jptm IJobPartTransferMgr, p pipeline.Pipeline, pacer pacer) {

	info := jptm.Info()
	// Get the source file url of file to delete
	u, _ := url.Parse(info.Source)

	srcFileUrl := azfile.NewFileURL(*u, p)

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
					jptm.Log(pipeline.LogInfo, fmt.Sprintf("DELETE SUCCESSFUL: %s", strings.Split(info.Destination, "?")[0]))
				}
			}
		}
		jptm.SetStatus(status)
		jptm.ReportTransferDone()
	}

	// Delete the source file
	_, err := srcFileUrl.Delete(jptm.Context())
	if err != nil {
		// If the delete failed with err 404, i.e resource not found, then mark the transfer as success.
		if strErr, ok := err.(azfile.StorageError); ok {
			if strErr.Response().StatusCode == http.StatusNotFound {
				transferDone(common.ETransferStatus.Success(), nil)
				return
			}
			// If the status code was 403, it means there was an authentication error and we exit.
			// User can resume the job if completely ordered with a new sas.
			if strErr.Response().StatusCode == http.StatusForbidden {
				errMsg := fmt.Sprintf("Authentication Failed. The SAS is not correct or expired or does not have the correct permission %s", err.Error())
				jptm.Log(pipeline.LogError, errMsg)
				common.GetLifecycleMgr().Error(errMsg)
			}
		}
		transferDone(common.ETransferStatus.Failed(), err)
	} else {
		transferDone(common.ETransferStatus.Success(), nil)
	}
}
