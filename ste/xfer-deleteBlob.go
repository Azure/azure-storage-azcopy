package ste

import (
	"errors"
	"fmt"
	"github.com/Azure/azure-sdk-for-go/sdk/azcore"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/blob"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/bloberror"
	"net/http"
	"strings"
	"sync"

	"github.com/Azure/azure-pipeline-go/pipeline"
	"github.com/Azure/azure-storage-azcopy/v10/common"
)

var explainedSkippedRemoveOnce sync.Once

func DeleteBlob(jptm IJobPartTransferMgr, p pipeline.Pipeline, pacer pacer) {

	// If the transfer was cancelled, then reporting transfer as done and increasing the bytestransferred by the size of the source.
	if jptm.WasCanceled() {
		jptm.ReportTransferDone()
		return
	}

	// schedule the work as a chunk, so it will run on the main goroutine pool, instead of the
	// smaller "transfer initiation pool", where this code runs.
	id := common.NewChunkID(jptm.Info().Source, 0, 0)
	cf := createChunkFunc(true, jptm, id, func() { doDeleteBlob(jptm, p) })
	jptm.ScheduleChunks(cf)
}

func doDeleteBlob(jptm IJobPartTransferMgr, p pipeline.Pipeline) {

	info := jptm.Info()
	// Get the source blob url of blob to delete
	blobClient, err := common.CreateBlobClient(info.Source, jptm.CredentialInfo(), jptm.CredentialOpOptions(), jptm.ClientOptions())
	if err != nil {
		jptm.LogError(info.Source, "DELETE ERROR (creating blob client): ", err)
	}

	// Internal function which checks the transfer status and logs the msg respectively.
	// Sets the transfer status and Report Transfer as Done.
	// Internal function is created to avoid redundancy of the above steps from several places in the api.
	transferDone := func(status common.TransferStatus, err error) {
		if status == common.ETransferStatus.Failed() {
			jptm.LogError(info.Source, "DELETE ERROR ", err)
		} else if status == common.ETransferStatus.SkippedBlobHasSnapshots() {
			explainedSkippedRemoveOnce.Do(func() {
				common.GetLifecycleMgr().Info("Blobs with snapshots are skipped. Please specify the --delete-snapshots flag for alternative behaviors.")
			})

			// log at error level so that it's clear why the transfer was skipped even when the log level is set to error
			jptm.Log(pipeline.LogError, fmt.Sprintf("DELETE SKIPPED(blob has snapshots): %s", strings.Split(info.Destination, "?")[0]))
		} else {
			jptm.Log(pipeline.LogInfo, fmt.Sprintf("DELETE SUCCESSFUL: %s", strings.Split(info.Destination, "?")[0]))
		}

		jptm.SetStatus(status)
		jptm.ReportTransferDone()
	}

	// note: if deleteSnapshotsOption is 'only', which means deleting all the snapshots but keep the root blob
	// we still count this delete operation as successful since we accomplished the desired outcome
	_, err = blobClient.Delete(jptm.Context(), &blob.DeleteOptions{
		DeleteSnapshots: jptm.DeleteSnapshotsOption().ToDeleteSnapshotsOptionType(),
		BlobDeleteType: jptm.PermanentDeleteOption().ToPermanentDeleteOptionType(),
	})
	if err != nil {
		var respErr *azcore.ResponseError
		if errors.As(err, &respErr) {
			// if the delete failed with err 404, i.e resource not found, then mark the transfer as success.
			if respErr.StatusCode == http.StatusNotFound {
				transferDone(common.ETransferStatus.Success(), nil)
			}
				return
			}
			// if the delete failed because the blob has snapshots, then skip it
			if respErr.StatusCode == http.StatusConflict && respErr.ErrorCode == string(bloberror.SnapshotsPresent) {
				transferDone(common.ETransferStatus.SkippedBlobHasSnapshots(), nil)
				return
			}
			// If the status code was 403, it means there was an authentication error and we exit.
			// User can resume the job if completely ordered with a new sas.
			if respErr.StatusCode == http.StatusForbidden {
				errMsg := fmt.Sprintf("Authentication Failed. The SAS is not correct or expired or does not have the correct permission %s", err.Error())
				jptm.Log(pipeline.LogError, errMsg)
				common.GetLifecycleMgr().Error(errMsg)
			}
		// in all other cases, make the transfer as failed
		transferDone(common.ETransferStatus.Failed(), err)
	} else {
		transferDone(common.ETransferStatus.Success(), nil)
	}
}
