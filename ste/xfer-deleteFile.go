package ste

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"strings"

	"github.com/Azure/azure-sdk-for-go/sdk/azcore"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azfile/file"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azfile/fileerror"

	"github.com/Azure/azure-storage-azcopy/v10/common"
)

func DeleteFile(jptm IJobPartTransferMgr, _ pacer) {

	// If the transfer was cancelled, then reporting transfer as done and increasing the bytestransferred by the size of the source.
	if jptm.WasCanceled() {
		jptm.ReportTransferDone()
		return
	}

	info := jptm.Info()
	source := info.Source
	srcURL, _ := url.Parse(source)

	// Register existence with the deletion manager. Do it now, before we make the chunk funcs,
	// to maximize the extent to which the manager knows about as many children as possible (i.e.
	// as much of the plan files as we have seen so far)
	// That minimizes case where the count of known children drops to zero (due simply to us
	// having not registered all of them yet); and the manager attempts a failed deletion;
	// and then we find more children in the plan files. Such failed attempts are harmless, but cause
	// unnecessary network round trips.
	// We must do this for all entity types, because even folders are children of their parents
	jptm.FolderDeletionManager().RecordChildExists(srcURL)

	if info.EntityType == common.EEntityType.Folder() {
		fileURLParts, _ := file.ParseURL(source)
		isFileShareRoot := fileURLParts.DirectoryOrFilePath == ""
		if !isFileShareRoot {
			jptm.LogAtLevelForCurrentTransfer(common.LogInfo, "Queuing folder, to be deleted after it's children are deleted")
			jptm.FolderDeletionManager().RequestDeletion(
				srcURL,
				func(ctx context.Context, logger common.ILogger) bool {
					return doDeleteFolder(ctx, source, jptm, logger)
				},
			)
		}
		// After requesting deletion, we have no choice but to report this as "done", because we are
		// in a transfer initiation func, and can't just block here for ages until the deletion actually happens.
		// Besides, we have made the decision that if the queued deletion fails, that's NOT a
		// job failure. (E.g. could happen because someone else dropped a new file
		// in there after we enumerated). Since the deferred action (by this definition)
		// will never fail, it's correct to report success here.
		jptm.SetStatus(common.ETransferStatus.Success())
		jptm.ReportTransferDone()

	} else {
		// schedule the work as a chunk, so it will run on the main goroutine pool, instead of the
		// smaller "transfer initiation pool", where this code runs.
		id := common.NewChunkID(source, 0, 0)
		cf := createChunkFunc(true, jptm, id, func() { doDeleteFile(jptm) })
		jptm.ScheduleChunks(cf)
	}
}

func doDeleteFile(jptm IJobPartTransferMgr) {
	info := jptm.Info()
	source := info.Source
	srcURL, _ := url.Parse(source)

	// Internal function which checks the transfer status and logs the msg respectively.
	// Sets the transfer status and Report Transfer as Done.
	// Internal function is created to avoid redundancy of the above steps from several places in the api.
	transferDone := func(status common.TransferStatus, err error) {
		if status == common.ETransferStatus.Success() {
			jptm.FolderDeletionManager().RecordChildDeleted(srcURL)
			// TODO: doing this only on success raises the possibility of the
			//   FolderDeletionManager's internal map growing rather large if there are lots of failures
			//   on a big folder tree. Is living with that preferable to the "incorrectness" of calling
			//   RecordChildDeleted when it wasn't actually deleted.  Yes, probably.  But think about it a bit more.
			//	 We'll favor correctness over memory-efficiency for now, and leave the code as it is.
			//   If we find that memory usage is an issue in cases with lots of failures, we can revisit in the future.
		}
		if jptm.ShouldLog(common.LogInfo) {
			if status == common.ETransferStatus.Failed() {
				jptm.LogError(info.Source, "DELETE ERROR ", err)
			} else {
				if jptm.ShouldLog(common.LogInfo) {
					jptm.Log(common.LogInfo, fmt.Sprintf("DELETE SUCCESSFUL: %s", strings.Split(info.Destination, "?")[0]))
				}
			}
		}
		jptm.SetStatus(status)
		jptm.ReportTransferDone()
	}

	s, err := jptm.SrcServiceClient().FileServiceClient()
	if err != nil {
		transferDone(common.ETransferStatus.Failed(), err)
		return
	}
	srcFileClient := s.NewShareClient(jptm.Info().SrcContainer).NewRootDirectoryClient().NewFileClient(jptm.Info().SrcFilePath)

	// Delete the source file
	err = common.DoWithOverrideReadOnlyOnAzureFiles(jptm.Context(),
		func() (interface{}, error) { return srcFileClient.Delete(jptm.Context(), nil) },
		srcFileClient,
		jptm.GetForceIfReadOnly())
	if err != nil {
		var respErr *azcore.ResponseError
		if errors.As(err, &respErr) {
			// If the delete failed with err 404, i.e resource not found, then mark the transfer as success.
			if respErr.StatusCode == http.StatusNotFound {
				transferDone(common.ETransferStatus.Success(), nil)
				return
			}
			// If the status code was 403, it means there was an authentication error and we exit.
			// User can resume the job if completely ordered with a new sas.
			if respErr.StatusCode == http.StatusForbidden {
				errMsg := fmt.Sprintf("Authentication Failed. The SAS is not correct or expired or does not have the correct permission %s", err.Error())
				jptm.Log(common.LogError, errMsg)
				jptm.GetJobErrorHandler()(errMsg)
			}
		}
		transferDone(common.ETransferStatus.Failed(), err)
	} else {
		transferDone(common.ETransferStatus.Success(), nil)
	}
}

func doDeleteFolder(ctx context.Context, folder string, jptm IJobPartTransferMgr, logger common.ILogger) bool {
	fileURLParts, err := file.ParseURL(folder)
	if err != nil {
		return false
	}

	s, err := jptm.SrcServiceClient().FileServiceClient()
	if err != nil {
		return false
	}

	srcDirClient := s.NewShareClient(jptm.Info().SrcContainer).NewDirectoryClient(jptm.Info().SrcFilePath)
	loggableName := fileURLParts.DirectoryOrFilePath
	logger.Log(common.LogDebug, "About to attempt to delete folder "+loggableName)

	err = common.DoWithOverrideReadOnlyOnAzureFiles(ctx,
		func() (interface{}, error) { return srcDirClient.Delete(ctx, nil) },
		srcDirClient,
		jptm.GetForceIfReadOnly())
	if err == nil {
		logger.Log(common.LogInfo, "Empty folder deleted "+loggableName) // not using capitalized DELETE SUCCESSFUL here because we can't use DELETE ERROR for folder delete failures (since there may be a retry if we delete more files, but we don't know that at time of logging)
		return true
	}
	var respErr *azcore.ResponseError
	if errors.As(err, &respErr) {
		// If the delete failed with err 404, i.e resource not found, then consider the deletion a success. (It's already gone)
		if respErr.StatusCode == http.StatusNotFound {
			logger.Log(common.LogDebug, "Folder already gone before call to delete "+loggableName)
			return true
		}
		if fileerror.HasCode(err, fileerror.DirectoryNotEmpty) {
			logger.Log(common.LogInfo, "Folder not deleted because it's not empty yet. Will retry if this job deletes more files from it. Folder name: "+loggableName)
			return false
		}
	}
	logger.Log(common.LogInfo,
		fmt.Sprintf("Folder not deleted due to error. Will retry if this job deletes more files from it. Folder name: %s Error: %s", loggableName, err),
	)
	return false
}
