package ste

import (
	"github.com/Azure/azure-storage-azcopy/v10/common"
)

func localToLocal_folder(jptm IJobPartTransferMgr) {
	if jptm.WasCanceled() {
		/* This is the earliest we detect jptm has been cancelled before scheduling chunks */
		jptm.SetStatus(common.ETransferStatus.Cancelled())
		jptm.ReportTransferDone()
		return
	}
	info := jptm.Info()

	// no chunks to schedule. Just run the folder handling operations
	t := jptm.GetFolderCreationTracker()
	defer t.StopTracking(info.Destination) // don't need it after this routine

	err := common.CreateDirectoryIfNotExist(info.Destination, t) // we may create it here, or possible there's already a file transfer for the folder that has created it, or maybe it already existed before this job
	if err != nil {
		jptm.FailActiveDownload("ensuring destination folder exists", err)
	} else {
		shouldSetProps := t.ShouldSetProperties(info.Destination, jptm.GetOverwriteOption(), jptm.GetOverwritePrompter())
		if !shouldSetProps {
			jptm.SetStatus(common.ETransferStatus.SkippedEntityAlreadyExists()) // using same status for both files and folders, for simplicity
			jptm.ReportTransferDone()
			return
		}
		//TODO: Set the folder properties
		//err = dl.SetFolderProperties(jptm)
		if err != nil {
			jptm.FailActiveDownload("setting folder properties", err)
		}
	}
	commonDownloaderCompletion(jptm, info, common.EEntityType.Folder()) // for consistency, always run the standard epilogue
	//There is no mutual Exclusion in the below code for creating the directories
	//we may create the directory here, or possible there's already a file transfer for the folder that has created it, or maybe it already existed before this job
	// if _, err := os.Stat(dst); os.IsNotExist(err) {
	// 	err = os.MkdirAll(filepath.Dir(dst), 0700) // Create the folder
	// 	if err != nil {
	// 		jptm.LogSendError(info.Source, info.Destination, "Could not Create the Folder"+err.Error(), 0)
	// 		jptm.SetStatus(common.ETransferStatus.Failed())
	// 		jptm.ReportTransferDone()
	// 		return
	// 	} else {
	// 		jptm.SetStatus(common.ETransferStatus.Success())
	// 		jptm.ReportTransferDone()
	// 		return
	// 	}
	// }
	// //path already exists previously or created by a file we are uploading
	// jptm.SetStatus(common.ETransferStatus.SkippedEntityAlreadyExists())
	// jptm.ReportTransferDone()
	// return

}
