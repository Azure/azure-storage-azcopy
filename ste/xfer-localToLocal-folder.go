package ste

import (
	"os"
	"path/filepath"

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
	dst := info.Destination
	//we may create the directory here, or possible there's already a file transfer for the folder that has created it, or maybe it already existed before this job
	if _, err := os.Stat(dst); os.IsNotExist(err) {
		err = os.MkdirAll(filepath.Dir(dst), 0700) // Create the folder
		if err != nil {
			jptm.LogSendError(info.Source, info.Destination, "Could not Create the Folder"+err.Error(), 0)
			jptm.SetStatus(common.ETransferStatus.Failed())
			jptm.ReportTransferDone()
			return
		} else {
			jptm.SetStatus(common.ETransferStatus.Success())
			jptm.ReportTransferDone()
			return
		}
	}
	//path already exists previously or created by a file we are uploading
	jptm.SetStatus(common.ETransferStatus.SkippedEntityAlreadyExists())
	jptm.ReportTransferDone()
	return

}
