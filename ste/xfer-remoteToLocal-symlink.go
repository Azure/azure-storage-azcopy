package ste

import (
	"github.com/Azure/azure-storage-azcopy/v10/common"
	"os"
)

func remoteToLocal_symlink(jptm IJobPartTransferMgr, df downloaderFactory) {
	info := jptm.Info()

	// Perform initial checks
	// If the transfer was cancelled, then report transfer as done
	if jptm.WasCanceled() {
		/* This is the earliest we detect that jptm was cancelled, before we go to destination */
		jptm.SetStatus(common.ETransferStatus.Cancelled())
		jptm.ReportTransferDone()
		return
	}
	// if the force Write flags is set to false or prompt
	// then check the file exists at the remote location
	// if it does, react accordingly
	if jptm.GetOverwriteOption() != common.EOverwriteOption.True() {
		dstProps, err := common.OSStat(info.Destination)
		if err == nil {
			// if the error is nil, then file exists locally
			shouldOverwrite := false

			// if necessary, prompt to confirm user's intent
			if jptm.GetOverwriteOption() == common.EOverwriteOption.Prompt() {
				shouldOverwrite = jptm.GetOverwritePrompter().ShouldOverwrite(info.Destination, common.EEntityType.File())
			} else if jptm.GetOverwriteOption() == common.EOverwriteOption.IfSourceNewer() {
				// only overwrite if source lmt is newer (after) the destination
				if jptm.LastModifiedTime().After(dstProps.ModTime()) {
					shouldOverwrite = true
				}
			}

			if !shouldOverwrite {
				// logging as Warning so that it turns up even in compact logs, and because previously we use Error here
				jptm.LogAtLevelForCurrentTransfer(common.LogWarning, "File already exists, so will be skipped")
				jptm.SetStatus(common.ETransferStatus.SkippedEntityAlreadyExists())
				jptm.ReportTransferDone()
				return
			} else {
				err = os.Remove(info.Destination)
				if err != nil && !os.IsNotExist(err) { // should not get back a non-existent error, but if we do, it's not a bad thing.
					jptm.FailActiveSend("deleting old file", err)
					jptm.ReportTransferDone()
					return
				}
			}
		}
	} else {
		err := os.Remove(info.Destination)
		if err != nil && !os.IsNotExist(err) { // it's OK to fail because it doesn't exist.
			jptm.FailActiveSend("deleting old file", err)
			jptm.ReportTransferDone()
			return
		}
	}

	d, err := df(jptm)
	if err != nil {
		jptm.LogDownloadError(info.Source, info.Destination, err.Error(), 0)
		jptm.SetStatus(common.ETransferStatus.Failed())
		jptm.ReportTransferDone()
		return
	}

	dl, ok := d.(symlinkDownloader)
	if !ok {
		jptm.LogDownloadError(info.Source, info.Destination, "downloader implementation does not support symlinks", 0)
		jptm.SetStatus(common.ETransferStatus.Failed())
		jptm.ReportTransferDone()
		return
	}

	err = dl.CreateSymlink(jptm)
	if err != nil {
		jptm.FailActiveSend("creating destination symlink", err)
	}

	commonDownloaderCompletion(jptm, info, common.EEntityType.Symlink())
}
