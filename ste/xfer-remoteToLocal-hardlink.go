package ste

import (
	"fmt"
	"os"

	"github.com/Azure/azure-storage-azcopy/v10/common"
)

func remoteToLocal_hardlink(jptm IJobPartTransferMgr, pacer pacer, df downloaderFactory) {
	info := jptm.Info()

	// Perform initial checks
	// If the transfer was cancelled, then report transfer as done
	if jptm.WasCanceled() {
		/* This is the earliest we detect that jptm was cancelled, before we go to destination */
		jptm.SetStatus(common.ETransferStatus.Cancelled())
		jptm.ReportTransferDone()
		return
	}
	if !jptm.FromTo().IsNFS() || info.HardlinkHandlingType != common.EHardlinkHandlingType.Preserve() {
		jptm.FailActiveSend("Creating hardlink", fmt.Errorf("hardlink creation requires NFS preserve mode"))
		jptm.ReportTransferDone()
		return
	}

	d, err := df(jptm)
	if err != nil {
		jptm.LogDownloadError(info.Source, info.Destination, err.Error(), 0)
		jptm.SetErrorMessage(err.Error())
		jptm.SetStatus(common.ETransferStatus.Failed())
		jptm.ReportTransferDone()
		return
	}
	dl, ok := d.(hardlinkDownloader)
	if !ok {
		message := "downloader implementation does not support hardlinks"
		jptm.LogDownloadError(info.Source, info.Destination, message, 0)
		jptm.SetErrorMessage(message)
		jptm.SetStatus(common.ETransferStatus.Failed())
		jptm.ReportTransferDone()
		return
	}
	// Establish support and initialize the downloader before unlinking anything.
	d.Prologue(jptm)
	// if the force Write flags is set to false or prompt
	// then check the file exists at the remote location
	// if it does, react accordingly
	if jptm.GetOverwriteOption() != common.EOverwriteOption.True() {
		dstProps, err := os.Lstat(info.Destination)
		if err != nil && !os.IsNotExist(err) {
			jptm.FailActiveSend("checking existing hardlink destination", err)
			jptm.ReportTransferDone()
			return
		}
		if err == nil {
			if dstProps.IsDir() {
				jptm.FailActiveSend("replacing hardlink destination", fmt.Errorf("destination is a directory"))
				jptm.ReportTransferDone()
				return
			}
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
				jptm.LogAtLevelForCurrentTransfer(common.LogWarning, "Unlinking the destination path before preserving the source hardlink group")
				err = os.Remove(info.Destination)
				if err != nil && !os.IsNotExist(err) { // should not get back a non-existent error, but if we do, it's not a bad thing.
					jptm.FailActiveSend("deleting old file", err)
					jptm.ReportTransferDone()
					return
				}
			}
		}
	} else {
		if props, statErr := os.Lstat(info.Destination); statErr == nil {
			if props.IsDir() {
				jptm.FailActiveSend("replacing hardlink destination", fmt.Errorf("destination is a directory"))
				jptm.ReportTransferDone()
				return
			}
			jptm.LogAtLevelForCurrentTransfer(common.LogWarning, "Unlinking the destination path before preserving the source hardlink group")
		} else if !os.IsNotExist(statErr) {
			jptm.FailActiveSend("checking existing hardlink destination", statErr)
			jptm.ReportTransferDone()
			return
		}
		err := os.Remove(info.Destination)
		if err != nil && !os.IsNotExist(err) { // it's OK to fail because it doesn't exist.
			jptm.FailActiveSend("deleting old file", err)
			jptm.ReportTransferDone()
			return
		}
	}

	err = dl.CreateHardlink()
	if err != nil {
		jptm.FailActiveSend("creating destination hardlink", err)
	}

	commonDownloaderCompletion(jptm, info, common.EEntityType.Hardlink())
}
