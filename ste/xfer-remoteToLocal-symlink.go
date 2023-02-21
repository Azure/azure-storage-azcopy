package ste

import (
	"github.com/Azure/azure-pipeline-go/pipeline"
	"github.com/Azure/azure-storage-azcopy/v10/common"
)

func remoteToLocal_symlink(jptm IJobPartTransferMgr, p pipeline.Pipeline, pacer pacer, df downloaderFactory) {
	info := jptm.Info()

	// Perform initial checks
	// If the transfer was cancelled, then report transfer as done
	if jptm.WasCanceled() {
		/* This is the earliest we detect that jptm was cancelled, before we go to destination */
		jptm.SetStatus(common.ETransferStatus.Cancelled())
		jptm.ReportTransferDone()
		return
	}

	dl, ok := df().(symlinkDownloader)
	if !ok {
		jptm.LogDownloadError(info.Source, info.Destination, "downloader implementation does not support symlinks", 0)
		jptm.SetStatus(common.ETransferStatus.Failed())
		jptm.ReportTransferDone()
		return
	}

	err := dl.CreateSymlink(jptm)
	if err != nil {
		jptm.FailActiveSend("creating destination symlink", err)
	}

	commonDownloaderCompletion(jptm, info, common.EEntityType.Symlink())
}
