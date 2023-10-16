// Copyright © 2017 Microsoft <wastore@microsoft.com>
//
// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files (the "Software"), to deal
// in the Software without restriction, including without limitation the rights
// to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
// copies of the Software, and to permit persons to whom the Software is
// furnished to do so, subject to the following conditions:
//
// The above copyright notice and this permission notice shall be included in
// all copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
// THE SOFTWARE.

package ste

import (
	"github.com/Azure/azure-storage-azcopy/v10/common"
)

// general-purpose "any remote persistence location" to local, for folders
func remoteToLocal_folder(jptm IJobPartTransferMgr, pacer pacer, df downloaderFactory) {

	info := jptm.Info()

	// Perform initial checks
	// If the transfer was cancelled, then report transfer as done
	if jptm.WasCanceled() {
		/* This is the earliest we detect that jptm was cancelled, before we go to destination */
		jptm.SetStatus(common.ETransferStatus.Cancelled())
		jptm.ReportTransferDone()
		return
	}

	d, err := df(jptm)
	if err != nil {
		jptm.LogDownloadError(info.Source, info.Destination, "failed to create downloader", 0)
		jptm.SetStatus(common.ETransferStatus.Failed())
		jptm.ReportTransferDone()
		return
	}

	dl, ok := d.(folderDownloader)
	if !ok {
		jptm.LogDownloadError(info.Source, info.Destination, "downloader implementation does not support folders", 0)
		jptm.SetStatus(common.ETransferStatus.Failed())
		jptm.ReportTransferDone()
		return
	}

	// no chunks to schedule. Just run the folder handling operations
	t := jptm.GetFolderCreationTracker()
	defer t.StopTracking(info.Destination) // don't need it after this routine

	err = common.CreateDirectoryIfNotExist(info.Destination, t) // we may create it here, or possible there's already a file transfer for the folder that has created it, or maybe it already existed before this job
	if err != nil {
		jptm.FailActiveDownload("ensuring destination folder exists", err)
	} else {
		shouldSetProps := t.ShouldSetProperties(info.Destination, jptm.GetOverwriteOption(), jptm.GetOverwritePrompter())
		if !shouldSetProps {
			jptm.LogAtLevelForCurrentTransfer(common.LogWarning, "Folder already exists, so due to the --overwrite option, its properties won't be set")
			jptm.SetStatus(common.ETransferStatus.SkippedEntityAlreadyExists()) // using same status for both files and folders, for simplicity
			jptm.ReportTransferDone()
			return
		}

		err = dl.SetFolderProperties(jptm)
		if err != nil {
			jptm.FailActiveDownload("setting folder properties", err)
		}
	}
	commonDownloaderCompletion(jptm, info, common.EEntityType.Folder()) // for consistency, always run the standard epilogue

}
