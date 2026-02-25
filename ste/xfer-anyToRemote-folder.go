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
	"github.com/Azure/azure-storage-azcopy/v10/pacer"
)

// anyToRemote_folder handles all kinds of sender operations for FOLDERS - both uploads from local files, and S2S copies
func anyToRemote_folder(jptm IJobPartTransferMgr, info *TransferInfo, pacer pacer.Interface, senderFactory senderFactory, sipf sourceInfoProviderFactory) {

	// step 1. perform initial checks
	if jptm.WasCanceled() {
		/* This is earliest we detect that jptm has been cancelled before we reach destination */
		jptm.SetStatus(common.ETransferStatus.Cancelled())
		jptm.ReportTransferDone()
		return
	}

	// step 2a. Create sender
	srcInfoProvider, err := sipf(jptm)
	if err != nil {
		jptm.LogSendError(info.Source, info.Destination, err.Error(), 0)
		jptm.SetStatus(common.ETransferStatus.Failed())
		jptm.ReportTransferDone()
		return
	}
	if srcInfoProvider.EntityType() != common.EEntityType.Folder() {
		panic("configuration error. Source Info Provider does not have Folder entity type")
	}

	baseSender, err := senderFactory(jptm, info.Destination, pacer, srcInfoProvider)
	if err != nil {
		jptm.LogSendError(info.Source, info.Destination, err.Error(), 0)
		jptm.SetStatus(common.ETransferStatus.Failed())
		jptm.ReportTransferDone()
		return
	}
	s, ok := baseSender.(folderSender)
	if !ok {
		jptm.LogSendError(info.Source, info.Destination, "sender implementation does not support folders", 0)
		jptm.SetStatus(common.ETransferStatus.Failed())
		jptm.ReportTransferDone()
		return
	}

	// No chunks to schedule. Just run the folder handling operations.
	// There are no checks for folders on LMT's changing while we read them. We need that for files,
	// so we don't use and out-dated size to plan chunks, or read a mix of old and new data, but neither
	// of those issues apply to folders.
	err = s.EnsureFolderExists() // we may create it here, or possible there's already a file transfer for the folder that has created it, or maybe it already existed before this job
	if err != nil {
		switch err {
		case folderPropertiesSetInCreation{}:
			// Continue to standard completion.
		case folderPropertiesNotOverwroteInCreation{}:
			jptm.LogAtLevelForCurrentTransfer(common.LogWarning, "Folder already exists, so due to the --overwrite option, its properties won't be set")
			jptm.SetStatus(common.ETransferStatus.SkippedEntityAlreadyExists()) // using same status for both files and folders, for simplicity
			jptm.ReportTransferDone()
			return
		default:
			jptm.FailActiveSend("ensuring destination folder exists", err)
		}
	} else {

		t := jptm.GetFolderCreationTracker()
		defer t.StopTracking(s.DirUrlToString()) // don't need it after this routine
		shouldSetProps := t.ShouldSetProperties(s.DirUrlToString(), jptm.GetOverwriteOption(), jptm.GetOverwritePrompter())
		if !shouldSetProps {
			jptm.LogAtLevelForCurrentTransfer(common.LogWarning, "Folder already exists, so due to the --overwrite option, its properties won't be set")
			jptm.SetStatus(common.ETransferStatus.SkippedEntityAlreadyExists()) // using same status for both files and folders, for simplicity
			jptm.ReportTransferDone()
			return
		}

		err = s.SetFolderProperties()
		if err != nil {
			jptm.FailActiveSend("setting folder properties", err)
		}
	}

	commonSenderCompletion(jptm, baseSender, info) // for consistency, always run the standard epilogue
}
