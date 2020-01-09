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
	"github.com/Azure/azure-pipeline-go/pipeline"
	"github.com/Azure/azure-storage-azcopy/common"
)

// anyToRemote_folder handles all kinds of sender operations for FOLDERs - both uploads from local files, and S2S copies
func anyToRemote_folder(jptm IJobPartTransferMgr, info TransferInfo, p pipeline.Pipeline, pacer pacer, senderFactory senderFactory, sipf sourceInfoProviderFactory) {

	// step 1. perform initial checks
	if jptm.WasCanceled() {
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

	sBase, err := senderFactory(jptm, info.Destination, p, pacer, srcInfoProvider)
	if err != nil {
		jptm.LogSendError(info.Source, info.Destination, err.Error(), 0)
		jptm.SetStatus(common.ETransferStatus.Failed())
		jptm.ReportTransferDone()
		return
	}
	s := sBase.(IFolderSender)

	/*TODO
	// step 3: check overwrite option
	// if the force Write flags is set to false or prompt
	// then check the file exists at the remote location
	// if it does, react accordingly
	if jptm.GetOverwriteOption() != common.EOverwriteOption.True() {
		TODO
		exists, existenceErr := s.RemoteFolderExists()
		if existenceErr != nil {
			jptm.LogSendError(info.Source, info.Destination, "Could not check existence at destination. "+existenceErr.Error(), 0)
			jptm.SetStatus(common.ETransferStatus.Failed()) // is a real failure, not just a SkippedFileAlreadyExists, in this case
			jptm.ReportTransferDone()
			return
		}
		if exists {
			shouldOverwrite := false
			var shouldLogSkipMessage bool
			var skipMessage string

			if info.IsFolderPropertiesTransfer() {
				// For folders, OverwriteOption = "prompt" is treated the same as OverwriteOption = "false"
				// This is because "prompt" just gets too confusing for users if it can apply to two different things (files and folder properties)
				skipMessage = "Folder already exists, so its properties won't be set"
				shouldLogSkipMessage = true // TODO: suppress the skip message if preserve-ntfs-atttributes and preserve-ntfs-acls are both false, since there's no point in saying we're skipping something there, when there's nothing to actually
			} else {
				// if necessary, prompt to confirm user's intent
				if jptm.GetOverwriteOption() == common.EOverwriteOption.Prompt() {
					// remove the SAS before prompting the user
					parsed, _ := url.Parse(info.Destination)
					parsed.RawQuery = ""
					shouldOverwrite = jptm.GetOverwritePrompter().shouldOverwrite(parsed.String())
				}
				shouldLogSkipMessage = !shouldOverwrite
				skipMessage = "File already exists, so will be skipped"
			}

			if shouldLogSkipMessage {
				// logging as Warning so that it turns up even in compact logs, and because previously we use Error here
				jptm.LogAtLevelForCurrentTransfer(pipeline.LogWarning, skipMessage)
			}
			if !shouldOverwrite {
				jptm.SetStatus(common.ETransferStatus.SkippedEntityAlreadyExists()) // using same status for both files and folders, for simplicty
				jptm.ReportTransferDone()
				return
			}
		}
	}
	*/

	// no chunks to schedule. Just run the folder handling operations
	err = s.EnsureFolderExists()
	if err != nil {
		jptm.FailActiveSend("ensuring destination folder exists", err)
	} else {
		err = s.SetFolderProperties()
		if err != nil {
			jptm.FailActiveSend("setting folder properties", err)
		}
	}
	commonSenderCompletion(jptm, s, info) // for consistency, always run the standard epilogue
}
