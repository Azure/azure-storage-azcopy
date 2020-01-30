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

// general-purpose "any remote persistence location" to local, for folders
func remoteToLocal_folder(jptm IJobPartTransferMgr, p pipeline.Pipeline, pacer pacer) {

	info := jptm.Info()

	// Perform initial checks
	// If the transfer was cancelled, then report transfer as done
	if jptm.WasCanceled() {
		jptm.ReportTransferDone()
		return
	}

	/* todo
	// if the force Write flags is set to false or prompt
	// then check the file exists at the remote location
	// if it does, react accordingly
	if jptm.GetOverwriteOption() != common.EOverwriteOption.True() {
		_, err := os.Stat(info.Destination)
		if err == nil {
			// if the error is nil, then file exists locally
			shouldOverwrite := false

			// if necessary, prompt to confirm user's intent
			if jptm.GetOverwriteOption() == common.EOverwriteOption.Prompt() {
				shouldOverwrite = jptm.GetOverwritePrompter().shouldOverwrite(info.Destination)
			}

			if !shouldOverwrite {
				// logging as Warning so that it turns up even in compact logs, and because previously we use Error here
				jptm.LogAtLevelForCurrentTransfer(pipeline.LogWarning, "File already exists, so will be skipped")
				jptm.SetStatus(common.ETransferStatus.SkippedEntityAlreadyExists())
				jptm.ReportTransferDone()
				return
			}
		}
	}*/

	// no chunks to schedule. Just run the folder handling operations
	err := common.CreateDirectoryIfNotExist(info.Destination)
	if err != nil {
		jptm.FailActiveSend("ensuring destination folder exists", err)
	} else {
		// TODO: in the later PR (to come) about actually transferring properties
		//    get the properties from somewhere (a source info provider, presumably?)
		// and set them
		//
		//		if err != nil {
		//			jptm.FailActiveSend("setting folder properties", err)
		//}
	}
	commonDownloaderCompletion(jptm, info, common.EEntityType.Folder()) // for consistency, always run the standard epilogue

}
