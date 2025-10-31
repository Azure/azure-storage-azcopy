// Copyright © 2025 Microsoft <azcopydev@microsoft.com>
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
	"fmt"
	"net/url"

	"github.com/Azure/azure-storage-azcopy/v10/common"
)

func anyToRemote_symlink(jptm IJobPartTransferMgr, info *TransferInfo, pacer pacer, senderFactory senderFactory, sipf sourceInfoProviderFactory) {
	// Check if cancelled
	if jptm.WasCanceled() {
		/* This is earliest we detect that jptm has been cancelled before we reach destination */
		jptm.SetStatus(common.ETransferStatus.Cancelled())
		jptm.ReportTransferDone()
		return
	}

	// Create SIP
	srcInfoProvider, err := sipf(jptm)
	if err != nil {
		jptm.LogSendError(info.Source, info.Destination, err.Error(), 0)
		jptm.SetStatus(common.ETransferStatus.Failed())
		jptm.ReportTransferDone()
		return
	}
	if srcInfoProvider.EntityType() != common.EEntityType.Symlink() {
		panic("configuration error. Source Info Provider does not have symlink entity type")
	}
	symSIP, ok := srcInfoProvider.(ISymlinkBearingSourceInfoProvider)
	if !ok {
		jptm.LogSendError(info.Source, info.Destination, "source info provider implementation does not support symlinks", 0)
		jptm.SetStatus(common.ETransferStatus.Failed())
		jptm.ReportTransferDone()
		return
	}

	path, err := symSIP.ReadLink()
	if err != nil {
		jptm.FailActiveSend("getting symlink path", err)
		jptm.SetStatus(common.ETransferStatus.Failed())
		jptm.ReportTransferDone()
		return
	}
	fmt.Println("ReadLink----------", path)

	baseSender, err := senderFactory(jptm, info.Destination, pacer, srcInfoProvider)
	if err != nil {
		jptm.LogSendError(info.Source, info.Destination, err.Error(), 0)
		jptm.SetStatus(common.ETransferStatus.Failed())
		jptm.ReportTransferDone()
		return
	}

	s, ok := baseSender.(symlinkSender) // todo: symlinkSender
	if !ok {
		jptm.LogSendError(info.Source, info.Destination, "sender implementation does not support symlinks", 0)
		jptm.SetStatus(common.ETransferStatus.Failed())
		jptm.ReportTransferDone()
		return
	}

	// check overwrite option
	// if the force Write flags is set to false or prompt
	// then check the file exists at the remote location
	// if it does, react accordingly
	if jptm.GetOverwriteOption() != common.EOverwriteOption.True() {
		exists, dstLmt, existenceErr := s.RemoteFileExists()
		if existenceErr != nil {
			jptm.LogSendError(info.Source, info.Destination, "Could not check destination file existence. "+existenceErr.Error(), 0)
			jptm.SetStatus(common.ETransferStatus.Failed()) // is a real failure, not just a SkippedFileAlreadyExists, in this case
			jptm.ReportTransferDone()
			return
		}
		if exists {
			shouldOverwrite := false

			// if necessary, prompt to confirm user's intent
			if jptm.GetOverwriteOption() == common.EOverwriteOption.Prompt() {
				// remove the SAS before prompting the user
				parsed, _ := url.Parse(info.Destination)
				parsed.RawQuery = ""
				shouldOverwrite = jptm.GetOverwritePrompter().ShouldOverwrite(parsed.String(), common.EEntityType.File())
			} else if jptm.GetOverwriteOption() == common.EOverwriteOption.IfSourceNewer() {
				// only overwrite if source lmt is newer (after) the destination
				if jptm.LastModifiedTime().After(dstLmt) {
					shouldOverwrite = true
				}
			}

			if !shouldOverwrite {
				// logging as Warning so that it turns up even in compact logs, and because previously we use Error here
				jptm.LogAtLevelForCurrentTransfer(common.LogWarning, "File already exists, so will be skipped")
				jptm.SetStatus(common.ETransferStatus.SkippedEntityAlreadyExists())
				jptm.ReportTransferDone()
				return
			}
		}
	}

	// write the symlink
	err = s.SendSymlink(path)
	if err != nil {
		jptm.FailActiveSend("creating destination symlink representative", err)
	}

	commonSenderCompletion(jptm, baseSender, info)
}
