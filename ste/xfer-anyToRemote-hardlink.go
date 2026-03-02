// Copyright © 2026 Microsoft <azcopydev@microsoft.com>
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
	"path"
	"strings"

	"github.com/Azure/azure-sdk-for-go/sdk/storage/azfile/file"
	"github.com/Azure/azure-storage-azcopy/v10/common"
)

func anyToRemote_hardlink(jptm IJobPartTransferMgr, info *TransferInfo, pacer pacer, senderFactory senderFactory, sipf sourceInfoProviderFactory) {
	// Check if cancelled
	if jptm.WasCanceled() {
		/* This is the earliest we detect jptm has been cancelled before scheduling chunks */
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

	if srcInfoProvider.EntityType() != common.EEntityType.Hardlink() {
		panic("configuration error. Source Info Provider does not have hardlink entity type")
	}

	baseSender, err := senderFactory(jptm, info.Destination, pacer, srcInfoProvider)
	if err != nil {
		jptm.LogSendError(info.Source, info.Destination, err.Error(), 0)
		jptm.SetStatus(common.ETransferStatus.Failed())
		jptm.ReportTransferDone()
		return
	}

	s, ok := baseSender.(hardlinkSender)
	if !ok {
		jptm.LogSendError(info.Source, info.Destination, "sender implementation does not support hardlinks", 0)
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

	// write the hardlink
	// Derive the target hardlink path full remote path approach:
	//
	// 1. Compute the current file's traversal-root-relative path from the local source paths.
	//    e.g. GetSourceRoot()="/home/azureuser/spe_dir", info.Source=".../spe_dir/hardlink/newlink.txt"
	//         => fileRelPath = "hardlink/newlink.txt"
	//
	// 2. Strip that suffix from the destination URL path to get the dest prefix.
	//    copy: destURLPath="meta/spe_dir/hardlink/newlink.txt" => destPrefix="meta/spe_dir/"
	//    sync: destURLPath="spe_dir/hardlink/newlink.txt"      => destPrefix="spe_dir/"
	//
	// 3. Join destPrefix + TargetHardlinkFilePath.
	//    copy: "meta/spe_dir/" + "hardlink/hlink.txt" = "meta/spe_dir/hardlink/hlink.txt" ✓
	//    sync: "spe_dir/"      + "hardlink/hlink.txt" = "spe_dir/hardlink/hlink.txt"      ✓
	sourceRoot := strings.TrimSuffix(jptm.GetSourceRoot(), common.AZCOPY_PATH_SEPARATOR_STRING)
	fileRelPath := strings.TrimPrefix(strings.TrimPrefix(info.Source, sourceRoot), common.AZCOPY_PATH_SEPARATOR_STRING)

	destURLParts, err := file.ParseURL(info.Destination)
	if err != nil {
		jptm.FailActiveSend("Parsing destination URL", err)
		return
	}
	destPrefix := strings.TrimSuffix(destURLParts.DirectoryOrFilePath, fileRelPath)
	targetHardlinkFullPath := "/" + path.Join(destPrefix, info.TargetHardlinkFilePath)
	fmt.Println("-----####### targetHardlinkPath", targetHardlinkFullPath)
	err = s.CreateHardlink(targetHardlinkFullPath)
	if err != nil {
		jptm.FailActiveSend("Creating hardlink", err)
		return
	}

	commonSenderCompletion(jptm, baseSender, info)
}
