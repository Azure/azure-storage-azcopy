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

	// TODO: support path derivation for FileNFSLocal and FileNFSFileNFS hardlink transfers
	// targethardlinkPath, err := derivePathForLocalToFileNFS(info.Destination, jptm.Info().TargetHardlinkFilePath)
	// if err != nil {
	// 	jptm.FailActiveSend("deriving target hardlink path", err)
	// 	return
	// }
	// write the hardlink
	err = s.CreateHardlink(jptm.Info().TargetHardlinkFilePath)
	if err != nil {
		jptm.FailActiveSend("creating destination hardlink representative", err)
	}

	commonSenderCompletion(jptm, baseSender, info)
}

func derivePathForLocalToFileNFS(urlStr, originalRelPath string) (string, error) {
	u, err := url.Parse(urlStr)
	if err != nil {
		return "", err
	}

	// Remove leading slash and split
	// /share/a/b/z/hardlink/file.txt → [share a b z hardlink file.txt]
	urlParts := strings.Split(strings.TrimPrefix(u.Path, "/"), "/")
	if len(urlParts) < 2 {
		return "", fmt.Errorf("invalid URL path")
	}

	// Drop share name
	shareRel := urlParts[1:]

	// originalRelPath: z/hh/file.txt
	origParts := strings.Split(originalRelPath, "/")
	if len(origParts) == 0 {
		return "", fmt.Errorf("invalid original path")
	}

	// Find first occurrence of origParts[0] in shareRel
	matchIdx := -1
	for i, p := range shareRel {
		if p == origParts[0] {
			matchIdx = i
			break
		}
	}
	if matchIdx == -1 {
		return "", fmt.Errorf("no common path element found")
	}

	// Prefix + remainder
	result := path.Join(
		append(shareRel[:matchIdx+1], origParts[1:]...)...,
	)

	return result, nil
}
