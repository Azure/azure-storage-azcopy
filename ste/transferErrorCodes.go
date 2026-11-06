// Copyright © Microsoft <wastore@microsoft.com>
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

// failTransferBeforeStart ends a transfer that failed before any chunk was
// scheduled. It records errMsg together with the HTTP status and error
// codes of err, which may be nil, the same way failActiveTransfer does for
// failures after that point.
func failTransferBeforeStart(jptm IJobPartTransferMgr, info *TransferInfo, errMsg string, err error) {
	codes := common.TransferFailureCodesFor(err, info.Source, info.Destination)
	jptm.LogSendError(info.Source, info.Destination, errMsg, int(codes.StatusCode))
	jptm.SetErrorCode(codes.StatusCode)
	jptm.SetTransferErrorCodes(codes)
	jptm.SetErrorMessage(errMsg)
	jptm.SetStatus(common.ETransferStatus.Failed())
	jptm.ReportTransferDone()
}
