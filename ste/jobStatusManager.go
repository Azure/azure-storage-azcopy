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
	"time"

	"github.com/Azure/azure-storage-azcopy/common"
)

type jobPartCreatedMsg struct {
	totalTransfers       uint32
	isFinalPart          bool
	totalBytesEnumerated uint64
	fileTransfers        uint32
	folderTransfer       uint32
}

type xferDoneMsg = common.TransferDetail
type jobStatusManager struct {
	js          common.ListJobSummaryResponse
	respChan    chan common.ListJobSummaryResponse
	listReq     chan bool
	partCreated chan jobPartCreatedMsg
	xferDone    chan xferDoneMsg
}

var jstm jobStatusManager

/* These functions should not fail */
func (jm *jobMgr) SendJobPartCreatedMsg(msg jobPartCreatedMsg) {
	jstm.partCreated <- msg
}

func (jm *jobMgr) SendXferDoneMsg(msg xferDoneMsg) {
	jstm.xferDone <- msg
}

func (jm *jobMgr) ListJobSummary() common.ListJobSummaryResponse {
	jstm.listReq <- true
	return <-jstm.respChan
}

func (jm *jobMgr) ResurrectSummary(js common.ListJobSummaryResponse) {
	jstm.js = js
}

func (jm *jobMgr) handleStatusUpdateMessage() {
	js := &jstm.js
	js.JobID = jm.jobID
	js.CompleteJobOrdered = false
	js.ErrorMsg = ""

	for {
		select {
		case msg := <-jstm.partCreated:
			js.CompleteJobOrdered = js.CompleteJobOrdered || msg.isFinalPart
			js.TotalTransfers += msg.totalTransfers
			js.FileTransfers += msg.fileTransfers
			js.FolderPropertyTransfers += msg.folderTransfer
			js.TotalBytesEnumerated += msg.totalBytesEnumerated
			js.TotalBytesExpected += msg.totalBytesEnumerated

		case msg := <-jstm.xferDone:
			switch msg.TransferStatus {
			case common.ETransferStatus.Success():
				js.TransfersCompleted++
				js.TotalBytesTransferred += msg.TransferSize
			case common.ETransferStatus.Failed(),
				common.ETransferStatus.TierAvailabilityCheckFailure(),
				common.ETransferStatus.BlobTierFailure():
				js.TransfersFailed++
				js.FailedTransfers = append(js.FailedTransfers, common.TransferDetail(msg))
			case common.ETransferStatus.SkippedEntityAlreadyExists(),
				common.ETransferStatus.SkippedBlobHasSnapshots():
				js.TransfersSkipped++
				js.SkippedTransfers = append(js.SkippedTransfers, common.TransferDetail(msg))
			}

		case <-jstm.listReq:
			/* Display stats */
			js.Timestamp = time.Now().UTC()
			jstm.respChan <- *js
		}
	}
}
