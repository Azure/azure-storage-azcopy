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
	"fmt"
	"time"

	"github.com/Azure/azure-pipeline-go/pipeline"
	"github.com/Azure/azure-storage-azcopy/v10/common"
)

type JobPartCreatedMsg struct {
	TotalTransfers       uint32
	IsFinalPart          bool
	TotalBytesEnumerated uint64
	FileTransfers        uint32
	FolderTransfer       uint32
}

type xferDoneMsg = common.TransferDetail
type jobStatusManager struct {
	js                            common.ListJobSummaryResponse
	respChan                      chan common.ListJobSummaryResponse
	listReq                       chan bool
	partCreated                   chan JobPartCreatedMsg
	xferDone                      chan xferDoneMsg
	done                          chan struct{}
	doneHandleStatusUpdateManager bool
	xferDoneDrainCalled           bool
	xferDoneDrained               bool
	drainXferDoneSignal           chan struct{} // This is signal to drain all messages in XferDone channel.
	xferDoneDrainedSignal         chan struct{} // This is signal when all messages in XferDone channel drained.
}

/* These functions should not fail */
func (jm *jobMgr) SendJobPartCreatedMsg(msg JobPartCreatedMsg) {
	jm.jstm.partCreated <- msg
}

func (jm *jobMgr) SendXferDoneMsg(msg xferDoneMsg) {
	jm.jstm.xferDone <- msg
}

func (jm *jobMgr) ListJobSummary() common.ListJobSummaryResponse {
	/*
	 * Query job summary from handleStatusUpdateManager, but only if it's running.
	 * Ideally we should never find it exited, but we play safe, else we will be blocked for ever.
	 */
	if !jm.jstm.doneHandleStatusUpdateManager {
		jm.jstm.listReq <- true
		return <-jm.jstm.respChan
	} else {
		return jm.jstm.js
	}
}

func (jm *jobMgr) ResurrectSummary(js common.ListJobSummaryResponse) {
	jm.jstm.js = js
}

// DrainXferDoneMessages() function drain all message on XferDone channel. There is risk of HandleStatusUpdateManager() already exited and we are blocked.
// We safeguard against it by checking boolean set on exit of HandleStatusUpdateManager(). We try to make risk factor as low as we can, by calling this function only once.
//
// Note: This is not thread safe function. Onus on caller to handle that.
func (jm *jobMgr) DrainXferDoneMessages() bool {
	// Poke handleStatusUpdateManager to drain the xferDone channel, but only if it's running.
	if !jm.jstm.doneHandleStatusUpdateManager && !jm.jstm.xferDoneDrainCalled {
		jm.jstm.xferDoneDrainCalled = true
		jm.jstm.drainXferDoneSignal <- struct{}{}
		close(jm.jstm.xferDone)

		// Wait for handleStatusUpdateManager while it drains messages from the xferDone channel.
		<-jm.jstm.xferDoneDrainedSignal

		return true
	} else {
		jm.Log(pipeline.LogError, fmt.Sprintf("DrainXferDoneMessages already called and its status: %s", common.IffString(jm.jstm.xferDoneDrained, "Success", "Running")))
		return false
	}
}

func (jm *jobMgr) CleanupJobStatusMgr() {
	jm.Log(pipeline.LogInfo, "CleanJobStatusMgr called.")
	jm.jstm.done <- struct{}{}
}

// drainXferDoneCh drains all pending message in xferDone channel.
func drainXferDoneCh(jm *jobMgr) {
	jstm := jm.jstm
	js := &jstm.js

	jm.Log(pipeline.LogError, fmt.Sprintf("xferDoneCh: len: %v, cap: %v", len(jstm.xferDone), cap(jstm.xferDone)))

	for msg := range jstm.xferDone {
		processXferDoneMsg(msg, js)
	}
	jstm.xferDoneDrained = true
	jstm.xferDoneDrainedSignal <- struct{}{}
}

// processXferDoneMsg process the XferDone message.
func processXferDoneMsg(msg common.TransferDetail, js *common.ListJobSummaryResponse) {
	msg.Src = common.URLStringExtension(msg.Src).RedactSecretQueryParamForLogging()
	msg.Dst = common.URLStringExtension(msg.Dst).RedactSecretQueryParamForLogging()

	switch msg.TransferStatus {
	case common.ETransferStatus.Success():
		js.TransfersCompleted++
		js.TotalBytesTransferred += msg.TransferSize
	case common.ETransferStatus.Failed(),
		common.ETransferStatus.TierAvailabilityCheckFailure(),
		common.ETransferStatus.BlobTierFailure():
		js.TransfersFailed++
		js.FailedTransfers = append(js.FailedTransfers, msg)
	case common.ETransferStatus.SkippedEntityAlreadyExists(),
		common.ETransferStatus.SkippedBlobHasSnapshots():
		js.TransfersSkipped++
		js.SkippedTransfers = append(js.SkippedTransfers, msg)
	}
}

func (jm *jobMgr) handleStatusUpdateMessage() {
	jstm := jm.jstm
	js := &jstm.js
	js.JobID = jm.jobID
	js.CompleteJobOrdered = false
	js.ErrorMsg = ""

	// Set jstm.doneHandleStatusUpdateManager when handleStatusUpdateMessage goroutine exits.
	defer func() {
		jstm.doneHandleStatusUpdateManager = true
	}()

	for {
		select {
		// drainXferDoneSignal is high priority channel,process any message pending on xferDone channel.
		case <-jstm.drainXferDoneSignal:
			drainXferDoneCh(jm)
		default:
			select {
			case msg := <-jstm.partCreated:
				js.CompleteJobOrdered = js.CompleteJobOrdered || msg.IsFinalPart
				js.TotalTransfers += msg.TotalTransfers
				js.FileTransfers += msg.FileTransfers
				js.FolderPropertyTransfers += msg.FolderTransfer
				js.TotalBytesEnumerated += msg.TotalBytesEnumerated
				js.TotalBytesExpected += msg.TotalBytesEnumerated

			case msg := <-jstm.xferDone:
				processXferDoneMsg(msg, js)

			case <-jstm.listReq:
				/* Display stats */
				js.Timestamp = time.Now().UTC()
				jstm.respChan <- *js

				// Reset the lists so that they don't keep accumulating and take up excessive memory
				// There is no need to keep sending the same items over and over again
				js.FailedTransfers = []common.TransferDetail{}
				js.SkippedTransfers = []common.TransferDetail{}

			case <-jstm.drainXferDoneSignal:
				drainXferDoneCh(jm)

			case <-jstm.done:
				fmt.Println("Cleanup JobStatusmgr")
				return
			}
		}
	}
}
