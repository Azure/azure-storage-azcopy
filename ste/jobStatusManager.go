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
	"sync"
	"time"

	"github.com/Azure/azure-storage-azcopy/v10/common"
)

type JobPartCreatedMsg struct {
	TotalTransfers       uint32
	IsFinalPart          bool
	TotalBytesEnumerated uint64
	FileTransfers        uint32
	FolderTransfer       uint32
	SymlinkTransfers     uint32
}

type xferDoneMsg = common.TransferDetail
type jobStatusManager struct {
	js               common.ListJobSummaryResponse
	respChan         chan common.ListJobSummaryResponse
	listReq          chan struct{}
	partCreated      chan JobPartCreatedMsg
	xferDone         chan xferDoneMsg
	xferDoneDrained  chan struct{} // To signal that all xferDone have been processed
	statusMgrDone    chan struct{} // To signal statusManager has closed
	isXferDoneClosed bool          // True (xferDone channel is closed)
	flagMutex        sync.RWMutex  // Read Write Mutex to prevent data race conditions
}

func (jm *jobMgr) waitToDrainXferDone() {
	<-jm.jstm.xferDoneDrained
}

func (jm *jobMgr) statusMgrClosed() bool {
	select {
	case <-jm.jstm.statusMgrDone:
		return true
	default:
		return false
	}
}

/* These functions should not fail */
func (jm *jobMgr) SendJobPartCreatedMsg(msg JobPartCreatedMsg) {
	jm.jstm.partCreated <- msg
	if msg.IsFinalPart {
		// Inform statusManager that this is all parts we've
		close(jm.jstm.partCreated)
	}
}

func (jm *jobMgr) SendXferDoneMsg(msg xferDoneMsg) {
	jm.jstm.flagMutex.RLock()
	defer jm.jstm.flagMutex.RUnlock()
	if jm.jstm.isXferDoneClosed {
		fmt.Println("Cannot send message on closed channel")
		return
	}
	// channel is open, can send message
	select {
	case jm.jstm.xferDone <- msg:
		fmt.Println("Message sent successfully!")
	default:
		fmt.Println("Cannot send message on channel")
	}
	//function will return triggering the read unlock
}

func (jm *jobMgr) ListJobSummary() common.ListJobSummaryResponse {
	if jm.statusMgrClosed() {
		return jm.jstm.js
	}

	select {
	case jm.jstm.listReq <- struct{}{}:
		return <-jm.jstm.respChan
	case <-jm.jstm.statusMgrDone:
		// StatusManager closed while we requested for an update.
		// Return the last update. This is okay because there will
		// be no further updates.
		return jm.jstm.js
	}
}

func (jm *jobMgr) ResurrectSummary(js common.ListJobSummaryResponse) {
	jm.jstm.js = js
}

func (jm *jobMgr) handleStatusUpdateMessage() {
	jstm := jm.jstm
	js := &jstm.js
	js.JobID = jm.jobID
	js.CompleteJobOrdered = false
	js.ErrorMsg = ""
	allXferDoneHandled := false

	for {
		select {
		case msg, ok := <-jstm.partCreated:
			if !ok {
				jstm.partCreated = nil
				continue
			}
			js.CompleteJobOrdered = js.CompleteJobOrdered || msg.IsFinalPart
			js.TotalTransfers += msg.TotalTransfers
			js.FileTransfers += msg.FileTransfers
			js.FolderPropertyTransfers += msg.FolderTransfer
			js.SymlinkTransfers += msg.SymlinkTransfers
			js.TotalBytesEnumerated += msg.TotalBytesEnumerated
			js.TotalBytesExpected += msg.TotalBytesEnumerated

		case msg, ok := <-jstm.xferDone:
			if !ok { // Channel is closed, all transfers have been attended.
				jstm.xferDone = nil

				// close drainXferDone so that other components can know no further updates happen
				allXferDoneHandled = true
				close(jstm.xferDoneDrained)
				continue
			}

			msg.Src = common.URLStringExtension(msg.Src).RedactSecretQueryParamForLogging()
			msg.Dst = common.URLStringExtension(msg.Dst).RedactSecretQueryParamForLogging()

			switch msg.TransferStatus {
			case common.ETransferStatus.Success():
				if msg.IsFolderProperties {
					js.FoldersCompleted++
				}
				js.TransfersCompleted++
				js.TotalBytesTransferred += msg.TransferSize
			case common.ETransferStatus.Failed(),
				common.ETransferStatus.TierAvailabilityCheckFailure(),
				common.ETransferStatus.BlobTierFailure():
				if msg.IsFolderProperties {
					js.FoldersFailed++
				}
				js.TransfersFailed++
				js.FailedTransfers = append(js.FailedTransfers, msg)
			case common.ETransferStatus.SkippedEntityAlreadyExists(),
				common.ETransferStatus.SkippedBlobHasSnapshots():
				if msg.IsFolderProperties {
					js.FoldersSkipped++
				}
				js.TransfersSkipped++
				js.SkippedTransfers = append(js.SkippedTransfers, msg)
			}

		case <-jstm.listReq:
			/* Display stats */
			js.Timestamp = time.Now().UTC()
			jstm.respChan <- *js

			// Reset the lists so that they don't keep accumulating and take up excessive memory
			// There is no need to keep sending the same items over and over again
			js.FailedTransfers = []common.TransferDetail{}
			js.SkippedTransfers = []common.TransferDetail{}

			if allXferDoneHandled {
				close(jstm.statusMgrDone)
				close(jstm.respChan)
				close(jstm.listReq)
				jstm.listReq = nil
				jstm.respChan = nil
				return
			}
		}
	}
}
