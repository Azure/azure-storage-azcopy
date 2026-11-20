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
	"sync"
	"time"

	"github.com/Azure/azure-storage-azcopy/v10/common"
	"github.com/Azure/azure-storage-azcopy/v10/common/buildmode"
)

type JobPartCreatedMsg struct {
	TotalTransfers          uint32
	IsFinalPart             bool
	TotalBytesEnumerated    uint64
	FileTransfers           uint32
	FolderTransfer          uint32
	SymlinkTransfers        uint32
	HardlinksConvertedCount uint32
	FilePropertyTransfers   uint32
}

type xferDoneMsg struct {
	common.TransferDetail
	replayed bool
}
type listRequest struct {
	resetLists bool
	resurrect  *common.ListJobSummaryResponse
}

type jobStatusManager struct {
	finalSummaryMu  sync.Mutex // Protects list consumption after the handler has stopped.
	js              common.ListJobSummaryResponse
	respChan        chan common.ListJobSummaryResponse
	listReq         chan listRequest
	partCreated     chan JobPartCreatedMsg
	xferDone        chan xferDoneMsg
	xferDoneDrained chan struct{} // To signal that all xferDone have been processed
	statusMgrDone   chan struct{} // To signal statusManager has closed
	once            sync.Once     // Ensure xferDoneDrained is closed once
}

func (jm *jobMgr) waitToDrainXferDone() {
	<-jm.jstm.xferDoneDrained
}

/* These functions should not fail */
func (jm *jobMgr) SendJobPartCreatedMsg(msg JobPartCreatedMsg) {
	defer func() {
		if recErr := recover(); recErr != nil {
			jm.Log(common.LogError, "Cannot send message on closed channel")
		}
	}()
	if jm.jstm.partCreated != nil { // Sends not allowed if channel is closed
		select {
		case jm.jstm.partCreated <- msg:
		case <-jm.jstm.statusMgrDone: // Nobody is listening anymore, let's back off.
		}

		if msg.IsFinalPart && jm.session == nil {
			// Inform statusManager that this is all parts we've
			close(jm.jstm.partCreated)
		}
	}
}

func (jm *jobMgr) SendXferDoneMsg(msg xferDoneMsg) {
	defer func() {
		if recErr := recover(); recErr != nil {
			jm.Log(common.LogError, "Cannot send message on channel")
		}
	}()
	jm.jstm.xferDone <- msg
}

func (jm *jobMgr) ListJobSummary(reset ...bool) common.ListJobSummaryResponse {
	resetLists := true // default value
	if len(reset) > 0 {
		resetLists = reset[0]
	}

	return jm.jstm.requestSummary(listRequest{resetLists: resetLists})
}

func (jstm *jobStatusManager) requestSummary(request listRequest) common.ListJobSummaryResponse {
	select {
	case jstm.listReq <- request:
		return <-jstm.respChan
	case <-jstm.statusMgrDone:
		// StatusManager closed while we requested for an update.
		// Return the last update; the handler will no longer update counters.
		jstm.finalSummaryMu.Lock()
		defer jstm.finalSummaryMu.Unlock()
		if request.resurrect != nil {
			jstm.js = *request.resurrect
		}
		return jstm.snapshot(request.resetLists)
	}
}

func (jm *jobMgr) ResurrectSummary(js common.ListJobSummaryResponse) {
	jm.jstm.requestSummary(listRequest{resurrect: &js})
}

// GetJobSummary uses the manager's own counters, lifecycle and pacing resources.
// Session shutdown freezes the summary before releasing those resources.
// The returned direction enables legacy whole-job benchmark advice only.
func (jm *jobMgr) GetJobSummary(resetLists bool) (common.ListJobSummaryResponse, common.FromTo) {
	if jm.session != nil {
		jm.session.metricsMu.RLock()
		defer jm.session.metricsMu.RUnlock()
	}
	return jm.getJobSummary(resetLists)
}

func (jm *jobMgr) getJobSummary(resetLists bool) (common.ListJobSummaryResponse, common.FromTo) {
	js := jm.ListJobSummary(resetLists)
	js.Timestamp, js.JobID, js.ErrorMsg = time.Now().UTC(), jm.jobID, ""
	if jm.session != nil && jm.session.finalSummary != nil {
		final := *jm.session.finalSummary
		final.Timestamp = js.Timestamp
		final.FailedTransfers, final.SkippedTransfers = js.FailedTransfers, js.SkippedTransfers
		return final, common.FromTo(0)
	}
	status, ok := jm.lifecycleStatus()
	if !ok {
		return js, common.FromTo(0)
	}
	fromTo := common.FromTo(0)
	if jm.session == nil {
		part0, _ := jm.JobPartMgr(0)
		fromTo = part0.Plan().FromTo
		// This is added to let FE to continue fetching the Job Progress Summary
		// in case of resume. In case of resume, the Job is already completely
		// ordered so the progress summary should be fetched until all job parts
		// are iterated and have been scheduled
		js.CompleteJobOrdered = js.CompleteJobOrdered || jm.AllTransfersScheduled()
		// If the status is cancelled, then no need to check for completerJobOrdered
		// since user must have provided the consent to cancel an incompleteJob if that
		// is the case.
		// Job is completed if Job order is complete AND ALL transfers are completed/failed
		// FIX: active or inactive state, then job order is said to be completed if final part of job has been ordered.
		if status == common.EJobStatus.Cancelled() || js.CompleteJobOrdered && status.IsJobDone() {
			js.JobStatus = status
		}
	} else {
		js.JobStatus = status
		js.CompleteJobOrdered = status.IsJobDone()
	}
	// Add on byte count from files in flight, to get a more accurate running total
	// Check is added to prevent double counting
	activeBytes := jm.SuccessfulBytesInActiveFiles()
	if js.TotalBytesTransferred <= js.TotalBytesExpected && activeBytes <= js.TotalBytesExpected-js.TotalBytesTransferred {
		js.TotalBytesTransferred += activeBytes
	}
	// if no bytes expected, and we should avoid dividing by 0 (which results in NaN)
	js.PercentComplete = 100
	if js.TotalBytesExpected != 0 {
		js.PercentComplete = 100 * float32(js.TotalBytesTransferred) / float32(js.TotalBytesExpected)
		if js.PercentComplete > 100 {
			js.PercentComplete = 100
		}
	}
	js.BytesOverWire = uint64(jm.pacer.GetTotalTraffic())
	// Get the number of active go routines performing the transfer or executing the chunk Func
	// TODO: added for debugging purpose. remove later (is covered by GetPerfInfo now anyway)
	js.ActiveConnections = jm.ActiveConnections()
	js.PerfStrings, js.PerfConstraint = jm.GetPerfInfo()
	if stats := jm.PipelineNetworkStats(); stats != nil {
		js.AverageIOPS = stats.OperationsPerSecond()
		js.AverageE2EMilliseconds = stats.AverageE2EMilliseconds()
		js.AverageConnWaitMs = stats.AverageConnWaitMs()
		js.AverageWireMs = stats.AverageWireMs()
		js.AverageDNSMs = stats.AverageDNSMs()
		js.AverageTLSMs = stats.AverageTLSMs()
		js.ConnNewCount, js.ConnReusedCount = stats.ConnNewCount(), stats.ConnReusedCount()
		js.NetworkErrorPercentage = stats.NetworkErrorPercentage()
		js.ServerBusyPercentage = stats.TotalServerBusyPercentage()
	}
	return js, fromTo
}

func (jstm *jobStatusManager) snapshot(resetLists bool) common.ListJobSummaryResponse {
	js := jstm.js
	js.Timestamp = time.Now().UTC()
	if resetLists {
		// Reset the lists so that they don't keep accumulating and take up excessive memory
		// There is no need to keep sending the same items over and over again
		jstm.js.FailedTransfers, jstm.js.SkippedTransfers = nil, nil
	} else {
		js.FailedTransfers = append([]common.TransferDetail(nil), js.FailedTransfers...)
		js.SkippedTransfers = append([]common.TransferDetail(nil), js.SkippedTransfers...)
	}
	return js
}

func (jm *jobMgr) handleStatusUpdateMessage() {
	jstm := jm.jstm
	js := &jstm.js
	partCreated, xferDone := jstm.partCreated, jstm.xferDone
	allXferDoneHandled := false
	defer close(jstm.statusMgrDone)

	for {
		if jm.session != nil && partCreated == nil && xferDone == nil {
			return
		}
		select {
		case msg, ok := <-partCreated:
			if !ok {
				partCreated = nil
				if jm.session == nil {
					jstm.partCreated = nil
				}
				continue
			}
			js.CompleteJobOrdered = js.CompleteJobOrdered || msg.IsFinalPart
			js.TotalTransfers += uint64(msg.TotalTransfers)
			js.FileTransfers += uint64(msg.FileTransfers)
			js.FolderPropertyTransfers += uint64(msg.FolderTransfer)
			js.SymlinkTransfers += msg.SymlinkTransfers
			js.FilePropertyTransfers += uint64(msg.FilePropertyTransfers)
			js.TotalBytesEnumerated += msg.TotalBytesEnumerated
			js.TotalBytesExpected += msg.TotalBytesEnumerated
			js.HardlinksConvertedCount += msg.HardlinksConvertedCount

		case msg, ok := <-xferDone:
			if !ok { // Channel is closed, all transfers have been attended.
				xferDone = nil
				if jm.session == nil {
					jstm.xferDone = nil
				}
				// close drainXferDone so that other components can know no further updates happen
				allXferDoneHandled = true
				jstm.once.Do(func() { close(jstm.xferDoneDrained) })
				continue
			}

			// Redacting here (before the switch) matches original/default behavior: every message's
			// Src/Dst is redacted regardless of outcome. Under mover-high-perf, skip this for the
			// (overwhelmingly common) Success() case, since msg.Src/Dst are only ever read later for
			// FailedTransfers/SkippedTransfers -- redacting on every successful transfer in a large
			// job is wasted url.Parse()/rebuild work that's never used.
			if !buildmode.HighPerf() {
				msg.Src = common.URLStringExtension(msg.Src).RedactSecretQueryParamForLogging()
				msg.Dst = common.URLStringExtension(msg.Dst).RedactSecretQueryParamForLogging()
			}

			switch msg.TransferStatus {
			case common.ETransferStatus.Success():
				if msg.IsFolderProperties {
					js.FoldersCompleted++
				}
				js.TransfersCompleted++
				js.TotalBytesTransferred += msg.TransferSize
				if !msg.replayed {
					js.TransfersCompletedThisExecution++
					js.BytesTransferredThisExecution += msg.TransferSize
				}
			case common.ETransferStatus.Failed(),
				common.ETransferStatus.TierAvailabilityCheckFailure(),
				common.ETransferStatus.BlobTierFailure():
				if msg.IsFolderProperties {
					js.FoldersFailed++
				}
				js.TransfersFailed++
				js.BytesFailed += msg.TransferSize
				if buildmode.HighPerf() {
					msg.Src = common.URLStringExtension(msg.Src).RedactSecretQueryParamForLogging()
					msg.Dst = common.URLStringExtension(msg.Dst).RedactSecretQueryParamForLogging()
				}
				js.FailedTransfers = append(js.FailedTransfers, msg.TransferDetail)
			case common.ETransferStatus.SkippedEntityAlreadyExists(),
				common.ETransferStatus.SkippedBlobHasSnapshots():
				if msg.IsFolderProperties {
					js.FoldersSkipped++
				}
				js.TransfersSkipped++
				if buildmode.HighPerf() {
					msg.Src = common.URLStringExtension(msg.Src).RedactSecretQueryParamForLogging()
					msg.Dst = common.URLStringExtension(msg.Dst).RedactSecretQueryParamForLogging()
				}
				js.SkippedTransfers = append(js.SkippedTransfers, msg.TransferDetail)
			case common.ETransferStatus.SkippedArchiveNotRestored():
				js.SkippedArchiveFileCount++
			}

		case request := <-jstm.listReq:
			/* Display stats */
			if request.resurrect != nil {
				*js = *request.resurrect
			}
			// Send on the channel
			jstm.respChan <- jstm.snapshot(request.resetLists)

			if jm.session == nil && allXferDoneHandled && request.resetLists {
				return
			}
		}
	}
}
