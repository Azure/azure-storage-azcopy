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
	"context"
	"encoding/json"
	"fmt"
	"github.com/Azure/azure-pipeline-go/pipeline"
	"github.com/Azure/azure-storage-azcopy/common"
	"io/ioutil"
	"net/http"
)

var steCtx = context.Background()

// mainSTE initializes the Storage Transfer Engine
func mainSTE(concurrentConnections int, targetRateInMBps int64) error {
	// Initialize the JobsAdmin, resurrect Job plan files
	initJobsAdmin(steCtx, 500, targetRateInMBps)
	JobsAdmin.ResurrectJobParts()
	// TODO: We may want to list listen first and terminate if there is already an instance listening

	deserialize := func(request *http.Request, v interface{}) {
		// TODO: Check the HTTP verb here?
		// reading the entire request body and closing the request body
		body, err := ioutil.ReadAll(request.Body)
		request.Body.Close()
		if err != nil {
			JobsAdmin.Panic(fmt.Errorf("error deserializing HTTP request"))
		}
		json.Unmarshal(body, v)
	}
	serialize := func(v interface{}, response http.ResponseWriter) {
		payload, err := json.Marshal(response)
		if err != nil {
			JobsAdmin.Panic(fmt.Errorf("error serializing HTTP response"))
		}
		// sending successful response back to front end
		response.WriteHeader(http.StatusAccepted)
		response.Write(payload)
	}

	http.HandleFunc(common.ERpcCmd.CopyJobPartOrder().Pattern(),
		func(writer http.ResponseWriter, request *http.Request) {
			var payload common.CopyJobPartOrderRequest
			deserialize(request, &payload)
			serialize(ExecuteNewCopyJobPartOrder(payload), writer)
		})
	http.HandleFunc(common.ERpcCmd.ListJobs().Pattern(),
		func(writer http.ResponseWriter, request *http.Request) {
			//var payload common.ListRequest
			//deserialize(request, &payload)
			serialize(listExistingJobs( /*payload*/ ), writer)
		})
	http.HandleFunc(common.ERpcCmd.ListJobSummary().Pattern(),
		func(writer http.ResponseWriter, request *http.Request) {
			var payload common.JobID
			deserialize(request, &payload)
			serialize(getJobSummary(payload), writer)
		})
	http.HandleFunc(common.ERpcCmd.ListJobTransfers().Pattern(),
		func(writer http.ResponseWriter, request *http.Request) {
			var payload common.ListRequest
			deserialize(request, &payload)
			serialize(getTransferList(payload), writer) // TODO: make struct
		})
	http.HandleFunc(common.ERpcCmd.CancelJob().Pattern(),
		func(writer http.ResponseWriter, request *http.Request) {
			var payload common.JobID
			deserialize(request, &payload)
			serialize(cancelpauseJobOrder(payload, common.EJobStatus.Cancelled()), writer)
		})
	http.HandleFunc(common.ERpcCmd.PauseJob().Pattern(),
		func(writer http.ResponseWriter, request *http.Request) {
			var payload common.JobID
			deserialize(request, &payload)
			serialize(cancelpauseJobOrder(payload, common.EJobStatus.Paused()), writer)
		})
	http.HandleFunc(common.ERpcCmd.ResumeJob().Pattern(),
		func(writer http.ResponseWriter, request *http.Request) {
			var payload common.JobID
			deserialize(request, &payload)
			serialize(ResumeJobOrder(payload), writer)
		})

	// Listen for front-end requests
	if err := http.ListenAndServe("localhost:1337", nil); err != nil {
		fmt.Print("Server already initialized")
		return err
	}
	return nil // TODO: don't return (like normal main)
}

///////////////////////////////////////////////////////////////////////////////

// ExecuteNewCopyJobPartOrder api executes a new job part order
func ExecuteNewCopyJobPartOrder(order common.CopyJobPartOrderRequest) common.CopyJobPartOrderResponse {
	// Get the file name for this Job Part's Plan
	jppfn := JobsAdmin.NewJobPartPlanFileName(order.JobID, order.PartNum)
	jppfn.Create(order)                                      // Convert the order to a plan file
	jpm := JobsAdmin.JobMgrEnsureExists(order.JobID, steCtx) // Get a this job part's job manager (create it if it doesn't exist)
	jpm.AddJobPart(order.PartNum, jppfn, true)               // Add this part to the Job and schedule its transfers
	return common.CopyJobPartOrderResponse{JobStarted: true}
}

// cancelpauseJobOrder api cancel/pause a job with given JobId
/* A Job cannot be cancelled/paused in following cases
	* If the Job has not been ordered completely it cannot be cancelled or paused
    * If all the transfers in the Job are either failed or completed, then Job cannot be cancelled or paused
    * If a job is already paused, it cannot be paused again
*/
func cancelpauseJobOrder(jobID common.JobID, desiredJobStatus common.JobStatus) common.CancelPauseResumeResponse {
	verb := common.IffString(desiredJobStatus == common.EJobStatus.Paused(), "pause", "cancel")
	jm, found := JobsAdmin.JobMgr(jobID) // Find Job being paused/canceled
	if !found {
		return common.CancelPauseResumeResponse{
			CancelledPauseResumed: false,
			ErrorMsg:              fmt.Sprintf("no active job with JobId %s exists", jobID.String()),
		}
	}

	completeJobOrdered := func(jm IJobMgr) bool {
		// completeJobOrdered determines whether final part for job with JobId has been ordered or not.
		completeJobOrdered := false
		for p := PartNumber(0); true; p++ {
			jpm, found := jm.JobPartMgr(p)
			if !found {
				break
			}
			completeJobOrdered = completeJobOrdered || jpm.Plan().IsFinalPart
		}
		return completeJobOrdered
	}

	// If the job has not been ordered completely, then job cannot be paused/cancelled
	if !completeJobOrdered(jm) {
		return common.CancelPauseResumeResponse{
			CancelledPauseResumed: false,
			ErrorMsg:              fmt.Sprintf("job with JobId %s hasn't been ordered completely", jobID.String()),
		}
	}

	// It's OK to pause an already-paused job
	// If all job parts are completed/failed, then job cannot be cancelled since it is already finished
	jpm, found := jm.JobPartMgr(0)
	if !found {
		return common.CancelPauseResumeResponse{
			CancelledPauseResumed: false,
			ErrorMsg:              fmt.Sprintf("job with JobId %s hasn't been ordered completely", jobID.String()),
		}
	}

	jpp0 := jpm.Plan()
	var jr common.CancelPauseResumeResponse
	switch jpp0.JobStatus() { // Current status
	case common.EJobStatus.InProgress(): // Changing to InProgress/Paused/Canceled is OK
		jpp0.SetJobStatus(desiredJobStatus) // Set status to paused/Canceled

	case common.EJobStatus.Completed(): // You can't change state of a completed job
		jr = common.CancelPauseResumeResponse{
			CancelledPauseResumed: false,
			ErrorMsg:              fmt.Sprintf("Can't %s JobID=%v because it has already completed", verb, jobID),
		}

	case common.EJobStatus.Paused(): // Logically, It's OK to pause an already-paused job
	case common.EJobStatus.Cancelled():
		jpp0.SetJobStatus(desiredJobStatus)
		msg := fmt.Sprintf("JobID=%v %s", jobID,
			common.IffString(desiredJobStatus == common.EJobStatus.Paused(), "paused", "canceled"))

		if jm.ShouldLog(pipeline.LogInfo) {
			jm.Log(pipeline.LogInfo, msg)
		}
		jm.Cancel() // Stop all inflight-chunks/transfer for this job (this includes all parts)
		jr = common.CancelPauseResumeResponse{
			CancelledPauseResumed: true,
			ErrorMsg:              msg,
		}
	}
	return jr
}

// ResumeJobOrder resumes the JobOrder for given JobId
/*
	* Checks the current JobStatus of the Job and if it is in JobInProgress, then send the HTTP Response
    * Iterate through each JobPartOrder of the Job and refresh the cancelled context of the JobPart Order
	* Iterate through each transfer of JobPart order and refresh the cancelled context of the transfer
    * Reschedule each transfer again into the transfer msg channel depending on the priority of the channel
*/
func ResumeJobOrder(jobID common.JobID) common.CancelPauseResumeResponse {
	jm, found := JobsAdmin.JobMgr(jobID) // Find Job being resumed
	if !found {
		return common.CancelPauseResumeResponse{
			CancelledPauseResumed: false,
			ErrorMsg:              fmt.Sprintf("no active job with JobId %v exists", jobID),
		}
	}

	var jr common.CancelPauseResumeResponse
	jpm, found := jm.JobPartMgr(0)
	if !found {
		return common.CancelPauseResumeResponse{
			CancelledPauseResumed: false,
			ErrorMsg:              fmt.Sprintf("JobID=%v, Part#=0 not found", jobID),
		}
	}

	jpp0 := jpm.Plan()
	switch jpp0.JobStatus() { // Current status
	case common.EJobStatus.InProgress(): // Changing to Resumed is OK
		break // Nothing to do

	case common.EJobStatus.Completed(): // You can't change state of a completed/canceled job
	case common.EJobStatus.Cancelled():
		jr = common.CancelPauseResumeResponse{
			CancelledPauseResumed: false,
			ErrorMsg:              fmt.Sprintf("Can't resume JobID=%v because it has already completed or been canceled", jobID),
		}

	case common.EJobStatus.Paused(): // Resuming a paused job
		jpp0.SetJobStatus(common.EJobStatus.InProgress())
		msg := fmt.Sprintf("JobID=%v resumed", jobID)
		if jm.ShouldLog(pipeline.LogInfo) {
			jm.Log(pipeline.LogInfo, msg)
		}
		jm.ResumeTransfers(steCtx) // Reschedule all job part's transfers
		jr = common.CancelPauseResumeResponse{
			CancelledPauseResumed: true,
			ErrorMsg:              msg,
		}
	}
	return jr
}

// getJobSummary api returns the job progress summary of an active job
/*
* Return following Properties in Job Progress Summary
* CompleteJobOrdered - determines whether final part of job has been ordered or not
* TotalNumberOfTransfers - total number of transfers available for the given job
* TotalNumberOfTransfersCompleted - total number of transfers in the job completed
* NumberOfTransfersCompletedAfterCheckpoint - number of transfers completed after the last checkpoint
* NumberOfTransferFailedAfterCheckpoint - number of transfers failed after last checkpoint timestamp
* PercentageProgress - job progress reported in terms of percentage
* FailedTransfers - list of transfer after last checkpoint timestamp that failed.
 */
func getJobSummary(jobID common.JobID) common.ListJobSummaryResponse {
	//fmt.Println("received a get job order status request for JobId ", jobId)
	// getJobPartMapFromJobPartInfoMap gives the map of partNo to JobPartPlanInfo Pointer for a given JobId
	jm, found := JobsAdmin.JobMgr(jobID)
	if !found {
		return common.ListJobSummaryResponse{
			ErrorMsg: fmt.Sprintf("no active job with JobId %s exists", jobID),
		}
	}

	js := common.ListJobSummaryResponse{
		JobID:              jobID,
		ErrorMsg:           "",
		JobStatus:          common.JobStatus{}.InProgress(), // Default
		CompleteJobOrdered: false,                           // default to false; returns true if ALL job parts have been ordered
		FailedTransfers:    []common.TransferDetail{},
	}

	for partNum := PartNumber(0); true; partNum++ {
		// currentJobPartPlanInfo represents the memory map JobPartPlanHeader for current partNo
		jpm, found := jm.JobPartMgr(partNum)
		if !found {
		}
		jpp := jpm.Plan()
		js.CompleteJobOrdered = js.CompleteJobOrdered || jpp.IsFinalPart
		js.TotalNumberOfTransfers += jpp.NumTransfers

		// Iterate through this job part's transfers
		for t := uint32(0); t < jpp.NumTransfers; t++ {
			// transferHeader represents the memory map transfer header of transfer at index position for given job and part number
			jppt := jpp.Transfer(t)
			// check for all completed transfer to calculate the progress percentage at the end
			switch jppt.TransferStatus() {
			case common.ETransferStatus.Success():
				js.TotalNumberOfTransferCompleted++
			case common.TransferStatus{}.Failed():
				js.TotalNumberOfFailedTransfer++
				// getting the source and destination for failed transfer at position - index
				src, dst := jpp.TransferSrcDstStrings(t)
				// appending to list of failed transfer
				js.FailedTransfers = append(js.FailedTransfers,
					common.TransferDetail{
						Src:            src,
						Dst:            dst,
						TransferStatus: common.ETransferStatus.Failed()}) // TODO: Optimize
			}
		}
	}

	// Job is completed if Job order is complete AND ALL transfers are completed/failed
	// FIX: active or inactive state, then job order is said to be completed if final part of job has been ordered.
	if (js.CompleteJobOrdered) && (js.TotalNumberOfTransfers == js.TotalNumberOfFailedTransfer+js.TotalNumberOfTransferCompleted) {
		js.JobStatus = common.JobStatus{}.Completed()
	}
	/*
		// get the throughput counts
		jobThroughput:= jm.Throughput()
		numOfBytesTransferredSinceLastCheckpoint := jobThroughput.CurrentBytes() - jobThroughput.LastCheckedBytes()
		if numOfBytesTransferredSinceLastCheckpoint == 0 {
			js.ThroughputInBytesPerSeconds = 0
		} else {
			js.ThroughputInBytesPerSeconds = float64(numOfBytesTransferredSinceLastCheckpoint) /
				time.Since(jobThroughput.LastCheckedTime()).Seconds()
		}
		// update the throughput state
		jobInfo.JobThroughPut.updateLastCheckedBytes(jobInfo.JobThroughPut.getCurrentBytes())
		jobInfo.JobThroughPut.updateLastCheckTime(time.Now())
	*/
	return js
}

// getTransferList api returns the list of transfer with specific status for given jobId in http response
func getTransferList(jobID common.JobID, ofStatus common.TransferStatus) common.ListJobTransfersResponse {
	// getJobPartInfoReferenceFromMap gives the JobPartPlanInfo Pointer for given JobId and partNumber
	jm, found := JobsAdmin.JobMgr(jobID)
	if !found {
		return common.ListJobTransfersResponse{
			ErrorMsg: fmt.Sprintf("there is no active Job with jobId %s", jobID),
		}
	}

	ljt := common.ListJobTransfersResponse{
		JobID:   jobID,
		Details: []common.TransferDetail{},
	}
	for partNum := PartNumber(0); true; partNum++ {
		jpm, found := jm.JobPartMgr(partNum)
		if !found {
			break
		}
		// jPartPlan represents the memory map JobPartPlanHeader for given jobid and part number
		jpp := jpm.Plan()
		//numTransfer := jPartPlan.NumTransfers
		// transferStatusList represents the list containing number of transfer for given jobID and part number
		for t := uint32(0); t < jpp.NumTransfers; t++ {
			// getting transfer header of transfer at index index for given jobId and part number
			transferEntry := jpp.Transfer(t)
			// if the expected status is not to list all transfer and status of current transfer is not equal to the expected status, then we skip this transfer
			if ofStatus != common.ETransferStatus.All() && transferEntry.TransferStatus() != ofStatus {
				continue
			}
			// getting source and destination of a transfer at index index for given jobId and part number.
			src, dst := jpp.TransferSrcDstStrings(t)
			ljt.Details = append(ljt.Details,
				common.TransferDetail{Src: src, Dst: dst, TransferStatus: transferEntry.TransferStatus()})
		}
	}
	return ljt
}

// listExistingJobs returns the jobId of all the jobs existing in the current instance of azcopy
func listExistingJobs() common.ListJobsResponse {
	// building the ListJobsResponse for sending response back to front-end
	return common.ListJobsResponse{ErrorMessage: "", JobIDs: JobsAdmin.JobIDs()}
}

// todo use this in case of panic
func assertOK(err error) {
	if err != nil {
		panic(err)
	}
}
