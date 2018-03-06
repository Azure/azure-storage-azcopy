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
	"errors"
	"fmt"
	"github.com/Azure/azure-storage-azcopy/common"
	"io/ioutil"
	"log"
	"net/http"
	"os"
	"time"
)

var emptyJobId = common.JobID{}
var steContext = context.Background()

// scheduleTransfers schedules the transfer of a JobPart order
// It does not schedules those transfers which are either completed or failed
func scheduleTransfers(jobId common.JobID, partNumber common.PartNumber, jobsInfoMap *JobsInfo, coordinatorChannels *CoordinatorChannels) {

	jobPartInfo := jobsInfoMap.JobInfo(jobId).JobPartPlanInfo(partNumber)

	// jPartPlanHeader represents the memory map JobPartPlan header
	jPartPlanHeader := jobPartInfo.getJobPartPlanPointer()

	// priority determines which priority channel the transfers of current jobpart order will be scheduled to
	priority := jPartPlanHeader.Priority

	// jobInfo defines the JobInfo instance for the current Job
	jobInfo := jobsInfoMap.JobInfo(jobId)

	for index := uint32(0); index < jPartPlanHeader.NumTransfers; index++ {
		transferCtx, transferCancelFunc := context.WithCancel(jobPartInfo.ctx)
		jobPartInfo.TransfersInfo[index] = TransferInfo{ctx: transferCtx, cancel: transferCancelFunc, NumberOfChunksDone: 0}
		currentTransferStatus := jobPartInfo.Transfer(index).transferStatus()
		source, destination := jobPartInfo.getTransferSrcDstDetail(index)
		//if the current transfer is already complete or failed, then it won't be scheduled
		if currentTransferStatus == common.TransferComplete ||
			currentTransferStatus == common.TransferFailed {
			jobPartInfo.numberOfTransfersDone_doNotUse++
			continue
		}
		sourceSize := jobPartInfo.Transfer(index).SourceSize
		blockSize := jPartPlanHeader.BlobData.BlockSize
		// creating transfer msg to schedule the transfer and queuing transferMsg into channels determined by the JobPriority
		transferMsg := TransferMsg{partNumber: partNumber, transferIndex: index, jobInfo: jobInfo, TransferContext: jobPartInfo.TransfersInfo[index].ctx,
			TransferCancelFunc: transferCancelFunc, MinimumLogLevel: jPartPlanHeader.LogSeverity, BlobType: jPartPlanHeader.BlobData.BlobType, SourceType: jPartPlanHeader.SrcLocationType,
			DestinationType: jPartPlanHeader.DstLocationType, Source: source, SourceSize: sourceSize, Destination: destination,
			NumChunks: getNumChunks(sourceSize, uint64(blockSize)), BlockSize: blockSize}
		switch priority {
		case HighJobPriority:
			coordinatorChannels.HighTransfer <- transferMsg
			jobInfo.Log(common.LogInfo,
				fmt.Sprintf("successfully scheduled transfer %v with priority %v for Job %v and part number %v",
					index, priority, jobId, partNumber))
		case LowJobPriority:
			coordinatorChannels.LowTransfer <- transferMsg
			jobInfo.Log(common.LogInfo,
				fmt.Sprintf("successfully scheduled transfer %v with priority %v for Job %v and part number %v",
					index, priority, jobId, partNumber))
		default:
			jobInfo.Log(common.LogInfo,
				fmt.Sprintf("invalid job part order priority %d for given Job jobId %s and part number %d and transfer Index %d",
					priority, jobId, partNumber, index))
		}
	}
}

// ExecuteNewCopyJobPartOrder api executes a new job part order
/*
* payload -- It is the input data for new job part order.
* coordinatorChannels -- coordinator channels has the High, Med and Low transfer msg channel to schedule the incoming transfers.
* jPartPlanInfoMap -- Map to hold JobPartPlanInfo reference for combination of JobId and part number.
* jobToLoggerMap -- Map to hold the logger instance specific to a job
 */
func ExecuteNewCopyJobPartOrder(payload common.CopyJobPartOrderRequest, coordinatorChannels *CoordinatorChannels,
	jobsInfoMap *JobsInfo, resp http.ResponseWriter) {
	/*
	* Convert the optional attributes of job part order to memory map compatible DestinationBlobData
	* Create a file for JobPartOrder and write data into that file.
	* Initialize the JobPartOrder
	* Initializes the logger for the new job
	*  Create a JobPartPlanInfo pointer for the new job and put it into the map
	* Schedule the transfers of Job by putting them into Transfermsg channels.
	 */

	data := payload.OptionalAttributes

	// Converting the optional attributes of job part order to memory map compatible DestinationBlobData
	destBlobData, err := dataToDestinationBlobData(data)
	if err != nil {
		panic(err)
	}

	jobId := payload.ID
	// unMarshalling the JobID to get the JobID passed from front-end
	//var jobId common.JobID
	//err = json.Unmarshal([]byte(payload.ID), &jobId)
	//if err != nil {
	//	panic(err)
	//}

	// Creating a file for JobPartOrder and write data into that file.
	fileName := createJobPartPlanFile(payload, destBlobData, jobsInfoMap)
	// If file creation fails, then request is terminated as a bad request

	// Creating JobPartPlanInfo reference for new job part order
	var jobHandler = new(JobPartPlanInfo)

	// creating context with cancel for the new job
	//jobHandler.ctx, jobHandler.cancel = context.WithCancel(context.Background())

	// Initializing the JobPartPlanInfo for new job
	(jobHandler).initialize(context.Background(), fileName)

	jobsInfoMap.AddJobPartPlanInfo(jobHandler)

	if coordinatorChannels == nil { // If the coordinator transfer channels are initialized properly, then incoming transfers can't be scheduled with current instance of transfer engine.
		jobsInfoMap.JobInfo(common.JobID(jobId)).Log(common.LogError, "coordinator channels not initialized properly")
	}

	// Scheduling each transfer in the new job according to the priority of the job
	scheduleTransfers(common.JobID(jobId), payload.PartNum, jobsInfoMap, coordinatorChannels)

	copyResponse := common.CopyJobPartOrderResponse{JobStarted: true}

	resultMsg, err := json.Marshal(copyResponse)

	if err != nil {
		panic(err)
	}
	// sending successful response back to front end
	resp.WriteHeader(http.StatusAccepted)
	resp.Write(resultMsg)
}

// SetJobStatus changes the status of Job in all parts of Job order to given status
func setJobStatus(jobId common.JobID, jobsInfoMap *JobsInfo, status JobStatusCode) {
	jobInfo := jobsInfoMap.JobInfo(jobId)
	// loading the jobPartPlanHeader for part number 0
	jPartPlanHeader := jobInfo.JobPartPlanInfo(0).getJobPartPlanPointer()
	if jPartPlanHeader == nil {
		panic(errors.New(fmt.Sprintf("no job found with JobId %s to clean up", jobId)))
	}
	// changing the JobPart status to given status
	jPartPlanHeader.SetJobStatus(status)
	jobInfo.Log(common.LogInfo, fmt.Sprintf("changed the status of Job %s to status %s", jobId, status.String()))
}

// ResumeJobOrder resumes the JobOrder for given JobId
/*
	* Checks the current JobStatus of the Job and if it is in JobInProgress, then send the HTTP Response
    * Iterate through each JobPartOrder of the Job and refresh the cancelled context of the JobPart Order
	* Iterate through each transfer of JobPart order and refresh the cancelled context of the transfer
    * Reschedule each transfer again into the transfer msg channel depending on the priority of the channel
*/
func ResumeJobOrder(jobId common.JobID, jobsInfoMap *JobsInfo, coordinatorChannels *CoordinatorChannels, resp http.ResponseWriter) {
	jobInfo := jobsInfoMap.JobInfo(jobId)
	resumeJobResponse := common.CancelPauseResumeResponse{}
	if jobInfo == nil || jobInfo.JobParts() == nil {
		resumeJobResponse.CancelledPauseResumed = false
		resumeJobResponse.ErrorMsg = fmt.Sprintf("no active job with JobId %s exists", jobId)
	} else {
		currentJobStatus := jobInfo.JobPartPlanInfo(0).getJobPartPlanPointer().Status()
		if currentJobStatus == JobInProgress || currentJobStatus == JobCompleted {
			resumeJobResponse.CancelledPauseResumed = false
			resumeJobResponse.ErrorMsg = fmt.Sprintf("Job %s is already %s", jobId, currentJobStatus.String())
			goto SendResponse
		}
		// set job status to JobInProgress
		setJobStatus(jobId, jobsInfoMap, JobInProgress)
		jobInfo.setNumberOfPartsDone(0)
		for partNumber, jPartPlanInfo := range jobInfo.JobParts() {
			jPartPlanInfo.ctx, jPartPlanInfo.cancel = context.WithCancel(context.Background())
			// reset in memory number of transfer done
			jPartPlanInfo.numberOfTransfersDone_doNotUse = 0
			// schedule transfer job part order
			scheduleTransfers(jobId, partNumber, jobsInfoMap, coordinatorChannels)
			// If all the transfer of the current part are either complete or failed, then the part is complete
			// There is no transfer in this part that is rescheduled
			if jPartPlanInfo.numberOfTransfersDone_doNotUse == jPartPlanInfo.getJobPartPlanPointer().NumTransfers {
				jobInfo.PartsDone()
			}
		}
		// If all the number of parts that are already done equals the total number of parts in Job
		// No need to resume the Job since there are no transfer to reschedule
		if jobInfo.numberOfPartsDone == jobInfo.NumberOfParts() {
			jobInfo.Log(common.LogInfo, fmt.Sprintf("all parts of Job %s are already complete and no transfer needs to be rescheduled", jobId))
			setJobStatus(jobId, jobsInfoMap, JobCompleted)
			resumeJobResponse.CancelledPauseResumed = false
			resumeJobResponse.ErrorMsg = fmt.Sprintf("Job %s was already complete hence no need to resume it", jobId)
			goto SendResponse
		}
		resumeJobResponse.CancelledPauseResumed = true
		jobInfo.Log(common.LogInfo, fmt.Sprintf("Job %s resumed and has been rescheduled", jobId))
	}
SendResponse:
	resp.WriteHeader(http.StatusAccepted)
	response, err := json.Marshal(resumeJobResponse)
	if err != nil {
		jobInfo.Panic(err)
	}
	resp.Write([]byte(response))
}

// PauseJobOrder api process the process job order request from front-end
func PauseJobOrder(jobId common.JobID, jobsInfoMap *JobsInfo, resp http.ResponseWriter) {
	cancelpauseJobOrder(jobId, jobsInfoMap, true, resp)
}

func CancelJobOrder(jobId common.JobID, jobsInfoMap *JobsInfo, resp http.ResponseWriter) {
	cancelpauseJobOrder(jobId, jobsInfoMap, false, resp)
}

// cancelpauseJobOrder api cancel/pause a job with given JobId
/* A Job cannot be cancelled/paused in following cases
	* If the Job has not been ordered completely it cannot be cancelled or paused
    * If all the transfers in the Job are either failed or completed, then Job cannot be cancelled or paused
    * If a job is already paused, it cannot be paused again
*/
func cancelpauseJobOrder(jobId common.JobID, jobsInfoMap *JobsInfo, isPaused bool, resp http.ResponseWriter) {
	jobInfo := jobsInfoMap.JobInfo(jobId)
	cancelJobResponse := common.CancelPauseResumeResponse{}
	if jobInfo == nil || jobInfo.JobParts() == nil {
		cancelJobResponse.CancelledPauseResumed = false
		cancelJobResponse.ErrorMsg = fmt.Sprintf("no active job with JobId %s exists", jobId.String())
		goto SendResponse
	} else {
		// completeJobOrdered determines whether final part for job with JobId has been ordered or not.
		var completeJobOrdered bool = false
		for _, jHandler := range jobInfo.JobParts() {
			// currentJobPartPlanInfo represents the memory map JobPartPlanHeader for current partNo
			currentJobPartPlanInfo := jHandler.getJobPartPlanPointer()
			completeJobOrdered = completeJobOrdered || currentJobPartPlanInfo.IsFinalPart
		}

		// If the job has not been ordered completely, then job cannot be cancelled
		if !completeJobOrdered {
			cancelJobResponse.CancelledPauseResumed = false
			cancelJobResponse.ErrorMsg = fmt.Sprintf("job with JobId %s hasn't been ordered completely", jobId.String())
			goto SendResponse
		}
		// If all parts of the job has either completed or failed, then job cannot be cancelled since it is already finished
		jPartPlanHeaderForPart0 := jobInfo.JobPartPlanInfo(0).getJobPartPlanPointer()
		if jPartPlanHeaderForPart0.Status() == JobCompleted {
			cancelJobResponse.CancelledPauseResumed = false
			if isPaused {
				cancelJobResponse.ErrorMsg = fmt.Sprintf("job with JobId %s has already completed, hence cannot pause the job", jobId.String())
			} else {
				cancelJobResponse.ErrorMsg = fmt.Sprintf("job with JobId %s has already completed, hence cannot cancel the job", jobId.String())
			}
			goto SendResponse
		}
		// If the Job is currently paused
		if jPartPlanHeaderForPart0.Status() == JobPaused {
			// If an already paused job is set to pause again
			if isPaused {
				cancelJobResponse.CancelledPauseResumed = false
				cancelJobResponse.ErrorMsg = fmt.Sprintf("job with JobId %s i already paused, cannot pause it again", jobId.String())
			} else {
				// If an already paused job has to be cancelled, then straight cleaning the paused job
				setJobStatus(jobId, jobsInfoMap, JobCancelled)
				cancelJobResponse.CancelledPauseResumed = true
				cancelJobResponse.ErrorMsg = fmt.Sprintf("succesfully cancelling job with JobId %s", jobId.String())
			}
			goto SendResponse
		}
		if isPaused {
			setJobStatus(jobId, jobsInfoMap, JobPaused)
		} else {
			setJobStatus(jobId, jobsInfoMap, JobCancelled)
		}
		// Iterating through all JobPartPlanInfo pointers and cancelling each part of the given Job
		for _, jHandler := range jobInfo.JobParts() {
			jHandler.cancel()
		}
		cancelJobResponse.CancelledPauseResumed = true
		cancelJobResponse.ErrorMsg = fmt.Sprintf("succesfully cancelling job with JobId %s", jobId.String())
	}
SendResponse:
	resp.WriteHeader(http.StatusAccepted)
	response, err := json.Marshal(cancelJobResponse)
	if err != nil {
		jobInfo.Panic(err)
	}
	resp.Write([]byte(response))
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
func getJobSummary(jobId common.JobID, jobsInfo *JobsInfo, resp http.ResponseWriter) {

	//fmt.Println("received a get job order status request for JobId ", jobId)
	// getJobPartMapFromJobPartInfoMap gives the map of partNo to JobPartPlanInfo Pointer for a given JobId
	jobInfo := jobsInfo.JobInfo(jobId)

	jobSummaryResponse := common.ListJobSummaryResponse{}
	// if partNumber to JobPartPlanInfo Pointer map is nil, then returning error
	if jobInfo == nil || jobInfo.JobParts() == nil {
		jobSummaryResponse.ErrorMessage = fmt.Sprintf("no active job with JobId %s exists", jobId)
	} else {
		jobSummaryResponse.JobId = jobId
		// completeJobOrdered determines whether final part for job with JobId has been ordered or not.
		var completeJobOrdered bool = false
		// failedTransfers represents the list of transfers which failed after the last checkpoint timestamp
		var failedTransfers []common.TransferDetail

		for _, jHandler := range jobInfo.JobParts() {
			// currentJobPartPlanInfo represents the memory map JobPartPlanHeader for current partNo
			currentJobPartPlanInfo := jHandler.getJobPartPlanPointer()

			completeJobOrdered = completeJobOrdered || currentJobPartPlanInfo.IsFinalPart
			jobSummaryResponse.TotalNumberOfTransfers += currentJobPartPlanInfo.NumTransfers
			// iterating through all transfers for current partNo and job with given jobId
			for index := uint32(0); index < currentJobPartPlanInfo.NumTransfers; index++ {

				// transferHeader represents the memory map transfer header of transfer at index position for given job and part number
				transferHeader := jHandler.Transfer(index)
				// check for all completed transfer to calculate the progress percentage at the end
				if transferHeader.transferStatus() == common.TransferComplete {
					jobSummaryResponse.TotalNumberofTransferCompleted++
				}
				if transferHeader.transferStatus() == common.TransferFailed {
					jobSummaryResponse.TotalNumberofFailedTransfer++
					// getting the source and destination for failed transfer at position - index
					source, destination := jHandler.getTransferSrcDstDetail(index)
					// appending to list of failed transfer
					failedTransfers = append(failedTransfers, common.TransferDetail{Src: source, Dst: destination, TransferStatus: common.TransferFailed.String()})
				}
			}
		}
		/*If each transfer in all parts of a job has either completed or failed and is not in active or inactive state, then job order is said to be completed
		if final part of job has been ordered.*/
		if (jobSummaryResponse.TotalNumberOfTransfers == jobSummaryResponse.TotalNumberofFailedTransfer+jobSummaryResponse.TotalNumberofTransferCompleted) && (completeJobOrdered) {
			jobSummaryResponse.JobStatus = JobCompleted.String()
		} else {
			jobSummaryResponse.JobStatus = JobInProgress.String()
		}
		jobSummaryResponse.CompleteJobOrdered = completeJobOrdered
		jobSummaryResponse.FailedTransfers = failedTransfers
		jobSummaryResponse.PercentageProgress = ((jobSummaryResponse.TotalNumberofTransferCompleted + jobSummaryResponse.TotalNumberofFailedTransfer) * 100) / jobSummaryResponse.TotalNumberOfTransfers

		// get the throughput counts
		numOfBytesTransferredSinceLastCheckpoint := jobInfo.JobThroughPut.getCurrentBytes() - jobInfo.JobThroughPut.getLastCheckedBytes()
		if numOfBytesTransferredSinceLastCheckpoint == 0 {
			jobSummaryResponse.ThroughputInBytesPerSeconds = 0
		} else {
			jobSummaryResponse.ThroughputInBytesPerSeconds = float64(numOfBytesTransferredSinceLastCheckpoint) / time.Since(jobInfo.JobThroughPut.getLastCheckedTime()).Seconds()
		}
		// update the throughput state
		jobInfo.JobThroughPut.updateLastCheckedBytes(jobInfo.JobThroughPut.getCurrentBytes())
		jobInfo.JobThroughPut.updateLastCheckTime(time.Now())
	}

	// marshalling the ListJobSummaryResponse struct to send back in response.
	jobProgressSummaryJson, err := json.MarshalIndent(jobSummaryResponse, "", "")
	if err != nil {
		jobInfo.Panic(fmt.Errorf("error marshalling the progress summary for Job jobId %s", jobId))
	}
	resp.WriteHeader(http.StatusAccepted)
	resp.Write(jobProgressSummaryJson)
}

// getTransferList api returns the list of transfer with specific status for given jobId in http response
func getTransferList(jobId common.JobID, ofStatus common.TransferStatus, jobsInfo *JobsInfo, resp http.ResponseWriter) {
	// getJobPartInfoReferenceFromMap gives the JobPartPlanInfo Pointer for given JobId and partNumber
	jobInfo := jobsInfo.JobInfo(jobId)
	listJobTransfersResponse := common.ListJobTransfersResponse{}
	// sending back the error status and error message in response
	if jobInfo == nil || jobInfo.JobParts() == nil {
		listJobTransfersResponse.ErrorMessage = fmt.Sprintf("there is no active Job with jobId %s", jobId)
	} else {
		var transferList []common.TransferDetail
		for _, jHandler := range jobInfo.JobParts() {
			// jPartPlan represents the memory map JobPartPlanHeader for given jobid and part number
			jPartPlan := jHandler.getJobPartPlanPointer()
			//numTransfer := jPartPlan.NumTransfers

			// trasnferStatusList represents the list containing number of transfer for given jobid and part number
			for index := uint32(0); index < jPartPlan.NumTransfers; index++ {
				// getting transfer header of transfer at index index for given jobId and part number
				transferEntry := jHandler.Transfer(index)
				// if the expected status is not to list all transfer and status of current transfer is not equal to the expected status, then we skip this transfer
				if ofStatus != common.TransferAny && transferEntry.transferStatus() != ofStatus {
					continue
				}
				// getting source and destination of a transfer at index index for given jobId and part number.
				source, destination := jHandler.getTransferSrcDstDetail(index)
				transferList = append(transferList, common.TransferDetail{Src: source, Dst: destination, TransferStatus: transferEntry.transferStatus().String()})
			}
		}
		listJobTransfersResponse.JobId = jobId
		listJobTransfersResponse.Details = transferList
	}
	// marshalling the ListJobTransfersResponse Struct to send back in response to front-end
	tStatusJson, err := json.MarshalIndent(listJobTransfersResponse, "", "")
	if err != nil {
		jobInfo.Panic(fmt.Errorf("error marshalling the transfer status for Job jobId %s", jobId))
	}
	resp.WriteHeader(http.StatusAccepted)
	resp.Write(tStatusJson)
}

// listExistingJobs returns the jobId of all the jobs existing in the current instance of azcopy
func listExistingJobs(jPartPlanInfoMap *JobsInfo, commonLogger *log.Logger, resp http.ResponseWriter) {
	// get the list of all the jobId from the JobsInfo
	jobIds := jPartPlanInfoMap.JobIds()

	// building the ListJobsResponse for sending response back to front-end
	existingJobDetails := common.ListJobsResponse{Errormessage: "", JobIds: jobIds}
	existingJobDetailsJson, err := json.Marshal(existingJobDetails)
	if err != nil {
		commonLogger.Panic("error marshalling the existing job list ")
	}
	resp.WriteHeader(http.StatusAccepted)
	resp.Write(existingJobDetailsJson)
}

// todo use this in case of panic
func assertOK(err error) {
	if err != nil {
		panic(err)
	}
}

// parseAndRouteHttpRequest parses the incoming http request from front-end and route it to respective api
func parseAndRouteHttpRequest(req *http.Request, coordinatorChannels *CoordinatorChannels,
	jobsInfoMap *JobsInfo, commonLogger *log.Logger, resp http.ResponseWriter) {

	// each request from front end has command type passed as a query param
	// commandType include copy, cancel, list, pause, resume
	var requestType = req.URL.Query()["commandType"][0]

	// reading the entire request body and closing the request body
	body, err := ioutil.ReadAll(req.Body)
	req.Body.Close()
	if err != nil {
		panic(fmt.Errorf("error reading the HTTP Request Body"))
	}

	switch requestType {
	// CopyJobPartOrderRequest request
	case "copy":
		var payload common.CopyJobPartOrderRequest
		err = json.Unmarshal(body, &payload)
		if err != nil {
			panic(fmt.Errorf("error UnMarshalling the HTTP Request Body"))
		}
		ExecuteNewCopyJobPartOrder(payload, coordinatorChannels, jobsInfoMap, resp)

	case "listJobs":
		var lsRequest common.ListRequest
		err := json.Unmarshal(body, &lsRequest)
		if err != nil {
			panic(err)
		}
		listExistingJobs(jobsInfoMap, commonLogger, resp)
	case "listJobProgressSummary":
		var lsRequest common.ListRequest
		err := json.Unmarshal(body, &lsRequest)
		if err != nil {
			panic(err)
		}
		getJobSummary(lsRequest.JobId, jobsInfoMap, resp)
	case "listJobTransfers":
		var lsRequest common.ListRequest
		err := json.Unmarshal(body, &lsRequest)
		if err != nil {
			panic(err)
		}
		getTransferList(lsRequest.JobId, common.TransferStatusStringToCode(lsRequest.OfStatus), jobsInfoMap, resp)
	case "cancel":
		var jobId common.JobID
		err := json.Unmarshal(body, &jobId)
		if err != nil {
			panic(err)
		}
		CancelJobOrder(jobId, jobsInfoMap, resp)
	case "pause":
		var jobId common.JobID
		err := json.Unmarshal(body, &jobId)
		if err != nil {
			panic(err)
		}
		PauseJobOrder(common.JobID(jobId), jobsInfoMap, resp)
	case "resume":
		var jobId common.JobID
		err := json.Unmarshal(body, &jobId)
		if err != nil {
			panic(err)
		}
		ResumeJobOrder(common.JobID(jobId), jobsInfoMap, coordinatorChannels, resp)
	default:
		// If the commandType doesn't fall into any of the case
		// then it is considered to be a Bad Request.
		resp.WriteHeader(http.StatusBadRequest)
		resp.Write([]byte("invalid command request received fromt front end"))
	}
}

// serveRequest process the incoming http request
/*
	* resp -- It is the http response write for send back the response for the http request
    * req  -- It is the new http request received
	* coordinatorChannels -- These are the High, Med and Low transfer channels for scheduling the incoming transfer. These channel are required in case of New JobPartOrder request
    * jobToLoggerMap -- This Map holds the logger instance for each job
*/
func serveRequest(resp http.ResponseWriter, req *http.Request, coordinatorChannels *CoordinatorChannels, jobsInfoMap *JobsInfo, commonLogger *log.Logger) {
	commonLogger.Println(fmt.Sprintf("http request recieved of type %s from %s for time %d", req.Method, req.RemoteAddr, time.Now().Second()))
	switch req.Method {
	case http.MethodPost:
		commonLogger.Println(fmt.Sprintf("http put request recieved of type %s from %s for time %s", req.Method, req.RemoteAddr, time.Now()))
		parseAndRouteHttpRequest(req, coordinatorChannels, jobsInfoMap, commonLogger, resp)
	case http.MethodGet:
		fallthrough
	case http.MethodPut:
		fallthrough
	case http.MethodDelete:
		fallthrough
	default:
		fmt.Println("Operation Not Supported by STE")
		resp.WriteHeader(http.StatusBadRequest)
		resp.Write([]byte("Not able to trigger the AZCopy request"))
	}
}

// InitializeChannels initializes the channels used further by coordinator and execution engine
func InitializeChannels(commonLogger *log.Logger) (*CoordinatorChannels, *EEChannels) {
	commonLogger.Println("initializing channels for execution engine and coordinator")
	// HighTransferMsgChannel takes high priority job part transfers from coordinator and feed to execution engine
	HighTransferMsgChannel := make(chan TransferMsg, 100000)

	// LowTransferMsgChannel takes low priority job part transfers from coordinator and feed to execution engine
	LowTransferMsgChannel := make(chan TransferMsg, 100000)

	// HighChunkMsgChannel queues high priority job part transfer chunk transactions
	HighChunkMsgChannel := make(chan ChunkMsg, 100000)

	// LowChunkMsgChannel queues low priority job part transfer chunk transactions
	LowChunkMsgChannel := make(chan ChunkMsg, 100000)

	// Create suicide channel which is used to scale back on the number of workers
	SuicideChannel := make(chan SuicideJob, 100000)

	transferEngineChannel := &CoordinatorChannels{
		HighTransfer: HighTransferMsgChannel,
		LowTransfer:  LowTransferMsgChannel,
	}

	executionEngineChanel := &EEChannels{
		HighTransfer:   HighTransferMsgChannel,
		LowTransfer:    LowTransferMsgChannel,
		HighChunk:      HighChunkMsgChannel,
		LowChunk:       LowChunkMsgChannel,
		SuicideChannel: SuicideChannel,
	}
	commonLogger.Println("successfully initialized channels for execution engine and coordinator")
	return transferEngineChannel, executionEngineChanel
}

// initializeAzCopyLogger initializes the logger instance for logging logs not related to any job order
func initializeAzCopyLogger(filename string) *log.Logger {
	// Creates the log file if it does not exists already else opens the file in append mode.
	file, err := os.OpenFile(filename, os.O_RDWR|os.O_CREATE|os.O_APPEND, 0666)
	if err != nil {
		panic(err)
	}
	return log.New(file, "", log.Llongfile)
}

// initializeCoordinator initializes the coordinator
/*
* reconstructs the existing job using job part file on disk
* creates a server listening on port 1337 for job part order requests from front end
 */
func initializeCoordinator(coordinatorChannels *CoordinatorChannels, commonLogger *log.Logger) error {

	jobHandlerMap := NewJobsInfo()
	reconstructTheExistingJobParts(jobHandlerMap, coordinatorChannels)
	http.HandleFunc("/", func(writer http.ResponseWriter, request *http.Request) {
		serveRequest(writer, request, coordinatorChannels, jobHandlerMap, commonLogger)
	})
	err := http.ListenAndServe("localhost:1337", nil)
	fmt.Print("Server Created")
	if err != nil {
		fmt.Print("Server already initialized")
	}
	return err
}

// InitializeSTE initializes the coordinator channels, execution engine channels, coordinator and execution engine
func InitializeSTE(numOfEngineWorker int, targetRateInMBps int) error {
	commonLogger := initializeAzCopyLogger("azCopyNg-Common.log")
	coordinatorChannel, execEngineChannels := InitializeChannels(commonLogger)
	go newExecutionEngine(execEngineChannels, numOfEngineWorker, targetRateInMBps).start()
	return initializeCoordinator(coordinatorChannel, commonLogger)
}
