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
	"math"
	"net/http"
	"os"
	"sync/atomic"
	"time"
)

var steContext = context.Background()
var realTimeThroughputCounter = throughputState{lastCheckedBytes: 0, currentBytes: 0, lastCheckedTime: time.Now()}

// putJobPartInfoHandlerIntoMap api put the JobPartPlanInfo pointer for given jobId and part number in map[common.JobID]map[common.PartNumber]*JobPartPlanInfo
func putJobPartInfoHandlerIntoMap(jobHandler *JobPartPlanInfo, jobId common.JobID,
	partNo common.PartNumber, jobLogVerbosity common.LogLevel, jPartInfoMap *JobsInfoMap) {
	jPartInfoMap.StoreJobPartPlanInfo(jobId, partNo, jobLogVerbosity, jobHandler)
}

// getJobPartMapFromJobPartInfoMap api gets the map[common.PartNumber]*JobPartPlanInfo for given jobId and part number from map[common.JobID]map[common.PartNumber]*JobPartPlanInfo
func getJobPartMapFromJobPartInfoMap(jobId common.JobID,
	jPartInfoMap *JobsInfoMap) (jPartMap map[common.PartNumber]*JobPartPlanInfo) {
	jPartMap, ok := jPartInfoMap.LoadJobPartsMapForJob(jobId)
	if !ok {
		errorMsg := fmt.Sprintf("no part number exists for given jobId %s", jobId)
		panic(errors.New(errorMsg))
	}
	return jPartMap
}

// getJobPartInfoReferenceFromMap returns the JobPartPlanInfo Pointer for the combination of JobId and part number
func getJobPartInfoReferenceFromMap(jobId common.JobID, partNo common.PartNumber,
	jPartInfoMap *JobsInfoMap) (*JobPartPlanInfo, error) {
	jHandler := jPartInfoMap.LoadJobPartPlanInfoForJobPart(jobId, partNo)
	if jHandler == nil {
		errorMsg := fmt.Sprintf("no jobPartPlanInfo handler exists for JobId %s and part number %d", jobId, partNo)
		return nil, errors.New(errorMsg)
	}
	return jHandler, nil
}

// ExecuteNewCopyJobPartOrder api executes a new job part order
/*
* payload -- It is the input data for new job part order.
* coordiatorChannels -- coordinator channels has the High, Med and Low transfer msg channel to schedule the incoming transfers.
* jPartPlanInfoMap -- Map to hold JobPartPlanInfo reference for combination of JobId and part number.
* jobToLoggerMap -- Map to hold the logger instance specific to a job
 */
func ExecuteNewCopyJobPartOrder(payload common.CopyJobPartOrder, coordiatorChannels *CoordinatorChannels,
								jobsInfoMap *JobsInfoMap) {
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
	// Creating a file for JobPartOrder and write data into that file.
	fileName := createJobPartPlanFile(payload, destBlobData)

	// Creating JobPartPlanInfo reference for new job part order
	var jobHandler = new(JobPartPlanInfo)

	jobHandler.fileName = fileName

	// creating context with cancel for the new job
	jobHandler.ctx, jobHandler.cancel = context.WithCancel(context.Background())

	// Initializing the JobPartPlanInfo for new job
	(jobHandler).initialize(jobHandler.ctx, fileName)

	putJobPartInfoHandlerIntoMap(jobHandler, payload.ID, payload.PartNum, payload.LogVerbosity, jobsInfoMap)

	if coordiatorChannels == nil { // If the coordinator transfer channels are initialized properly, then incoming transfers can't be scheduled with current instance of transfer engine.
		getLoggerForJobId(payload.ID, jobsInfoMap).Logf(common.LogError, "coordinator channels not initialized properly")
	}
	// Scheduling each transfer in the new job according to the priority of the job
	numTransfer := jobHandler.getJobPartPlanPointer().NumTransfers
	for index := uint32(0); index < numTransfer; index++ {
		transferMsg := TransferMsg{payload.ID, payload.PartNum, index, jobsInfoMap}
		switch payload.Priority {
		case HighJobPriority:
			coordiatorChannels.HighTransfer <- transferMsg
			getLoggerForJobId(payload.ID, jobsInfoMap).Logf(common.LogInfo,
								"successfully scheduled transfer %v with priority %v for Job %v and part number %v",
									index, payload.Priority, string(payload.ID), payload.PartNum)
		case MediumJobPriority:
			coordiatorChannels.MedTransfer <- transferMsg
			getLoggerForJobId(payload.ID, jobsInfoMap).Logf(common.LogInfo,
				"successfully scheduled transfer %v with priority %v for Job %v and part number %v",
				index, payload.Priority, string(payload.ID), payload.PartNum)
		case LowJobPriority:
			coordiatorChannels.LowTransfer <- transferMsg
			getLoggerForJobId(payload.ID, jobsInfoMap).Logf(common.LogInfo,
				"successfully scheduled transfer %v with priority %v for Job %v and part number %v",
				index, payload.Priority, string(payload.ID), payload.PartNum)
		default:
			getLoggerForJobId(payload.ID, jobsInfoMap).Logf(common.LogInfo,
									"invalid job part order priority %d for given Job Id %s and part number %d and transfer Index %d",
										payload.Priority, payload.ID, payload.PartNum, index)
		}
	}
}

// getJobStatus api returns the current status of given JobId
func getJobStatus(jobId common.JobID, jobsInfoMap *JobsInfoMap) (JobStatusCode){
	logger := getLoggerForJobId(jobId, jobsInfoMap)
	jobInfo := jobsInfoMap.LoadJobPartPlanInfoForJobPart(jobId, 0)
	if jobInfo == nil{
		panic(errors.New(fmt.Sprintf("no job found with JobId %s to clean up", jobId)))
	}
	status := jobInfo.getJobPartPlanPointer().JobStatus
	logger.Logf(common.LogInfo, "current job status of JobId %s is %s", getJobStatusStringFromCode(status))
	return status
}

// changeJobStatus changes the status of Job in all parts of Job order to given status
func changeJobStatus(jobId common.JobID, jobsInfoMap *JobsInfoMap, status JobStatusCode){
	logger := getLoggerForJobId(jobId, jobsInfoMap)
	// loading the part map for given jobId
	jPartMap, ok := jobsInfoMap.LoadJobPartsMapForJob(jobId)
	if !ok{
		panic(errors.New(fmt.Sprintf("no job found with JobId %s to clean up", jobId)))
	}
	// changing the JobPart status to given status
	for _, jobHandler := range jPartMap {
		jobHandler.getJobPartPlanPointer().JobStatus = status
	}
	logger.Logf(common.LogInfo, "changed the status of Job %s to status %s", jobId, getJobStatusStringFromCode(status))
}

// cleanUpJob api unmaps all the memory map JobPartFile and deletes the JobPartFile
func cleanUpJob(jobId common.JobID, jobsInfoMap *JobsInfoMap){
	jPartMap, ok := jobsInfoMap.LoadJobPartsMapForJob(jobId)
	if !ok{
		panic(errors.New(fmt.Sprintf("no job found with JobId %s to clean up", jobId)))
	}
	for _, jobHandler := range jPartMap{
		// unmapping the memory map JobPart file
		err := jobHandler.memMap.Unmap()
		if err != nil{
			errorMsg := fmt.Sprintf("error unmapping the memory map file %s. Failed with following error %s", jobHandler.fileName, err.Error())
			getLoggerForJobId(jobId, jobsInfoMap).Logf(common.LogError, errorMsg)
			panic(errors.New(errorMsg))
		}
		// deleting the JobPartFile
		err = os.Remove(jobHandler.fileName)
		if err != nil{
			errorMsg := fmt.Sprintf("error removing the job part file %s. Failed with following error %s", jobHandler.fileName, err.Error())
			getLoggerForJobId(jobId, jobsInfoMap).Logf(common.LogError, errorMsg)
			panic(errors.New(errorMsg))
		}
	}

	// deletes the entry for given JobId from Map
	jobsInfoMap.DeleteJobInfoForJobId(jobId)
}

// ExecuteResumeJobOrder resumes the JobOrder for given JobId
/*
	* Checks the current JobStatus of the Job and if it is in InProgress, then send the HTTP Response
    * Iterate through each JobPartOrder of the Job and refresh the cancelled context of the JobPart Order
	* Iterate through each transfer of JobPart order and refresh the cancelled context of the transfer
    * Reschedule each transfer again into the transfer msg channel depending on the priority of the channel
 */
func ExecuteResumeJobOrder(jobId common.JobID, jobsInfoMap *JobsInfoMap, coordinatorChannels *CoordinatorChannels, resp *http.ResponseWriter){
	if getJobStatus(jobId, jobsInfoMap) == InProgress{
		(*resp).WriteHeader(http.StatusBadRequest)
		(*resp).Write([]byte(fmt.Sprintf("Job %s is already in Progress", jobId)))
	}
	jPartMap, ok := jobsInfoMap.LoadJobPartsMapForJob(jobId)
	if !ok{
		(*resp).WriteHeader(http.StatusBadRequest)
		errorMsg := fmt.Sprintf("no active job with JobId %s exists", jobId)
		(*resp).Write([]byte(errorMsg))
		return
	}
	logger := getLoggerForJobId(jobId, jobsInfoMap)
	for partNumber, jobInfo := range jPartMap{
		priority := jobInfo.getJobPartPlanPointer().Priority
		jobInfo.ctx, jobInfo.cancel = context.WithCancel(context.Background())
		for index := 0; index < len(jobInfo.TransferInfo); index++{
			// creating new context with cancel for the transfer of Job
			// since existing context have been cancelled
			jobInfo.TransferInfo[index].ctx, jobInfo.TransferInfo[index].cancel = context.WithCancel(jobInfo.ctx)
			transferMsg := TransferMsg{jobId, partNumber, uint32(index), jobsInfoMap}
			switch  priority{
			case HighJobPriority:
				coordinatorChannels.HighTransfer <- transferMsg
				logger.Logf(common.LogInfo,
					"successfully scheduled transfer %v with priority %v for Job %v and part number %v",
					index, priority, jobId, partNumber)
			case MediumJobPriority:
				coordinatorChannels.MedTransfer <- transferMsg
				logger.Logf(common.LogInfo,
					"successfully scheduled transfer %v with priority %v for Job %v and part number %v",
					index, priority, jobId, partNumber)
			case LowJobPriority:
				coordinatorChannels.LowTransfer <- transferMsg
				logger.Logf(common.LogInfo,
					"successfully scheduled transfer %v with priority %v for Job %v and part number %v",
					index, priority, jobId, partNumber)
			default:
				logger.Logf(common.LogInfo,
					"invalid job priority of transfer %v with priority %v for Job %v and part number %v",
					index, priority, jobId, partNumber)
				break
			}
		}
	}
	logger.Logf(common.LogInfo, "Job %s resumed and has been rescheduled", jobId)
}

// ExecuteCancelJobOrder api cancel a job with given JobId
/* A Job cannot be cancelled in following cases
	* If the Job has not been ordered completely
    * If all the transfers in the Job are either failed or completed, then Job cannot be cancelled
 */
func ExecuteCancelJobOrder(jobId common.JobID, jobsInfoMap *JobsInfoMap, isPaused bool, resp *http.ResponseWriter) {
	jPartMap, ok := jobsInfoMap.LoadJobPartsMapForJob(jobId)
	if !ok{
		(*resp).WriteHeader(http.StatusBadRequest)
		errorMsg := fmt.Sprintf("no active job with JobId %s exists", jobId)
		(*resp).Write([]byte(errorMsg))
		return
	}

	// completeJobOrdered determines whether final part for job with JobId has been ordered or not.
	var completeJobOrdered bool = false
	// totalNumberOfTransfers determines the total number of transfers in all parts of the given Job
	var totalNumberOfTransfers uint32 = 0
	// totalNumberOfTransfersCompleted determines the total number of completed transfers in all parts of the given Job
	var totalNumberOfTransfersCompleted uint32 = 0
	// totalNumberOfTransfersFailed determines the total number of failed transfers in all parts of the given Job
	var totalNumberOfTransfersFailed uint32 = 0
	for _, jHandler := range jPartMap {
		// currentJobPartPlanInfo represents the memory map JobPartPlanHeader for current partNo
		currentJobPartPlanInfo := jHandler.getJobPartPlanPointer()

		completeJobOrdered = completeJobOrdered || currentJobPartPlanInfo.IsFinalPart
		totalNumberOfTransfers += currentJobPartPlanInfo.NumTransfers
		// iterating through all transfers for current partNo and job with given jobId
		for index := uint32(0); index < currentJobPartPlanInfo.NumTransfers; index++ {

			// transferHeader represents the memory map transfer header of transfer at index position for given job and part number
			transferHeader := jHandler.Transfer(index)
			// check for all completed transfer to calculate the progress percentage at the end
			if transferHeader.Status == common.TransferStatusComplete {
				totalNumberOfTransfersCompleted ++
			}
			if transferHeader.Status == common.TransferStatusFailed {
				totalNumberOfTransfersFailed ++
			}
		}
	}
	// If the job has not been ordered completely, then job cannot be cancelled
	if !completeJobOrdered{
		(*resp).WriteHeader(http.StatusBadRequest)
		errorMsg := fmt.Sprintf("job with JobId %s hasn't been ordered completely", jobId)
		(*resp).Write([]byte(errorMsg))
		return
	}
	// If all parts of the job has either completed or failed, then job cannot be cancelled since it is already finished
	if totalNumberOfTransfers == (totalNumberOfTransfersFailed + totalNumberOfTransfersCompleted){
		errorMsg := ""
		(*resp).WriteHeader(http.StatusBadRequest)
		if isPaused{
			errorMsg = fmt.Sprintf("job with JobId %s is already completed, hence cannot pause the job", jobId)
		}else {
			errorMsg = fmt.Sprintf("job with JobId %s is already completed, hence cannot cancel the job", jobId)
		}
		(*resp).Write([]byte(errorMsg))
		return
	}
	// Iterating through all JobPartPlanInfo pointers and cancelling each part of the given Job
	for _, jHandler := range jPartMap{
		jHandler.cancel()
	}

	// If the Job is paused but not cancelled
	if isPaused{
		// change the JobStatus from InProgress to Paused State
		changeJobStatus(jobId, jobsInfoMap, Paused)
	}else{ // If the Job is not paused but cancelled
		// cleaning up the job from JobsInfoMap and removing the JobPartsFile
		cleanUpJob(jobId, jobsInfoMap)
	}
	(*resp).WriteHeader(http.StatusAccepted)
	resultMsg := ""
	if isPaused{
		resultMsg = fmt.Sprintf("succesfully paused job with JobId %s", jobId)
	}else{
		resultMsg = fmt.Sprintf("succesfully cancelled job with JobId %s", jobId)
	}
	(*resp).Write([]byte(resultMsg))
}

// getJobSummary api returns the job progress summary of an active job
/*
* Return following Properties in Job Progress Summary
* CompleteJobOrdered - determines whether final part of job has been ordered or not
* TotalNumberOfTransfer - total number of transfers available for the given job
* TotalNumberofTransferCompleted - total number of transfers in the job completed
* NumberOfTransferCompletedafterCheckpoint - number of transfers completed after the last checkpoint
* NumberOfTransferFailedAfterCheckpoint - number of transfers failed after last checkpoint timestamp
* PercentageProgress - job progress reported in terms of percentage
* FailedTransfers - list of transfer after last checkpoint timestamp that failed.
 */
func getJobSummary(jobId common.JobID, jPartPlanInfoMap *JobsInfoMap, resp *http.ResponseWriter) {

	//fmt.Println("received a get job order status request for JobId ", jobId)
	// getJobPartMapFromJobPartInfoMap gives the map of partNo to JobPartPlanInfo Pointer for a given JobId
	jPartMap := getJobPartMapFromJobPartInfoMap(jobId, jPartPlanInfoMap)

	// if partNumber to JobPartPlanInfo Pointer map is nil, then returning error
	if jPartMap == nil {
		(*resp).WriteHeader(http.StatusBadRequest)
		errorMsg := fmt.Sprintf("no active job with JobId %s exists", jobId)
		(*resp).Write([]byte(errorMsg))
		return
	}

	// completeJobOrdered determines whether final part for job with JobId has been ordered or not.
	var completeJobOrdered bool = false
	// failedTransfers represents the list of transfers which failed after the last checkpoint timestamp
	var failedTransfers []common.TransferStatus

	progressSummary := common.JobProgressSummary{}
	for _, jHandler := range jPartMap {
		// currentJobPartPlanInfo represents the memory map JobPartPlanHeader for current partNo
		currentJobPartPlanInfo := jHandler.getJobPartPlanPointer()

		completeJobOrdered = completeJobOrdered || currentJobPartPlanInfo.IsFinalPart
		progressSummary.TotalNumberOfTransfer += currentJobPartPlanInfo.NumTransfers
		// iterating through all transfers for current partNo and job with given jobId
		for index := uint32(0); index < currentJobPartPlanInfo.NumTransfers; index++ {

			// transferHeader represents the memory map transfer header of transfer at index position for given job and part number
			transferHeader := jHandler.Transfer(index)
			// check for all completed transfer to calculate the progress percentage at the end
			if transferHeader.Status == common.TransferStatusComplete {
				progressSummary.TotalNumberofTransferCompleted ++
			}
			if transferHeader.Status == common.TransferStatusFailed {
				progressSummary.TotalNumberofFailedTransfer ++
				// getting the source and destination for failed transfer at position - index
				source, destination := jHandler.getTransferSrcDstDetail(index)
				// appending to list of failed transfer
				failedTransfers = append(failedTransfers, common.TransferStatus{source, destination, common.TransferStatusFailed})
			}
		}
	}
	/*If each transfer in all parts of a job has either completed or failed and is not in active or inactive state, then job order is said to be completed
	if final part of job has been ordered.*/
	if (progressSummary.TotalNumberOfTransfer == progressSummary.TotalNumberofFailedTransfer+progressSummary.TotalNumberofTransferCompleted) && (completeJobOrdered) {
		progressSummary.JobStatus = common.StatusCompleted
	} else {
		progressSummary.JobStatus = common.StatusInProgress
	}
	progressSummary.CompleteJobOrdered = completeJobOrdered
	progressSummary.FailedTransfers = failedTransfers
	progressSummary.PercentageProgress = ((progressSummary.TotalNumberofTransferCompleted + progressSummary.TotalNumberofFailedTransfer) * 100) / progressSummary.TotalNumberOfTransfer

	// get the throughput counts
	numOfBytesTransferredSinceLastCheckpoint := atomic.LoadInt64(&realTimeThroughputCounter.currentBytes) - realTimeThroughputCounter.lastCheckedBytes
	if numOfBytesTransferredSinceLastCheckpoint == 0 {
		progressSummary.ThroughputInBytesPerSeconds = 0
	} else {
		progressSummary.ThroughputInBytesPerSeconds = float64(numOfBytesTransferredSinceLastCheckpoint) / time.Since(realTimeThroughputCounter.lastCheckedTime).Seconds()
	}
	// update the throughput state
	snapshotThroughputCounter()

	// marshalling the JobProgressSummary struct to send back in response.
	jobProgressSummaryJson, err := json.MarshalIndent(progressSummary, "", "")
	if err != nil {
		result := fmt.Sprintf("error marshalling the progress summary for Job Id %s", jobId)
		(*resp).WriteHeader(http.StatusInternalServerError)
		(*resp).Write([]byte(result))
		return
	}
	(*resp).WriteHeader(http.StatusAccepted)
	(*resp).Write(jobProgressSummaryJson)
}

func updateThroughputCounter(numBytes int64) {
	atomic.AddInt64(&realTimeThroughputCounter.currentBytes, numBytes)
}

func snapshotThroughputCounter() {
	realTimeThroughputCounter.lastCheckedBytes = atomic.LoadInt64(&realTimeThroughputCounter.currentBytes)
	realTimeThroughputCounter.lastCheckedTime = time.Now()
}

// getTransferList api returns the list of transfer with specific status for given jobId in http response
func getTransferList(jobId common.JobID, expectedStatus common.Status, jPartPlanInfoMap *JobsInfoMap, resp *http.ResponseWriter) {
	// getJobPartInfoReferenceFromMap gives the JobPartPlanInfo Pointer for given JobId and PartNumber
	jPartMap, ok := jPartPlanInfoMap.LoadJobPartsMapForJob(jobId)
	// sending back the error status and error message in response
	if !ok {
		(*resp).WriteHeader(http.StatusBadRequest)
		(*resp).Write([]byte(fmt.Sprintf("invalid jobId %s", jobId)))
		return
	}
	var transferList []common.TransferStatus
	for _, jHandler := range jPartMap {
		// jPartPlan represents the memory map JobPartPlanHeader for given jobid and part number
		jPartPlan := jHandler.getJobPartPlanPointer()
		numTransfer := jPartPlan.NumTransfers

		// trasnferStatusList represents the list containing number of transfer for given jobid and part number
		for index := uint32(0); index < numTransfer; index++ {
			// getting transfer header of transfer at index index for given jobId and part number
			transferEntry := jHandler.Transfer(index)
			// if the expected status is not to list all transfer and status of current transfer is not equal to the expected status, then we skip this transfer
			if expectedStatus != common.TranferStatusAll && transferEntry.Status != expectedStatus {
				continue
			}
			// getting source and destination of a transfer at index index for given jobId and part number.
			source, destination := jHandler.getTransferSrcDstDetail(index)
			transferList = append(transferList, common.TransferStatus{source, destination, transferEntry.Status})
		}
	}
	// marshalling the TransfersStatus Struct to send back in response to front-end
	tStatusJson, err := json.MarshalIndent(common.TransfersStatus{transferList}, "", "")
	if err != nil {
		result := fmt.Sprintf("error marshalling the transfer status for Job Id %s", jobId)
		(*resp).WriteHeader(http.StatusInternalServerError)
		(*resp).Write([]byte(result))
		return
	}
	(*resp).WriteHeader(http.StatusAccepted)
	(*resp).Write(tStatusJson)
}

// listExistingJobs returns the jobId of all the jobs existing in the current instance of azcopy
func listExistingJobs(jPartPlanInfoMap *JobsInfoMap, resp *http.ResponseWriter) {
	// get the list of all the jobId from the JobsInfoMap
	jobIds := jPartPlanInfoMap.LoadExistingJobIds()

	// building the ExistingJobDetails for sending response back to front-end
	existingJobDetails := common.ExistingJobDetails{jobIds}
	existingJobDetailsJson, err := json.Marshal(existingJobDetails)
	if err != nil {
		(*resp).WriteHeader(http.StatusInternalServerError)
		(*resp).Write([]byte("error marshalling the existing job list"))
		return
	}
	(*resp).WriteHeader(http.StatusAccepted)
	(*resp).Write(existingJobDetailsJson)
}

// parsePostHttpRequest parses the incoming CopyJobPartOrder request
func parsePostHttpRequest(req *http.Request) (common.CopyJobPartOrder, error) {
	var payload common.CopyJobPartOrder
	if req.Body == nil {
		return payload, errors.New("the http Request Does not have a valid body definition")
	}
	body, err := ioutil.ReadAll(req.Body)
	if err != nil {
		return payload, errors.New("error reading the HTTP Request Body")
	}
	err = json.Unmarshal(body, &payload)
	if err != nil {
		return payload, errors.New("error UnMarshalling the HTTP Request Body")
	}
	return payload, nil
}

// serveRequest process the incoming http request
/*
	* resp -- It is the http response write for send back the response for the http request
    * req  -- It is the new http request received
	* coordinatorChannels -- These are the High, Med and Low transfer channels for scheduling the incoming transfer. These channel are required in case of New JobPartOrder request
    * jobToLoggerMap -- This Map holds the logger instance for each job
*/
func serveRequest(resp http.ResponseWriter, req *http.Request, coordinatorChannels *CoordinatorChannels, jobsInfoMap *JobsInfoMap) {
	switch req.Method {
	case http.MethodGet:
		// request type defines the type of GET request supported by transfer engine
		// currently Transfer Engine is supporting list and kill type of GET request
		// list type is used by the request for list commands
		// kill type is used when frontend wants the existing instance of transfer engine to kill itself
		var requestType = req.URL.Query()["Type"][0]
		switch requestType {
		case "list":
			var params = req.URL.Query()["command"][0]
			listCommand := []byte(params)
			var lsCommand common.ListJobPartsTransfers
			err := json.Unmarshal(listCommand, &lsCommand)
			if err != nil {
				panic(err)
			}
			if lsCommand.JobId == "" {
				listExistingJobs(jobsInfoMap, &resp)
			} else if lsCommand.ExpectedTransferStatus == math.MaxUint8 {
				getJobSummary(lsCommand.JobId, jobsInfoMap, &resp)
			} else {
				getTransferList(lsCommand.JobId, lsCommand.ExpectedTransferStatus, jobsInfoMap, &resp)
			}
		case "cancel":
			var jobId = req.URL.Query()["jobId"][0]
			ExecuteCancelJobOrder(common.JobID(jobId), jobsInfoMap, false, &resp)
		}

	case http.MethodPost:
		jobRequestData, err := parsePostHttpRequest(req)
		if err != nil {
			resp.WriteHeader(http.StatusBadRequest)
			resp.Write([]byte("Not able to trigger the AZCopy request" + " : " + err.Error()))
		}
		ExecuteNewCopyJobPartOrder(jobRequestData, coordinatorChannels, jobsInfoMap)
		resp.WriteHeader(http.StatusBadRequest)
		resp.Write([]byte("Not able to trigger the AZCopy request"))
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
func InitializeChannels() (*CoordinatorChannels, *EEChannels) {

	// HighTransferMsgChannel takes high priority job part transfers from coordinator and feed to execution engine
	HighTransferMsgChannel := make(chan TransferMsg, 500)
	// MedTransferMsgChannel takes medium priority job part transfers from coordinator and feed to execution engine
	MedTransferMsgChannel := make(chan TransferMsg, 500)
	// LowTransferMsgChannel takes low priority job part transfers from coordinator and feed to execution engine
	LowTransferMsgChannel := make(chan TransferMsg, 500)

	// HighChunkMsgChannel queues high priority job part transfer chunk transactions
	HighChunkMsgChannel := make(chan ChunkMsg, 500)
	// MedChunkMsgChannel queues medium priority job part transfer chunk transactions
	MedChunkMsgChannel := make(chan ChunkMsg, 500)
	// LowChunkMsgChannel queues low priority job part transfer chunk transactions
	LowChunkMsgChannel := make(chan ChunkMsg, 500)

	// Create suicide channel which is used to scale back on the number of workers
	SuicideChannel := make(chan SuicideJob, 100)

	transferEngineChannel := &CoordinatorChannels{
		HighTransfer: HighTransferMsgChannel,
		MedTransfer:  MedTransferMsgChannel,
		LowTransfer:  LowTransferMsgChannel,
	}

	executionEngineChanel := &EEChannels{
		HighTransfer:         HighTransferMsgChannel,
		MedTransfer:          MedTransferMsgChannel,
		LowTransfer:          LowTransferMsgChannel,
		HighChunkTransaction: HighChunkMsgChannel,
		MedChunkTransaction:  MedChunkMsgChannel,
		LowChunkTransaction:  LowChunkMsgChannel,
		SuicideChannel:       SuicideChannel,
	}
	return transferEngineChannel, executionEngineChanel
}

// initializeCoordinator initializes the coordinator
/*
* reconstructs the existing job using job part file on disk
* creates a server listening on port 1337 for job part order requests from front end
 */
func initializeCoordinator(coordinatorChannels *CoordinatorChannels) error {

	jobHandlerMap := NewJobPartPlanInfoMap()
	reconstructTheExistingJobParts(jobHandlerMap)
	http.HandleFunc("/", func(writer http.ResponseWriter, request *http.Request) {
		serveRequest(writer, request, coordinatorChannels, jobHandlerMap)
	})
	err := http.ListenAndServe("localhost:1337", nil)
	fmt.Print("Server Created")
	if err != nil {
		fmt.Print("Server already initialized")
	}
	return err
}

// InitializeSTE initializes the coordinator channels, execution engine channels, coordinator and execution engine
func InitializeSTE() error {
	coordinatorChannel, execEngineChannels := InitializeChannels()
	go InitializeExecutionEngine(execEngineChannels)
	return initializeCoordinator(coordinatorChannel)
}
