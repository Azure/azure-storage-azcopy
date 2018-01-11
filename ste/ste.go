package ste

import (
	"fmt"
	"encoding/json"
	"net/http"
	"io/ioutil"
	"errors"
	"github.com/edsrzf/mmap-go"
	"os"
	"github.com/Azure/azure-storage-blob-go/2016-05-31/azblob"
	"context"
	"bytes"
	"github.com/Azure/azure-storage-azcopy/common"
	"strconv"
	"runtime"
)

var JobPartInfoMap = map[common.JobID]map[common.PartNumber]*JobPartPlanInfo{}
var steContext = context.Background()

func uploadTheBlocksSequentially(ctx context.Context, blobUrl azblob.BlockBlobURL,
	memMap mmap.MMap, base64BlockIDs []string) (bool){

	memMapByteLength := len(memMap)
	bytesLeft := memMapByteLength
	startOffset := 0
	bytesPutInBlock := 0

	totalNumBlocks := memMapByteLength / BLOCK_LENGTH

	if(memMapByteLength % BLOCK_LENGTH) > 0 {
		totalNumBlocks += 1
	}
	for blockIndex := 0 ; blockIndex < totalNumBlocks; blockIndex++{
		var currBlock []byte
		if bytesLeft <= 0 {
			break
		}
		if bytesLeft < BLOCK_LENGTH {
			currBlock = memMap[startOffset: startOffset + bytesLeft]
			bytesPutInBlock = bytesLeft
		}else{
			currBlock = memMap[startOffset : startOffset + BLOCK_LENGTH]
			bytesPutInBlock = BLOCK_LENGTH
		}
		result := uploadTheBlocks(ctx, blobUrl, base64BlockIDs[blockIndex], blockIndex, currBlock)
		if !result {
			fmt.Errorf("Uploading Failed For url %s and blockId %s with ", blobUrl.String(), base64BlockIDs[blockIndex])
			return false
		}
		startOffset += bytesPutInBlock
		bytesLeft -= bytesPutInBlock
	}
	return true
}

func uploadTheBlocks(ctx context.Context, blobURL azblob.BlockBlobURL,
	blockID string, blockIndex int, dataBuffer []byte)  (bool){
	if(ctx == nil) ||
		(blobURL == azblob.BlockBlobURL{}) ||
		(blockID == "") || (dataBuffer == nil){
		panic ("STE: uploadTheBlocks Invalid params passed to the function")
	}
	_, err := blobURL.PutBlock(ctx, blockID, bytes.NewReader(dataBuffer), azblob.LeaseAccessConditions{})
	if err != nil {
		fmt.Println("Uploading Failed For url %s and blockId %s with err %s", blobURL.String(), blockID, err)
		return false
	}
	fmt.Println("Uploading Successful For url ", blobURL.String(), " and block No ", blockIndex)
	return true
}

// putJobPartInfoHandlerIntoMap api put the JobPartPlanInfo pointer for given jobId and part number in map[common.JobID]map[common.PartNumber]*JobPartPlanInfo
func putJobPartInfoHandlerIntoMap(jobHandler *JobPartPlanInfo, jobId common.JobID, partNo common.PartNumber,
									jPartInfoMap *map[common.JobID]map[common.PartNumber]*JobPartPlanInfo){
	// get map[common.PartNumber]*JobPartPlanInfo for given jobId and part number
	jPartMap := (*jPartInfoMap)[jobId]
	// If no map exists, then creating a new map map[common.PartNumber]*JobPartPlanInfo for given jobId
	if jPartMap == nil {
		(*jPartInfoMap)[jobId] = make(map[common.PartNumber]*JobPartPlanInfo)
		(*jPartInfoMap)[jobId][partNo] = jobHandler
	}else { // if map exists for given jobId then putting JobPartPlanInfo Pointer for given part number
		(*jPartInfoMap)[jobId][partNo] = jobHandler
	}
}

// getJobPartMapFromJobPartInfoMap api gets the map[common.PartNumber]*JobPartPlanInfo for given jobId and part number from map[common.JobID]map[common.PartNumber]*JobPartPlanInfo
func getJobPartMapFromJobPartInfoMap(jobId common.JobID,
										jPartInfoMap *map[common.JobID]map[common.PartNumber]*JobPartPlanInfo)  (jPartMap map[common.PartNumber]*JobPartPlanInfo){
	jPartMap = (*jPartInfoMap)[jobId]
	if jPartMap == nil{
		return
	}
	return jPartMap
}

// getJobPartInfoHandlerFromMap
func getJobPartInfoHandlerFromMap(jobId common.JobID, partNo common.PartNumber,
										jPartInfoMap *map[common.JobID]map[common.PartNumber]*JobPartPlanInfo) (*JobPartPlanInfo, error){
	jPartMap := (*jPartInfoMap)[jobId]
	if jPartMap == nil{
		err := fmt.Errorf(InvalidJobId, jobId)
		return nil, err
	}
	jHandler := jPartMap[partNo]
	if jHandler == nil{
		err := fmt.Errorf(InvalidPartNo, partNo, jobId)
		return nil, err
	}
	return jHandler, nil
}

// ExecuteNewCopyJobPartOrder api executes a new job part order
func ExecuteNewCopyJobPartOrder(payload common.CopyJobPartOrder, coordiatorChannels *CoordinatorChannels){
	/*
		* Convert the blobdata to memory map compatible DestinationBlobData
		* Create a file for JobPartOrder and write data into that file.
		* Initialize the JobPartOrder
		*  Create a JobPartInfo pointer for the new job and put it into the map
		* Schedule the transfers of Job by putting them into Transfermsg channels.
	 */
	data := payload.OptionalAttributes
	var crc [128/ 8]byte
	copy(crc[:], CRC64BitExample)
	destBlobData, err := dataToDestinationBlobData(data)
	if err != nil {
		panic(err)
	}
	fileName := createJobPartPlanFile(payload, destBlobData)
	var jobHandler  = new(JobPartPlanInfo)
	jobHandler.ctx, jobHandler.cancel = context.WithCancel(context.Background())
	err = (jobHandler).initialize(jobHandler.ctx, fileName)
	if err != nil {
		panic(err)
	}

	putJobPartInfoHandlerIntoMap(jobHandler, payload.ID, payload.PartNum, &JobPartInfoMap)

	if coordiatorChannels == nil{
		panic(errors.New("channel not initialized"))
	}
	numTransfer := jobHandler.getJobPartPlanPointer().NumTransfers
	for index := uint32(0); index < numTransfer; index ++{
		transferMsg := TransferMsg{payload.ID, payload.PartNum, index}
		switch payload.Priority{
		case HighJobPriority:
			coordiatorChannels.HighTransfer <- transferMsg
		case MediumJobPriority:
			coordiatorChannels.MedTransfer <- transferMsg
		case LowJobPriority:
			coordiatorChannels.LowTransfer <- transferMsg
		default:
			fmt.Println("invalid job part order priority")
		}
	}

	//// Test Cases
	//jobHandler.updateTheChunkInfo(0,0, crc, ChunkTransferStatusComplete)
	//
	//jobHandler.updateTheChunkInfo(1,0, crc, ChunkTransferStatusComplete)
	//
	//jobHandler.getChunkInfo(1,0)
	//
	//cInfo := jobHandler.getChunkInfo(1,2)
	//fmt.Println("Chunk Crc ", string(cInfo.BlockId[:]), " ",cInfo.Status)
	//
	//cInfo  = jobHandler.getChunkInfo(0,1)
	//fmt.Println("Chunk Crc ", string(cInfo.BlockId[:]), " ",cInfo.Status)
	//
	//cInfo  = jobHandler.getChunkInfo(0,2)
	//fmt.Println("Chunk Crc ", string(cInfo.BlockId[:]), " ",cInfo.Status)
}

func unMappingtheMemoryMapFile(mMap mmap.MMap, file *os.File){
	if mMap == nil {
		panic("Unferenced Memory Map Byte Array Passed")
	}
	err := mMap.Unmap()
	if err != nil {
		panic(err)
	}
	err = file.Close()
	if err != nil {
		fmt.Println("File is Already CLosed.")
	}
}

func ExecuteAZCopyDownload(payload common.CopyJobPartOrder){
	fmt.Println("Executing the AZ Copy Download Request in different Go Routine ")
}

func validateAndRouteHttpPostRequest(payload common.CopyJobPartOrder, coordintorChannels *CoordinatorChannels) (bool){
	switch {
	case payload.SourceType == common.Local &&
		payload.DestinationType == common.Blob:
			ExecuteNewCopyJobPartOrder(payload, coordintorChannels)
			return true
	case payload.SourceType == common.Blob &&
		payload.DestinationType == common.Local:
		ExecuteAZCopyDownload(payload)
		return true
	default:
		fmt.Println("Command %d Type Not Supported by STE", payload)
		return false
	}
	return false
}

// getJobStatus api returns the job progress summary of an active job
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
func getJobStatus(jobId common.JobID, lastCheckPointTimeStamp uint64, resp *http.ResponseWriter){

	// getJobPartMapFromJobPartInfoMap gives the map of partNo to JobPartPlanInfo Pointer for a given JobId
	jPartMap := getJobPartMapFromJobPartInfoMap(jobId, &JobPartInfoMap)

	// if partNumber to JobPartPlanInfo Pointer map is nil, then returning error
	if jPartMap == nil{
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
	for partNo, jHandler := range jPartMap{
		fmt.Println("part no ", partNo)

		// currentJobPartPlanInfo represents the memory map JobPartPlanHeader for current partNo
		currentJobPartPlanInfo := jHandler.getJobPartPlanPointer()

		completeJobOrdered = completeJobOrdered || currentJobPartPlanInfo.IsFinalPart
		progressSummary.TotalNumberOfTransfer += currentJobPartPlanInfo.NumTransfers

		// iterating through all transfers for current partNo and job with given jobId
		for index := uint32(0); index < currentJobPartPlanInfo.NumTransfers; index++{

			// transferHeader represents the memory map transfer header of transfer at index position for given job and part number
			transferHeader := jHandler.Transfer(index)

			// if transferHeader completionTime is greater than lastCheckPointTimeStamp, it means the transfer was updated after lastCheckPointTimeStamp
			if transferHeader.CompletionTime > lastCheckPointTimeStamp {
				// check for completed transfer
				if transferHeader.Status == TransferStatusComplete{
					progressSummary.NumberOfTransferCompletedafterCheckpoint += 1
				}else if transferHeader.Status == TransferStatusFailed{ // check for failed transfer
					progressSummary.NumberOfTransferFailedAfterCheckpoint += 1
					// getting the source and destination for failed transfer at position - index
					source, destination := jHandler.getTransferSrcDstDetail(index)
					// appending to list of failed transfer
					failedTransfers = append(failedTransfers, common.TransferStatus{source, destination, TransferStatusFailed})
				}
			}
			// check for all completed transfer to calculate the progress percentage at the end
			if transferHeader.Status == TransferStatusComplete{
				progressSummary.TotalNumberofTransferCompleted += 1
			}
			if transferHeader.Status == TransferStatusFailed{
				progressSummary.TotalNumberofFailedTransfer += 1
			}
		}
	}
	progressSummary.CompleteJobOrdered = completeJobOrdered
	progressSummary.FailedTransfers = failedTransfers
	progressSummary.PercentageProgress = ( progressSummary.TotalNumberofTransferCompleted  + progressSummary.TotalNumberofFailedTransfer) / progressSummary.TotalNumberOfTransfer * 100

	// marshalling the JobProgressSummary struct to send back in response.
	jobProgressSummaryJson, err := json.MarshalIndent(progressSummary, "", "")
	if err != nil{
		result := fmt.Sprintf("error marshalling the progress summary for Job Id %s", jobId)
		(*resp).WriteHeader(http.StatusInternalServerError)
		(*resp).Write([]byte(result))
		return
	}
	(*resp).WriteHeader(http.StatusAccepted)
	(*resp).Write(jobProgressSummaryJson)
}

func getJobPartStatus(jobId common.JobID, partNo common.PartNumber, resp *http.ResponseWriter) {
	// getJobPartInfoHandlerFromMap gives the JobPartPlanInfo Pointer for given JobId and PartNumber
	jHandler, err := getJobPartInfoHandlerFromMap(jobId, partNo, &JobPartInfoMap)
	// sending back the error status and error message in response
	if err != nil{
		(*resp).WriteHeader(http.StatusBadRequest)
		(*resp).Write([]byte(err.Error()))
		return
	}
	// jPartPlan represents the memory map JobPartPlanHeader for given jobid and part number
	jPartPlan := jHandler.getJobPartPlanPointer()
	numTransfer := jPartPlan.NumTransfers

	// trasnferStatusList represents the list containing number of transfer for given jobid and part number
	transferStatusList := make([]common.TransferStatus, numTransfer)
	for index := uint32(0); index < numTransfer; index ++{
		// getting transfer header of transfer at index index for given jobId and part number
		transferEntry := jHandler.Transfer(index)
		// getting source and destination of a transfer at index index for given jobId and part number.
		source, destination := jHandler.getTransferSrcDstDetail(index)
		transferStatusList[index].Status = transferEntry.Status
		transferStatusList[index].Src = source
		transferStatusList[index].Dst = destination
	}
	// marshalling the TransfersStatus Struct to send back in response to front-end
	tStatusJson, err := json.MarshalIndent(common.TransfersStatus{transferStatusList}, "", "")
	if err != nil{
		result := fmt.Sprintf(TransferStatusMarshallingError, jobId, partNo)
		(*resp).WriteHeader(http.StatusInternalServerError)
		(*resp).Write([]byte(result))
		return
	}
	(*resp).WriteHeader(http.StatusAccepted)
	(*resp).Write(tStatusJson)
}

func listExistingJobs(resp *http.ResponseWriter){
	var jobIds []common.JobID
	for jobId := range JobPartInfoMap{
		jobIds = append(jobIds, jobId)
	}
	existingJobDetails := common.ExistingJobDetails{jobIds}
	existingJobDetailsJson, err:= json.Marshal(existingJobDetails)
	if err != nil{
		(*resp).WriteHeader(http.StatusInternalServerError)
		(*resp).Write([]byte("error marshalling the existing job list"))
		return
	}
	(*resp).WriteHeader(http.StatusAccepted)
	(*resp).Write(existingJobDetailsJson)
}

func getJobOrderDetails(jobId common.JobID, resp *http.ResponseWriter){
	// getJobPartMapFromJobPartInfoMap gives the map of partNo to JobPartPlanInfo Pointer for a given JobId
	jPartMap := getJobPartMapFromJobPartInfoMap(jobId, &JobPartInfoMap)

	// if partNumber to JobPartPlanInfo Pointer map is nil, then returning error
	if jPartMap == nil{
		(*resp).WriteHeader(http.StatusBadRequest)
		errorMsg := fmt.Sprintf("no active job with JobId %s exists", jobId)
		(*resp).Write([]byte(errorMsg))
		return
	}
	var jobPartDetails []common.JobPartDetails
	for partNo, jHandler := range jPartMap{
		jPartDetails := common.JobPartDetails{}
		jPartDetails.PartNum = partNo
		var trasnferList []common.TransferStatus
		currentJobPartPlanInfo := jHandler.getJobPartPlanPointer()
		for index := uint32(0); index < currentJobPartPlanInfo.NumTransfers; index++{
			source, destination :=	jHandler.getTransferSrcDstDetail(index)
			trasnferList = append(trasnferList, common.TransferStatus{source, destination, jHandler.Transfer(index).Status})
		}
		jPartDetails.TransferDetails = trasnferList
		jobPartDetails = append(jobPartDetails, jPartDetails)
	}
	jobPartDetailJson, err := json.MarshalIndent(common.JobOrderDetails{jobPartDetails}, "", "")
	if err != nil{
		result := fmt.Sprintf("error marshalling the job detail for Job Id %s", jobId)
		(*resp).WriteHeader(http.StatusInternalServerError)
		(*resp).Write([]byte(result))
		return
	}
	(*resp).WriteHeader(http.StatusAccepted)
	(*resp).Write(jobPartDetailJson)
}

func parsePostHttpRequest(req *http.Request) (common.CopyJobPartOrder, error){
	var payload common.CopyJobPartOrder
	if req.Body == nil{
		return payload, errors.New(InvalidHttpRequestBody)
	}
	body, err := ioutil.ReadAll(req.Body)
	if err != nil {
		return payload, errors.New(HttpRequestBodyReadError)
	}
	err = json.Unmarshal(body, &payload)
	if err != nil {
		return payload, errors.New(HttpRequestUnmarshalError)
	}
	return payload, nil
}

func serveRequest(resp http.ResponseWriter, req *http.Request, coordinatorChannels *CoordinatorChannels){
	switch req.Method {
	case "GET":
		var queryType = req.URL.Query()["type"][0]
		switch queryType{
		case "JobStatus":
		var guUID common.JobID = common.JobID(req.URL.Query()["GUID"][0])
			checkPointTime, err := strconv.ParseUint(req.URL.Query()["CheckpointTime"][0], 10, 64)
		if err != nil{
			resp.WriteHeader(http.StatusBadRequest)
			resp.Write([]byte(err.Error()))
			return
		}
			getJobStatus(guUID, checkPointTime, &resp)

		case "PartStatus":
			var guUID common.JobID = common.JobID(req.URL.Query()["GUID"][0])
			partNoString := req.URL.Query()["Part"][0]
			partNo, err := strconv.ParseUint(partNoString, 10, 32)
		if err != nil{
			resp.WriteHeader(http.StatusBadRequest)
			resp.Write([]byte(err.Error()))
				return
			}
			getJobPartStatus(guUID, common.PartNumber(partNo), &resp)
		case "JobListing":
			listExistingJobs(&resp)

		case "JobDetail":
			var guUID common.JobID = common.JobID(req.URL.Query()["GUID"][0])
			getJobOrderDetails(guUID, &resp)

		default:
			resp.WriteHeader(http.StatusBadRequest)
			resp.Write([]byte("operation not supported"))
		}

	case "POST":
		jobRequestData, err := parsePostHttpRequest(req)
		if err != nil {
			resp.WriteHeader(http.StatusBadRequest)
			resp.Write([]byte(UnsuccessfulAZCopyRequest + " : " + err.Error()))
		}
		isValid := validateAndRouteHttpPostRequest(jobRequestData, coordinatorChannels)
		if isValid {
			resp.WriteHeader(http.StatusAccepted)
			resp.Write([]byte(SuccessfulAZCopyRequest))
		} else{
			resp.WriteHeader(http.StatusBadRequest)
			resp.Write([]byte(UnsuccessfulAZCopyRequest))
		}
	case "PUT":

	case "DELETE":

	default:
		fmt.Println("Operation Not Supported by STE")
		resp.WriteHeader(http.StatusBadRequest)
		resp.Write([]byte(UnsuccessfulAZCopyRequest))
	}
}

// InitializedChannels initializes the channels used further by coordinator and execution engine
func InitializedChannels() (*CoordinatorChannels, *EEChannels){
	fmt.Println("Initializing Channels")
	// HighTransferMsgChannel takes high priority job part transfers from coordinator and feed to execution engine
	HighTransferMsgChannel := make(chan TransferMsg, 500)
	// MedTransferMsgChannel takes high priority job part transfers from coordinator and feed to execution engine
	MedTransferMsgChannel := make(chan TransferMsg, 500)
	// LowTransferMsgChannel takes high priority job part transfers from coordinator and feed to execution engine
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
		HighTransfer : HighTransferMsgChannel,
		MedTransfer	: MedTransferMsgChannel,
		LowTransfer : LowTransferMsgChannel,
	}

	executionEngineChanel := &EEChannels{
		HighTransfer:HighTransferMsgChannel,
		MedTransfer:MedTransferMsgChannel,
		LowTransfer:LowTransferMsgChannel,
		HighChunkTransaction:HighChunkMsgChannel,
		MedChunkTransaction:MedChunkMsgChannel,
		LowChunkTransaction:LowChunkMsgChannel,
		SuicideChannel: SuicideChannel,
	}
	return transferEngineChannel, executionEngineChanel
}

// initializeCoordinator initializes the coordinator
/*
	* reconstructs the existing job using job part file on disk
	* creater a server listening on port 1337 for job part order requests from front end
 */
func initializeCoordinator(coordinatorChannels *CoordinatorChannels) {
	fmt.Println("STORAGE TRANSFER ENGINE")
	reconstructTheExistingJobPart()
	http.HandleFunc("/", func(writer http.ResponseWriter, request *http.Request) {
		serveRequest(writer, request, coordinatorChannels)
	})
	err := http.ListenAndServe("localhost:1337", nil)
	fmt.Print("Server Created")
	if err != nil{
		fmt.Print("Server already initialized")
	}
}

// InitializeSTE initializes the coordinator channels, execution engine channels, coordinator and execution engine
func InitializeSTE(){
	runtime.GOMAXPROCS(4)
	coordinatorChannel, execEngineChannels := InitializedChannels()
	initializeCoordinator(coordinatorChannel)
	InitializeExecutionEngine(execEngineChannels)
}
