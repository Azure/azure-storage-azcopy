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
	"time"
	"sync/atomic"
	"unsafe"
)

var JobPartInfoMap = map[common.JobID]map[common.PartNumber]*JobPartPlanInfo{}
var steContext = context.Background()
var transferEngineChannel *TEChannels
var executionEngineChanel *EEChannels
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
	if(err != nil){
		fmt.Println("Uploading Failed For url %s and blockId %s with err %s", blobURL.String(), blockID, err)
		return false
	}
	fmt.Println("Uploading Successful For url ", blobURL.String(), " and block No ", blockIndex)
	return true
}

func putJobPartInfoHandlerIntoMap(jobHandler *JobPartPlanInfo, jobId common.JobID, partNo common.PartNumber,
									jPartInfoMap *map[common.JobID]map[common.PartNumber]*JobPartPlanInfo){
	jPartMap := (*jPartInfoMap)[jobId]
	if jPartMap == nil {
		(*jPartInfoMap)[jobId] = make(map[common.PartNumber]*JobPartPlanInfo)
		(*jPartInfoMap)[jobId][partNo] = jobHandler
	}else {
		(*jPartInfoMap)[jobId][partNo] = jobHandler
	}
}

func getJobPartMapFromJobPartInfoMap(jobId common.JobID,
										jPartInfoMap *map[common.JobID]map[common.PartNumber]*JobPartPlanInfo)  (jPartMap map[common.PartNumber]*JobPartPlanInfo){
	jPartMap = (*jPartInfoMap)[jobId]
	if jPartMap == nil{
		return
	}
	return jPartMap
}

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

func ExecuteNewCopyJobPartOrder(payload common.JobPartToUnknown){
	data := common.BlobData{}
	err := json.Unmarshal(payload.Data, &data)
	var crc [128/ 8]byte
	copy(crc[:], CRC64BitExample)
	if err != nil {
		panic(err)
	}
	destBlobData, err := dataToDestinationBlobData(data)
	if err != nil {
		panic(err)
	}
	fileName := createJobPartPlanFile(payload.JobPart, destBlobData)
	var jobHandler  = new(JobPartPlanInfo)
	jobHandler.ctx, jobHandler.cancel = context.WithCancel(context.Background())
	err = (jobHandler).initialize(jobHandler.ctx, fileName)
	if err != nil {
		panic(err)
	}

	putJobPartInfoHandlerIntoMap(jobHandler, payload.JobPart.ID, payload.JobPart.PartNum, &JobPartInfoMap)

	if transferEngineChannel == nil{
		panic(errors.New("channel not initialized"))
	}
	numTransfer := jobHandler.getJobPartPlanPointer().NumTransfers
	for index := uint32(0); index < numTransfer; index ++{
		transferMsg := TransferMsg{payload.JobPart.ID, payload.JobPart.PartNum, index}
		switch payload.JobPart.Priority{
		case HighJobPriority:
			transferEngineChannel.HighTransfer <- transferMsg
		case MediumJobPriority:
			transferEngineChannel.MedTransfer <- transferMsg
		case LowJobPriority:
			transferEngineChannel.LowTransfer <- transferMsg
		default:
			fmt.Println("invalid job part order priority")
		}
	}

	// Test Cases
	jobHandler.updateTheChunkInfo(0,1, crc, ChunkTransferStatusComplete)

	jobHandler.updateTheChunkInfo(1,3, crc, ChunkTransferStatusComplete)

	jobHandler.getChunkInfo(1,3)

	cInfo := jobHandler.getChunkInfo(1,2)
	fmt.Println("Chunk Crc ", string(cInfo.BlockId[:]), " ",cInfo.Status)

	cInfo  = jobHandler.getChunkInfo(0,1)
	fmt.Println("Chunk Crc ", string(cInfo.BlockId[:]), " ",cInfo.Status)

	cInfo  = jobHandler.getChunkInfo(0,2)
	fmt.Println("Chunk Crc ", string(cInfo.BlockId[:]), " ",cInfo.Status)
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

func ExecuteAZCopyDownload(payload common.JobPartToUnknown){
	fmt.Println("Executing the AZ Copy Download Request in different Go Routine ")
}

func validateAndRouteHttpPostRequest(payload common.JobPartToUnknown) (bool){
	switch {
	case payload.JobPart.SourceType == common.Local &&
		payload.JobPart.DestinationType == common.Blob:
			teChannelUnsafePtr := unsafe.Pointer(transferEngineChannel)
			if atomic.LoadPointer(&teChannelUnsafePtr) == nil{
				initializedChannels()
			}
			transferEngineChannel.JobOrderChan <- payload
			return true
	case payload.JobPart.SourceType == common.Blob &&
		payload.JobPart.DestinationType == common.Local:
		go ExecuteAZCopyDownload(payload)
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
		}
	}
	progressSummary.CompleteJobOrdered = completeJobOrdered
	progressSummary.FailedTransfers = failedTransfers
	progressSummary.PercentageProgress = progressSummary.TotalNumberofTransferCompleted / progressSummary.TotalNumberOfTransfer * 100

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

func parsePostHttpRequest(req *http.Request) (common.JobPartToUnknown, error){
	var payload common.JobPartToUnknown
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

func serveRequest(resp http.ResponseWriter, req *http.Request){
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
		isValid := validateAndRouteHttpPostRequest(jobRequestData)
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

// processJobPartOrder api takes JobPartOrder from JobOrderChannel and execute it
func processJobPartOrder(){
	fmt.Println("Routine For processing Job Order Started")
	// check to verify if transferEngineChannel has been initialized or not. If not then initializes it.
	// since multiple routines can access the transferEngineChannel pointer at same time, loading pointer through atomic operation to avoid race condition
	teChannelUnsafePointer := unsafe.Pointer(transferEngineChannel)
	if atomic.LoadPointer(&teChannelUnsafePointer) == nil{
		initializedChannels()
	}
	// taking the JobPartOrderChannel from TransferEngineChannels
	jobPartOrderChannel := transferEngineChannel.JobOrderChan
	for {
		select {
		case job := <- jobPartOrderChannel:
			fmt.Println("Started processing Job Order")
			//processing new Copy Job Part Order
			ExecuteNewCopyJobPartOrder(job)
		default:
			// routine is set to sleep for 3 seconds in case there is no JobPartOrder to execute further in channel
			time.Sleep(3 * time.Second)
		}
	}
}

func CreateServer(){
	http.HandleFunc("/", serveRequest)
	err := http.ListenAndServe("localhost:1337", nil)
	fmt.Print("Server Created")
	if err != nil{
		fmt.Print("Server already initialized")
	}
}

// initializedChannels initializes the channels used further by coordinator and execution engine
func initializedChannels(){
	fmt.Println("Initializing Channels")
	// HighTransferMsgChannel takes high priority job part transfers from coordinator and feed to execution engine
	HighTransferMsgChannel := make(chan TransferMsg, 500)
	// MedTransferMsgChannel takes high priority job part transfers from coordinator and feed to execution engine
	MedTransferMsgChannel := make(chan TransferMsg, 500)
	// LowTransferMsgChannel takes high priority job part transfers from coordinator and feed to execution engine
	LowTransferMsgChannel := make(chan TransferMsg, 500)
	// JobPartOrderChannel takes JobPartOrder from main routine of coordinator and feed to processJobPartOrder routine
	JobPartOrderChannel := make(chan common.JobPartToUnknown, 500)

	// HighChunkMsgChannel queues high priority job part transfer chunk transactions
	HighChunkMsgChannel := make(chan ChunkMsg, 500)
	// MedChunkMsgChannel queues medium priority job part transfer chunk transactions
	MedChunkMsgChannel := make(chan ChunkMsg, 500)
	// LowChunkMsgChannel queues low priority job part transfer chunk transactions
	LowChunkMsgChannel := make(chan ChunkMsg, 500)

	transferEngineChannel = new(TEChannels)
	transferEngineChannel.HighTransfer = HighTransferMsgChannel
	transferEngineChannel.MedTransfer = MedTransferMsgChannel
	transferEngineChannel.LowTransfer = LowTransferMsgChannel
	transferEngineChannel.JobOrderChan = JobPartOrderChannel

	executionEngineChanel = new(EEChannels)
	executionEngineChanel.HighTransfer = HighTransferMsgChannel
	executionEngineChanel.LowTransfer = LowTransferMsgChannel
	executionEngineChanel.MedTransfer = MedTransferMsgChannel
	executionEngineChanel.HighChunkTransaction = HighChunkMsgChannel
	executionEngineChanel.MedChunkTransaction = MedChunkMsgChannel
	executionEngineChanel.LowChunkTransaction = LowChunkMsgChannel
}

func main() {
	fmt.Println("STORAGE TRANSFER ENGINE")
	reconstructTheExistingJobPart()
	go processJobPartOrder()
	CreateServer()
}
