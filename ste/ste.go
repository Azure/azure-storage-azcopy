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
)

type jobPartToUnknown common.JobPartToUnknown
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

func ExecuteNewCopyJobPartOrder(payload jobPartToUnknown){
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

func ExecuteAZCopyDownload(payload jobPartToUnknown){
	fmt.Println("Executing the AZ Copy Download Request in different Go Routine ")
}

func validateAndRouteHttpPostRequest(payload jobPartToUnknown) (bool){
	switch {
	case payload.JobPart.SourceType == common.Local &&
		payload.JobPart.DestinationType == common.Blob:
		go ExecuteNewCopyJobPartOrder(payload)
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

func getJobPartStatus(jobId common.JobID, partNo common.PartNumber)  (transfersStatus, error){
	jHandler, err := getJobPartInfoHandlerFromMap(jobId, partNo, &JobPartInfoMap)
	if err != nil{
		return transfersStatus{}, err
	}
	jPartPlan := jHandler.getJobPartPlanPointer()
	numTransfer := jPartPlan.NumTransfers
	transferStatusList := make([]transferStatus, numTransfer)
	for index := uint32(0); index < numTransfer; index ++{
		transferEntry := jHandler.Transfer(index)

		source, destination := jHandler.getTransferSrcDstDetail(index)
		transferStatusList[index].Status = transferEntry.Status
		transferStatusList[index].Src = source
		transferStatusList[index].Dst = destination
	}
	return transfersStatus{transferStatusList}, nil
}

func parsePostHttpRequest(req *http.Request) (jobPartToUnknown, error){
	var payload jobPartToUnknown
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
		var guUID common.JobID = common.JobID(req.URL.Query()["GUID"][0])
		partNoString := req.URL.Query()["Part"][0]
		partNo, err := strconv.ParseUint(partNoString, 10, 32)
		if err != nil{
			resp.WriteHeader(http.StatusBadRequest)
			resp.Write([]byte(err.Error()))
			return
		}
		tStatus, err := getJobPartStatus(guUID, common.PartNumber(partNo))
		if err != nil{
			resp.WriteHeader(http.StatusBadRequest)
			resp.Write([]byte(err.Error()))
		}else{
			tStatusJson, err := json.MarshalIndent(tStatus, "", "")
			if err != nil{
				result := fmt.Sprintf(TransferStatusMarshallingError, guUID, partNoString)
				resp.WriteHeader(http.StatusInternalServerError)
				resp.Write([]byte(result))
			}else{
				resp.WriteHeader(http.StatusAccepted)
				resp.Write(tStatusJson)
			}
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

func CreateServer(){
	http.HandleFunc("/", serveRequest)
	err := http.ListenAndServe("localhost:1337", nil)
	fmt.Println("Error recieved ", err)
}

func InitTransferEngine() {
	fmt.Println("STORAGE TRANSFER ENGINE")
	reconstructTheExistingJobPart()
	CreateServer()
}
