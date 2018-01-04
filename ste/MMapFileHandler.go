package ste

import (
	"errors"
	"time"
	"fmt"
	"os"
	"io/ioutil"
	"strings"
	"bytes"
	"encoding/binary"
	"github.com/edsrzf/mmap-go"
	"strconv"
	"encoding/hex"
	"unsafe"
	"io"
	"reflect"
	"context"
	"path/filepath"
	"regexp"
	"github.com/Azure/azure-storage-azcopy/common"
)

var currFileDirectory string = "."


/*

 */

 // todo params - filename
func (job *JobPartPlanInfo) initialize(jobContext context.Context, jobPart JobPart,
											destBlobData JobPartPlanBlobData, isFileAlreadyCreated bool) (error){

	// todo remove this outside this function
	/*
		* Memory Map File
		* Get JobPlanPointer
		* walk transfers infos
	 */
	if !isFileAlreadyCreated {
	err := createJobPartPlanFile(jobPart, destBlobData)
	if err != nil {
		return err
	}
	}

	job.ctx, job.cancel = context.WithCancel(jobContext)

	//todo remove this
	jobIdBytes, err  := convertJobIdToByteFormat(jobPart.JobID)
	if err != nil {
		return err
	}
	//todo remove this
	partNo, err := convertStringTouInt32(jobPart.PartNo)
	if err != nil {
		return err
	}

	job.memMap, err = memoryMapTheJobFile(jobIdBytes, partNo)
	if err != nil {
		return err
	}

	jPartPlan, err := job.getJobPartPlanPointer()
	if err != nil {
		return err
	}

	transferInfo := make([]TransferInfo, jPartPlan.NumTransfers)
	for index := uint32(0); index < jPartPlan.NumTransfers; index++ {
		transferCtx, transferCancel := context.WithCancel(job.ctx)
		transferInfo[index] = TransferInfo{transferCtx, transferCancel, 0}
	}
	job.TrasnferInfo = transferInfo
	return nil
}

func (job *JobPartPlanInfo)shutDownHandler(){
	if job.memMap == nil {
		panic(errors.New(MemoryMapFileUnmappedAlreadyError))
	}
	err := job.memMap.Unmap()
	if err != nil {
		panic(err)
	}
}

//todo remove the error and panic
func (job *JobPartPlanInfo) getJobPartPlanPointer() (*JobPartPlanHeader){
	var memMap []byte = job.memMap
	jPart := (*JobPartPlanHeader)(unsafe.Pointer((*reflect.SliceHeader)(unsafe.Pointer(&memMap)).Data))
	return jPart
}

//todo remove func
func (job *JobPartPlanInfo)getNumTasksInJobPartFile() (uint32, error){
	return job.getJobPartPlanPointer().NumTransfers
}

//todo remove the error
func (job *JobPartPlanInfo) getTransferSrcDstDetail(entryIndex uint32) (source, destination string, err error){
	tEntry, err := job.Transfer(entryIndex)
	if err != nil{
		return
	}
	numChunks := tEntry.ChunkNum
	srcStringOffset := tEntry.Offset + (uint64(unsafe.Sizeof(JobPartPlanTransferChunk{})) * uint64(numChunks))
	dstStringOffset := srcStringOffset + uint64(tEntry.SrcLength)
	srcStringSlice := job.memMap[srcStringOffset : srcStringOffset + uint64(tEntry.SrcLength)]
	dstStringSlice := job.memMap[dstStringOffset : dstStringOffset + uint64(tEntry.DstLength)]
	srcString := string(srcStringSlice)
	dstString := string(dstStringSlice)
	return srcString, dstString, nil
}

//todo remove the error and panic
func (job *JobPartPlanInfo) Transfer(index uint32) (*JobPartPlanTransfer){

	//todo no more error
	jPartPlan := job.getJobPartPlanPointer()
	if index >= jPartPlan.NumTransfers {
		// get string in once place
		panic(errors.New(TransferIndexOutOfBoundError))
	}
	transferEntryOffsetIndex := uint64(unsafe.Sizeof(*jPartPlan)) + uint64(unsafe.Sizeof(JobPartPlanBlobData{})) + (uint64(unsafe.Sizeof(JobPartPlanTransfer{})) * uint64(Index))
	transferEntrySlice := job.memMap[transferEntryOffsetIndex :]
	tEntry := (*JobPartPlanTransfer)(unsafe.Pointer((*reflect.SliceHeader)(unsafe.Pointer(&transferEntrySlice)).Data))
	return tEntry
}

//todo remove the error and panic
func (job *JobPartPlanInfo)updateTheChunkInfo(transferIndex uint32, chunkIndex uint16,
	crc [128 / 8]byte, status uint8) (error){

	jPartPlan := job.getJobPartPlanPointer()

	//todo no error
	cInfo, err := job.getChunkInfo(transferIndex, chunkIndex)
	if err != nil{
		return err
	}

	copy(cInfo.BlockId[:],crc[:])
	cInfo.Status = status
	result := fmt.Sprintf(TransferEntryChunkUpdateSuccess, chunkIndex, transferIndex, convertJobIdBytesToString(jPartPlan.JobId))
	fmt.Println(result)
	return nil
}

//todo remove error and panic
func (job JobPartPlanInfo)getChunkInfo(transferIndex uint32, chunkIndex uint16)(*JobPartPlanTransferChunk, error){
	if job.memMap == nil{
		return nil, errors.New(MemoryMapFileUnmappedAlreadyError)
	}
	jPartPlan := job.getJobPartPlanPointer()

	tEntry := job.Transfer(transferIndex)

	//remove this
	if chunkIndex >= tEntry.ChunkNum {
		result := fmt.Sprintf(ChunkIndexOutOfBoundError, chunkIndex, transferIndex, convertJobIdBytesToString(jPartPlan.Id))
		return nil , errors.New(result)
	}
	chunkInfoOffset := tEntry.Offset
	chunkInfoByteSlice := job.memMap[chunkInfoOffset :]
	chunkInfo := (*JobPartPlanTransferChunk)(unsafe.Pointer((*reflect.SliceHeader)(unsafe.Pointer(&chunkInfoByteSlice)).Data))
	return chunkInfo, nil
}



func writeInterfaceDataToWriter( writer io.Writer, f interface{}, structSize uint64) (int, error){
	var bytesWritten uint64 = 0
	var nextOffset uint64= 0
	var currentOffset uint64 = 0
	var padBytes [8]byte
	var elements = reflect.ValueOf(f).Elem()
	for val := 0; val < elements.NumField(); val++{
		align := elements.Type().Field(val).Type.FieldAlign()
		nextOffset = roundUp(currentOffset, uint64(align))
		err := binary.Write(writer, binary.LittleEndian, padBytes[0: (nextOffset - currentOffset)])
		if err != nil {
			return 0, err
		}
		bytesWritten += uint64(nextOffset - currentOffset)
		valueOfField := elements.Field(val)
		elementValue := reflect.ValueOf(valueOfField.Interface()).Interface()
		sizeElementValue := uint64(valueOfField.Type().Size())
		err = binary.Write(writer, binary.LittleEndian, elementValue)
		if err != nil {
			return 0, err
		}
		bytesWritten += sizeElementValue
		currentOffset = bytesWritten
	}

	err := binary.Write(writer, binary.LittleEndian, padBytes[0: (structSize - bytesWritten)])
	if err != nil {
		return 0, err
	}
	bytesWritten += (structSize - bytesWritten)
	return int(bytesWritten), nil
}




func createFileName(jobId [128/8]byte, partNo uint32) (string, error) {
	jobIdString := convertJobIdBytesToString(jobId)
	partNoString := strconv.FormatUint(uint64(partNo), 10)
	return jobIdString + "-" + partNoString + (".STE"), nil
}

//todo : remove the func and use code straight
func CreateFile(jobId [128/8]byte, partNo uint32)  (*os.File, error){
	fileName, err := createFileName(jobId, partNo)
	if err != nil {
		return nil, err
	}
	fileAbsolutePath := currFileDirectory +"/" + fileName
	file, err := os.Create(fileAbsolutePath)
	if err != nil {
		errorMsg := fmt.Sprintf(FileCreationError, err.Error(), convertJobIdBytesToString(jobId), partNo)
		return nil, errors.New(errorMsg)
	}
	return file, err
}

func convertStringTouInt32(element string) (uint32, error){
	partNo64, err := strconv.ParseUint(element, 10, 32)
	if err != nil {
		return 0, err
	}
	partNo := uint32(partNo64)
	return partNo, nil
}

func convertJobIdToByteFormat(jobIDString string) ([128 / 8]byte, error){
	var jobID [128 /8] byte
	guIdBytes, err := hex.DecodeString(jobIDString)
	if err != nil {
		return jobID, err
	}
	copy(jobID[:], guIdBytes)
	return jobID, nil
}

func convertJobIdBytesToString(jobId [128 /8]byte) (string){
	jobIdString := fmt.Sprintf("%x%x%x%x%x", jobId[0:4], jobId[4:6], jobId[6:8], jobId[8:10], jobId[10:])
	return jobIdString
}

func memoryMapTheJobFile(jobId [128/8]byte, partNo uint32)(mmap.MMap, error){
	//
	fileName, err := createFileName(jobId, partNo)
	if err != nil {
		err = fmt.Errorf(InvalidFileName, convertJobIdBytesToString(jobId), partNo)
		return nil, err
	}
	f, err:= os.OpenFile(fileName, os.O_RDWR , 0644)
	if err != nil {
		panic(err)
	}
	defer  f.Close()
	mMap, err := mmap.Map(f, mmap.RDWR, 0)
	if err != nil {
		err = fmt.Errorf(MemoryMapFileInitializationError, convertJobIdBytesToString(jobId), partNo, err.Error())
		return nil, err
	}
	return mMap, nil
}

func createJobPartPlanFile(jobPartOrder common.CopyJobPartOrder) (error){
	var currentEndOffsetOfFile uint64 = 0
	/*todo comments:
	*		Get File Name from JobId and Part Num
	*		Create the File
	*       Create Job Part Plan From Job Part Order
	*       Write Data to file
	* 		Close the file
	* 		Return File Name
	*/
	jPartPlan, err := jobPartTojobPartPlan(jobPartOrder)
	if err != nil {
		err = fmt.Errorf("error converting Job Part to Job Part In File with err %s", err.Error())
		panic(err)
	}
	file, err := CreateFile(jPartPlan.JobId, jPartPlan.PartNo)
	if err != nil{
		return err
	}
	numBytesWritten, err := writeInterfaceDataToWriter(file, &jPartPlan, uint64(unsafe.Sizeof(JobPartPlan{})))
	if err != nil {
		err = fmt.Errorf("error writing Data To The File %s", err.Error())
		return err
	}
	currentEndOffsetOfFile += uint64(numBytesWritten)

	numBytesWritten, err = writeInterfaceDataToWriter(file, &destBlobData, uint64(unsafe.Sizeof(JobPartPlanBlobData{})))
	if err != nil {
		err = fmt.Errorf("error writing Data To The File %s", err.Error())
		return err
	}
	currentEndOffsetOfFile += uint64(numBytesWritten)
	trasnferEntryOffsets := make([]uint64, jPartPlan.NumTransfers)
	transferEntryList := make([]JobPartPlanTransfer, jPartPlan.NumTransfers)
	currentTransferChunkOffset := uint64(currentEndOffsetOfFile) + uint64(uint64(unsafe.Sizeof(JobPartPlanTransfer{})) * uint64(jPartPlan.NumTransfers))

	for index := range jobPartOrder.Tasks{
		currentTransferEntry := JobPartPlanTransfer{currentTransferChunkOffset, uint16(len(jobPartOrder.Tasks[index].Src)),
			uint16(len(jobPartOrder.Tasks[index].Dst)),
			getNumChunks(jobPartOrder.SrcLocationType, jobPartOrder.DstLocationType,
				jobPartOrder.Tasks[index], destBlobData),
			uint32(jobPartOrder.Tasks[index].SecLastModifiedTime.Nanosecond()), ChunkTransferStatusInactive}
		numBytesWritten, err = writeInterfaceDataToWriter(file, &currentTransferEntry, uint64(unsafe.Sizeof(JobPartPlanTransfer{})))
		if err != nil{
			return err
		}
		transferEntryList[index] = currentTransferEntry
		trasnferEntryOffsets[index] = currentTransferChunkOffset
		currentEndOffsetOfFile += uint64(numBytesWritten)
		currentTransferChunkOffset += uint64(currentTransferEntry.ChunkNum* uint16(unsafe.Sizeof(JobPartPlanTransferChunk{}))) +
												uint64(currentTransferEntry.SrcLength) + uint64(currentTransferEntry.DstLength)
	}

	for index := range jobPartOrder.Tasks{
		currentTransferEntry := transferEntryList[index]
		if currentEndOffsetOfFile != trasnferEntryOffsets[index]{
			errorMsg := fmt.Sprintf(TransferTaskOffsetCalculationError, trasnferEntryOffsets[index],
											currentEndOffsetOfFile, convertJobIdBytesToString(jPartPlan.JobId), jPartPlan.PartNo,index)
			return errors.New(errorMsg)
		}
		for  cIndex := uint16(0); cIndex < currentTransferEntry.ChunkNum; cIndex++ {
			chunk := JobPartPlanTransferChunk{[128 / 8]byte{}, ChunkTransferStatusInactive}
			numBytesWritten, err = writeInterfaceDataToWriter(file, &chunk, uint64(unsafe.Sizeof(JobPartPlanTransferChunk{})))
			currentEndOffsetOfFile += uint64(numBytesWritten)
		}
		numBytesWritten, err = file.WriteString(jobPartOrder.Tasks[index].Src)
		if err != nil{
			return err
		}
		currentEndOffsetOfFile += uint64(numBytesWritten)
		numBytesWritten, err = file.WriteString(jobPartOrder.Tasks[index].Dst)
		if err != nil{
			return err
		}
		currentEndOffsetOfFile += uint64(numBytesWritten)
	}
	return nil
}

//todo remove error
func jobPartTojobPartPlan(jobPart JobPart) (JobPartPlan, error){
	var jobID [128 /8] byte
	versionID := jobPart.Version
	jobID, err := convertJobIdToByteFormat(jobPart.JobID)
	if err != nil {
		return JobPartPlan{}, err
	}
	partNo64, err := strconv.ParseUint(jobPart.PartNo, 10, 32)
	if err != nil {
		return JobPartPlan{}, err
	}
	partNo := uint32(partNo64)
	binary.Read(bytes.NewReader([]byte(jobPart.PartNo)), binary.LittleEndian, &partNo)
	TTA := uint32(time.Now().Nanosecond())
	numTransfer := uint32(len(jobPart.Tasks))
	jPartInFile := JobPartPlan{versionID, jobID, partNo, DefaultJobPriority, TTA,
		jobPart.SrcLocationType, jobPart.DstLocationType,
		numTransfer}
	return jPartInFile, nil
}

func getNumChunks(srcLocationType LocationType, dstLocationType LocationType,
					task task, destBlobData JobPartPlanBlobData) uint16{
	return 10
}

func dataToDestinationBlobData(data blockBlobData) (JobPartPlanBlobData, error){
	var contentTypeBytes [256]byte
	var contentEncodingBytes [256]byte
	var metaDataBytes [1000]byte
	blockSize := data.BlockSize
	contentType := data.BlobData.ContentType
	contentEncoding := data.BlobData.ContentEncoding
	metaData := data.BlobData.MetaData
	if len(contentEncoding) > MAX_SIZE_CONTENT_ENCODING {
		return JobPartPlanBlobData{}, errors.New(ContentEncodingLengthError)
	}
	if len(contentType) > MAX_SIZE_CONTENT_TYPE {
		return JobPartPlanBlobData{}, errors.New(ContentTypeLengthError)
	}
	if len(metaData) > MAX_SIZE_META_DATA {
		return JobPartPlanBlobData{}, errors.New(MetaDataLengthError)
	}
	copy(contentTypeBytes[:], contentType)
	copy(contentEncodingBytes[:], contentEncoding)
	copy(metaDataBytes[:], metaData)

	return JobPartPlanBlobData{uint8(len(contentType)), contentTypeBytes,
								uint8(len(contentEncoding)), contentEncodingBytes,
								uint16(len(metaData)), metaDataBytes, uint16(blockSize)}, nil
}
