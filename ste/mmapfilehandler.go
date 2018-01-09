package ste

import (
	"errors"
	"time"
	"fmt"
	"os"
	"github.com/edsrzf/mmap-go"
	"unsafe"
	"reflect"
	"context"
	"github.com/Azure/azure-storage-azcopy/common"
	"io/ioutil"
	"strings"
)

var currFileDirectory string = "."

// initialize func initializes the JobPartPlanInfo handler for given JobPartOrder
func (job *JobPartPlanInfo) initialize(jobContext context.Context, fileName string) (error){

	/*
	 *       creates the job context with cancel using the given context
	 *       memory maps the jobpartorder file for given fileName
	 *       gets the memory map JobPartPlanHeader for given jobpartorder
	 *       initializes the transferInfo struct for each transfer in jobpartorder
	 */

	 // creating the context with cancel
	job.ctx, job.cancel = context.WithCancel(jobContext)

	// memory map the JobPartOrder File with given filename
	job.memMap = memoryMapTheJobFile(fileName)

	// gets the memory map JobPartPlanHeader for given JobPartOrder
	jPartPlan := job.getJobPartPlanPointer()

	// initializes the transferInfo struct for each transfer in jobpartorder
	// Each transfer has context with cancel and numberofchunks completed
	// NumChunkCompleted represents the number of chunks whose transaction has been completed for a transfer
	transferInfo := make([]TransferInfo, jPartPlan.NumTransfers)
	for index := uint32(0); index < jPartPlan.NumTransfers; index++ {
		transferCtx, transferCancel := context.WithCancel(job.ctx)
		transferInfo[index] = TransferInfo{transferCtx, transferCancel, 0}
	}
	job.TrasnferInfo = transferInfo
	return nil
}

// shutDownHandler unmaps the memory map file for given JobPartOrder
func (job *JobPartPlanInfo)shutDownHandler(){
	if job.memMap == nil {
		panic(errors.New(MemoryMapFileUnmappedAlreadyError))
	}
	err := job.memMap.Unmap()
	if err != nil {
		panic(err)
	}
}

// getJobPartPlanPointer returns the memory map JobPartPlanHeader pointer
func (job *JobPartPlanInfo) getJobPartPlanPointer() (*JobPartPlanHeader){

	// memMap represents the slice of memory map file of JobPartOrder
	var memMap []byte = job.memMap

	// casting the memMap slice to JobPartPlanHeader Pointer
	jPart := (*JobPartPlanHeader)(unsafe.Pointer((*reflect.SliceHeader)(unsafe.Pointer(&memMap)).Data))
	return jPart
}

// getTransferSrcDstDetail return the source and destination string for a transfer at given transferIndex in JobPartOrder
func (job *JobPartPlanInfo) getTransferSrcDstDetail(entryIndex uint32) (source, destination string){

	// get JobPartPlanTransfer Header of transfer in JobPartOrder at given index
	tEntry  := job.Transfer(entryIndex)
	if tEntry == nil{
		return
	}

	numChunks := tEntry.ChunkNum

	// srcStringOffset is the startOffset of source string in memoryMap slice of JobPartOrder for a given transfer
	srcStringOffset := tEntry.Offset + (uint64(unsafe.Sizeof(JobPartPlanTransferChunk{})) * uint64(numChunks))

	// dstStringOffset is the startOffset of destination string in memoryMap slice of JobPartOrder for a given transfer
	dstStringOffset := srcStringOffset + uint64(tEntry.SrcLength)

	// srcStringSlice represents the slice of memoryMap byte slice starting from srcStringOffset
	srcStringSlice := job.memMap[srcStringOffset : srcStringOffset + uint64(tEntry.SrcLength)]

	// dstStringSlice represents the slice of memoryMap byte slice starting from dstStringOffset
	dstStringSlice := job.memMap[dstStringOffset : dstStringOffset + uint64(tEntry.DstLength)]

	srcString := string(srcStringSlice)
	dstString := string(dstStringSlice)
	return srcString, dstString
}

// Transfer api gives memory map JobPartPlanTransfer header for given index
func (job *JobPartPlanInfo) Transfer(index uint32) (*JobPartPlanTransfer){

	// get memory map JobPartPlan Header Pointer
	jPartPlan := job.getJobPartPlanPointer()
	if index >= jPartPlan.NumTransfers {
		panic(errors.New("transfer %d of JobPart %s does not exists. Transfer Index exceeds number of transfer for this JobPart"))
	}

	// transferEntryOffsetIndex is the start offset of transfer header in memory map
	transferEntryOffsetIndex := uint64(unsafe.Sizeof(*jPartPlan))  + ((uint64(unsafe.Sizeof(JobPartPlanTransfer{})) * uint64(index)))

	// transferEntrySlice represents the slice of job part order memory map starting from transferEntryOffsetIndex
	transferEntrySlice := job.memMap[transferEntryOffsetIndex :]

	// Casting Slice into transfer header Pointer
	tEntry := (*JobPartPlanTransfer)(unsafe.Pointer((*reflect.SliceHeader)(unsafe.Pointer(&transferEntrySlice)).Data))
	return tEntry
}


// updateTheChunkInfo api updates the memory map JobPartPlanTransferHeader for transfer and chunk at given index
func (job *JobPartPlanInfo)updateTheChunkInfo(transferIndex uint32, chunkIndex uint16,
													crc [128 / 8]byte, status uint8) {
	// get memory map JobPartPlanHeader
	jPartPlan := job.getJobPartPlanPointer()

	// get memory map JobPartPlanTransferChunk Header
	cInfo := job.getChunkInfo(transferIndex, chunkIndex)

	//copy the given CRC into chunk's blockId
	copy(cInfo.BlockId[:],crc[:])

	//updating the chunk status with given status
	cInfo.Status = status
	result := fmt.Sprintf(TransferEntryChunkUpdateSuccess, chunkIndex, transferIndex, convertJobIdBytesToString(jPartPlan.Id))
	fmt.Println(result)
}

// getChunkInfo returns the memory map JobPartPlanTransferChunkHeader for given transfer and chunk index of JobPartOrder
func (job JobPartPlanInfo)getChunkInfo(transferIndex uint32, chunkIndex uint16)(*JobPartPlanTransferChunk){

	// get memory map JobPartPlanHeader
	jPartPlan := job.getJobPartPlanPointer()

	// get memory map JobPartPlanTransfer header for given transferIndex
	tEntry := job.Transfer(transferIndex)

	// Check to verify the bound of chunkIndex
	if chunkIndex >= tEntry.ChunkNum {
		errorMsg := fmt.Sprintf("given chunk %d of transfer %d of JobPart %s does not exists. Chunk Index exceeds number of chunks for transfer",
								chunkIndex, transferIndex, convertJobIdBytesToString(jPartPlan.Id))
		panic (errors.New(errorMsg))
	}

	chunkInfoOffset := tEntry.Offset

	// chunkInfoByteSlice represents the slice of memorymap buffer starting from chunkInfoOffset
	chunkInfoByteSlice := job.memMap[chunkInfoOffset :]

	// casting the chunkInfoByteSlice to memory map JobPartPlanTransferChunk header
	chunkInfo := (*JobPartPlanTransferChunk)(unsafe.Pointer((*reflect.SliceHeader)(unsafe.Pointer(&chunkInfoByteSlice)).Data))
	return chunkInfo
}



// memoryMapTheJobFile api memory maps the file with given fileName
// Returns the memory map byte slice
func memoryMapTheJobFile(filename string)	(mmap.MMap){

	// opening the file with given filename
	f, err:= os.OpenFile(filename, os.O_RDWR , 0644)
	if err != nil {
		panic(err)
	}

	// defer file closing to user the memory map byte slice later
	defer  f.Close()

	mMap, err := mmap.Map(f, mmap.RDWR, 0)
	if err != nil {
		err = fmt.Errorf("error memory mapping the file %s with err %s", filename, err.Error())
		panic(err)
	}
	return mMap
}

// fileAlreadyExists api determines whether file with fileName exists in directory dir or not
// Returns true is file with fileName exists else returns false
func fileAlreadyExists(fileName string, dir string) (bool, error){

	// listing the content of directory dir
	fileInfos, err := ioutil.ReadDir(dir)
	if err != nil {
		errorMsg := fmt.Sprintf(DirectoryListingError, dir)
		return false, errors.New(errorMsg)
	}

	// iterating through each file and comparing the file name with given fileName
	for index := range fileInfos {
		if strings.Compare(fileName, fileInfos[index].Name()) == 0 {
			errorMsg := fmt.Sprintf(FileAlreadyExists, fileName)
			return true, errors.New(errorMsg)
		}
	}
	return false, nil
}

// createJobPartPlanFile creates the memory map JobPartPlanHeader using the given JobPartOrder and JobPartPlanBlobData
func createJobPartPlanFile(jobPartOrder common.CopyJobPartOrder, data JobPartPlanBlobData) (string){
	var currentEndOffsetOfFile uint64 = 0
	/*
	*       Following Steps are executed:
	*		1. Get File Name from JobId and Part Number
	*		2. Create the File with filename
	*       3. Create Job Part Plan From Job Part Order
	*       4. Write Data to file
	* 		5. Close the file
	* 		6. Return File Name
	*/
	// creating file name from job Id, part number and version id of job part order
	fileName := formatJobInfoToString(jobPartOrder)

	fileAbsolutePath := currFileDirectory +"/" + fileName

	// Check if file already exist or not
	doesFileExists, err := fileAlreadyExists(fileName, currFileDirectory)
	if err != nil{
		panic(err)
	}

	// If file exists, it returns the file name without any further operations
	if(doesFileExists){
		return fileName
	}

	// creating the file
	file, err := os.Create(fileAbsolutePath)
	if err != nil {
		errorMsg := fmt.Sprintf("error %s Occured while creating the File for JobId %s \n", err.Error(), jobPartOrder.ID)
		panic(errors.New(errorMsg))
	}

	// creating memory map file jobpartplan header
	jPartPlan := jobPartTojobPartPlan(jobPartOrder, data)
	if err != nil {
		err = fmt.Errorf("error converting Job Part to Job Part In File with err %s", err.Error())
		panic(err)
	}

	numBytesWritten, err := writeInterfaceDataToWriter(file, &jPartPlan, uint64(unsafe.Sizeof(JobPartPlanHeader{})))
	if err != nil {
		err = fmt.Errorf("error writing Data To The File %s", err.Error())
		panic(err)
	}

	// currentEndOffsetOfFile always tracks the total number of bytes written in the memory map file
	currentEndOffsetOfFile += uint64(numBytesWritten)

	// transferEntryOffsets stores the start offset for transfer chunks in memory map file for each transfer.
	transferEntryOffsets := make([]uint64, jPartPlan.NumTransfers)

	// transferEntryList stores the memory map JobPartPlanTransfer header for each transfer in Job Part Order
	transferEntryList := make([]JobPartPlanTransfer, jPartPlan.NumTransfers)

	//currentTransferChunkOffset stores the start offset the transfer chunks of current transfer
	currentTransferChunkOffset := uint64(currentEndOffsetOfFile) + uint64(uint64(unsafe.Sizeof(JobPartPlanTransfer{})) * uint64(jPartPlan.NumTransfers))

	// This loop creates the JobPartPlan Transfer Header for each transfer
	// Calculates the start offset of the chunk header for each transfer
	for index := range jobPartOrder.Transfers{
		// currentTransferEntry represents the JobPartPlan Transfer Header of a transfer.
		currentTransferEntry := JobPartPlanTransfer{currentTransferChunkOffset, uint16(len(jobPartOrder.Transfers[index].Source)),
			uint16(len(jobPartOrder.Transfers[index].Destination)),
			getNumChunks(jobPartOrder.SourceType, jobPartOrder.DestinationType,
				jobPartOrder.Transfers[index], data),
			uint32(jobPartOrder.Transfers[index].LastModifiedTime.Nanosecond()), ChunkTransferStatusInactive}
		numBytesWritten, err = writeInterfaceDataToWriter(file, &currentTransferEntry, uint64(unsafe.Sizeof(JobPartPlanTransfer{})))
		if err != nil{
			panic(err)
		}
		transferEntryList[index] = currentTransferEntry
		transferEntryOffsets[index] = currentTransferChunkOffset
		currentEndOffsetOfFile += uint64(numBytesWritten)

		currentTransferChunkOffset += uint64(currentTransferEntry.ChunkNum* uint16(unsafe.Sizeof(JobPartPlanTransferChunk{}))) +
												uint64(currentTransferEntry.SrcLength) + uint64(currentTransferEntry.DstLength)
	}


	for index := range jobPartOrder.Transfers{
		currentTransferEntry := transferEntryList[index]
		//compares the calculated start offset and actual start offset for chunk headers of a transfer
		if currentEndOffsetOfFile != transferEntryOffsets[index]{
			errorMsg := fmt.Sprintf(TransferTaskOffsetCalculationError, transferEntryOffsets[index],
											currentEndOffsetOfFile, convertJobIdBytesToString(jPartPlan.Id), jPartPlan.PartNum,index)
			panic(errors.New(errorMsg))
		}
		// creating memory map file chunk transfer header JobPartPlanTransferChunk of each chunk in a transfer
		for  cIndex := uint16(0); cIndex < currentTransferEntry.ChunkNum; cIndex++ {
			chunk := JobPartPlanTransferChunk{[128 / 8]byte{}, ChunkTransferStatusInactive}
			numBytesWritten, err = writeInterfaceDataToWriter(file, &chunk, uint64(unsafe.Sizeof(JobPartPlanTransferChunk{})))
			currentEndOffsetOfFile += uint64(numBytesWritten)
		}

		// write the source string in memory map file
		numBytesWritten, err = file.WriteString(jobPartOrder.Transfers[index].Source)
		if err != nil{
			panic(err)
		}

		// write the destination string in memory map file
		currentEndOffsetOfFile += uint64(numBytesWritten)
		numBytesWritten, err = file.WriteString(jobPartOrder.Transfers[index].Destination)
		if err != nil{
			panic(err)
		}
		currentEndOffsetOfFile += uint64(numBytesWritten)
	}

	// closing the memory map file
	file.Close()
	return fileName
}

// Creates the memory map Job Part Plan Header from CopyJobPartOrder and JobPartPlanBlobData
func jobPartTojobPartPlan(jobPart common.CopyJobPartOrder, data JobPartPlanBlobData) (JobPartPlanHeader){
	var jobID [128 /8] byte
	versionID := jobPart.Version
	// converting the job Id string to [128 / 8] byte format
	jobID = convertJobIdToByteFormat(jobPart.ID)
	partNo := jobPart.PartNum
	TTA := uint32(time.Now().Nanosecond())

	// calculating the number of transfer for given CopyJobPartOrder
	numTransfer := uint32(len(jobPart.Transfers))
	jPartInFile := JobPartPlanHeader{versionID, jobID, uint32(partNo),
					jobPart.IsFinalPart,DefaultJobPriority, TTA,
		jobPart.SourceType, jobPart.DestinationType,
		numTransfer, data}
	return jPartInFile
}

// getNumChunks api returns the number of chunks depending on source Type and destination type
func getNumChunks(srcLocationType common.LocationType, dstLocationType common.LocationType,
					transfer common.CopyTransfer, destBlobData JobPartPlanBlobData) uint16{
	return 10
}

// dataToDestinationBlobData api creates memory map JobPartPlanBlobData from BlobData sent in the request from-end
func dataToDestinationBlobData(data common.BlobData) (JobPartPlanBlobData, error){
	var contentTypeBytes [256]byte
	var contentEncodingBytes [256]byte
	var metaDataBytes [1000]byte
	blockSize := data.BlockSizeinKB
	contentType := data.ContentType
	contentEncoding := data.ContentEncoding
	metaData := data.MetaData
	// check to verify whether content-length exceeds maximum content encoding length or not
	if len(contentEncoding) > MAX_SIZE_CONTENT_ENCODING {
		return JobPartPlanBlobData{}, errors.New(ContentEncodingLengthError)
	}

	// check to verify whether content-length exceeds maximum content type length or not
	if len(contentType) > MAX_SIZE_CONTENT_TYPE {
		return JobPartPlanBlobData{}, errors.New(ContentTypeLengthError)
	}

	// check to verify whether meta data length exceeds maximum length or not
	if len(metaData) > MAX_SIZE_META_DATA {
		return JobPartPlanBlobData{}, errors.New(MetaDataLengthError)
	}

	// copying contentType string in fixed size byte array
	copy(contentTypeBytes[:], contentType)

	// copying contentEncoding string in fixed size byte array
	copy(contentEncodingBytes[:], contentEncoding)

	//copying metadata string in fixed size byte array
	copy(metaDataBytes[:], metaData)

	return JobPartPlanBlobData{uint8(len(contentType)), contentTypeBytes,
								uint8(len(contentEncoding)), contentEncodingBytes,
								uint16(len(metaData)), metaDataBytes, uint16(blockSize)}, nil
}
