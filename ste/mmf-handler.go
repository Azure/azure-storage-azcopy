package ste

import (
	"context"
	"errors"
	"fmt"
	"github.com/Azure/azure-storage-azcopy/common"
	"github.com/edsrzf/mmap-go"
	"os"
	"reflect"
	"time"
	"unsafe"
)

var currFileDirectory string = "."

// initialize func initializes the JobPartPlanInfo handler for given JobPartOrder
func (job *JobPartPlanInfo) initialize(jobContext context.Context, fileName string) {

	/*
	 *       creates the job context with cancel using the given context
	 *       memory maps the jobPartOrder file for given fileName
	 *       gets the memory map JobPartPlanHeader for given jobpartorder
	 *       initializes the transferInfo struct for each transfer in jobpartorder
	 */

	// creating the context with cancel
	job.ctx, job.cancel = context.WithCancel(jobContext)

	// memory map the JobPartOrder File with given filename
	job.memMap = memoryMapTheJobFile(fileName)

	job.fileName = fileName

	// gets the memory map JobPartPlanHeader for given JobPartOrder
	jPartPlan := job.getJobPartPlanPointer()

	// initializes the transferInfo slice
	transferInfo := make([]TransferInfo, jPartPlan.NumTransfers)

	job.TransfersInfo = transferInfo

	job.numberOfTransfersDone_doNotUse = 0
}

// shutDownHandler unmaps the memory map file for given JobPartOrder
func (job *JobPartPlanInfo) shutDownHandler() error {
	//if job.memMap == nil {
	//	return errors.New(fmt.Sprintf("memory map file %s already unmapped. Map it again to use further", job.fileName))
	//}
	err := job.memMap.Unmap()
	if err != nil {
		return err
	}
	return nil
}

// getJobPartPlanPointer returns the memory map JobPartPlanHeader pointer
func (job *JobPartPlanInfo) getJobPartPlanPointer() *JobPartPlanHeader {
	// memMap represents the slice of memory map file of JobPartOrder
	memMap := job.memMap

	// casting the memMap slice to JobPartPlanHeader Pointer
	jPart := (*JobPartPlanHeader)(unsafe.Pointer((*reflect.SliceHeader)(unsafe.Pointer(&memMap)).Data))

	return jPart
}

// getTransferSrcDstDetail return the source and destination string for a transfer at given transferIndex in JobPartOrder
func (job *JobPartPlanInfo) getTransferSrcDstDetail(entryIndex uint32) (source, destination string) {
	// get JobPartPlanTransfer Header of transfer in JobPartOrder at given index
	tEntry := job.Transfer(entryIndex)

	// srcStringOffset is the startOffset of source string in memoryMap slice of JobPartOrder for a given transfer
	srcStringOffset := tEntry.Offset

	// dstStringOffset is the startOffset of destination string in memoryMap slice of JobPartOrder for a given transfer
	dstStringOffset := srcStringOffset + uint64(tEntry.SrcLength)

	// srcStringSlice represents the slice of memoryMap byte slice starting from srcStringOffset
	srcStringSlice := job.memMap[srcStringOffset : srcStringOffset+uint64(tEntry.SrcLength)]

	// dstStringSlice represents the slice of memoryMap byte slice starting from dstStringOffset
	dstStringSlice := job.memMap[dstStringOffset : dstStringOffset+uint64(tEntry.DstLength)]

	srcString := string(srcStringSlice)
	dstString := string(dstStringSlice)
	return srcString, dstString
}

// Transfer api gives memory map JobPartPlanTransfer header for given index
func (job *JobPartPlanInfo) Transfer(index uint32) *JobPartPlanTransfer {
	// get memory map JobPartPlan Header Pointer
	jPartPlan := job.getJobPartPlanPointer()
	if index >= jPartPlan.NumTransfers {
		panic(errors.New("transfer %d of JobPart %s does not exists. Transfer Index exceeds number of transfer for this JobPart"))
	}

	// transferEntryOffsetIndex is the start offset of transfer header in memory map
	transferEntryOffsetIndex := uint64(unsafe.Sizeof(*jPartPlan)) + (uint64(unsafe.Sizeof(JobPartPlanTransfer{})) * uint64(index))

	// transferEntrySlice represents the slice of job part order memory map starting from transferEntryOffsetIndex
	transferEntrySlice := job.memMap[transferEntryOffsetIndex:]

	// Casting Slice into transfer header Pointer
	tEntry := (*JobPartPlanTransfer)(unsafe.Pointer((*reflect.SliceHeader)(unsafe.Pointer(&transferEntrySlice)).Data))
	return tEntry
}

func memoryMapTheJobFile(filename string) mmap.MMap {

	// opening the file with given filename
	f, err := os.OpenFile(filename, os.O_RDWR, 0644)
	if err != nil {
		panic(err)
	}

	// defer file closing to user the memory map byte slice later
	defer f.Close()

	mMap, err := mmap.Map(f, mmap.RDWR, 0)
	if err != nil {
		err = fmt.Errorf("error memory mapping the file %s with err %s", filename, err.Error())
		panic(err)
	}
	return mMap
}

// createJobPartPlanFile creates the memory map JobPartPlanHeader using the given JobPartOrder and JobPartPlanBlobData
func createJobPartPlanFile(jobPartOrder common.CopyJobPartOrder, data JobPartPlanBlobData, jobsInfoMap *JobsInfo) (string, error) {
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
	// creating file name from job jobId, part number and version id of job part order
	fileName := formatJobInfoToString(jobPartOrder)

	fileAbsolutePath := currFileDirectory + "/" + fileName

	// Check if file already exist or not
	if fileAlreadyExists(fileName, jobsInfoMap) {
		return "", fmt.Errorf("a job file already exists with create for uuid passed in this job part order %s", fileName)
	}

	// creating the file
	file, err := os.Create(fileAbsolutePath)
	if err != nil {
		errorMsg := fmt.Sprintf("error %s Occured while creating the File for JobId %s \n", err.Error(), jobPartOrder.ID)
		panic(errors.New(errorMsg))
	}

	// creating memory map file jobPartPlan header
	jPartPlan := jobPartToJobPartPlan(jobPartOrder, data)
	if err != nil {
		err = fmt.Errorf("error converting Job Part to Job Part In File with err %s", err.Error())
		panic(err)
	}

	numBytesWritten := writeInterfaceDataToWriter(file, &jPartPlan, uint64(unsafe.Sizeof(JobPartPlanHeader{})))

	// currentEndOffsetOfFile always tracks the total number of bytes written in the memory map file
	currentEndOffsetOfFile += uint64(numBytesWritten)

	// transferEntryOffsets stores the start offset for transfer chunks in memory map file for each transfer.
	transferEntryOffsets := make([]uint64, jPartPlan.NumTransfers)

	//currentTransferChunkOffset stores the start offset the transfer chunks of current transfer
	currentTransferChunkOffset := uint64(currentEndOffsetOfFile) + uint64(uint64(unsafe.Sizeof(JobPartPlanTransfer{}))*uint64(jPartPlan.NumTransfers))

	// This loop creates the JobPartPlan Transfer Header for each transfer
	// Calculates the start offset of the chunk header for each transfer
	for index := range jobPartOrder.Transfers {
		// currentTransferEntry represents the JobPartPlan Transfer Header of a transfer.
		currentTransferEntry := JobPartPlanTransfer{Offset: currentTransferChunkOffset,
			SrcLength:               uint16(len(jobPartOrder.Transfers[index].Source)),
			DstLength:               uint16(len(jobPartOrder.Transfers[index].Destination)),
			ChunkNum:                getNumChunks(uint64(jobPartOrder.Transfers[index].SourceSize), uint64(data.BlockSize)),
			ModifiedTime:            uint32(jobPartOrder.Transfers[index].LastModifiedTime.Nanosecond()),
			SourceSize:              uint64(jobPartOrder.Transfers[index].SourceSize),
			CompletionTime:          0,
			transferStatus_doNotUse: common.TransferInProgress}
		numBytesWritten = writeInterfaceDataToWriter(file, &currentTransferEntry, uint64(unsafe.Sizeof(JobPartPlanTransfer{})))
		transferEntryOffsets[index] = currentTransferChunkOffset
		currentEndOffsetOfFile += uint64(numBytesWritten)

		currentTransferChunkOffset += uint64(currentTransferEntry.SrcLength) + uint64(currentTransferEntry.DstLength)
	}

	for index := range jobPartOrder.Transfers {
		//compares the calculated start offset and actual start offset for chunk headers of a transfer
		if currentEndOffsetOfFile != transferEntryOffsets[index] {
			errorMsg := fmt.Sprintf("calculated offset %d and actual offset %d of Job %s part %d and transfer entry %d does not match", transferEntryOffsets[index],
				currentEndOffsetOfFile, convertJobIdBytesToString(jPartPlan.Id), jPartPlan.PartNum, index)
			panic(errors.New(errorMsg))
		}

		// write the source string in memory map file
		numBytesWritten, err = file.WriteString(jobPartOrder.Transfers[index].Source)
		if err != nil {
			panic(err)
		}

		// write the destination string in memory map file
		currentEndOffsetOfFile += uint64(numBytesWritten)
		numBytesWritten, err = file.WriteString(jobPartOrder.Transfers[index].Destination)
		if err != nil {
			panic(err)
		}
		currentEndOffsetOfFile += uint64(numBytesWritten)
	}

	// closing the memory map file
	file.Close()
	return fileName, nil
}

// Creates the memory map Job Part Plan Header from CopyJobPartOrder and JobPartPlanBlobData
func jobPartToJobPartPlan(jobPart common.CopyJobPartOrder, data JobPartPlanBlobData) JobPartPlanHeader {
	var jobID [128 / 8]byte
	versionID := jobPart.Version
	// converting the job jobId string to [128 / 8] byte format
	jobID = common.JobID(jobPart.ID)
	partNo := jobPart.PartNum

	// calculating the number of transfer for given CopyJobPartOrder
	numTransfer := uint32(len(jobPart.Transfers))
	jPartInFile := JobPartPlanHeader{Version: versionID, Id: jobID, PartNum: partNo,
		IsFinalPart: jobPart.IsFinalPart, Priority: DefaultJobPriority, TTLAfterCompletion: uint32(time.Now().Nanosecond()),
		SrcLocationType: jobPart.SourceType, DstLocationType: jobPart.DestinationType,
		NumTransfers: numTransfer, LogSeverity: jobPart.LogVerbosity, BlobData: data, jobStatus_doNotUse: JobInProgress}
	return jPartInFile
}

// getNumChunks api returns the number of chunks depending on source Type and destination type
func getNumChunks(sourceSize uint64, blockSize uint64) uint16 {
	if uint64(sourceSize)%blockSize == 0 {
		return uint16(uint64(sourceSize) / blockSize)
	} else {
		return uint16(uint64(sourceSize)/blockSize) + 1
	}
}

// dataToDestinationBlobData api creates memory map JobPartPlanBlobData from BlobData sent in the request from-end
func dataToDestinationBlobData(data common.BlobTransferAttributes) (JobPartPlanBlobData, error) {
	var contentTypeBytes [256]byte
	var contentEncodingBytes [256]byte
	var metaDataBytes [1000]byte
	blockSize := data.BlockSizeinBytes
	contentType := data.ContentType
	contentEncoding := data.ContentEncoding
	metaData := data.Metadata
	noGuessMimeType := data.NoGuessMimeType
	// check to verify whether content-length exceeds maximum content encoding length or not
	if len(contentEncoding) > MAX_SIZE_CONTENT_ENCODING {
		return JobPartPlanBlobData{}, fmt.Errorf("size %d of content encoding exceeds the max content-encoding size %d", len(contentEncoding), MAX_SIZE_CONTENT_ENCODING)
	}

	// check to verify whether content-length exceeds maximum content type length or not
	if len(contentType) > MAX_SIZE_CONTENT_TYPE {
		return JobPartPlanBlobData{}, fmt.Errorf("size %d of content type exceeds the max content type size %d", len(contentType), MAX_SIZE_CONTENT_TYPE)
	}

	// check to verify whether meta data length exceeds maximum length or not
	if len(metaData) > MAX_SIZE_META_DATA {
		return JobPartPlanBlobData{}, fmt.Errorf("size %d of metadata exceeds the max metadata size %d", len(metaData), MAX_SIZE_META_DATA)
	}

	// copying contentType string in fixed size byte array
	copy(contentTypeBytes[:], contentType)

	// copying contentEncoding string in fixed size byte array
	copy(contentEncodingBytes[:], contentEncoding)

	//copying metadata string in fixed size byte array
	copy(metaDataBytes[:], metaData)

	// if block size from the front-end is set to 0, block size is set to default block size
	if blockSize == 0 {
		blockSize = common.DefaultBlockSize
	}

	return JobPartPlanBlobData{NoGuessMimeType: noGuessMimeType, ContentTypeLength: uint8(len(contentType)),
		ContentType: contentTypeBytes, ContentEncodingLength: uint8(len(contentEncoding)),
		ContentEncoding: contentEncodingBytes, MetaDataLength: uint16(len(metaData)),
		MetaData: metaDataBytes, PreserveLastModifiedTime: data.PreserveLastModifiedTime,
		BlockSize: uint32(blockSize)}, nil
}
