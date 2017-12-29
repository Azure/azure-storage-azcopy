package main

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
)

var currFileDirectory string = "."

type TransferInfo struct {
	ctx context.Context
	cancel context.CancelFunc
	NumChunksCompleted uint16
}

type JobPartPlanInfo struct {
	ctx          context.Context
	cancel       context.CancelFunc
	memMap       mmap.MMap
	TrasnferInfo []TransferInfo
}

func (job *JobPartPlanInfo) initializeJobPart(jobContext context.Context, jobPart JobPart,
											destBlobData destinationBlobData, isFileAlreadyCreated bool) (error){

	if !isFileAlreadyCreated {
	err := createwriteBlockBlobJobFile(jobPart, destBlobData)
	if err != nil {
		return err
	}
	}

	job.ctx, job.cancel = context.WithCancel(jobContext)

	jobIdBytes, err  := convertJobIdToByteFormat(jobPart.JobID)
	if err != nil {
		return err
	}
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

func (job *JobPartPlanInfo)shutDownHandler()  (error){
	if job.memMap == nil {
		return errors.New(MemoryMapFileUnmappedAlreadyError)
	}
	err := job.memMap.Unmap()
	if err != nil {
		return err
	}
	return nil
}

func (job *JobPartPlanInfo) getJobPartPlanPointer() (*JobPartPlan, error){
	if job.memMap == nil {
		return nil, errors.New(MemoryMapFileUnmappedAlreadyError)
	}
	var memMap []byte = job.memMap
	if len(memMap) <= 0 {
		fmt.Println("mem map of job hander is nil")
		return nil,  errors.New(MemoryMapFileUnmappedAlreadyError)
	}
	jPart := (*JobPartPlan)(unsafe.Pointer((*reflect.SliceHeader)(unsafe.Pointer(&memMap)).Data))
	return jPart, nil
}

func (job *JobPartPlanInfo)getNumTasksInJobPartFile() (uint32, error){
	if job.memMap == nil {
		return 0, errors.New(MemoryMapFileUnmappedAlreadyError)
	}
	var memMap []byte = job.memMap
	if len(memMap) <= 0 {
		fmt.Println("mem map of job hander is nil")
		return 0,  errors.New(MemoryMapFileUnmappedAlreadyError)
	}
	jPart := (*JobPartPlan)(unsafe.Pointer((*reflect.SliceHeader)(unsafe.Pointer(&memMap)).Data))
	return jPart.NumTransfers, nil
}

func (job *JobPartPlanInfo) getTransferSrcDstDetail(entryIndex uint32) (SrcDstPair, error){
	tEntry, err := job.getTransferEntryForEntryIndex(entryIndex)
	if err != nil{
		return SrcDstPair{}, err
	}
	numChunks := tEntry.NumChunks
	srcStringOffset := tEntry.Offset + (uint64(unsafe.Sizeof(chunkInfo{})) * uint64(numChunks))
	dstStringOffset := srcStringOffset + uint64(tEntry.SrcLength)
	srcStringSlice := job.memMap[srcStringOffset : srcStringOffset + uint64(tEntry.SrcLength)]
	dstStringSlice := job.memMap[dstStringOffset : dstStringOffset + uint64(tEntry.DstLength)]
	srcString := string(srcStringSlice)
	dstString := string(dstStringSlice)
	return SrcDstPair{srcString, dstString}, nil
}

func (job *JobPartPlanInfo) getTransferEntryForEntryIndex(entryIndex uint32) (*transferEntry, error){
	if job.memMap == nil{
		return nil, errors.New(MemoryMapFileUnmappedAlreadyError)
	}
	numTasks, err := job.getNumTasksInJobPartFile()
	if err != nil {
		return nil, err
	}
	if entryIndex >= numTasks {
		return nil, errors.New(TransferIndexOutOfBoundError)
	}
	jPartPlan, err := job.getJobPartPlanPointer()
	if err != nil{
		return nil, err
	}
	transferEntryOffsetIndex := uint64(unsafe.Sizeof(*jPartPlan)) + uint64(unsafe.Sizeof(destinationBlobData{})) + (uint64(unsafe.Sizeof(transferEntry{})) * uint64(entryIndex))
	transferEntrySlice := job.memMap[transferEntryOffsetIndex :]
	tEntry := (*transferEntry)(unsafe.Pointer((*reflect.SliceHeader)(unsafe.Pointer(&transferEntrySlice)).Data))
	return tEntry, nil
}

func (job *JobPartPlanInfo)updateTheChunkInfo(entryIndex uint32, chunkIndex uint16,
	crc [128 / 8]byte, status uint8) (error){
	if job.memMap == nil{
		return errors.New(MemoryMapFileUnmappedAlreadyError)
	}
	jPartPlan, err := job.getJobPartPlanPointer()
	if err != nil{
		return err
	}
	cInfo, err := job.getChunkInfo(entryIndex, chunkIndex)
	if err != nil{
		return err
	}
	copy(cInfo.BlockId[:],crc[:])
	cInfo.Status = status
	result := fmt.Sprintf(TransferEntryChunkUpdateSuccess, chunkIndex, entryIndex, convertJobIdBytesToString(jPartPlan.JobId))
	fmt.Println(result)
	return nil
}

func (job JobPartPlanInfo)getChunkInfo(entryIndex uint32, chunkIndex uint16)(*chunkInfo, error){
	if job.memMap == nil{
		return nil, errors.New(MemoryMapFileUnmappedAlreadyError)
	}
	jPartPlan, err := job.getJobPartPlanPointer()
	if err != nil{
		return nil , err
	}
	tEntry, err := job.getTransferEntryForEntryIndex(entryIndex)
	if err != nil{
		return nil , err
	}
	if chunkIndex >= tEntry.NumChunks{
		result := fmt.Sprintf(ChunkIndexOutOfBoundError, chunkIndex, entryIndex, convertJobIdBytesToString(jPartPlan.JobId))
		return nil , errors.New(result)
	}
	chunkInfoOffset := tEntry.Offset
	chunkInfoByteSlice := job.memMap[chunkInfoOffset :]
	chunkInfo := (*chunkInfo)(unsafe.Pointer((*reflect.SliceHeader)(unsafe.Pointer(&chunkInfoByteSlice)).Data))
	return chunkInfo, nil
}

func roundUp(numToRound uint64, multipleOf uint64) (uint64){
	if multipleOf <= 1{
		return numToRound
	}
	if numToRound == 0 {
		return 0
	}
	remainder := numToRound % multipleOf
	if remainder == 0{
		return numToRound;
	}
	return numToRound + multipleOf - remainder
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

func fileAlreadyExists(fileName string, dir string) (bool, error){
	fileInfos, err := ioutil.ReadDir(dir)
	if err != nil {
		errorMsg := fmt.Sprintf(DirectoryListingError, dir)
		return false, errors.New(errorMsg)
	}
	for index := range fileInfos {
		if strings.Compare(fileName, fileInfos[index].Name()) == 0 {
			errorMsg := fmt.Sprintf(FileAlreadyExists, fileName)
			return true, errors.New(errorMsg)
		}
	}
	return false, nil
}

func listFileWithExtension(ext string) []os.FileInfo {
	pathS, err := os.Getwd()
	if err != nil {
		panic(err)
	}
	var files []os.FileInfo
	filepath.Walk(pathS, func(path string, f os.FileInfo, _ error) error {
		if !f.IsDir() {
			r, err := regexp.MatchString(ext, f.Name())
			if err == nil && r {
				files = append(files, f)
			}
		}
		return nil
	})
	return files
}

func createFileName(jobId [128/8]byte, partNo uint32) (string, error) {
	jobIdString := convertJobIdBytesToString(jobId)
	partNoString := strconv.FormatUint(uint64(partNo), 10)
	return jobIdString + "-" + partNoString + (".STE"), nil
}


func CreateFile(jobId [128/8]byte, partNo uint32)  (*os.File, error){
	fileName, err := createFileName(jobId, partNo)
	if err != nil {
		return nil, err
	}
	doesExists, err := fileAlreadyExists(fileName, currFileDirectory)
	if doesExists || err != nil {
		if doesExists {	return nil, errors.New(FileAlreadyExists)	}
		if err != nil {	return nil, err 	}
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

func createwriteBlockBlobJobFile(jobPart JobPart, destBlobData destinationBlobData) (error){
	var currentEndOffsetOfFile uint64 = 0
	jPartPlan, err := jobPartTojobPartPlan(jobPart)
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

	numBytesWritten, err = writeInterfaceDataToWriter(file, &destBlobData, uint64(unsafe.Sizeof(destinationBlobData{})))
	if err != nil {
		err = fmt.Errorf("error writing Data To The File %s", err.Error())
		return err
	}
	currentEndOffsetOfFile += uint64(numBytesWritten)
	trasnferEntryOffsets := make([]uint64, jPartPlan.NumTransfers)
	transferEntryList := make([]transferEntry, jPartPlan.NumTransfers)
	currentTransferChunkOffset := uint64(currentEndOffsetOfFile) + uint64(uint64(unsafe.Sizeof(transferEntry{})) * uint64(jPartPlan.NumTransfers))

	for index := range jobPart.Tasks{
		currentTransferEntry := transferEntry{currentTransferChunkOffset, uint16(len(jobPart.Tasks[index].Src)),
			uint16(len(jobPart.Tasks[index].Dst)),
			getNumChunks(jobPart.SrcLocationType, jobPart.DstLocationType,
				jobPart.Tasks[index], destBlobData),
			uint32(jobPart.Tasks[index].SecLastModifiedTime.Nanosecond()), ChunkTransferStatusInactive}
		numBytesWritten, err = writeInterfaceDataToWriter(file, &currentTransferEntry, uint64(unsafe.Sizeof(transferEntry{})))
		if err != nil{
			return err
		}
		transferEntryList[index] = currentTransferEntry
		trasnferEntryOffsets[index] = currentTransferChunkOffset
		currentEndOffsetOfFile += uint64(numBytesWritten)
		currentTransferChunkOffset += uint64(currentTransferEntry.NumChunks * uint16(unsafe.Sizeof(chunkInfo{}))) +
												uint64(currentTransferEntry.SrcLength) + uint64(currentTransferEntry.DstLength)
	}

	for index := range jobPart.Tasks{
		currentTransferEntry := transferEntryList[index]
		if currentEndOffsetOfFile != trasnferEntryOffsets[index]{
			errorMsg := fmt.Sprintf(TransferTaskOffsetCalculationError, trasnferEntryOffsets[index],
											currentEndOffsetOfFile, convertJobIdBytesToString(jPartPlan.JobId), jPartPlan.PartNo,index)
			return errors.New(errorMsg)
		}
		for  cIndex := uint16(0); cIndex < currentTransferEntry.NumChunks; cIndex++ {
			chunk := chunkInfo{[128 / 8]byte{}, ChunkTransferStatusInactive}
			numBytesWritten, err = writeInterfaceDataToWriter(file, &chunk, uint64(unsafe.Sizeof(chunkInfo{})))
			currentEndOffsetOfFile += uint64(numBytesWritten)
		}
		numBytesWritten, err = file.WriteString(jobPart.Tasks[index].Src)
		if err != nil{
			return err
		}
		currentEndOffsetOfFile += uint64(numBytesWritten)
		numBytesWritten, err = file.WriteString(jobPart.Tasks[index].Dst)
		if err != nil{
			return err
		}
		currentEndOffsetOfFile += uint64(numBytesWritten)
	}
	return nil
}

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
					task task, destBlobData destinationBlobData) uint16{
	return 10
}

func dataToDestinationBlobData(data blockBlobData) (destinationBlobData, error){
	var contentTypeBytes [256]byte
	var contentEncodingBytes [256]byte
	var metaDataBytes [1000]byte
	blockSize := data.BlockSize
	contentType := data.BlobData.ContentType
	contentEncoding := data.BlobData.ContentEncoding
	metaData := data.BlobData.MetaData
	if len(contentEncoding) > MAX_SIZE_CONTENT_ENCODING {
		return destinationBlobData{}, errors.New(ContentEncodingLengthError)
	}
	if len(contentType) > MAX_SIZE_CONTENT_TYPE {
		return destinationBlobData{}, errors.New(ContentTypeLengthError)
	}
	if len(metaData) > MAX_SIZE_META_DATA {
		return destinationBlobData{}, errors.New(MetaDataLengthError)
	}
	copy(contentTypeBytes[:], contentType)
	copy(contentEncodingBytes[:], contentEncoding)
	copy(metaDataBytes[:], metaData)

	return destinationBlobData{uint8(len(contentType)), contentTypeBytes,
								uint8(len(contentEncoding)), contentEncodingBytes,
								uint16(len(metaData)), metaDataBytes, uint16(blockSize)}, nil
}
