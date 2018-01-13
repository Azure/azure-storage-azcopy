package ste

import (
	"github.com/Azure/azure-storage-azcopy/common"
	"os"
	"path/filepath"
	"regexp"
	"fmt"
	"encoding/hex"
	"io"
	"reflect"
	"encoding/binary"
	"encoding/base64"
	"strings"
	"strconv"
	"errors"
	"unsafe"
	"io/ioutil"
	"sync"
)

type JobPartPlanInfoMap struct {
	sync.RWMutex
	internalMap map[common.JobID]map[common.PartNumber]*JobPartPlanInfo
}

func (jMap *JobPartPlanInfoMap) LoadPartPlanMapforJob(jobId common.JobID) (map[common.PartNumber]*JobPartPlanInfo, bool) {
	jMap.RLock()
	partMap, ok := jMap.internalMap[jobId]
	jMap.RUnlock()
	return partMap, ok
}

func (jMap *JobPartPlanInfoMap) LoadJobPartPlanInfoForJobPart(jobId common.JobID, partNumber common.PartNumber) (*JobPartPlanInfo){
	jMap.RLock()
	partMap := jMap.internalMap[jobId]
	if partMap == nil{
		jMap.RUnlock()
		return nil
	}
	jHandler := partMap[partNumber]
	jMap.RUnlock()
	return jHandler
}

func (jMap *JobPartPlanInfoMap) StoreJobPartPlanInfo(jobId common.JobID, partNumber common.PartNumber, jHandler *JobPartPlanInfo) {
	jMap.Lock()
	partMap  := jMap.internalMap[jobId]
	if partMap == nil { // there is no previous entry for given jobId
	fmt.Println("there is no previous entry for given jobId", jobId)
		partMap = make(map[common.PartNumber]*JobPartPlanInfo)
		partMap[partNumber] = jHandler
		jMap.internalMap[jobId] = partMap
	}else{
		fmt.Println("//there already exists some entry for given jobID", jobId)
		//there already exists some entry for given jobID
		jMap.internalMap[jobId][partNumber] = jHandler
	}
	jMap.Unlock()
}

func NewJobPartPlanInfoMap() (*JobPartPlanInfoMap) {
	return &JobPartPlanInfoMap{
		internalMap:make(map[common.JobID]map[common.PartNumber]*JobPartPlanInfo),
	}
}

// parseStringToJobInfo api parses the file name to extract the job Id, part number and schema version number
// Returns the JobId, PartNumber and data schema version
func parseStringToJobInfo(s string) (jobId common.JobID, partNo common.PartNumber, version common.Version){

	/*
		* Split string using delimeter '-'
		* First part of string is JobId
		* Other half of string contains part number and version number separated by '.'
		* split other half using '.' as delimeter
	    * first half of this split is part number while the other half is version number with stev as prefix
	    * remove the stev prefix from version number
	    * parse part number string and version number string into uint32 integer
	 */
	// split the s string using '-' which separates the jobId from the rest of string in filename
	parts := strings.Split(s, "-")
	jobIdString := parts[0]
	partNo_versionNo := parts[1]

	// after jobId string, partNo and schema version are separated using '.'
	parts = strings.Split(partNo_versionNo, ".")
	partNoString := parts[0]

	// removing 'stev' prefix from version number
	versionString := parts[1][4:]

	// parsing part number string into uint32 integer
	partNo64, err := strconv.ParseUint(partNoString, 10, 32)
	if err != nil{
		errMsg := fmt.Sprintf("error parsing the mememory map file name %s", s)
		panic(errors.New(errMsg))
	}

	// parsing version number string into uint32 integer
	versionNo64, err := strconv.ParseUint(versionString, 10, 32)
	if err != nil{
		errMsg := fmt.Sprintf("error parsing the mememory map file name %s", s)
		panic(errors.New(errMsg))
	}
	fmt.Println(" string ", common.JobID(jobIdString), " ", common.PartNumber(partNo64), " ", common.Version(versionNo64))
	return common.JobID(jobIdString), common.PartNumber(partNo64), common.Version(versionNo64)
}

func formatJobInfoToString(jobPartOrder common.CopyJobPartOrder) (string){
	versionIdString := fmt.Sprintf("%05d", jobPartOrder.Version)
	partNoString := fmt.Sprintf("%05d", jobPartOrder.PartNum)
	fileName := string(jobPartOrder.ID) + "-" + partNoString + ".stev" + versionIdString
	return fileName
}


func convertJobIdToByteFormat(jobIDString common.JobID) ([128 / 8]byte){
	var jobID [128 /8] byte
	guIdBytes, err := hex.DecodeString(string(jobIDString))
	if err != nil {
		panic(err)
	}
	copy(jobID[:], guIdBytes)
	return jobID
}

// writeInterfaceDataToWriter api writes the content of given interface to the io writer
func writeInterfaceDataToWriter( writer io.Writer, f interface{}, structSize uint64) (int, error){
	rv := reflect.ValueOf(f)
	var byteSliceStruct = struct {
		addr uintptr
		len int
		cap int
	}{uintptr(rv.Pointer()), int(structSize), int(structSize)}
	structByteSlice := *(*[]byte)(unsafe.Pointer(&byteSliceStruct))
	err := binary.Write(writer, binary.LittleEndian, structByteSlice)
	if err != nil{
		panic (err)
	}
	return int(structSize), nil
}

func convertJobIdBytesToString(jobId [128 /8]byte) (string){
	jobIdString := fmt.Sprintf("%x%x%x%x%x", jobId[0:4], jobId[4:6], jobId[6:8], jobId[8:10], jobId[10:])
	return jobIdString
}

func reconstructTheExistingJobPart(jPartPlanInfoMap *JobPartPlanInfoMap) (error){
	files := listFileWithExtension(".stev")
	for index := 0; index < len(files) ; index++{
		fileName := files[index].Name()
		jobIdString, partNumber, versionNumber := parseStringToJobInfo(fileName)
		if versionNumber != dataSchemaVersion{
			continue
		}
		jobHandler := new(JobPartPlanInfo)
		err := jobHandler.initialize(steContext, fileName)
		if err != nil{
			return err
		}
		//fmt.Println("jobID & partno ", jobIdString, " ", partNumber, " ", versionNumber)
		putJobPartInfoHandlerIntoMap(jobHandler, jobIdString, partNumber, jPartPlanInfoMap)
	}
	return nil
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

func creatingTheBlockIds(numBlocks int) ([] string){
	blockIDBinaryToBase64 := func(blockID []byte) string { return base64.StdEncoding.EncodeToString(blockID) }

	blockIDIntToBase64 := func(blockID int) string {
		binaryBlockID := (&[4]byte{})[:] // All block IDs are 4 bytes long
		binary.LittleEndian.PutUint32(binaryBlockID, uint32(blockID))
		return blockIDBinaryToBase64(binaryBlockID)
	}

	base64BlockIDs := make([]string, numBlocks)

	for index := 0; index < numBlocks ; index++ {
		base64BlockIDs[index] = blockIDIntToBase64(index)
	}
	return base64BlockIDs
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

func getTransferMsgDetail (jobId common.JobID, partNo common.PartNumber, transferEntryIndex uint32, jPartPlanInfoMap *JobPartPlanInfoMap) (TransferMsgDetail){
	jHandler, err := getJobPartInfoHandlerFromMap(jobId, partNo, jPartPlanInfoMap)
	if err != nil{
		panic(err)
	}
	jPartPlanPointer := jHandler.getJobPartPlanPointer()
	sourceType := jPartPlanPointer.SrcLocationType
	destinationType := jPartPlanPointer.DstLocationType
	source, destination := jHandler.getTransferSrcDstDetail(transferEntryIndex)
	chunkSize := jPartPlanPointer.BlobData.BlockSize
	return TransferMsgDetail{jobId, partNo,transferEntryIndex, chunkSize, sourceType,
		source, destinationType, destination, jHandler.TrasnferInfo[transferEntryIndex].ctx,
						jHandler.TrasnferInfo[transferEntryIndex].cancel, jPartPlanInfoMap}
}

func updateChunkInfo(jobId common.JobID, partNo common.PartNumber, transferEntryIndex uint32, chunkIndex uint16, status uint8, jPartPlanInfoMap *JobPartPlanInfoMap) {
	jHandler, err := getJobPartInfoHandlerFromMap(jobId, partNo, jPartPlanInfoMap)
	if err != nil{
		panic(err)
	}
	jHandler.updateTheChunkInfo(transferEntryIndex, chunkIndex, [128 /8]byte{}, status)
}

func updateTransferStatus(jobId common.JobID, partNo common.PartNumber, transferIndex uint32, transferStatus uint8, jPartPlanInfoMap *JobPartPlanInfoMap){
	jHandler, err := getJobPartInfoHandlerFromMap(jobId, partNo, jPartPlanInfoMap)
	if err != nil{
		panic (err)
	}
	transferHeader := jHandler.Transfer(transferIndex)
	transferHeader.Status = transferStatus
}