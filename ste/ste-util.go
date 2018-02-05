package ste

import (
	"encoding/binary"
	"encoding/hex"
	"errors"
	"fmt"
	"github.com/Azure/azure-storage-azcopy/common"
	"io"
	"io/ioutil"
	"os"
	"path/filepath"
	"reflect"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"unsafe"
	//"golang.org/x/net/context"
	"context"
	"github.com/Azure/azure-pipeline-go/pipeline"
	"sync/atomic"
)

// JobInfo contains JobPartsMap and Logger
// JobPartsMap maps part number to JobPartPlanInfo reference for a given JobId
// Logger is the logger instance for a given JobId
type JobInfo struct {
	JobPartsMap       map[common.PartNumber]*JobPartPlanInfo
	NumberOfPartsDone uint32
	Logger            *common.Logger
}

// JobToLoggerMap is the Synchronous Map of Map to hold JobPartPlanPointer reference for combination of JobId and partNum.
// Provides the thread safe Load and Store Method
type JobsInfoMap struct {
	// ReadWrite Mutex
	sync.RWMutex
	// map jobId -->[partNo -->JobPartPlanInfo Pointer]
	internalMap map[common.JobID]*JobInfo
}

// LoadJobPartsMapForJob returns the map of PartNumber to JobPartPlanInfo Pointer for given JobId in thread-safe manner.
func (jMap *JobsInfoMap) LoadJobPartsMapForJob(jobId common.JobID) (map[common.PartNumber]*JobPartPlanInfo, bool) {
	jMap.RLock()
	jobInfo, ok := jMap.internalMap[jobId]
	jMap.RUnlock()
	if !ok {
		return nil, ok
	}
	return jobInfo.JobPartsMap, ok
}

// LoadJobPartPlanInfoForJobPart returns the JobPartPlanInfo Pointer for given combination of JobId and part number in thread-safe manner.
func (jMap *JobsInfoMap) LoadJobPartPlanInfoForJobPart(jobId common.JobID, partNumber common.PartNumber) *JobPartPlanInfo {
	jMap.RLock()
	partMap := jMap.internalMap[jobId]
	if partMap == nil {
		jMap.RUnlock()
		return nil
	}
	jHandler := partMap.JobPartsMap[partNumber]
	jMap.RUnlock()
	return jHandler
}

// LoadExistingJobIds returns the list of existing JobIds for which there are entries in the internal map in thread-safe manner.
func (jMap *JobsInfoMap) LoadExistingJobIds() []common.JobID {
	jMap.RLock()
	var existingJobs []common.JobID
	for jobId, _ := range jMap.internalMap {
		existingJobs = append(existingJobs, jobId)
	}
	jMap.RUnlock()
	return existingJobs
}

// StoreJobPartPlanInfo stores the JobPartPlanInfo reference for given combination of JobId and part number in thread-safe manner.
func (jMap *JobsInfoMap) StoreJobPartPlanInfo(jobId common.JobID, partNumber common.PartNumber, jobLogVerbosity pipeline.LogLevel, jHandler *JobPartPlanInfo) {
	jMap.Lock()
	var jobInfo = jMap.internalMap[jobId]
	// If there is no JobInfo instance for given jobId
	if jobInfo == nil {
		jobInfo = new(JobInfo)
		jobInfo.JobPartsMap = make(map[common.PartNumber]*JobPartPlanInfo)
	} else if jobInfo.JobPartsMap == nil {
		// If the current JobInfo instance for given jobId has not JobPartsMap initialized
		jobInfo.JobPartsMap = make(map[common.PartNumber]*JobPartPlanInfo)
	}
	// If there is no logger instance for the current Job,
	// initialize the logger instance with log severity and jobId
	// log filename is $JobId.log
	if jobInfo.Logger == nil {
		logger := new(common.Logger)
		logger.Initialize(jobLogVerbosity, fmt.Sprintf("%s.log", jobId))
		jobInfo.Logger = logger
		//jobInfo.Logger.Initialize(jobLogVerbosity, fmt.Sprintf("%s.log", jobId))
	}
	jobInfo.JobPartsMap[partNumber] = jHandler
	jMap.internalMap[jobId] = jobInfo
	jMap.Unlock()
}

// LoadLoggerForJob loads the logger instance for given jobId in thread safe manner
func (jMap *JobsInfoMap) LoadLoggerForJob(jobId common.JobID) *common.Logger {
	jMap.RLock()
	jobInfo := jMap.internalMap[jobId]
	jMap.RUnlock()
	if jobInfo == nil {
		return nil
	} else {
		return jobInfo.Logger
	}
}

// DeleteJobInfoForJobId api deletes an entry of given JobId the JobsInfoMap
func (jMap *JobsInfoMap) DeleteJobInfoForJobId(jobId common.JobID) {
	jMap.Lock()
	delete(jMap.internalMap, jobId)
	jMap.Unlock()
}

// NewJobPartPlanInfoMap returns a new instance of synchronous JobsInfoMap to hold JobPartPlanInfo Pointer for given combination of JobId and part number.
func NewJobPartPlanInfoMap() *JobsInfoMap {
	return &JobsInfoMap{
		internalMap: make(map[common.JobID]*JobInfo),
	}
}

// parseStringToJobInfo api parses the file name to extract the job Id, part number and schema version number
// Returns the JobId, PartNumber and data schema version
func parseStringToJobInfo(s string) (jobId common.JobID, partNo common.PartNumber, version common.Version) {

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
	if err != nil {
		errMsg := fmt.Sprintf("error parsing the mememory map file name %s", s)
		panic(errors.New(errMsg))
	}

	// parsing version number string into uint32 integer
	versionNo64, err := strconv.ParseUint(versionString, 10, 32)
	if err != nil {
		errMsg := fmt.Sprintf("error parsing the mememory map file name %s", s)
		panic(errors.New(errMsg))
	}
	//fmt.Println(" string ", common.JobID(jobIdString), " ", common.PartNumber(partNo64), " ", common.Version(versionNo64))
	return common.JobID(jobIdString), common.PartNumber(partNo64), common.Version(versionNo64)
}

// formatJobInfoToString builds the JobPart file name using the given JobId, part number and data schema version
// fileName format := $jobId-$partnumber.stev$dataschemaversion
func formatJobInfoToString(jobPartOrder common.CopyJobPartOrder) string {
	versionIdString := fmt.Sprintf("%05d", jobPartOrder.Version)
	partNoString := fmt.Sprintf("%05d", jobPartOrder.PartNum)
	fileName := string(jobPartOrder.ID) + "-" + partNoString + ".steV" + versionIdString
	return fileName
}

// convertJobIdToByteFormat converts the JobId from string format to 16 byte array
func convertJobIdToByteFormat(jobIDString common.JobID) [128 / 8]byte {
	var jobID [128 / 8]byte
	guIdBytes, err := hex.DecodeString(string(jobIDString))
	if err != nil {
		panic(err)
	}
	copy(jobID[:], guIdBytes)
	return jobID
}

// writeInterfaceDataToWriter api writes the content of given interface to the io writer
func writeInterfaceDataToWriter(writer io.Writer, f interface{}, structSize uint64) (int, error) {
	rv := reflect.ValueOf(f)
	var byteSliceStruct = struct {
		addr uintptr
		len  int
		cap  int
	}{uintptr(rv.Pointer()), int(structSize), int(structSize)}
	structByteSlice := *(*[]byte)(unsafe.Pointer(&byteSliceStruct))
	err := binary.Write(writer, binary.LittleEndian, structByteSlice)
	if err != nil {
		panic(err)
	}
	return int(structSize), nil
}

func convertJobIdBytesToString(jobId [128 / 8]byte) string {
	jobIdString := fmt.Sprintf("%x%x%x%x%x", jobId[0:4], jobId[4:6], jobId[6:8], jobId[8:10], jobId[10:])
	return jobIdString
}

// reconstructTheExistingJobParts reconstructs the in memory JobPartPlanInfo for existing memory map JobFile
func reconstructTheExistingJobParts(jPartPlanInfoMap *JobsInfoMap) {
	versionIdString := fmt.Sprintf("%05d", dataSchemaVersion)
	// list memory map files with .steV$dataschemaVersion to avoid the reconstruction of old schema version memory map file
	files := listFileWithExtension(".steV" + versionIdString)
	for index := 0; index < len(files); index++ {
		fileName := files[index].Name()
		// extracting the jobId and part number from file name
		jobIdString, partNumber, _ := parseStringToJobInfo(fileName)
		// creating a new JobPartPlanInfo pointer and initializing it
		jobHandler := new(JobPartPlanInfo)
		// Initializing the JobPartPlanInfo for existing Job file
		jobHandler.initialize(steContext, fileName)

		jobHandler.fileName = fileName

		// storing the JobPartPlanInfo pointer for given combination of JobId and part number
		putJobPartInfoHandlerIntoMap(jobHandler, jobIdString, partNumber, jobHandler.getJobPartPlanPointer().LogSeverity, jPartPlanInfoMap)
	}
}

// listFileWithExtension list all files in the current directory that has given extension
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

// fileAlreadyExists api determines whether file with fileName exists in directory dir or not
// Returns true is file with fileName exists else returns false
func fileAlreadyExists(fileName string, dir string) (bool, error) {

	// listing the content of directory dir
	fileInfos, err := ioutil.ReadDir(dir)
	if err != nil {
		errorMsg := fmt.Sprintf("not able to list contents of directory %s", dir)
		return false, errors.New(errorMsg)
	}

	// iterating through each file and comparing the file name with given fileName
	for index := range fileInfos {
		if strings.Compare(fileName, fileInfos[index].Name()) == 0 {
			errorMsg := fmt.Sprintf("file %s already exists", fileName)
			return true, errors.New(errorMsg)
		}
	}
	return false, nil
}

// getTransferMsgDetail returns the details of a transfer for given JobId, part number and transfer index
func getTransferMsgDetail(jobId common.JobID, partNo common.PartNumber, transferEntryIndex uint32, jPartPlanInfoMap *JobsInfoMap) TransferMsgDetail {
	// jHandler is the JobPartPlanInfo Pointer for given JobId and part number
	jHandler, err := getJobPartInfoReferenceFromMap(jobId, partNo, jPartPlanInfoMap)
	if err != nil {
		panic(err)
	}
	// jPartPlanPointer is the memory map JobPartPlan for given JobId and part number
	jPartPlanPointer := jHandler.getJobPartPlanPointer()
	sourceType := jPartPlanPointer.SrcLocationType
	destinationType := jPartPlanPointer.DstLocationType
	source, destination := jHandler.getTransferSrcDstDetail(transferEntryIndex)
	chunkSize := jPartPlanPointer.BlobData.BlockSize
	return TransferMsgDetail{jobId, partNo, transferEntryIndex, chunkSize, sourceType,
		source, destinationType, destination, jHandler.TransferInfo[transferEntryIndex].ctx,
		jHandler.TransferInfo[transferEntryIndex].cancel, jPartPlanInfoMap}
}

// updateChunkInfo updates the chunk at given chunkIndex for given JobId, partNumber and transfer
func updateChunkInfo(jobId common.JobID, partNo common.PartNumber, transferEntryIndex uint32, chunkIndex uint16, status uint8, jobsInfoMap *JobsInfoMap, transferCtx context.Context) {
	//select {
	//case <- transferCtx.Done():
	//	return
	//default:
	//	jHandler, err := getJobPartInfoReferenceFromMap(jobId, partNo, jobsInfoMap)
	//	if err != nil {
	//		panic(err)
	//	}
	//	resultMessage := jHandler.updateTheChunkInfo(transferEntryIndex, chunkIndex, [128 / 8]byte{}, status)
	//	getLoggerForJobId(jobId, jobsInfoMap).Logf(common.LogInfo, "%s for jobId %s and part number %d", resultMessage, jobId, partNo)
	//}
}

// updateTransferStatus updates the status of given transfer for given jobId and partNumber in thread safe manner
func updateTransferStatus(jobId common.JobID, partNo common.PartNumber, transferIndex uint32, transferStatus uint8, jPartPlanInfoMap *JobsInfoMap) {
	jHandler, err := getJobPartInfoReferenceFromMap(jobId, partNo, jPartPlanInfoMap)
	if err != nil {
		panic(err)
	}
	transferHeader := jHandler.Transfer(transferIndex)
	if transferHeader == nil {
		getLoggerForJobId(jobId, jPartPlanInfoMap).Logf(common.LogWarning, "no transfer header found for JobId %s part number %d and transfer index %d", jobId, partNo, transferIndex)
		return
	}
	// If the status of transfer is failed, then it means that one of the chunk has failed
	// and there is no need to update the status
	if atomic.LoadUintptr((*uintptr)(unsafe.Pointer(&transferHeader.Status))) == common.TransferStatusFailed{
		return
	}
	atomic.StoreUintptr((*uintptr)(unsafe.Pointer(&transferHeader.Status)), *(*uintptr)(unsafe.Pointer(&transferStatus)))
	//transferHeader.Status = common.Status(transferStatus)
}

func updateNumberOfTransferDone(jobId common.JobID, partNumber common.PartNumber, jobsInfoMap *JobsInfoMap) {
	jHandler, err := getJobPartInfoReferenceFromMap(jobId, partNumber, jobsInfoMap)
	if err != nil {
		panic(err)
	}
	jPartPlanInfo := jHandler.getJobPartPlanPointer()
	if jPartPlanInfo == nil {
		panic(errors.New(fmt.Sprintf("job part plan reference nil for Job %s and part number %d", jobId, partNumber)))
	}
	if atomic.AddUint32(&jHandler.NumberOfTransfersCompleted, 1) == jPartPlanInfo.NumTransfers {
		jPartPlanInfo.JobStatus = Completed
	}
}

// getLoggerForJobId returns the logger instance for a given JobId
func getLoggerForJobId(jobId common.JobID, jobsInfoMap *JobsInfoMap) *common.Logger {
	logger := jobsInfoMap.LoadLoggerForJob(jobId)
	if logger == nil {
		panic(errors.New(fmt.Sprintf("no logger instance initialized for jobId %s", jobId)))
	}
	return logger
}
