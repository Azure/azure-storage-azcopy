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
	lock sync.RWMutex
	// map jobId -->[partNo -->JobPartPlanInfo Pointer]
	internalMap map[common.JobID]*JobInfo
}

// LoadJobPartsMapForJob returns the map of PartNumber to JobPartPlanInfo Pointer for given JobId in thread-safe manner.
func (jMap *JobsInfoMap) LoadJobPartsMapForJob(jobId common.JobID) (map[common.PartNumber]*JobPartPlanInfo, bool) {
	jMap.lock.RLock()
	jobInfo, ok := jMap.internalMap[jobId]
	jMap.lock.RUnlock()
	if !ok {
		return nil, ok
	}
	return jobInfo.JobPartsMap, ok
}

// LoadJobInfoForJob returns the JobInfo pointer stored in JobsInfoMap for given JobId in thread-safe manner.
func (jMap *JobsInfoMap) LoadJobInfoForJob(jobId common.JobID) (*JobInfo) {
	jMap.lock.RLock()
	jobInfo, ok := jMap.internalMap[jobId]
	jMap.lock.RUnlock()
	if !ok {
		return nil
	}
	return jobInfo
}

// LoadJobPartPlanInfoForJobPart returns the JobPartPlanInfo Pointer for given combination of JobId and part number in thread-safe manner.
func (jMap *JobsInfoMap) LoadJobPartPlanInfoForJobPart(jobId common.JobID, partNumber common.PartNumber) *JobPartPlanInfo {
	jMap.lock.RLock()
	partMap := jMap.internalMap[jobId]
	if partMap == nil {
		jMap.lock.RUnlock()
		return nil
	}
	jHandler := partMap.JobPartsMap[partNumber]
	jMap.lock.RUnlock()
	return jHandler
}

// LoadExistingJobIds returns the list of existing JobIds for which there are entries in the internal map in thread-safe manner.
func (jMap *JobsInfoMap) LoadExistingJobIds() []common.JobID {
	jMap.lock.RLock()
	//TODO : make existing job as array of size of len
	existingJobs := make([]common.JobID, len(jMap.internalMap))
	//var existingJobs []common.JobID
	for jobId, _ := range jMap.internalMap {
		existingJobs = append(existingJobs, jobId)
	}
	jMap.lock.RUnlock()
	return existingJobs
}

// GetNumberOfPartsForJob returns the number of part order for job with given JobId
func (jMap *JobsInfoMap) GetNumberOfPartsForJob(jobId common.JobID) (uint32){
	jMap.lock.RLock()
	jobInfo := jMap.internalMap[jobId]
	if jobInfo == nil{
		jMap.lock.RUnlock()
		return 0
	}
	partMap := jobInfo.JobPartsMap
	jMap.lock.RUnlock()
	return uint32(len(partMap))
}

// StoreJobPartPlanInfo stores the JobPartPlanInfo reference for given combination of JobId and part number in thread-safe manner.
func (jMap *JobsInfoMap) StoreJobPartPlanInfo(jobId common.JobID, partNumber common.PartNumber, jobLogVerbosity pipeline.LogLevel, jHandler *JobPartPlanInfo) {
	jMap.lock.Lock()
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
	jMap.lock.Unlock()
}

// LoadLoggerForJob loads the logger instance for given jobId in thread safe manner
func (jMap *JobsInfoMap) LoadLoggerForJob(jobId common.JobID) *common.Logger {
	jMap.lock.RLock()
	jobInfo := jMap.internalMap[jobId]
	jMap.lock.RUnlock()
	if jobInfo == nil {
		return nil
	} else {
		return jobInfo.Logger
	}
}

// DeleteJobInfoForJobId api deletes an entry of given JobId the JobsInfoMap
func (jMap *JobsInfoMap) DeleteJobInfoForJobId(jobId common.JobID) {
	jMap.lock.Lock()
	delete(jMap.internalMap, jobId)
	jMap.lock.Unlock()
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
func writeInterfaceDataToWriter(writer io.Writer, f interface{}, structSize uint64) (int) {
	rv := reflect.ValueOf(f)
	interfaceSlice := reflect.SliceHeader{Data:rv.Pointer(),
						Len:int(structSize),
						Cap:int(structSize)}
	interfaceByteSlice := *(*[]byte)(unsafe.Pointer(&interfaceSlice))
	err := binary.Write(writer, binary.LittleEndian, interfaceByteSlice)
	if err != nil {
		panic(err)
	}
	return int(structSize)
}

func convertJobIdBytesToString(jobId [128 / 8]byte) string {
	jobIdString := fmt.Sprintf("%x%x%x%x%x", jobId[0:4], jobId[4:6], jobId[6:8], jobId[8:10], jobId[10:])
	return jobIdString
}

// reconstructTheExistingJobParts reconstructs the in memory JobPartPlanInfo for existing memory map JobFile
func reconstructTheExistingJobParts(jobsInfoMap *JobsInfoMap, coordinatorChannels *CoordinatorChannels) {
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

		scheduleTransfers(jobIdString, partNumber, jobsInfoMap, coordinatorChannels)
		// storing the JobPartPlanInfo pointer for given combination of JobId and part number
		putJobPartInfoHandlerIntoMap(jobHandler, jobIdString, partNumber, jobHandler.getJobPartPlanPointer().LogSeverity, jobsInfoMap)

		// If the Job was cancelled, but cleanup was not done for the Job, cleaning up the jobfile
		if jobHandler.getJobPartPlanPointer().getJobStatus() == Cancelled{
			cleanUpJob(jobIdString, jobsInfoMap)
		}
	}
	// checking for cancelled jobs and to cleanup those jobs
	// this api is called to ensure that no cancelled jobs exists in in-memory
	// this api is called to ensure that no cancelled jobs exists in in-memory
	checkCancelledJobsInJobMap(jobsInfoMap)
}

// checkCancelledJobsInJobMap api checks the JobPartPlan header of part 0 of each job
// JobPartPlan header of part 0 of each job determines the actual status of each job
// if the job status is cancelled, then it cleans up the job
func checkCancelledJobsInJobMap(jobsInfoMap *JobsInfoMap){
	jobIds := jobsInfoMap.LoadExistingJobIds()
	for index := 0; index < len(jobIds); index++ {
		// getting the jobInfo for part 0 of current jobId
		// since the status of Job is determined by the job status in JobPartPlan header of part 0
		jobInfo := jobsInfoMap.LoadJobPartPlanInfoForJobPart(jobIds[index], 0)

		// if the jobstatus in JobPartPlan header of part 0 is cancelled and cleanup wasn't successful
		// cleaning up the job now
		if jobInfo.getJobPartPlanPointer().getJobStatus() == Cancelled{
			cleanUpJob(jobIds[index], jobsInfoMap)
		}
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
//TODO : check guid in the map
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
func getTransferMsgDetail(jobId common.JobID, partNo common.PartNumber, transferEntryIndex uint32, jobsInfoMap *JobsInfoMap) TransferMsgDetail {
	// jHandler is the JobPartPlanInfo Pointer for given JobId and part number
	jHandler := jobsInfoMap.LoadJobPartPlanInfoForJobPart(jobId, partNo)

	// jPartPlanPointer is the memory map JobPartPlan for given JobId and part number
	jPartPlanPointer := jHandler.getJobPartPlanPointer()

	sourceType := jPartPlanPointer.SrcLocationType
	destinationType := jPartPlanPointer.DstLocationType
	source, destination := jHandler.getTransferSrcDstDetail(transferEntryIndex)
	chunkSize := jPartPlanPointer.BlobData.BlockSize
	return TransferMsgDetail{jobId, partNo, transferEntryIndex, chunkSize, sourceType,
		source, destinationType, destination, jHandler.TransferInfo[transferEntryIndex].ctx,
		jHandler.TransferInfo[transferEntryIndex].cancel, jobsInfoMap}
}

// updateTransferStatus updates the status of given transfer for given jobId and partNumber in thread safe manner
func updateTransferStatus(jobId common.JobID, partNo common.PartNumber, transferIndex uint32, transferStatus TransferStatus, jPartPlanInfoMap *JobsInfoMap) {
	jHandler := getJobPartInfoReferenceFromMap(jobId, partNo, jPartPlanInfoMap)
	transferHeader := jHandler.Transfer(transferIndex)
	transferHeader.setTransferStatus(transferStatus)
}

func updateNumberOfPartsDone(jobId common.JobID, jobsInfoMap *JobsInfoMap){
	logger := getLoggerForJobId(jobId, jobsInfoMap)
	jobInfo := jobsInfoMap.LoadJobInfoForJob(jobId)
	numPartsForJob := jobsInfoMap.GetNumberOfPartsForJob(jobId)
	//TODO atomic load and store attached with private variable
	totalNumberOfPartsDone := atomic.LoadUint32(&jobInfo.NumberOfPartsDone)
	logger.Logf(common.LogInfo, "total number of parts done for Job %s is %d", jobId, totalNumberOfPartsDone)
	if atomic.AddUint32(&jobInfo.NumberOfPartsDone, 1) == numPartsForJob{
		logger.Logf(common.LogInfo, "all parts of Job %s successfully completedm, cancelled or paused", jobId)
		jPartHeader := jobsInfoMap.LoadJobPartPlanInfoForJobPart(jobId, 0).getJobPartPlanPointer()
		if jPartHeader.getJobStatus() == Cancelled{
			logger.Logf(common.LogInfo, "all parts of Job %s successfully cancelled and hence cleaning up the Job", jobId)
			cleanUpJob(jobId, jobsInfoMap)
		}else if jPartHeader.getJobStatus() == InProgress{
			jPartHeader.getJobStatus() = Completed
		}
	}
}

// UpdateNumTransferDone api increments the var NumberOfTransfersCompleted by 1 atomically
// If this NumberOfTransfersCompleted equals the number of transfer in a job part,
// all transfers of Job Part have either paused, cancelled or completed
func updateNumberOfTransferDone(jobId common.JobID, partNumber common.PartNumber, jobsInfoMap *JobsInfoMap) {
	logger := getLoggerForJobId(jobId, jobsInfoMap)
	jHandler := jobsInfoMap.LoadJobPartPlanInfoForJobPart(jobId, partNumber)
	jPartPlanInfo := jHandler.getJobPartPlanPointer()
	//TODO : atomic load and store attached with private variable
	totalNumberofTransfersCompleted := atomic.LoadUint32(&jHandler.NumberOfTransfersCompleted)
	logger.Logf(common.LogInfo, "total number of transfers paused, cancelled or completed for Job %s and part number %d is %d", jobId, partNumber, totalNumberofTransfersCompleted)
	if atomic.AddUint32(&jHandler.NumberOfTransfersCompleted, 1) == jPartPlanInfo.NumTransfers {
		updateNumberOfPartsDone(jobId, jobsInfoMap)
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
