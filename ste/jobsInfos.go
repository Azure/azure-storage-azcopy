package ste

import (
	"sync"
	"github.com/Azure/azure-storage-azcopy/common"
	"log"
)

// JobToLoggerMap is the Synchronous Map of Map to hold JobPartPlanPointer reference for combination of JobId and partNum.
// Provides the thread safe Load and Store Method
type JobsInfoMap_New struct {
	// ReadWrite Mutex
	lock sync.RWMutex
	// map jobId -->[partNo -->JobPartPlanInfo Pointer]
	internalMap map[common.JobID]*JobInfo
}

// LoadJobPartsMapForJob returns the map of PartNumber to JobPartPlanInfo Pointer for given JobId in thread-safe manner.
func (jMap *JobsInfoMap_New) LoadJobPartsMapForJob(jobId common.JobID) (map[common.PartNumber]*JobPartPlanInfo, bool) {
	return nil, false
}

// LoadJobInfoForJob returns the JobInfo pointer stored in JobsInfoMap for given JobId in thread-safe manner.
func (jMap *JobsInfoMap_New) LoadJobInfoForJob(jobId common.JobID) *JobInfo {
	return nil
}

// LoadJobPartPlanInfoForJobPart returns the JobPartPlanInfo Pointer for given combination of JobId and part number in thread-safe manner.
func (jMap *JobsInfoMap_New) LoadJobPartPlanInfoForJobPart(jobId common.JobID, partNumber common.PartNumber) *JobPartPlanInfo {
	return nil
}

// LoadExistingJobIds returns the list of existing JobIds for which there are entries in the internal map in thread-safe manner.
func (jMap *JobsInfoMap_New) LoadExistingJobIds() []common.JobID {
	return nil
}

// GetNumberOfPartsForJob returns the number of part order for job with given JobId
func (jMap *JobsInfoMap_New) GetNumberOfPartsForJob(jobId common.JobID) uint32 {
	return 0
}

// StoreJobPartPlanInfo stores the JobPartPlanInfo reference for given combination of JobId and part number in thread-safe manner.
func (jMap *JobsInfoMap_New) StoreJobPartPlanInfo(jobId common.JobID, partNumber common.PartNumber, jobLogVerbosity common.LogLevel, jHandler *JobPartPlanInfo) {

}

// LoadLoggerForJob loads the logger instance for given jobId in thread safe manner
func (jMap *JobsInfoMap_New) LoadLoggerForJob(jobId common.JobID) *log.Logger {
	return nil
}

// DeleteJobInfoForJobId api deletes an entry of given JobId the JobsInfoMap
func (jMap *JobsInfoMap_New) DeleteJobInfoForJobId(jobId common.JobID) {
	return
}

// NewJobPartPlanInfoMap returns a new instance of synchronous JobsInfoMap to hold JobPartPlanInfo Pointer for given combination of JobId and part number.
func NewJobPartPlanInfoMap_New() *JobsInfoMap_New {
	return &JobsInfoMap_New{
		internalMap: make(map[common.JobID]*JobInfo),
	}
}
