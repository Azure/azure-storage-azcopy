// Copyright © 2017 Microsoft <wastore@microsoft.com>
//
// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files (the "Software"), to deal
// in the Software without restriction, including without limitation the rights
// to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
// copies of the Software, and to permit persons to whom the Software is
// furnished to do so, subject to the following conditions:
//
// The above copyright notice and this permission notice shall be included in
// all copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
// THE SOFTWARE.

package ste

import (
	"fmt"
	"github.com/Azure/azure-storage-azcopy/common"
	"log"
	"os"
	"sync"
	"errors"
)

// JobToLoggerMap is the Synchronous Map of Map to hold JobPartPlanPointer reference for combination of JobId and partNum.
// Provides the thread safe Load and Store Method
type JobsInfo struct {
	// ReadWrite Mutex
	lock sync.RWMutex
	// map jobId -->[partNo -->JobPartPlanInfo Pointer]
	internalMap map[common.JobID]*JobInfo
}

// JobInfo returns the JobInfo pointer stored in JobsInfo for given JobId in thread-safe manner.
func (jMap *JobsInfo) JobInfo(jobId common.JobID) *JobInfo {
	jMap.lock.RLock()
	jobInfo, ok := jMap.internalMap[jobId]
	jMap.lock.RUnlock()
	if !ok {
		return nil
	}
	return jobInfo
}

// JobPartPlanInfo returns the JobPartPlanInfo Pointer for given combination of JobId and part number in thread-safe manner.
func (jMap *JobsInfo) JobPartPlanInfo(jobId common.JobID, partNumber common.PartNumber) *JobPartPlanInfo {
	jMap.lock.RLock()
	partMap := jMap.internalMap[jobId]
	if partMap == nil {
		jMap.lock.RUnlock()
		return nil
	}
	jHandler := partMap.jobPartsMap[partNumber]
	jMap.lock.RUnlock()
	return jHandler
}

// JobIds returns the list of existing JobIds for which there are entries in the internal map in thread-safe manner.
func (jMap *JobsInfo) JobIds() []common.JobID {
	jMap.lock.RLock()
	existingJobs := make([]common.JobID, len(jMap.internalMap))
	index := 0
	for jobId, _ := range jMap.internalMap {
		existingJobs[index] = jobId
	}
	jMap.lock.RUnlock()
	return existingJobs
}

// NumberOfParts returns the number of part order for job with given JobId
func (jMap *JobsInfo) NumberOfParts(jobId common.JobID) uint32 {
	jMap.lock.RLock()
	jobInfo := jMap.internalMap[jobId]
	if jobInfo == nil {
		jMap.lock.RUnlock()
		return 0
	}
	partMap := jobInfo.jobPartsMap
	jMap.lock.RUnlock()
	return uint32(len(partMap))
}

// AddJobPartPlanInfo stores the JobPartPlanInfo reference for given combination of JobId and part number in thread-safe manner.
func (jMap *JobsInfo) AddJobPartPlanInfo(jpp *JobPartPlanInfo) {
	jMap.lock.Lock()
	jPartPlanInfo := jpp.getJobPartPlanPointer()
	var jobInfo = jMap.internalMap[jPartPlanInfo.Id]
	// If there is no JobInfo instance for given jobId
	if jobInfo == nil {
		jobInfo = NewJobInfo(jPartPlanInfo.Id, jMap)
	} else if jobInfo.jobPartsMap == nil {
		// If the current JobInfo instance for given jobId has not jobPartsMap initialized
		jobInfo.jobPartsMap = make(map[common.PartNumber]*JobPartPlanInfo)
	}
	// If there is no logger instance for the current Job,
	// initialize the logger instance with log severity and jobId
	// log filename is $JobId.log
	if jobInfo.logger == nil {
		file, err := os.OpenFile(fmt.Sprintf("%s.log", jPartPlanInfo.Id.String()), os.O_RDWR|os.O_CREATE|os.O_APPEND, 0666)
		if err != nil {
			panic(err)
		}
		jobInfo.maximumLogLevel = jPartPlanInfo.LogSeverity
		jobInfo.logFile = file
		jobInfo.logger = log.New(jobInfo.logFile, "", log.Llongfile)
		//jobInfo.logger.Initialize(jobLogVerbosity, fmt.Sprintf("%s.log", jobId))
	}
	jobInfo.jobPartsMap[jPartPlanInfo.PartNum] = jpp
	jMap.internalMap[jPartPlanInfo.Id] = jobInfo
	jMap.lock.Unlock()
}

// DeleteJobInfo api deletes an entry of given JobId the JobsInfo
func (jMap *JobsInfo) DeleteJobInfo(jobId common.JobID) {
	jMap.lock.Lock()
	delete(jMap.internalMap, jobId)
	jMap.lock.Unlock()
}

// cleanUpJob api unmaps all the memory map JobPartFile and deletes the JobPartFile
/*
	* Load PartMap for given JobId
    * Iterate through each part order of given Job and then shutdowns the JobInfo handler
    * Iterate through each part order of given Job and then shutdowns the JobInfo handler
	* Delete all the job part files stored on disk
    * Closes the logger file opened for logging logs related to given job
	* Removes the entry of given JobId from JobsInfo
*/
func (jMap *JobsInfo) cleanUpJob(jobId common.JobID) {
	jMap.lock.Lock()
	jobInfo := jMap.internalMap[jobId]
	jPart := jobInfo.JobParts()
	if jPart == nil {
		panic(errors.New(fmt.Sprintf("no job found with JobId %s to clean up", jobId)))
	}
	for _, jobHandler := range jPart {
		// unmapping the memory map JobPart file
		jobHandler.shutDownHandler(jobInfo.logger)

		// deleting the JobPartFile
		err := os.Remove(jobHandler.file.Name())
		if err != nil {
			jobInfo.Panic(errors.New(fmt.Sprintf("error removing the job part file %s. Failed with following error %s", jobHandler.file.Name(), err.Error())))
		}
	}

	jobInfo.closeLogForJob()
	// deletes the entry for given JobId from Map
	delete(jMap.internalMap, jobId)
	jMap.lock.Lock()
}

// NewJobsInfo returns a new instance of synchronous JobsInfo to hold JobPartPlanInfo Pointer for given combination of JobId and part number.
func NewJobsInfo() *JobsInfo {
	return &JobsInfo{
		internalMap: make(map[common.JobID]*JobInfo),
	}
}


