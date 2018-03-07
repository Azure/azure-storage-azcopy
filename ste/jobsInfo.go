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
	"errors"
	"fmt"
	"github.com/Azure/azure-storage-azcopy/common"
	"log"
	"os"
	"sync"
)

type syncJobsInfoMap struct {
	lock sync.RWMutex
	m    map[common.JobID]*JobInfo
}

func (sm *syncJobsInfoMap) Set(key common.JobID, value *JobInfo) {
	sm.lock.Lock()
	sm.m[key] = value
	sm.lock.Unlock()
}
func (sm *syncJobsInfoMap) Get(key common.JobID) (value *JobInfo, ok bool) {
	sm.lock.RLock()
	value, ok = sm.m[key]
	sm.lock.RUnlock()
	return
}
func (sm *syncJobsInfoMap) Delete(key common.JobID) {
	sm.lock.Lock()
	delete(sm.m, key)
	sm.lock.Unlock()
}

func (sm *syncJobsInfoMap) Iterate(readonly bool, f func(k common.JobID, v *JobInfo)) {
	locker := sync.Locker(&sm.lock)
	if !readonly {
		locker = sm.lock.RLocker()
	}
	locker.Lock()
	for k, v := range sm.m {
		f(k, v)
	}
	locker.Unlock()
}

func newSyncJobsInfoMap() *syncJobsInfoMap {
	return &syncJobsInfoMap{
		m: make(map[common.JobID]*JobInfo),
	}
}

// JobToLoggerMap is the Synchronous Map of Map to hold JobPartPlanPointer reference for combination of JobId and partNum.
// Provides the thread safe Load and Store Method
type JobsInfo struct {
	// map jobId -->[partNo -->JobPartPlanInfo Pointer]
	internalMap *syncJobsInfoMap
}

// JobInfo returns the JobInfo pointer stored in JobsInfo for given JobId in thread-safe manner.
func (jMap *JobsInfo) JobInfo(jobId common.JobID) *JobInfo {
	jobInfo, ok := jMap.internalMap.Get(jobId)
	if !ok {
		return nil
	}
	return jobInfo
}

// JobIds returns the list of existing JobIds for which there are entries in the internal map in thread-safe manner.
func (jMap *JobsInfo) JobIds() []common.JobID {

	var existingJobs []common.JobID
	jMap.internalMap.Iterate(true, func(k common.JobID, v *JobInfo) {
		existingJobs = append(existingJobs, k)
	})
	return existingJobs
}

// AddJobPartPlanInfo stores the JobPartPlanInfo reference for given combination of JobId and part number in thread-safe manner.
func (jMap *JobsInfo) AddJobPartPlanInfo(jpp *JobPartPlanInfo) {
	jPartPlanInfo := jpp.getJobPartPlanPointer()
	var jobInfo, ok = jMap.internalMap.Get(jPartPlanInfo.Id)
	// If there is no JobInfo instance for given jobId
	if !ok {
		jobInfo = NewJobInfo(jPartPlanInfo.Id, jMap)
	}
	// If there is no logger instance for the current Job,
	// initializeJobPartPlanInfo the logger instance with log severity and jobId
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
	jobInfo.AddPartPlanInfo(jPartPlanInfo.PartNum, jpp)
	jMap.internalMap.Set(jPartPlanInfo.Id, jobInfo)
}

// DeleteJobInfo api deletes an entry of given JobId the JobsInfo
func (jMap *JobsInfo) DeleteJobInfo(jobId common.JobID) {
	jMap.internalMap.Delete(jobId)
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
	jobInfo, ok := jMap.internalMap.Get(jobId)
	if !ok || (jobInfo.JobParts() == nil) {
		panic(errors.New(fmt.Sprintf("no job found with JobId %s to clean up", jobId)))
	}
	for _, jobHandler := range jobInfo.JobParts() {
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
	jMap.DeleteJobInfo(jobId)
}

// NewJobsInfo returns a new instance of synchronous JobsInfo to hold JobPartPlanInfo Pointer for given combination of JobId and part number.
func NewJobsInfo() *JobsInfo {
	return &JobsInfo{
		internalMap: newSyncJobsInfoMap(),
	}
}
