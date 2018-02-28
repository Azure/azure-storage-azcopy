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
	"github.com/Azure/azure-storage-azcopy/common"
	"log"
	"os"
	"sync/atomic"
	"fmt"
)

// JobInfo contains jobPartsMap and logger
// jobPartsMap maps part number to JobPartPlanInfo reference for a given JobId
// logger is the logger instance for a given JobId
type JobInfo struct {
	jobId             common.JobID
	// jobsInfo is the reference of JobsInfo of which current jobInfo is part of.
	jobsInfo          *JobsInfo
	// jobPartsMap maps part number to JobPartPlanInfo reference for a given JobId
	jobPartsMap       map[common.PartNumber]*JobPartPlanInfo
	// maximum loglevel represents the maximum severity of log messages which can be logged to Job Log file.
	// any message with severity higher than this will be ignored.
	maximumLogLevel   common.LogLevel
	logger            *log.Logger
	numberOfPartsDone uint32
	logFile           *os.File
}

// Returns the combination of PartNumber and respective JobPartPlanInfo reference.
func (ji *JobInfo) JobParts() map[common.PartNumber]*JobPartPlanInfo {
	return ji.jobPartsMap
}

// JobPartPlanInfo returns the JobPartPlanInfo reference of a Job for given part number
func (ji *JobInfo) JobPartPlanInfo(partNumber common.PartNumber) *JobPartPlanInfo {
	jPartPlanInfo := ji.jobPartsMap[partNumber]
	return jPartPlanInfo
}

func (ji *JobInfo) NumberOfParts() (uint32){
	return uint32(len(ji.jobPartsMap))
}

// NumberOfPartsDone returns the number of parts of job either completed or failed
// in a thread safe manner
func (ji *JobInfo) NumberOfPartsDone() uint32 {
	return atomic.LoadUint32(&ji.numberOfPartsDone)
}

// PartsDone increments the number of parts either completed or failed
// in a thread safe manner
func (ji *JobInfo) PartsDone()  {

	totalNumberOfPartsDone := ji.NumberOfPartsDone()
	ji.Log(common.LogInfo, fmt.Sprintf("is part of Job which %d total number of parts done ", totalNumberOfPartsDone))
	if atomic.AddUint32(&ji.numberOfPartsDone, 1) == ji.NumberOfParts() {
		ji.Log(common.LogInfo, fmt.Sprintf("all parts of Job %s successfully completed, cancelled or paused", ji.jobId.String()))
		jPartHeader := ji.JobPartPlanInfo(0).getJobPartPlanPointer()
		if jPartHeader.Status() == JobCancelled {
			ji.Log(common.LogInfo, fmt.Sprintf("all parts of Job %s successfully cancelled and hence cleaning up the Job"))
			ji.jobsInfo.cleanUpJob(ji.jobId)
		} else if jPartHeader.Status() == JobInProgress {
			jPartHeader.SetJobStatus(JobCompleted)
		}
	}
}

// setNumberOfPartsDone sets the number of part done for a job to the given value
// in a thread-safe manner
func (ji *JobInfo) setNumberOfPartsDone(val uint32) {
	atomic.StoreUint32(&ji.numberOfPartsDone, val)
}

//  closeLogForJob closes the log file for the Job
func (ji *JobInfo) closeLogForJob() {
	err := ji.logFile.Close()
	if err != nil {
		panic(err)
	}
}

// Log method is used to log the message of Job.
// If the maximumLogLevel of Job is less than given message severity,
// then the message is not logged.
func (ji *JobInfo) Log(severity common.LogLevel, logMessage string) {
	if severity > ji.maximumLogLevel {
		return
	}
	ji.logger.Println(logMessage)
}

// Panic method fir logs the panic error to the Job log file and then panics with given error.
func (ji *JobInfo) Panic(err error) {
	ji.logger.Panic(err)
}

// NewJobsInfo returns a new instance of synchronous JobsInfo to hold JobPartPlanInfo Pointer for given combination of JobId and part number.
func NewJobInfo(jobId common.JobID, jobsInfo *JobsInfo) *JobInfo {
	return &JobInfo{
		jobId:jobId,
		jobsInfo:jobsInfo,
		jobPartsMap: make(map[common.PartNumber]*JobPartPlanInfo),
	}
}
