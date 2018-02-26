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
)

// JobInfo contains jobPartsMap and logger
// jobPartsMap maps part number to JobPartPlanInfo reference for a given JobId
// logger is the logger instance for a given JobId
type JobInfo struct {
	jobPartsMap       map[common.PartNumber]*JobPartPlanInfo
	minimumLogLevel   common.LogLevel
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

// NumberOfPartsDone returns the number of parts of job either completed or failed
// in a thread safe manner
func (ji *JobInfo) NumberOfPartsDone() uint32 {
	return atomic.LoadUint32(&ji.numberOfPartsDone)
}

// incrementNumberOfPartsDone increments the number of parts either completed or failed
// in a thread safe manner
func (ji *JobInfo) incrementNumberOfPartsDone() uint32 {
	return atomic.AddUint32(&ji.numberOfPartsDone, 1)
}

// setNumberOfPartsDone sets the number of part done for a job to the given value
// in a thread-safe manner
func (ji *JobInfo) setNumberOfPartsDone(val uint32) {
	atomic.StoreUint32(&ji.numberOfPartsDone, val)
}

func (ji *JobInfo) initializeLogForJob(logSeverity common.LogLevel, fileName string) {
	// Creates the log file if it does not exists already else opens the file in append mode.
	file, err := os.OpenFile(fileName, os.O_RDWR|os.O_CREATE|os.O_APPEND, 0666)
	if err != nil {
		panic(err)
	}
	ji.minimumLogLevel = logSeverity
	ji.logFile = file
	ji.logger = log.New(ji.logFile, "", log.Llongfile)
}

func (ji *JobInfo) closeLogForJob() {
	err := ji.logFile.Close()
	if err != nil {
		panic(err)
	}
}

func (ji *JobInfo) Log(severity common.LogLevel, logMessage string) {
	if severity > ji.minimumLogLevel {
		return
	}
	ji.logger.Println(logMessage)
}

func (ji *JobInfo) Panic(err error) {
	ji.logger.Panic(err)
}
