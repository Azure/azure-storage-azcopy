package ste

import (
	"github.com/Azure/azure-storage-azcopy/common"
	"log"
	"os"
)

// JobInfo contains JobPartsMap and logger
// JobPartsMap maps part number to JobPartPlanInfo reference for a given JobId
// logger is the logger instance for a given JobId
type JobInfo_New struct {
	JobPartsMap       map[common.PartNumber]*JobPartPlanInfo
	minimumLogLevel   common.LogLevel
	logger            *log.Logger
	numberOfPartsDone uint32
	logFile           *os.File
	logFileName       string
}

// getNumberOfPartsDone returns the number of parts of job either completed or failed
// in a thread safe manner
func (jobInfo *JobInfo) getNumberOfPartsDone_New() uint32 {
	return 0
}

// incrementNumberOfPartsDone increments the number of parts either completed or failed
// in a thread safe manner
func (jobInfo *JobInfo) incrementNumberOfPartsDone_New () uint32 {
	return 0
}

// setNumberOfPartsDone sets the number of part done for a job to the given value
// in a thread-safe manner
func (jobInfo *JobInfo) setNumberOfPartsDone_New(val uint32) {

}

func (jobInfo *JobInfo) Log_New(severity common.LogLevel, logMessage string) {

}

func (jobInfo *JobInfo) Panic_New(err error) {

}

// UpdateNumTransferDone api increments the var numberOfTransfersDone_doNotUse by 1 atomically
// If this numberOfTransfersDone_doNotUse equals the number of transfer in a job part,
// all transfers of Job Part have either paused, cancelled or completed
func (jMap *JobInfo) updateNumberOfTransferDone() {

}
