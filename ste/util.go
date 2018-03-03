package ste

import (
	"encoding/binary"
	"errors"
	"fmt"
	"github.com/Azure/azure-storage-azcopy/common"
	"io"
	"os"
	"path/filepath"
	"reflect"
	"regexp"
	"strconv"
	"strings"
	"unsafe"
)

// parseStringToJobInfo api parses the file name to extract the job jobId, part number and schema version number
// Returns the JobId, partNumber and data schema version
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
	parts := strings.Split(s, "--")
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

	parsedJobId, err := common.ParseUUID(jobIdString)
	if err != nil {
		panic(err)
	}

	return common.JobID(parsedJobId), common.PartNumber(partNo64), common.Version(versionNo64)
}

// formatJobInfoToString builds the JobPart file name using the given JobId, part number and data schema version
// fileName format := $jobId-$partnumber.stev$dataschemaversion
func formatJobInfoToString(jobPartOrder common.CopyJobPartOrder) string {
	versionIdString := fmt.Sprintf("%05d", jobPartOrder.Version)
	partNoString := fmt.Sprintf("%05d", jobPartOrder.PartNum)
	fileName := jobPartOrder.ID.String() + "--" + partNoString + ".steV" + versionIdString
	return fileName
}

// writeInterfaceDataToWriter api writes the content of given interface to the io writer
func writeInterfaceDataToWriter(writer io.Writer, f interface{}, structSize uint64) int {
	rv := reflect.ValueOf(f)
	interfaceSlice := reflect.SliceHeader{Data: rv.Pointer(),
		Len: int(structSize),
		Cap: int(structSize)}
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
func reconstructTheExistingJobParts(jobsInfoMap *JobsInfo, coordinatorChannels *CoordinatorChannels) {
	versionIdString := fmt.Sprintf("%05d", dataSchemaVersion)
	// list memory map files with .steV$dataschemaVersion to avoid the reconstruction of old schema version memory map file
	files := listFileWithExtension(".steV" + versionIdString)
	for index := 0; index < len(files); index++ {
		fileName := files[index].Name()
		// extracting the jobId and part number from file name
		jobId, partNumber, _ := parseStringToJobInfo(fileName)
		// creating a new JobPartPlanInfo pointer and initializing it
		jobHandler := new(JobPartPlanInfo)
		// Initializing the JobPartPlanInfo for existing Job file
		jobHandler.initialize(steContext, fileName)

		// storing the JobPartPlanInfo pointer for given combination of JobId and part number
		jobsInfoMap.AddJobPartPlanInfo(jobHandler)

		scheduleTransfers(jobId, partNumber, jobsInfoMap, coordinatorChannels)

		// If the Job was cancelled, but cleanup was not done for the Job, cleaning up the jobfile
		if jobHandler.getJobPartPlanPointer().jobStatus() == JobCancelled {
			cleanUpJob(jobId, jobsInfoMap)
		}
	}
	// checking for cancelled jobs and to cleanup those jobs
	// this api is called to ensure that no cancelled jobs exists in in-memory
	// this api is called to ensure that no cancelled jobs exists in in-memory
	//checkCancelledJobsInJobMap(jobsInfoMap)
}

// checkCancelledJobsInJobMap api checks the JobPartPlan header of part 0 of each job
// JobPartPlan header of part 0 of each job determines the actual status of each job
// if the job status is cancelled, then it cleans up the job
func checkCancelledJobsInJobMap(jobsInfoMap *JobsInfo) {
	jobIds := jobsInfoMap.JobIds()
	for index := 0; index < len(jobIds); index++ {
		// getting the jobInfo for part 0 of current jobId
		// since the status of Job is determined by the job status in JobPartPlan header of part 0
		jobInfo := jobsInfoMap.JobPartPlanInfo(jobIds[index], 0)

		// if the jobstatus in JobPartPlan header of part 0 is cancelled and cleanup wasn't successful
		// cleaning up the job now
		if jobInfo.getJobPartPlanPointer().jobStatus() == JobCancelled {
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
func fileAlreadyExists(fileName string, jobsInfoMap *JobsInfo) bool {

	jobId, partNumber, _ := parseStringToJobInfo(fileName)

	jobPartInfo := jobsInfoMap.JobPartPlanInfo(jobId, partNumber)

	if jobPartInfo == nil {
		return false
	}
	return true
}

func updateNumberOfPartsDone(jobId common.JobID, jobsInfoMap *JobsInfo) {
	jobInfo := jobsInfoMap.JobInfo(jobId)
	numPartsForJob := jobsInfoMap.NumberOfParts(jobId)
	totalNumberOfPartsDone := jobInfo.NumberOfPartsDone()
	jobInfo.Log(common.LogInfo, fmt.Sprintf("total number of parts done for Job %s is %d", jobId, totalNumberOfPartsDone))
	if jobInfo.incrementNumberOfPartsDone() == numPartsForJob {
		jobInfo.Log(common.LogInfo, fmt.Sprintf("all parts of Job %s successfully completed, cancelled or paused", jobId))
		jPartHeader := jobsInfoMap.JobPartPlanInfo(jobId, 0).getJobPartPlanPointer()
		if jPartHeader.jobStatus() == JobCancelled {
			jobInfo.Log(common.LogInfo, fmt.Sprintf("all parts of Job %s successfully cancelled and hence cleaning up the Job", jobId))
			cleanUpJob(jobId, jobsInfoMap)
		} else if jPartHeader.jobStatus() == JobInProgress {
			jPartHeader.setJobStatus(JobCompleted)
		}
	}
}
