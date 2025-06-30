package jobsAdmin

import (
	"errors"
	"fmt"
	"github.com/Azure/azure-storage-azcopy/v10/common"
	"github.com/Azure/azure-storage-azcopy/v10/ste"
	"os"
	"path"
	"path/filepath"
	"strings"
)

// These methods read common.AzcopyJobPlanFolder and common.LogPathFolder to list and remove job plan files and logs.

// ListJobs returns the jobId of all the jobs existing in the current instance of azcopy
func ListJobs(givenStatus common.JobStatus) common.ListJobsResponse {
	ret := common.ListJobsResponse{JobIDDetails: []common.JobIDDetails{}}
	files := func(ext string) []os.FileInfo {
		var files []os.FileInfo
		_ = filepath.Walk(common.AzcopyJobPlanFolder, func(path string, fileInfo os.FileInfo, _ error) error {
			if !fileInfo.IsDir() && strings.HasSuffix(fileInfo.Name(), ext) {
				files = append(files, fileInfo)
			}
			return nil
		})
		return files
	}(fmt.Sprintf(".steV%d", ste.DataSchemaVersion))

	// TODO : sort the file.
	for f := 0; f < len(files); f++ {
		planFile := ste.JobPartPlanFileName(files[f].Name())
		jobID, partNum, err := planFile.Parse()
		if err != nil || partNum != 0 { // Summary is in 0th JobPart
			continue
		}

		mmf := planFile.Map()
		jpph := mmf.Plan()

		if givenStatus == common.EJobStatus.All() || givenStatus == jpph.JobStatus() {
			ret.JobIDDetails = append(ret.JobIDDetails,
				common.JobIDDetails{JobId: jobID, CommandString: jpph.CommandString(),
					StartTime: jpph.StartTime, JobStatus: jpph.JobStatus()})
		}

		mmf.Unmap()
	}

	return ret
}

// TODO (gapra): Re-evaluate the need for currentJobID.

// BlindDeleteAllJobFiles removes all job plan files and log files in the specified folders.
func BlindDeleteAllJobFiles(currentJobID common.JobID) (int, error) {
	// get rid of the job plan files
	numPlanFilesRemoved, err := removeFilesWithPredicate(common.AzcopyJobPlanFolder, func(s string) bool {
		return strings.Contains(s, ".steV")
	})
	if err != nil {
		return numPlanFilesRemoved, err
	}
	// get rid of the logs
	numLogFilesRemoved, err := removeFilesWithPredicate(common.LogPathFolder, func(s string) bool {
		// Do not remove the current job's log file this will cause the cleanup job to fail.
		if strings.Contains(s, currentJobID.String()) {
			return false
		} else if strings.HasSuffix(s, ".log") {
			return true
		}
		return false
	})

	return numPlanFilesRemoved + numLogFilesRemoved, err
}

func RemoveSingleJobFiles(jobID common.JobID) (int, error) {
	// get rid of the job plan files
	numPlanFileRemoved, err := removeFilesWithPredicate(common.AzcopyJobPlanFolder, func(s string) bool {
		if strings.Contains(s, jobID.String()) && strings.Contains(s, ".steV") {
			return true
		}
		return false
	})
	if err != nil {
		return numPlanFileRemoved, err
	}

	// get rid of the logs
	// even though we only have 1 file right now, still scan the directory since we may change the
	// way we name the logs in the future (with suffix or whatnot)
	numLogFileRemoved, err := removeFilesWithPredicate(common.LogPathFolder, func(s string) bool {
		if strings.Contains(s, jobID.String()) && strings.HasSuffix(s, ".log") {
			return true
		}
		return false
	})
	if err != nil {
		return numPlanFileRemoved + numLogFileRemoved, err
	}

	if numLogFileRemoved+numPlanFileRemoved == 0 {
		return 0, errors.New("cannot find any log or job plan file with the specified ID")
	}

	return numPlanFileRemoved + numLogFileRemoved, nil
}

// remove all files whose names are approved by the predicate in the targetFolder
func removeFilesWithPredicate(targetFolder string, predicate func(string) bool) (int, error) {
	count := 0
	files, err := os.ReadDir(targetFolder)
	if err != nil {
		return count, err
	}

	// go through the files and return if any of them fail to be removed
	for _, singleFile := range files {
		if predicate(singleFile.Name()) {
			err := os.Remove(path.Join(targetFolder, singleFile.Name()))
			if err != nil {
				return count, err
			}
			count += 1
		}
	}

	return count, nil
}

func ListJobs(givenStatus common.JobStatus) common.ListJobsResponse {
	ret := common.ListJobsResponse{JobIDDetails: []common.JobIDDetails{}}
	files := func(ext string) []os.FileInfo {
		var files []os.FileInfo
		_ = filepath.Walk(common.AzcopyJobPlanFolder, func(path string, fileInfo os.FileInfo, _ error) error {
			if !fileInfo.IsDir() && strings.HasSuffix(fileInfo.Name(), ext) {
				files = append(files, fileInfo)
			}
			return nil
		})
		return files
	}(fmt.Sprintf(".steV%d", ste.DataSchemaVersion))

	// TODO : sort the file.
	for f := 0; f < len(files); f++ {
		planFile := ste.JobPartPlanFileName(files[f].Name())
		jobID, partNum, err := planFile.Parse()
		if err != nil || partNum != 0 { // Summary is in 0th JobPart
			continue
		}

		mmf := planFile.Map()
		jpph := mmf.Plan()

		if givenStatus == common.EJobStatus.All() || givenStatus == jpph.JobStatus() {
			ret.JobIDDetails = append(ret.JobIDDetails,
				common.JobIDDetails{JobId: jobID, CommandString: jpph.CommandString(),
					StartTime: jpph.StartTime, JobStatus: jpph.JobStatus()})
		}

		mmf.Unmap()
	}

	return ret
}
