package jobsAdmin

import (
	"github.com/Azure/azure-storage-azcopy/v10/common"
	"os"
	"path"
	"strings"
)

func BlindDeleteAllJobFiles(jobPlanFolder, logPathFolder string, currentJobID common.JobID) (int, error) {
	// get rid of the job plan files
	numPlanFilesRemoved, err := RemoveFilesWithPredicate(jobPlanFolder, func(s string) bool {
		return strings.Contains(s, ".steV")
	})
	if err != nil {
		return numPlanFilesRemoved, err
	}
	// get rid of the logs
	numLogFilesRemoved, err := RemoveFilesWithPredicate(logPathFolder, func(s string) bool {
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

// remove all files whose names are approved by the predicate in the targetFolder
func RemoveFilesWithPredicate(targetFolder string, predicate func(string) bool) (int, error) {
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
