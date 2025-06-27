package jobsAdmin

import (
	"errors"
	"github.com/Azure/azure-storage-azcopy/v10/common"
	"os"
	"path"
	"strings"
)

// TODO (gapra): Re-evaluate the need for currentJobID.

// BlindDeleteAllJobFiles removes all job plan files and log files in the specified folders.
func BlindDeleteAllJobFiles(jobPlanFolder, logPathFolder string, currentJobID common.JobID) (int, error) {
	// get rid of the job plan files
	numPlanFilesRemoved, err := removeFilesWithPredicate(jobPlanFolder, func(s string) bool {
		return strings.Contains(s, ".steV")
	})
	if err != nil {
		return numPlanFilesRemoved, err
	}
	// get rid of the logs
	numLogFilesRemoved, err := removeFilesWithPredicate(logPathFolder, func(s string) bool {
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

func RemoveSingleJobFiles(jobPlanFolder, logPathFolder string, jobID common.JobID) error {
	// get rid of the job plan files
	numPlanFileRemoved, err := removeFilesWithPredicate(jobPlanFolder, func(s string) bool {
		if strings.Contains(s, jobID.String()) && strings.Contains(s, ".steV") {
			return true
		}
		return false
	})
	if err != nil {
		return err
	}

	// get rid of the logs
	// even though we only have 1 file right now, still scan the directory since we may change the
	// way we name the logs in the future (with suffix or whatnot)
	numLogFileRemoved, err := removeFilesWithPredicate(logPathFolder, func(s string) bool {
		if strings.Contains(s, jobID.String()) && strings.HasSuffix(s, ".log") {
			return true
		}
		return false
	})
	if err != nil {
		return err
	}

	if numLogFileRemoved+numPlanFileRemoved == 0 {
		return errors.New("cannot find any log or job plan file with the specified ID")
	}

	return nil
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
