package common

import (
	"log"
	"os"
	"path"
)

var AzcopyJobPlanFolder string
var LogPathFolder string

func InitializeFolders() {
	var logFolderOK, planFolderOK bool
	LogPathFolder, _, logFolderOK = EEnvironmentVariable.LogLocation().Lookup()            // user specified location for log files
	AzcopyJobPlanFolder, _, planFolderOK = EEnvironmentVariable.JobPlanLocation().Lookup() // user specified location for plan files

	// note: azcopyAppPathFolder is the default location for all AzCopy data (logs, job plans, oauth token on Windows)
	// but all the above can be put elsewhere as they can become very large
	azcopyAppPathFolder := getAzCopyAppPath()

	// the user can optionally put the log files somewhere else
	if !logFolderOK {
		LogPathFolder = azcopyAppPathFolder
	}
	if err := os.MkdirAll(LogPathFolder, os.ModeDir|os.ModePerm); err != nil && !os.IsExist(err) {
		log.Fatalf("Problem making .azcopy directory. Try setting AZCOPY_LOG_LOCATION env variable. %v", err)
	}

	// the user can optionally put the plan files somewhere else
	if !planFolderOK {
		AzcopyJobPlanFolder = path.Join(azcopyAppPathFolder, "plans")
	}

	if err := os.MkdirAll(AzcopyJobPlanFolder, os.ModeDir|os.ModePerm); err != nil && !os.IsExist(err) {
		log.Fatalf("Problem making .azcopy directory. Try setting AZCOPY_JOB_PLAN_LOCATION env variable. %v", err)
	}
}
