package common

import (
	"log"
	"os"
	"path"
)

func InitializeFolders() (azcopyLogPathFolder, azcopyJobPlanFolder string) {
	azcopyLogPathFolder = GetEnvironmentVariable(EEnvironmentVariable.LogLocation())     // user specified location for log files
	azcopyJobPlanFolder = GetEnvironmentVariable(EEnvironmentVariable.JobPlanLocation()) // user specified location for plan files

	// note: azcopyAppPathFolder is the default location for all AzCopy data (logs, job plans, oauth token on Windows)
	// but all the above can be put elsewhere as they can become very large
	azcopyAppPathFolder := getAzCopyAppPath()

	// the user can optionally put the log files somewhere else
	if azcopyLogPathFolder == "" {
		azcopyLogPathFolder = azcopyAppPathFolder
	}
	if err := os.Mkdir(azcopyLogPathFolder, os.ModeDir|os.ModePerm); err != nil && !os.IsExist(err) {
		log.Fatalf("Problem making .azcopy directory. Try setting AZCOPY_LOG_LOCATION env variable. %v", err)
	}

	// the user can optionally put the plan files somewhere else
	if azcopyJobPlanFolder == "" {
		// make the app path folder ".azcopy" first so we can make a plans folder in it
		if err := os.MkdirAll(azcopyAppPathFolder, os.ModeDir); err != nil && !os.IsExist(err) {
			log.Fatalf("Problem making .azcopy directory. Try setting AZCOPY_JOB_PLAN_LOCATION env variable. %v", err)
		}
		azcopyJobPlanFolder = path.Join(azcopyAppPathFolder, "plans")
	}

	if err := os.MkdirAll(azcopyJobPlanFolder, os.ModeDir|os.ModePerm); err != nil && !os.IsExist(err) {
		log.Fatalf("Problem making .azcopy directory. Try setting AZCOPY_JOB_PLAN_LOCATION env variable. %v", err)
	}
	return
}
