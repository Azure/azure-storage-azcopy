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

package main

import (
	"log"
	"os"
	"path"
	"runtime"

	"github.com/Azure/azure-storage-azcopy/v10/cmd"
	"github.com/Azure/azure-storage-azcopy/v10/common"
)

// get the lifecycle manager to print messages
var glcm = common.GetLifecycleMgr()

func main() {
	azcopyLogPathFolder := common.GetEnvironmentVariable(common.EEnvironmentVariable.LogLocation())     // user specified location for log files
	azcopyJobPlanFolder := common.GetEnvironmentVariable(common.EEnvironmentVariable.JobPlanLocation()) // user specified location for plan files

	// note: azcopyAppPathFolder is the default location for all AzCopy data (logs, job plans, oauth token on Windows)
	// but all the above can be put elsewhere as they can become very large
	azcopyAppPathFolder := GetAzCopyAppPath()

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

	jobID := common.NewJobID()
	// If insufficient arguments, show usage & terminate
	if len(os.Args) == 1 {
		cmd.Execute(azcopyLogPathFolder, azcopyJobPlanFolder, 0, jobID)
		return
	}

	configureGoMaxProcs()

	// Perform os specific initialization
	maxFileAndSocketHandles, err := ProcessOSSpecificInitialization()
	if err != nil {
		log.Fatalf("initialization failed: %v", err)
	}

	cmd.Execute(azcopyLogPathFolder, azcopyJobPlanFolder, maxFileAndSocketHandles, jobID)
	glcm.Exit(nil, common.EExitCode.Success())
}

// Ensure we always have more than 1 OS thread running goroutines, since there are issues with having just 1.
// (E.g. version check doesn't happen at login time, if have only one go proc. Not sure why that happens if have only one
// proc. Is presumably due to the high CPU usage we see on login if only 1 CPU, even tho can't see any busy-wait in that code)
func configureGoMaxProcs() {
	isOnlyOne := runtime.GOMAXPROCS(0) == 1
	if isOnlyOne {
		runtime.GOMAXPROCS(2)
	}
}
