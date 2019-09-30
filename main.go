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
	"math/rand"
	"os"
	"path"
	"runtime"
	"runtime/debug"
	"time"

	"github.com/Azure/azure-pipeline-go/pipeline"
	"github.com/Azure/azure-storage-azcopy/cmd"
	"github.com/Azure/azure-storage-azcopy/common"
)

// get the lifecycle manager to print messages
var glcm = common.GetLifecycleMgr()

func main() {
	pipeline.SetLogSanitizer(common.NewAzCopyLogSanitizer()) // make sure ForceLog logs get secrets redacted

	rand.Seed(time.Now().UnixNano()) // make sure our random numbers actually are random (but remember, use crypto/rand for anything where strong/reliable randomness is required

	// note: azcopyAppPathFolder is the default location for all AzCopy data (logs, job plans, oauth token on Windows)
	// but both logs and job plans can be put elsewhere as they can become very large
	azcopyAppPathFolder := GetAzCopyAppPath()

	// the user can optionally put the log files somewhere else
	azcopyLogPathFolder := common.GetLifecycleMgr().GetEnvironmentVariable(common.EEnvironmentVariable.LogLocation())
	if azcopyLogPathFolder == "" {
		azcopyLogPathFolder = azcopyAppPathFolder
	}
	if err := os.Mkdir(azcopyLogPathFolder, os.ModeDir|os.ModePerm); err != nil && !os.IsExist(err) {
		common.PanicIfErr(err)
	}

	// the user can optionally put the plan files somewhere else
	azcopyJobPlanFolder := common.GetLifecycleMgr().GetEnvironmentVariable(common.EEnvironmentVariable.JobPlanLocation())
	if azcopyJobPlanFolder == "" {
		azcopyJobPlanFolder = path.Join(azcopyAppPathFolder, "plans")
	}
	if err := os.Mkdir(azcopyJobPlanFolder, os.ModeDir|os.ModePerm); err != nil && !os.IsExist(err) {
		common.PanicIfErr(err)
	}

	// If insufficient arguments, show usage & terminate
	if len(os.Args) == 1 {
		cmd.Execute(azcopyAppPathFolder, azcopyLogPathFolder, azcopyJobPlanFolder, 0)
		return
	}

	configureGoMaxProcs()
	configureGC()

	// Perform os specific initialization
	maxFileAndSocketHandles, err := ProcessOSSpecificInitialization()
	if err != nil {
		log.Fatalf("initialization failed: %v", err)
	}

	cmd.Execute(azcopyAppPathFolder, azcopyLogPathFolder, azcopyJobPlanFolder, maxFileAndSocketHandles)
	glcm.Exit(nil, common.EExitCode.Success())
}

// Golang's default behaviour is to GC when new objects = (100% of) total of objects surviving previous GC.
// But our "survivors" add up to many GB, so its hard for users to be confident that we don't have
// a memory leak (since with that default setting new GCs are very rare in our case). So configure them to be more frequent.
func configureGC() {
	go func() {
		time.Sleep(20 * time.Second) // wait a little, so that our initial pool of buffers can get allocated without heaps of (unnecessary) GC activity
		debug.SetGCPercent(20)       // activate more aggressive/frequent GC than the default
	}()
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
