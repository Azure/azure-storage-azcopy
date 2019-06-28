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
	"runtime"
	"runtime/debug"
	"time"

	"github.com/Azure/azure-storage-azcopy/cmd"
	"github.com/Azure/azure-storage-azcopy/common"
	"github.com/Azure/azure-storage-azcopy/ste"
)

// get the lifecycle manager to print messages
var glcm = common.GetLifecycleMgr()

func main() {
	azcopyAppPathFolder := GetAzCopyAppPath()
	azcopyLogPathFolder := common.GetLifecycleMgr().GetEnvironmentVariable(common.EEnvironmentVariable.LogLocation())
	if azcopyLogPathFolder == "" {
		azcopyLogPathFolder = azcopyAppPathFolder
	}

	// If insufficient arguments, show usage & terminate
	if len(os.Args) == 1 {
		cmd.Execute(azcopyAppPathFolder, azcopyLogPathFolder)
		return
	}

	configureGC()

	// Perform os specific initialization
	maxFileAndSocketHandles, err := ProcessOSSpecificInitialization()
	if err != nil {
		log.Fatalf("initialization failed: %v", err)
	}

	concurrentConnections := common.ComputeConcurrencyValue(runtime.NumCPU())
	concurrentFilesLimit := computeConcurrentFilesLimit(maxFileAndSocketHandles, concurrentConnections)

	err = ste.MainSTE(concurrentConnections, concurrentFilesLimit, 2400, azcopyAppPathFolder, azcopyLogPathFolder)
	common.PanicIfErr(err)

	cmd.Execute(azcopyAppPathFolder, azcopyLogPathFolder)
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

// ComputeConcurrentFilesLimit finds a number of concurrently-openable files
// such that we'll have enough handles left, after using some as network handles
// TODO: add environment var to optionally allow bringing concurrentFiles down lower
//    (and, when we do, actually USE it for uploads, since currently we're only using it on downloads)
//    (update logging
func computeConcurrentFilesLimit(maxFileAndSocketHandles int, concurrentConnections int) int {

	allowanceForOnGoingEnumeration := 1 // might still be scanning while we are transferring. Make this bigger if we ever do parallel scanning

	// Compute a very conservative estimate for total number of connections that we may have
	// To get a conservative estimate we pessimistically assume that the pool of idle conns is full,
	// but all the ones we are actually using are (by some fluke of timing) not in the pool.
	// TODO: consider actually SETTING AzCopyMaxIdleConnsPerHost to say, max(0.3 * FileAndSocketHandles, 1000), instead of using the hard-coded value we currently have
	possibleMaxTotalConcurrentHttpConnections := concurrentConnections + ste.AzCopyMaxIdleConnsPerHost + allowanceForOnGoingEnumeration

	concurrentFilesLimit := maxFileAndSocketHandles - possibleMaxTotalConcurrentHttpConnections

	if concurrentFilesLimit < ste.NumTransferInitiationRoutines {
		concurrentFilesLimit = ste.NumTransferInitiationRoutines // Set sensible floor, so we don't get negative or zero values if maxFileAndSocketHandles is low
	}
	return concurrentFilesLimit
}
