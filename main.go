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
	"github.com/Azure/azure-storage-azcopy/cmd"
	"github.com/Azure/azure-storage-azcopy/common"
	"github.com/Azure/azure-storage-azcopy/ste"
	"log"
	"os"
	"runtime"
	"runtime/debug"
	"time"
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

	// Perform os specific initialization
	_, err := ProcessOSSpecificInitialization()
	if err != nil {
		log.Fatalf("initialization failed: %v", err)
	}

	configureGC()

	ste.MainSTE(common.ComputeConcurrencyValue(runtime.NumCPU()), 2400, azcopyAppPathFolder, azcopyLogPathFolder)
	cmd.Execute(azcopyAppPathFolder, azcopyLogPathFolder)
	glcm.Exit("", common.EExitCode.Success())
}

// Golang's default behaviour is to GC when new objects = (100% of) total of objects surviving previous GC.
// But our "survivors" add up to many GB, so its hard for users to be confident that we don't have
// a memory leak (since with that default setting new GCs are very rare in our case). So configure them to be more frequent.
func configureGC() {
	go func() {
		time.Sleep(20 * time.Second) // wait a little, so that our initial pool of buffers can get allocated without heaps of (unnecessary) GC activity
		debug.SetGCPercent(20)       // active more aggressive/frequent GC than the default
	}()
}
