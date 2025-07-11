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
	"fmt"
	"log"
	"os"
	"path"
	"path/filepath"
	"runtime"
	pprof "runtime/pprof"
	runtimeTrace "runtime/trace"
	"time"
	"net/http"          // for pprof server
	_ "net/http/pprof"   // registers pprof handlers

	"github.com/Azure/azure-storage-azcopy/v10/cmd"
	"github.com/Azure/azure-storage-azcopy/v10/common"
)

// get the lifecycle manager to print messages
var glcm = common.GetLifecycleMgr()

func main() {
	// ---------------------------------------------------------------------
	// Automatic pprof capture setup
	// ---------------------------------------------------------------------
	if basePath := os.Getenv("PPROF_FOLDER_PATH"); basePath != "" {
		// Create a unique sub-directory for this run using UTC timestamp.
		sessionDir := filepath.Join(basePath, time.Now().UTC().Format("20060102_150405"))
		if err := os.MkdirAll(sessionDir, os.ModePerm); err != nil {
			log.Printf("[pprof] Failed to create output directory %s: %v", sessionDir, err)
		} else {
			// Start background collection goroutine.
			go collectPprofPeriodically(sessionDir)
		}
	}

	// Start a pprof metrics endpoint so that runtime and application metrics can be scraped.
	// The default listen address is ":6060" but can be overridden by setting the AZCOPY_PPROF_PORT environment variable.
	go func() {
		addr := ":6060"
		if port := os.Getenv("AZCOPY_PPROF_PORT"); port != "" {
			// allow user to specify full address (e.g. ":7070") or just port (e.g. "7070")
			if port[0] != ':' {
				addr = ":" + port
			} else {
				addr = port
			}
		}

		// It is safe to ignore the error returned here because azcopy's primary functionality
		// should not be blocked if the metrics endpoint fails to start. Log and continue.
		if err := http.ListenAndServe(addr, nil); err != nil {
			log.Printf("pprof endpoint failed to start on %s: %v", addr, err)
		}
	}()

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

// collectPprofPeriodically writes various pprof data to outputDir every 10 minutes.
// A collection cycle includes:
//   • 30-second CPU profile
//   • 5-second execution trace
//   • All available runtime/pprof profiles (heap, goroutine, allocs, etc.)
// Each output file is named <timestamp>_<profile>.pprof or .out.
func collectPprofPeriodically(outputDir string) {
	// Immediately collect once so that we have data early, then every 10 minutes.
	collectPprofOnce(outputDir)

	ticker := time.NewTicker(10 * time.Minute)
	defer ticker.Stop()
	for {
		<-ticker.C
		collectPprofOnce(outputDir)
	}
}

func collectPprofOnce(outputDir string) {
	ts := time.Now().UTC().Format("20060102_150405")

	// ---------------- CPU Profile ----------------
	cpuPath := filepath.Join(outputDir, fmt.Sprintf("%s_cpu.pprof", ts))
	if f, err := os.Create(cpuPath); err == nil {
		if err := pprof.StartCPUProfile(f); err == nil {
			time.Sleep(30 * time.Second)
			pprof.StopCPUProfile()
		} else {
			log.Printf("[pprof] Could not start CPU profile: %v", err)
		}
		f.Close()
	} else {
		log.Printf("[pprof] Could not create CPU profile file %s: %v", cpuPath, err)
	}

	// ---------------- Execution Trace ----------------
	tracePath := filepath.Join(outputDir, fmt.Sprintf("%s_trace.out", ts))
	if f, err := os.Create(tracePath); err == nil {
		if err := runtimeTrace.Start(f); err == nil {
			time.Sleep(5 * time.Second)
			runtimeTrace.Stop()
		} else {
			log.Printf("[pprof] Could not start execution trace: %v", err)
		}
		f.Close()
	} else {
		log.Printf("[pprof] Could not create trace file %s: %v", tracePath, err)
	}

	// ---------------- Standard Profiles ----------------
	for _, prof := range pprof.Profiles() {
		profilePath := filepath.Join(outputDir, fmt.Sprintf("%s_%s.pprof", ts, prof.Name()))
		if f, err := os.Create(profilePath); err == nil {
			if err := prof.WriteTo(f, 0); err != nil {
				log.Printf("[pprof] Error writing profile %s: %v", prof.Name(), err)
			}
			f.Close()
		} else {
			log.Printf("[pprof] Could not create profile file %s: %v", profilePath, err)
		}
	}
}
