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

package cmd

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log"
	"os"
	"path"
	"path/filepath"
	"runtime"
	"runtime/pprof"
	"strings"
	"sync"
	"time"

	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/blob"
	"github.com/Azure/azure-storage-azcopy/v10/jobsAdmin"

	"github.com/Azure/azure-storage-azcopy/v10/common"
	"github.com/Azure/azure-storage-azcopy/v10/ste"
	"github.com/spf13/cobra"
)

var azcopyLogPathFolder string
var outputFormatRaw string
var outputVerbosityRaw string
var logVerbosityRaw string
var cancelFromStdin bool
var OutputFormat common.OutputFormat
var OutputLevel common.OutputVerbosity
var LogLevel common.LogLevel
var CapMbps float64
var SkipVersionCheck bool

// It's not pretty that this one is read directly by credential util.
// But doing otherwise required us passing it around in many places, even though really
// it can be thought of as an "ambient" property. That's the (weak?) justification for implementing
// it as a global
var TrustedSuffixes string
var azcopyAwaitContinue bool
var azcopyAwaitAllowOpenFiles bool
var azcopyScanningLogger common.ILoggerResetable
var azcopyCurrentJobID common.JobID
var isPipeDownload bool
var retryStatusCodes string
var debugMemoryProfile string

type jobLoggerInfo struct {
	jobID         common.JobID
	logFileFolder string
}

// It would be preferable if this was a local variable, since it just gets altered and shot off to the STE
var debugSkipFiles string

// rootCmd represents the base command when called without any subcommands
var rootCmd = &cobra.Command{
	Version: common.AzcopyVersion, // will enable the user to see the version info in the standard posix way: --version
	Use:     "azcopy",
	Short:   rootCmdShortDescription,
	Long:    rootCmdLongDescription,
	PersistentPreRunE: func(cmd *cobra.Command, args []string) error {
		glcm.RegisterCloseFunc(func() {
			if debugMemoryProfile != "" {
				memProfDir := filepath.Dir(debugMemoryProfile)
				err := os.MkdirAll(memProfDir, 0777)
				if err != nil {
					panic(fmt.Sprintf("Failed to create memory profile parent dir: %v", err))
				}

				f, err := os.OpenFile(debugMemoryProfile, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0666)
				if err != nil {
					panic(fmt.Sprintf("Failed to open memory profile: %v", err))
				}
				defer f.Close()
				runtime.GC()
				if err := pprof.WriteHeapProfile(f); err != nil {
					log.Fatal("could not write memory profile: ", err)
				}
			}
		})

		// referencing https://github.com/Azure/azure-sdk-for-go/blob/main/sdk/azcore/policy/policy.go#L114
		rscList := "408;429;500;502;503;504"
		if retryStatusCodes != "" {
			rscList += ";" + retryStatusCodes
		}
		rsc, err := ste.ParseRetryCodes(rscList)
		if err != nil {
			return fmt.Errorf("failed to parse requested retry status code list: %w", err)
		}
		ste.RetryStatusCodes = rsc

		glcm.E2EEnableAwaitAllowOpenFiles(azcopyAwaitAllowOpenFiles)
		if azcopyAwaitContinue {
			glcm.E2EAwaitContinue()
		}

		err = OutputFormat.Parse(outputFormatRaw)
		if err != nil {
			return err
		}

		err = OutputLevel.Parse(outputVerbosityRaw)
		if err != nil {
			return err
		}

		err = LogLevel.Parse(logVerbosityRaw)
		if err != nil {
			return err
		}

		// If the command is for resuming a job with a specific JobID,
		// use the provided JobID to resume the job; otherwise, create a new JobID.
		var resumeJobID common.JobID
		if cmd.Use == "resume [jobID]" {
			// If no argument is passed then it is not valid
			if len(args) != 1 {
				return errors.New("this command requires jobId to be passed as argument")
			}
			resumeJobID, err = common.ParseJobID(args[0])
			if err != nil {
				return err
			}
		}

		// Check if we are downloading to Pipe so we can bypass version check and not write it to stdout, customer is
		// only expecting blob data in stdout
		var fromToFlagValue string
		if cmd.Flags().Changed("from-to") {
			// Access the value of the "from-to" flag
			fromToFlagValue, err = cmd.Flags().GetString("from-to")
			if err != nil {
				return fmt.Errorf("error accessing 'from-to' flag: %v", err)
			}
			if fromToFlagValue == "BlobPipe" {
				isPipeDownload = true
			}
		}

		// warn Windows users re quoting (since our docs all use single quotes, but CMD needs double)
		// Single ones just come through as part of the args, in CMD.
		// Ideally, for usability, we'd ideally have this info come back in the result of url.Parse. But that's hard to
		// arrange. So we check it here.
		if runtime.GOOS == "windows" {
			for _, a := range args {
				a = strings.ToLower(a)
				if strings.HasPrefix(a, "'http") { // note the single quote
					glcm.Info("")
					glcm.Info("*** When running from CMD, surround URLs with double quotes. Only using single quotes from PowerShell. ***")
					glcm.Info("")
					break
				}
			}
		}

		if debugSkipFiles != "" {
			for _, v := range strings.Split(debugSkipFiles, ";") {
				if strings.HasPrefix(v, "/") {
					v = strings.TrimPrefix(v, common.AZCOPY_PATH_SEPARATOR_STRING)
				}

				ste.DebugSkipFiles[v] = true
			}
		}

		isBench := cmd.Use == "bench [destination]"

		return Initialize(resumeJobID, isBench)
	},
}

func Initialize(resumeJobID common.JobID, isBench bool) error {
	azcopyLogPathFolder, common.AzcopyJobPlanFolder = initializeFolders()

	configureGoMaxProcs()

	// Perform os specific initialization
	azcopyMaxFileAndSocketHandles, err := processOSSpecificInitialization()
	if err != nil {
		log.Fatalf("initialization failed: %v", err)
	}
	jobID := common.NewJobID()
	azcopyCurrentJobID = jobID
	loggerInfo := jobLoggerInfo{jobID, azcopyLogPathFolder}

	timeAtPrestart := time.Now()
	glcm.SetOutputFormat(OutputFormat)
	glcm.SetOutputVerbosity(OutputLevel)

	if !resumeJobID.IsEmpty() {
		loggerInfo.jobID = resumeJobID
	}

	common.AzcopyCurrentJobLogger = common.NewJobLogger(loggerInfo.jobID, LogLevel, loggerInfo.logFileFolder, "")
	common.AzcopyCurrentJobLogger.OpenLog()

	glcm.SetForceLogging()

	// currently, we only automatically do auto-tuning when benchmarking
	preferToAutoTuneGRs, providePerformanceAdvice := isBench, isBench

	// startup of the STE happens here, so that the startup can access the values of command line parameters that are defined for "root" command
	concurrencySettings := ste.NewConcurrencySettings(azcopyMaxFileAndSocketHandles, preferToAutoTuneGRs)
	err = jobsAdmin.MainSTE(concurrencySettings, float64(CapMbps), common.AzcopyJobPlanFolder, azcopyLogPathFolder, providePerformanceAdvice)
	if err != nil {
		return err
	}
	EnumerationParallelism = concurrencySettings.EnumerationPoolSize.Value
	EnumerationParallelStatFiles = concurrencySettings.ParallelStatFiles.Value

	// Log a clear ISO 8601-formatted start time, so it can be read and use in the --include-after parameter
	// Subtract a few seconds, to ensure that this date DEFINITELY falls before the LMT of any file changed while this
	// job is running. I.e. using this later with --include-after is _guaranteed_ to pick up all files that changed during
	// or after this job
	adjustedTime := timeAtPrestart.Add(-5 * time.Second)
	startTimeMessage := fmt.Sprintf("ISO 8601 START TIME: to copy files that changed before or after this job started, use the parameter --%s=%s or --%s=%s",
		common.IncludeBeforeFlagName, IncludeBeforeDateFilter{}.FormatAsUTC(adjustedTime),
		common.IncludeAfterFlagName, IncludeAfterDateFilter{}.FormatAsUTC(adjustedTime))
	jobsAdmin.JobsAdmin.LogToJobLog(startTimeMessage, common.LogInfo)

	if !SkipVersionCheck && !isPipeDownload {
		// spawn a routine to fetch and compare the local application's version against the latest version available
		// if there's a newer version that can be used, then write the suggestion to stderr
		// however if this takes too long the message won't get printed
		// Note: this function is necessary for non-help, non-login commands, since they don't reach the corresponding
		// beginDetectNewVersion call in Execute (below)
		beginDetectNewVersion()
	}

	return nil

}

// hold a pointer to the global lifecycle controller so that commands could output messages and exit properly
var glcm = common.GetLifecycleMgr()
var glcmSwapOnce = &sync.Once{}

// Execute adds all child commands to the root command and sets flags appropriately.
// This is called by main.main(). It only needs to happen once to the rootCmd.

var Execute func() error = rootCmd.Execute

func InitializeAndExecute() {
	if err := Execute(); err != nil {
		glcm.Error(err.Error())
	} else {
		if !SkipVersionCheck && !isPipeDownload {
			// our commands all control their own life explicitly with the lifecycle manager
			// only commands that don't explicitly exit actually reach this point (e.g. help commands)
			select {
			case <-beginDetectNewVersion():
				// noop
			case <-time.After(time.Second * 8):
				// don't wait too long
			}
		}
		glcm.Exit(nil, common.EExitCode.Success())
	}
}

func initializeFolders() (azcopyLogPathFolder, azcopyJobPlanFolder string) {
	azcopyLogPathFolder = common.GetEnvironmentVariable(common.EEnvironmentVariable.LogLocation())     // user specified location for log files
	azcopyJobPlanFolder = common.GetEnvironmentVariable(common.EEnvironmentVariable.JobPlanLocation()) // user specified location for plan files

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

// Ensure we always have more than 1 OS thread running goroutines, since there are issues with having just 1.
// (E.g. version check doesn't happen at login time, if have only one go proc. Not sure why that happens if have only one
// proc. Is presumably due to the high CPU usage we see on login if only 1 CPU, even tho can't see any busy-wait in that code)
func configureGoMaxProcs() {
	isOnlyOne := runtime.GOMAXPROCS(0) == 1
	if isOnlyOne {
		runtime.GOMAXPROCS(2)
	}
}

func init() {
	// replace the word "global" to avoid confusion (e.g. it doesn't affect all instances of AzCopy)
	rootCmd.SetUsageTemplate(strings.Replace((&cobra.Command{}).UsageTemplate(), "Global Flags", "Flags Applying to All Commands", -1))

	rootCmd.PersistentFlags().Float64Var(&CapMbps, "cap-mbps", 0,
		"Caps the transfer rate, in megabits per second. "+
			"\n Moment-by-moment throughput might vary slightly from the cap."+
			"\n If this option is set to zero, or it is omitted, the throughput isn't capped.")
	rootCmd.PersistentFlags().StringVar(&outputFormatRaw, "output-type", "text",
		"Format of the command's output. The choices include: text, json. "+
			"\n The default value is 'text'.")
	rootCmd.PersistentFlags().StringVar(&outputVerbosityRaw, "output-level", "default",
		"Define the output verbosity. Available levels: essential, quiet.")
	rootCmd.PersistentFlags().StringVar(&logVerbosityRaw, "log-level", "INFO",
		"Define the log verbosity for the log file, "+
			"\n available levels: DEBUG(detailed trace), INFO(all requests/responses), WARNING(slow responses),"+
			"\n ERROR(only failed requests), and NONE(no output logs). (default 'INFO').")

	rootCmd.PersistentFlags().StringVar(&TrustedSuffixes, trustedSuffixesNameAAD, "",
		"\nSpecifies additional domain suffixes where Azure Active Directory login tokens may be sent.  \nThe default is '"+
			trustedSuffixesAAD+"'. \n Any listed here are added to the default. For security, you should only put Microsoft Azure domains here. "+
			"\n Separate multiple entries with semi-colons.")

	rootCmd.PersistentFlags().BoolVar(&SkipVersionCheck, "skip-version-check", false,
		"Do not perform the version check at startup. \nIntended for automation scenarios & airgapped use.")

	// Note: this is due to Windows not supporting signals properly
	rootCmd.PersistentFlags().BoolVar(&cancelFromStdin, "cancel-from-stdin", false,
		"Used by partner teams to send in `cancel` through stdin to stop a job.")

	// special E2E testing flags
	rootCmd.PersistentFlags().BoolVar(&azcopyAwaitContinue, "await-continue", false,
		"Used when debugging, to tell AzCopy to await `continue` on stdin before starting any work. "+
			"\n Assists with debugging AzCopy via attach-to-process")
	rootCmd.PersistentFlags().BoolVar(&azcopyAwaitAllowOpenFiles, "await-open", false,
		"Used when debugging, to tell AzCopy to await `open` on stdin, after scanning but before opening the first file. "+
			"\n Assists with testing cases around file modifications between scanning and usage")
	rootCmd.PersistentFlags().StringVar(&debugSkipFiles, "debug-skip-files", "",
		"Used when debugging, to tell AzCopy to cancel the job midway."+
			"\n List of relative paths to skip in the STE.")

	// reserved for partner teams
	_ = rootCmd.PersistentFlags().MarkHidden("cancel-from-stdin")

	// special flags to be used in case of unexpected service errors.
	rootCmd.PersistentFlags().StringVar(&retryStatusCodes, "retry-status-codes", "",
		"Comma-separated list of HTTP status codes to retry on. (default '408;429;500;502;503;504')")
	_ = rootCmd.PersistentFlags().MarkHidden("retry-status-codes")
	rootCmd.PersistentFlags().StringVar(&debugMemoryProfile, "memory-profile", "", "Export pprof memory profile")
	_ = rootCmd.PersistentFlags().MarkHidden("memory-profile")
}

const versionMetadataUrl = "https://azcopyvnextrelease.z22.web.core.windows.net/releasemetadata/latest_version.txt"

// always spins up a new goroutine, because sometimes the aka.ms URL can't be reached (e.g. a constrained environment where
// aka.ms is not resolvable to a reachable IP address). In such cases, this routine will run for ever, and the caller should
// just give up on it.
// We spin up the GR here, not in the caller, so that the need to use a separate GC can never be forgotten
// (if do it synchronously, and can't resolve URL, this blocks caller for ever)
func beginDetectNewVersion() chan struct{} {
	completionChannel := make(chan struct{})
	go func() {
		// step 0: check the Stderr, check local version
		_, err := os.Stderr.Stat()
		if err != nil {
			return
		}

		localVersion, err := NewVersion(common.AzcopyVersion)
		if err != nil {
			return
		}

		// step 1: fetch & validate cached version and if it is updated, return without making API calls
		filePath := filepath.Join(azcopyLogPathFolder, "latest_version.txt")
		cachedVersion, err := ValidateCachedVersion(filePath) // same as the remote version
		if err == nil {
			PrintOlderVersion(*cachedVersion, *localVersion)
		} else {
			// step 2: initialize pipeline
			options := createClientOptions(nil, nil, nil)

			// step 3: start download
			blobClient, err := blob.NewClientWithNoCredential(versionMetadataUrl, &blob.ClientOptions{ClientOptions: options})
			if err != nil {
				return
			}

			downloadBlobResp, err := blobClient.DownloadStream(context.TODO(), nil)
			if err != nil {
				return
			}

			// step 4: read newest version str
			data := make([]byte, *downloadBlobResp.ContentLength)
			_, err = downloadBlobResp.Body.Read(data)
			defer downloadBlobResp.Body.Close()
			if err != nil && err != io.EOF {
				return
			}

			remoteVersion, err := NewVersion(string(data))
			if err != nil {
				return
			}

			PrintOlderVersion(*remoteVersion, *localVersion)

			// step 5: persist remote version in local
			err = localVersion.CacheRemoteVersion(*remoteVersion, filePath)
			if err != nil {
				return
			}
		}

		// let caller know we have finished, if they want to know
		close(completionChannel)
	}()

	return completionChannel
}
