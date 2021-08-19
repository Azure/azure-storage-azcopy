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
	"bytes"
	"context"
	"fmt"
	"github.com/Azure/azure-pipeline-go/pipeline"
	"net/url"
	"os"
	"runtime"
	"strings"
	"sync"
	"time"

	"github.com/Azure/azure-storage-azcopy/v10/common"
	"github.com/Azure/azure-storage-azcopy/v10/ste"
	"github.com/Azure/azure-storage-blob-go/azblob"
	"github.com/spf13/cobra"
)

var azcopyAppPathFolder string
var azcopyLogPathFolder string
var azcopyJobPlanFolder string
var azcopyMaxFileAndSocketHandles int
var outputFormatRaw string
var cancelFromStdin bool
var azcopyOutputFormat common.OutputFormat
var cmdLineCapMegaBitsPerSecond float64
var azcopyAwaitContinue bool
var azcopyAwaitAllowOpenFiles bool
var azcopyScanningLogger common.ILoggerResetable
var azcopyCurrentJobID common.JobID

// It's not pretty that this one is read directly by credential util.
// But doing otherwise required us passing it around in many places, even though really
// it can be thought of as an "ambient" property. That's the (weak?) justification for implementing
// it as a global
var cmdLineExtraSuffixesAAD string

// rootCmd represents the base command when called without any subcommands
var rootCmd = &cobra.Command{
	Version: common.AzcopyVersion, // will enable the user to see the version info in the standard posix way: --version
	Use:     "azcopy",
	Short:   rootCmdShortDescription,
	Long:    rootCmdLongDescription,
	PersistentPreRunE: func(cmd *cobra.Command, args []string) error {
		if glcm.GetEnvironmentVariable(common.EEnvironmentVariable.RequestTryTimeout()) != "" {
			timeout, err := time.ParseDuration(glcm.GetEnvironmentVariable(common.EEnvironmentVariable.RequestTryTimeout()) + "m")
			if err == nil {
				ste.UploadTryTimeout = timeout
			}
		}
		glcm.E2EEnableAwaitAllowOpenFiles(azcopyAwaitAllowOpenFiles)
		if azcopyAwaitContinue {
			glcm.E2EAwaitContinue()
		}

		timeAtPrestart := time.Now()

		err := azcopyOutputFormat.Parse(outputFormatRaw)
		glcm.SetOutputFormat(azcopyOutputFormat)
		if err != nil {
			return err
		}

		glcm.SetForceLogging()

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

		// currently, we only automatically do auto-tuning when benchmarking
		preferToAutoTuneGRs := cmd == benchCmd // TODO: do we have a better way to do this than making benchCmd global?
		providePerformanceAdvice := cmd == benchCmd

		// startup of the STE happens here, so that the startup can access the values of command line parameters that are defined for "root" command
		concurrencySettings := ste.NewConcurrencySettings(azcopyMaxFileAndSocketHandles, preferToAutoTuneGRs)
		err = ste.MainSTE(concurrencySettings, float64(cmdLineCapMegaBitsPerSecond), azcopyJobPlanFolder, azcopyLogPathFolder, providePerformanceAdvice)
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
		ste.JobsAdmin.LogToJobLog(startTimeMessage, pipeline.LogInfo)

		// spawn a routine to fetch and compare the local application's version against the latest version available
		// if there's a newer version that can be used, then write the suggestion to stderr
		// however if this takes too long the message won't get printed
		// Note: this function is necessary for non-help, non-login commands, since they don't reach the corresponding
		// beginDetectNewVersion call in Execute (below)
		beginDetectNewVersion()

		return nil
	},
}

// hold a pointer to the global lifecycle controller so that commands could output messages and exit properly
var glcm = common.GetLifecycleMgr()
var glcmSwapOnce = &sync.Once{}

// Execute adds all child commands to the root command and sets flags appropriately.
// This is called by main.main(). It only needs to happen once to the rootCmd.
func Execute(azsAppPathFolder, logPathFolder string, jobPlanFolder string, maxFileAndSocketHandles int) {
	azcopyAppPathFolder = azsAppPathFolder
	azcopyLogPathFolder = logPathFolder
	azcopyJobPlanFolder = jobPlanFolder
	azcopyMaxFileAndSocketHandles = maxFileAndSocketHandles
	azcopyCurrentJobID = common.NewJobID()

	if err := rootCmd.Execute(); err != nil {
		glcm.Error(err.Error())
	} else {
		// our commands all control their own life explicitly with the lifecycle manager
		// only commands that don't explicitly exit actually reach this point (e.g. help commands and login commands)
		select {
		case <-beginDetectNewVersion():
			// noop
		case <-time.After(time.Second * 8):
			// don't wait too long
		}
		glcm.Exit(nil, common.EExitCode.Success())
	}
}

func init() {
	// replace the word "global" to avoid confusion (e.g. it doesn't affect all instances of AzCopy)
	rootCmd.SetUsageTemplate(strings.Replace((&cobra.Command{}).UsageTemplate(), "Global Flags", "Flags Applying to All Commands", -1))

	rootCmd.PersistentFlags().Float64Var(&cmdLineCapMegaBitsPerSecond, "cap-mbps", 0, "Caps the transfer rate, in megabits per second. Moment-by-moment throughput might vary slightly from the cap. If this option is set to zero, or it is omitted, the throughput isn't capped.")
	rootCmd.PersistentFlags().StringVar(&outputFormatRaw, "output-type", "text", "Format of the command's output. The choices include: text, json. The default value is 'text'.")

	rootCmd.PersistentFlags().StringVar(&cmdLineExtraSuffixesAAD, trustedSuffixesNameAAD, "", "Specifies additional domain suffixes where Azure Active Directory login tokens may be sent.  The default is '"+
		trustedSuffixesAAD+"'. Any listed here are added to the default. For security, you should only put Microsoft Azure domains here. Separate multiple entries with semi-colons.")

	// Note: this is due to Windows not supporting signals properly
	rootCmd.PersistentFlags().BoolVar(&cancelFromStdin, "cancel-from-stdin", false, "Used by partner teams to send in `cancel` through stdin to stop a job.")

	// special E2E testing flags
	rootCmd.PersistentFlags().BoolVar(&azcopyAwaitContinue, "await-continue", false, "Used when debugging, to tell AzCopy to await `continue` on stdin before starting any work. Assists with debugging AzCopy via attach-to-process")
	rootCmd.PersistentFlags().BoolVar(&azcopyAwaitAllowOpenFiles, "await-open", false, "Used when debugging, to tell AzCopy to await `open` on stdin, after scanning but before opening the first file. Assists with testing cases around file modifications between scanning and usage")

	// reserved for partner teams
	rootCmd.PersistentFlags().MarkHidden("cancel-from-stdin")

	// debug-only
	rootCmd.PersistentFlags().MarkHidden("await-continue")
	rootCmd.PersistentFlags().MarkHidden("await-open")
}

// always spins up a new goroutine, because sometimes the aka.ms URL can't be reached (e.g. a constrained environment where
// aka.ms is not resolvable to a reachable IP address). In such cases, this routine will run for ever, and the caller should
// just give up on it.
// We spin up the GR here, not in the caller, so that the need to use a separate GC can never be forgotten
// (if do it synchronously, and can't resolve URL, this blocks caller for ever)
func beginDetectNewVersion() chan struct{} {
	completionChannel := make(chan struct{})
	go func() {
		const versionMetadataUrl = "https://aka.ms/azcopyv10-version-metadata"

		// step 0: check the Stderr before checking version
		_, err := os.Stderr.Stat()
		if err != nil {
			return
		}

		// step 1: initialize pipeline
		p, err := createBlobPipeline(context.TODO(), common.CredentialInfo{CredentialType: common.ECredentialType.Anonymous()}, pipeline.LogNone)
		if err != nil {
			return
		}

		// step 2: parse source url
		u, err := url.Parse(versionMetadataUrl)
		if err != nil {
			return
		}

		// step 3: start download
		blobURL := azblob.NewBlobURL(*u, p)
		blobStream, err := blobURL.Download(context.TODO(), 0, azblob.CountToEnd, azblob.BlobAccessConditions{}, false, azblob.ClientProvidedKeyOptions{})
		if err != nil {
			return
		}

		blobBody := blobStream.Body(azblob.RetryReaderOptions{MaxRetryRequests: ste.MaxRetryPerDownloadBody})
		defer blobBody.Close()

		// step 4: read newest version str
		buf := new(bytes.Buffer)
		n, err := buf.ReadFrom(blobBody)
		if n == 0 || err != nil {
			return
		}
		// only take the first line, in case the version metadata file is upgraded in the future
		remoteVersion := strings.Split(buf.String(), "\n")[0]

		// step 5: compare remote version to local version to see if there's a newer AzCopy
		v1, err := NewVersion(common.AzcopyVersion)
		if err != nil {
			return
		}
		v2, err := NewVersion(remoteVersion)
		if err != nil {
			return
		}

		if v1.OlderThan(*v2) {
			executablePathSegments := strings.Split(strings.Replace(os.Args[0], "\\", "/", -1), "/")
			executableName := executablePathSegments[len(executablePathSegments)-1]

			// output in info mode instead of stderr, as it was crashing CI jobs of some people
			glcm.Info(executableName + ": A newer version " + remoteVersion + " is available to download\n")
		}

		// let caller know we have finished, if they want to know
		close(completionChannel)
	}()

	return completionChannel
}
