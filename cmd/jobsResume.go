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
	"github.com/Azure/azure-storage-azcopy/v10/azcopy"
	"strings"
	"time"

	"github.com/Azure/azure-sdk-for-go/sdk/azcore"
	"github.com/Azure/azure-storage-azcopy/v10/azcopy"
	"github.com/Azure/azure-storage-azcopy/v10/jobsAdmin"
	"github.com/Azure/azure-storage-azcopy/v10/ste"

	"github.com/Azure/azure-storage-azcopy/v10/common"
	"github.com/spf13/cobra"
)

// TODO the behavior of the resume command should be double-checked
// TODO figure out how to merge resume job with copy
// TODO the progress reporting code is almost the same as the copy command, the copy-paste should be avoided
type resumeJobController struct {
	// generated
	jobID common.JobID

	// variables used to calculate progress
	// intervalStartTime holds the last time value when the progress summary was fetched
	// the value of this variable is used to calculate the throughput
	// it gets updated every time the progress summary is fetched
	intervalStartTime        time.Time
	intervalBytesTransferred uint64

	// used to calculate job summary
	jobStartTime time.Time
}

// wraps call to lifecycle manager to wait for the job to complete
// if blocking is specified to true, then this method will never return
// if blocking is specified to false, then another goroutine spawns and wait out the job
func (cca *resumeJobController) waitUntilJobCompletion(blocking bool) {
	// print initial message to indicate that the job is starting
	// Output the log location if log-level is set to other then NONE
	var logPathFolder string
	if common.LogPathFolder != "" {
		logPathFolder = fmt.Sprintf("%s%s%s.log", common.LogPathFolder, common.OS_PATH_SEPARATOR, cca.jobID)
	}
	glcm.OnStart(common.JobContext{JobID: cca.jobID, LogPath: logPathFolder})

	// initialize the times necessary to track progress
	cca.jobStartTime = time.Now()
	cca.intervalStartTime = time.Now()
	cca.intervalBytesTransferred = 0

	// hand over control to the lifecycle manager if blocking
	if blocking {
		glcm.InitiateProgressReporting(cca)
		glcm.SurrenderControl()
	} else {
		// non-blocking, return after spawning a go routine to watch the job
		glcm.InitiateProgressReporting(cca)
	}
}

func (cca *resumeJobController) Cancel(lcm common.LifecycleMgr) {
	err := cookedCancelCmdArgs{jobID: cca.jobID}.process()
	if err != nil {
		lcm.Error("error occurred while cancelling the job " + cca.jobID.String() + ". Failed with error " + err.Error())
	}
}

// TODO: can we combine this with the copy one (and the sync one?)
func (cca *resumeJobController) ReportProgressOrExit(lcm common.LifecycleMgr) (totalKnownCount uint32) {
	// fetch a job status
	summary := jobsAdmin.GetJobSummary(cca.jobID)
	jobDone := summary.JobStatus.IsJobDone()
	totalKnownCount = summary.TotalTransfers

	// if json is not desired, and job is done, then we generate a special end message to conclude the job
	duration := time.Since(cca.jobStartTime) // report the total run time of the job

	var computeThroughput = func() float64 {
		// compute the average throughput for the last time interval
		bytesInMb := float64(float64(summary.BytesOverWire-cca.intervalBytesTransferred) / float64(base10Mega))
		timeElapsed := time.Since(cca.intervalStartTime).Seconds()

		// reset the interval timer and byte count
		cca.intervalStartTime = time.Now()
		cca.intervalBytesTransferred = summary.BytesOverWire

		return common.Iff(timeElapsed != 0, bytesInMb/timeElapsed, 0) * 8
	}

	throughput := computeThroughput()
	transferProgress := common.TransferProgress{
		ListJobSummaryResponse: summary,
		Throughput:             throughput,
		ElapsedTime:            duration,
		JobType:                common.EJobType.Resume(),
	}
	if common.AzcopyCurrentJobLogger != nil {
		common.AzcopyCurrentJobLogger.Log(common.LogInfo, common.GetProgressOutputBuilder(transferProgress)(common.EOutputFormat.Text()))
	}
	glcm.OnTransferProgress(transferProgress)

	if jobDone {
		// TODO (gapra): Why doesnt resume use the same logic as copy for exit code? if summary.TransfersFailed > 0 || summary.JobStatus == common.EJobStatus.Cancelled() || summary.JobStatus == common.EJobStatus.Cancelling() {
		exitCode := common.EExitCode.Success()
		if summary.TransfersFailed > 0 {
			exitCode = common.EExitCode.Error()
		}
		jobSummary := common.JobSummary{
			ExitCode:               exitCode,
			ListJobSummaryResponse: summary,
			ElapsedTime:            duration,
			JobType:                common.EJobType.Resume(),
		}
		lcm.OnComplete(jobSummary)
	}

	return
}

func parseTransfers(arg string) map[string]int {
	transfersMap := make(map[string]int)
	for i, t := range strings.Split(arg, ";") {
		if t == "" {
			continue // skip empty entries from misplaced ';'
		}
		transfersMap[t] = i
	}
	return transfersMap
}

func init() {
	commandLineArgs := resumeCmdArgs{}

	// resumeCmd represents the resume command
	resumeCmd := &cobra.Command{
		Use:        "resume [jobID]",
		SuggestFor: []string{"resme", "esume", "resue"},
		Short:      resumeJobsCmdShortDescription,
		Long:       resumeJobsCmdLongDescription,
		Args: func(cmd *cobra.Command, args []string) error {
			// the resume command requires necessarily to have an argument
			// resume jobId -- resumes all the parts of an existing job for given jobId

			// If no argument is passed then it is not valid
			if len(args) != 1 {
				return errors.New("this command requires jobId to be passed as argument")
			}
			commandLineArgs.jobID = args[0]

			glcm.EnableInputWatcher()
			if cancelFromStdin {
				glcm.EnableCancelFromStdIn()
			}
			return nil
		},
		Run: func(cmd *cobra.Command, args []string) {
			includeTransfer := parseTransfers(commandLineArgs.includeTransfer)
			excludeTransfer := parseTransfers(commandLineArgs.excludeTransfer)
			if len(includeTransfer) > 0 || len(excludeTransfer) > 0 {
				panic("List of transfers is obsolete.")
			}
			err := commandLineArgs.process()
			if err != nil {
				glcm.Error(fmt.Sprintf("failed to perform resume command due to error: %s", err.Error()))
			}
		},
	}

	jobsCmd.AddCommand(resumeCmd)
	resumeCmd.PersistentFlags().StringVar(&commandLineArgs.includeTransfer, "include", "", "Filter: Include only these failed transfer(s) when resuming the job. "+
		"Files should be separated by ';'.")
	resumeCmd.PersistentFlags().StringVar(&commandLineArgs.excludeTransfer, "exclude", "", "Filter: Exclude these failed transfer(s) when resuming the job. "+
		"Files should be separated by ';'.")
	// oauth options
	resumeCmd.PersistentFlags().StringVar(&commandLineArgs.SourceSAS, "source-sas", "", "Source SAS token of the source for a given Job ID.")
	resumeCmd.PersistentFlags().StringVar(&commandLineArgs.DestinationSAS, "destination-sas", "", "Destination SAS token of the destination for a given Job ID.")
}

type resumeCmdArgs struct {
	jobID           string
	includeTransfer string
	excludeTransfer string

	SourceSAS      string
	DestinationSAS string
}

// processes the resume command,
// dispatches the resume Job order to the storage engine.
func (rca resumeCmdArgs) process() error {
	// parsing the given JobId to validate its format correctness
	jobID, err := common.ParseJobID(rca.jobID)
	if err != nil {
		// If parsing gives an error, hence it is not a valid JobId format
		return fmt.Errorf("error parsing the jobId %s. Failed with error %w", rca.jobID, err)
	}

	err = Client.ResumeJob(azcopy.ResumeJobOptions{JobID: jobID, SourceSAS: rca.SourceSAS, DestinationSAS: rca.DestinationSAS})
	if err != nil {
		return err
	}

	// TODO: Replace context with root context

	controller := resumeJobController{jobID: jobID}
	controller.waitUntilJobCompletion(true)

	return nil
}
