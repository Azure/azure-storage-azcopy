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
	"errors"
	"fmt"
	"github.com/Azure/azure-storage-azcopy/common"
	"github.com/Azure/azure-storage-azcopy/ste"
	"github.com/spf13/cobra"
	"strings"
	"time"
)

// TODO the behavior of the resume command should be double-checked
// TODO ex: does it output json??
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

func (cca *resumeJobController) PrintJobStartedMsg() {
	glcm.Info("\nJob " + cca.jobID.String() + " has started\n")
}

func (cca *resumeJobController) CancelJob() {
	err := cookedCancelCmdArgs{jobID: cca.jobID}.process()
	if err != nil {
		glcm.ExitWithError("error occurred while cancelling the job "+cca.jobID.String()+". Failed with error "+err.Error(), common.EExitCode.Error())
	}
}

func (cca *resumeJobController) InitializeProgressCounters() {
	cca.jobStartTime = time.Now()
	cca.intervalStartTime = time.Now()
	cca.intervalBytesTransferred = 0
}

func (cca *resumeJobController) PrintJobProgressStatus() {
	// fetch a job status
	var summary common.ListJobSummaryResponse
	Rpc(common.ERpcCmd.ListJobSummary(), &cca.jobID, &summary)
	jobDone := summary.JobStatus == common.EJobStatus.Completed() || summary.JobStatus == common.EJobStatus.Cancelled()

	// if json is not desired, and job is done, then we generate a special end message to conclude the job
	if jobDone {
		duration := time.Now().Sub(cca.jobStartTime) // report the total run time of the job

		glcm.ExitWithSuccess(fmt.Sprintf(
			"\n\nJob %s summary\nElapsed Time (Minutes): %v\nTotal Number Of Transfers: %v\nNumber of Transfers Completed: %v\nNumber of Transfers Failed: %v\nFinal Job Status: %v\n",
			summary.JobID.String(),
			ste.ToFixed(duration.Minutes(), 4),
			summary.TotalTransfers,
			summary.TransfersCompleted,
			summary.TransfersFailed,
			summary.JobStatus), common.EExitCode.Success())
	}

	// if json is not needed, and job is not done, then we generate a message that goes nicely on the same line
	// display a scanning keyword if the job is not completely ordered
	var scanningString = ""
	if !summary.CompleteJobOrdered {
		scanningString = "(scanning...)"
	}

	// compute the average throughput for the last time interval
	bytesInMB := float64(float64(summary.BytesOverWire-cca.intervalBytesTransferred) / float64(1024*1024))
	timeElapsed := time.Since(cca.intervalStartTime).Seconds()
	throughPut := common.Iffloat64(timeElapsed != 0, bytesInMB/timeElapsed, 0)

	// reset the interval timer and byte count
	cca.intervalStartTime = time.Now()
	cca.intervalBytesTransferred = summary.BytesOverWire

	glcm.Progress(fmt.Sprintf("%v Done, %v Failed, %v Pending, %v Total%s, 2-sec Throughput (MB/s): %v",
		summary.TransfersCompleted,
		summary.TransfersFailed,
		summary.TotalTransfers-(summary.TransfersCompleted+summary.TransfersFailed),
		summary.TotalTransfers, scanningString, ste.ToFixed(throughPut, 4)))
}

func init() {
	var commandLineInput = ""
	var includeTransfer = ""
	var excludeTransfer = ""
	rawResumeJobCommand := common.ResumeJob{}
	// resumeCmd represents the resume command
	resumeCmd := &cobra.Command{
		Use:        "resume jobID",
		SuggestFor: []string{"resme", "esume", "resue"},
		Short:      "Resume the existing job with the given job ID",
		Long: `
Resume the existing job with the given job ID.`,
		Args: func(cmd *cobra.Command, args []string) error {
			// the resume command requires necessarily to have an argument
			// resume jobId -- resumes all the parts of an existing job for given jobId

			// If no argument is passed then it is not valid
			if len(args) != 1 {
				return errors.New("this command requires jobId to be passed as argument")
			}
			commandLineInput = args[0]
			return nil
		},
		Run: func(cmd *cobra.Command, args []string) {
			jobID, err := common.ParseJobID(commandLineInput)
			if err != nil {
				glcm.ExitWithError("error parsing the jobId "+commandLineInput+". Failed with error "+err.Error(), common.EExitCode.Error())
			}
			rawResumeJobCommand.JobID = jobID
			rawResumeJobCommand.IncludeTransfer = make(map[string]int)
			rawResumeJobCommand.ExcludeTransfer = make(map[string]int)

			// If the transfer has been provided with the include
			// parse the transfer list
			if len(includeTransfer) > 0 {
				// Split the Include Transfer using ';'
				transfers := strings.Split(includeTransfer, ";")
				for index := range transfers {
					if len(transfers[index]) == 0 {
						// If the transfer provided is empty
						// skip the transfer
						// This is to handle the misplaced ';'
						continue
					}
					rawResumeJobCommand.IncludeTransfer[transfers[index]] = index
				}
			}
			// If the transfer has been provided with the exclude
			// parse the transfer list
			if len(excludeTransfer) > 0 {
				// Split the Exclude Transfer using ';'
				transfers := strings.Split(excludeTransfer, ";")
				for index := range transfers {
					if len(transfers[index]) == 0 {
						// If the transfer provided is empty
						// skip the transfer
						// This is to handle the misplaced ';'
						continue
					}
					rawResumeJobCommand.ExcludeTransfer[transfers[index]] = index
				}
			}
			HandleResumeCommand(rawResumeJobCommand)
		},
	}
	rootCmd.AddCommand(resumeCmd)
	resumeCmd.PersistentFlags().StringVar(&includeTransfer, "include", "", "Filter: only include these failed transfer(s) when resuming the job. "+
		"Files should be separated by ';'.")
	resumeCmd.PersistentFlags().StringVar(&excludeTransfer, "exclude", "", "Filter: exclude these failed transfer(s) when resuming the job. "+
		"Files should be separated by ';'.")
}

// handles the resume command
// dispatches the resume Job order to the storage engine
func HandleResumeCommand(resJobOrder common.ResumeJob) {

	var resumeJobResponse common.CancelPauseResumeResponse
	Rpc(common.ERpcCmd.ResumeJob(), resJobOrder, &resumeJobResponse)
	if !resumeJobResponse.CancelledPauseResumed {
		glcm.ExitWithError(resumeJobResponse.ErrorMsg, common.EExitCode.Error())
	}

	controller := resumeJobController{jobID: resJobOrder.JobID}
	glcm.WaitUntilJobCompletion(&controller)
}
