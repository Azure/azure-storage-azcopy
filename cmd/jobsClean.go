// Copyright © Microsoft <wastore@microsoft.com>
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
	"strings"

	"github.com/spf13/cobra"

	"github.com/Azure/azure-storage-azcopy/v10/common"
)

var JobsCleanupSuccessMsg = "Successfully removed all jobs."

func init() {
	type JobsCleanReq struct {
		withStatus string
	}

	commandLineInput := JobsCleanReq{}

	// remove a single job's log and plan file
	jobsCleanCmd := &cobra.Command{
		Use:     "clean",
		Aliases: []string{"cl"},
		Short:   cleanJobsCmdShortDescription,
		Long:    cleanJobsCmdLongDescription,
		Example: cleanJobsCmdExample,
		Args: func(cmd *cobra.Command, args []string) error {
			if len(args) != 0 {
				return errors.New("clean command does not accept arguments")
			}
			return nil
		},
		Run: func(cmd *cobra.Command, args []string) {
			withStatus := common.EJobStatus
			err := withStatus.Parse(commandLineInput.withStatus)
			if err != nil {
				glcm.Error(fmt.Sprintf("Failed to parse --with-status due to error: %s.", err))
			}

			err = handleCleanJobsCommand(withStatus)
			if err == nil {
				if withStatus == common.EJobStatus.All() {
					glcm.Exit(func(format common.OutputFormat) string {
						return "Successfully removed all jobs."
					}, common.EExitCode.Success())
				} else {
					glcm.Exit(func(format common.OutputFormat) string {
						return fmt.Sprintf("Successfully removed jobs with status: %s.", withStatus)
					}, common.EExitCode.Success())
				}
			} else {
				glcm.Error(fmt.Sprintf("Failed to remove log/plan files due to error: %s.", err))
			}
		},
	}

	jobsCmd.AddCommand(jobsCleanCmd)

	// NOTE: we have way more job status than we normally need, only show the most common ones
	jobsCleanCmd.PersistentFlags().StringVar(&commandLineInput.withStatus, "with-status", "All",
		"Only remove the jobs with the specified status. Available values include: "+
			"\n All, Cancelled, Failed, Completed,"+
			" CompletedWithErrors, CompletedWithSkipped, CompletedWithErrorsAndSkipped")
}

func handleCleanJobsCommand(givenStatus common.JobStatus) error {
	if givenStatus == common.EJobStatus.All() {
		numFilesDeleted, err := blindDeleteAllJobFiles()
		glcm.Info(fmt.Sprintf("Removed %v files.", numFilesDeleted))
		return err
	}

	// we must query the jobs and find out which one to remove
	resp := common.ListJobsResponse{}
	Rpc(common.ERpcCmd.ListJobs(), common.EJobStatus.All(), &resp)

	if resp.ErrorMessage != "" {
		return errors.New("failed to query the list of jobs")
	}

	for _, job := range resp.JobIDDetails {
		// delete all jobs matching the givenStatus
		if job.JobStatus == givenStatus {
			glcm.Info(fmt.Sprintf("Removing files for job %s", job.JobId))
			err := handleRemoveSingleJob(job.JobId)
			if err != nil {
				return err
			}
		}
	}

	return nil
}

func blindDeleteAllJobFiles() (int, error) {
	// get rid of the job plan files
	numPlanFilesRemoved, err := removeFilesWithPredicate(common.AzcopyJobPlanFolder, func(s string) bool {
		if strings.Contains(s, ".steV") {
			return true
		}
		return false
	})
	if err != nil {
		return numPlanFilesRemoved, err
	}
	// get rid of the logs
	numLogFilesRemoved, err := removeFilesWithPredicate(azcopyLogPathFolder, func(s string) bool {
		// Do not remove the current job's log file this will cause the cleanup job to fail.
		if strings.Contains(s, azcopyCurrentJobID.String()) {
			return false
		} else if strings.HasSuffix(s, ".log") {
			return true
		}
		return false
	})

	return numPlanFilesRemoved + numLogFilesRemoved, err
}
