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

	"github.com/Azure/azure-sdk-for-go/sdk/azcore/to"
	"github.com/Azure/azure-storage-azcopy/v10/azcopy"
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
	result, err := Client.CleanJobs(azcopy.CleanJobsOptions{WithStatus: to.Ptr(givenStatus)})
	if err != nil {
		return err
	}

	if givenStatus == common.EJobStatus.All() {
		glcm.Info(fmt.Sprintf("Removed %v files.", result.Count))
	} else {
		for _, job := range result.Jobs {
			glcm.Info(fmt.Sprintf("Removing files for job %s", job))
		}
	}
	return nil
}
