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
	"encoding/json"
	"fmt"
	"github.com/Azure/azure-sdk-for-go/sdk/azcore/to"
	"github.com/Azure/azure-storage-azcopy/v10/azcopy"
	"strings"
	"time"

	"github.com/Azure/azure-storage-azcopy/v10/common"
	"github.com/spf13/cobra"
)

type ListResponse struct {
	ErrorMsg string
}

func init() {
	type JobsListReq struct {
		withStatus string
	}

	commandLineInput := JobsListReq{}

	// lsCmd represents the listJob command
	lsCmd := &cobra.Command{
		Use:     "list",
		Aliases: []string{"ls"},
		Short:   listJobsCmdShortDescription,
		Long:    listJobsCmdLongDescription,
		Args: func(cmd *cobra.Command, args []string) error {

			// if there is any argument passed
			// it is an error
			if len(args) > 0 {
				return fmt.Errorf("listJobs does not require any argument")
			}
			return nil
		},
		Run: func(cmd *cobra.Command, args []string) {
			withStatus := common.EJobStatus
			err := withStatus.Parse(commandLineInput.withStatus)
			if err != nil {
				glcm.OnError(fmt.Sprintf("Failed to parse --with-status due to error: %s.", err))
			}

			err = HandleListJobsCommand(withStatus)
			if err != nil {
				glcm.OnError(fmt.Sprintf("failed to perform jobs list command due to error: %s", err.Error()))
			}
		},
	}

	jobsCmd.AddCommand(lsCmd)

	lsCmd.PersistentFlags().StringVar(&commandLineInput.withStatus, "with-status", "All",
		"List the jobs with the specified status. "+
			"\n Available values include: "+
			"\n All, Cancelled, Failed, InProgress, Completed,"+
			" CompletedWithErrors, CompletedWithFailures, CompletedWithErrorsAndSkipped")
}

// HandleListJobsCommand sends the ListJobs request to transfer engine
// Print the Jobs in the history of Azcopy
func HandleListJobsCommand(jobStatus common.JobStatus) error {
	resp, err := Client.ListJobs(azcopy.ListJobsOptions{WithStatus: to.Ptr(jobStatus)})
	if err != nil {
		return err
	}
	return PrintExistingJobIds(resp)
}

// PrintExistingJobIds prints the response of listOrder command when listOrder command requested the list of existing jobs
func PrintExistingJobIds(listJobResponse azcopy.ListJobsResponse) error {

	glcm.Exit(func(format common.OutputFormat) string {
		if format == common.EOutputFormat.Json() {
			// Create the response structure using types from the common package.
			resp := common.ListJobsResponse{
				JobIDDetails: make([]common.JobIDDetails, len(listJobResponse.Details)),
			}

			// Convert from azcopy.JobDetail to common.JobIDDetails.
			for i, d := range listJobResponse.Details {
				resp.JobIDDetails[i] = common.JobIDDetails{
					JobId:         d.JobID,
					CommandString: d.Command,
					StartTime:     d.StartTime.Unix(),
					JobStatus:     d.Status,
				}
			}

			jsonOutput, err := json.Marshal(resp)
			common.PanicIfErr(err)
			return string(jsonOutput)
		}

		var sb strings.Builder
		sb.WriteString("Existing Jobs \n")
		for _, detail := range listJobResponse.Details {
			sb.WriteString(fmt.Sprintf("JobId: %s\nStart Time: %s\nStatus: %s\nCommand: %s\n\n",
				detail.JobID.String(),
				detail.StartTime.Format(time.RFC850),
				detail.Status,
				detail.Command))
		}
		return sb.String()
	}, common.EExitCode.Success())
	return nil
}
