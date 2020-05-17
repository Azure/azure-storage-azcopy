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
	"sort"
	"strings"
	"time"

	"github.com/Azure/azure-storage-azcopy/common"
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
				glcm.Error(fmt.Sprintf("Failed to parse --with-status due to error: %s.", err))
			}

			err = HandleListJobsCommand(withStatus)
			if err == nil {
				glcm.Exit(nil, common.EExitCode.Success())
			} else {
				glcm.Error(err.Error())
			}
		},
	}

	jobsCmd.AddCommand(lsCmd)

	jobsCmd.PersistentFlags().StringVar(&commandLineInput.withStatus, "with-status", "All",
		"only remove the jobs with this status, available values: Cancelled, Completed, Failed, InProgress, All")
}

// HandleListJobsCommand sends the ListJobs request to transfer engine
// Print the Jobs in the history of Azcopy
func HandleListJobsCommand(jobStatus common.JobStatus) error {
	resp := common.ListJobsResponse{}
	Rpc(common.ERpcCmd.ListJobs(), nil, &resp)
	return PrintExistingJobIds(resp, jobStatus)
}

// PrintExistingJobIds prints the response of listOrder command when listOrder command requested the list of existing jobs
func PrintExistingJobIds(listJobResponse common.ListJobsResponse, jobStatus common.JobStatus) error {
	if listJobResponse.ErrorMessage != "" {
		return fmt.Errorf("request failed with following error message: %s", listJobResponse.ErrorMessage)
	}

	// before displaying the jobs, sort them accordingly so that they are displayed in a consistent way
	sortJobs(listJobResponse.JobIDDetails)

	glcm.Exit(func(format common.OutputFormat) string {
		if format == common.EOutputFormat.Json() {
			jsonOutput, err := json.Marshal(listJobResponse)
			common.PanicIfErr(err)
			return string(jsonOutput)
		}

		var sb strings.Builder
		sb.WriteString("Existing Jobs \n")
		for index := 0; index < len(listJobResponse.JobIDDetails); index++ {
			jobDetail := listJobResponse.JobIDDetails[index]
			if jobDetail.JobStatus != jobStatus {
				continue
			}
			sb.WriteString(fmt.Sprintf("JobId: %s\nStart Time: %s\nStatus: %s\nCommand: %s\n\n",
				jobDetail.JobId.String(),
				time.Unix(0, jobDetail.StartTime).Format(time.RFC850),
				jobDetail.JobStatus,
				jobDetail.CommandString))
		}
		return sb.String()
	}, common.EExitCode.Success())
	return nil
}

func sortJobs(jobsDetails []common.JobIDDetails) {
	// sort the jobs so that the latest one is shown first
	sort.Slice(jobsDetails, func(i, j int) bool {
		// this function essentially asks whether i should be placed before j
		// we say yes if the job i is more recent
		return jobsDetails[i].StartTime > jobsDetails[j].StartTime
	})
}
