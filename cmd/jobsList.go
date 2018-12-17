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
	"fmt"

	"github.com/Azure/azure-storage-azcopy/common"
	"github.com/spf13/cobra"
)

type ListResponse struct {
	ErrorMsg string
}

func init() {
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
			err := HandleListJobsCommand()
			if err == nil {
				glcm.Exit("", common.EExitCode.Success())
			} else {
				glcm.Exit(err.Error(), common.EExitCode.Error())
			}
		},
	}

	jobsCmd.AddCommand(lsCmd)
}

// HandleListJobsCommand sends the ListJobs request to transfer engine
// Print the Jobs in the history of Azcopy
func HandleListJobsCommand() error {
	resp := common.ListJobsResponse{}
	Rpc(common.ERpcCmd.ListJobs(), nil, &resp)
	return PrintExistingJobIds(resp)
}

// PrintExistingJobIds prints the response of listOrder command when listOrder command requested the list of existing jobs
func PrintExistingJobIds(listJobResponse common.ListJobsResponse) error {
	if listJobResponse.ErrorMessage != "" {
		return fmt.Errorf("request failed with following error message: %s", listJobResponse.ErrorMessage)
	}

	glcm.Info("Existing Jobs ")
	for index := 0; index < len(listJobResponse.JobIDDetails); index++ {
		jobDetail := listJobResponse.JobIDDetails[index]
		glcm.Info(fmt.Sprintf("JobId: %s\nCommand: %s\n", jobDetail.JobId.String(), jobDetail.CommandString))
	}
	return nil
}
