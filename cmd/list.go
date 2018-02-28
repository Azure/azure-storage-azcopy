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
	"github.com/Azure/azure-storage-azcopy/common"
	"github.com/Azure/azure-storage-azcopy/handlers"
	"github.com/spf13/cobra"
	"fmt"
	"encoding/json"
)

type ListReq struct {
	JobID common.JobID
	StatusOf common.TransferStatus
}

type ListResponse struct {
 	ErrorMsg string

}

type ListExistingJobResponse struct {

}

type ListProgressSummaryResponse struct {

}

var rpc func(cmd string, request interface{}) []byte

func init() {
	commandLineInput := common.ListCmdArgsAndFlags{}

	// lsCmd represents the ls command
	lsCmd := &cobra.Command{
		Use:        "list",
		Aliases:    []string{"ls"},
		SuggestFor: []string{"lst", "lt", "ist"}, //TODO why does message appear twice on the console
		Short:      "list(ls) lists the specifics of an existing job.",
		Long: `list(ls) lists the specifics of an existing job. The most common cases are:
  - lists all the existing jobs.
  - lists all the part numbers of an existing jobs.
  - lists all the transfers of an existing job.`,
		Args: func(cmd *cobra.Command, args []string) error {
			// the list command does not require necessarily to have an argument
			/*
			* list -- lists all the existing jobs
			* list jobId -- lists the progress summary of the job for given jobId
			* list jobId --with-status -- lists all the transfers of an existing job which has the given status
			 */
			jobId := common.JobID{}
			// if there is more than one argument passed, then it is taken as a jobId
			if len(args) > 0 {
				jobId, err := common.ParseJobID(args[0])
				if err != nil{
					fmt.Print("invalid job Id given ", args[0])
					return nil
				}
			}
	resp := rpc( 0, ListReq{})
	var lr ListResponse
	json.Unmarshal(resp, &lr)
	if lr.ErrorMsg != "" {

	}
	commandLineInput.JobId = jobId
			return nil
		},
		Run: func(cmd *cobra.Command, args []string) {
			handlers.HandleListCommand(commandLineInput)
		},
	}

	rootCmd.AddCommand(lsCmd)

	// define the flags relevant to the ls command

	// filters
	lsCmd.PersistentFlags().StringVar(&commandLineInput.OfStatus, "with-status", "", "Filter: list transfers of job only with this status")
}
