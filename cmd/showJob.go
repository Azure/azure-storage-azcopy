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
	"os"
)

type ListReq struct {
	JobID    common.JobID
	StatusOf common.TransferStatus
}

func init() {
	commandLineInput := common.ListRequest{}

	// shJob represents the ls command
	shJob := &cobra.Command{
		Use:        "showJob [jobID]",
		Aliases:    []string{"showJob"},
		SuggestFor: []string{"shwJob", "shJob", "showJb"},
		Short:      "Show detailed information for the given job ID",
		Long: `
Show detailed information for the given job ID: if only the job ID is supplied without flag, then the progress summary of the job is returned.
If the with-status flag is set, then the list of transfers in the job with the given value will be shown.`,
		Args: func(cmd *cobra.Command, args []string) error {

			// if there is any argument passed
			// it is an error
			if len(args) == 0 {
				fmt.Println("showJob require atleast the JobID")
				os.Exit(1)
			}
			// Parse the JobId
			jobId, err := common.ParseJobID(args[0])
			if err != nil {
				fmt.Println("invalid jobId given ", args[0])
				return nil
			}
			commandLineInput.JobID = jobId
			return nil
		},
		Run: func(cmd *cobra.Command, args []string)  {
			err := HandleShowCommand(commandLineInput)
			if err != nil {
				fmt.Println(err.Error())
				os.Exit(1)
			}
		},
	}

	rootCmd.AddCommand(shJob)

	// filters
	shJob.PersistentFlags().StringVar(&commandLineInput.OfStatus, "with-status", "", "only list the transfers of job with this status, available values: NotStarted, Started, Success, Failed")
}

// handles the list command
// dispatches the list order to the transfer engine
func HandleShowCommand(listRequest common.ListRequest) error {
	rpcCmd := common.ERpcCmd.None()
	if listRequest.OfStatus == "" {
		resp := common.ListJobSummaryResponse{}
		rpcCmd = common.ERpcCmd.ListJobSummary()
		Rpc(rpcCmd, &listRequest.JobID, &resp)
		PrintJobProgressSummary(resp)
	} else {
		lsRequest := common.ListJobTransfersRequest{}
		lsRequest.JobID = listRequest.JobID
		// Parse the given expected Transfer Status
		// If there is an error parsing, then kill return the error
		err := lsRequest.OfStatus.Parse(listRequest.OfStatus)
		if err != nil {
			return fmt.Errorf("cannot parse the given Transfer Status %s", listRequest.OfStatus)
		}
		resp := common.ListJobTransfersResponse{}
		rpcCmd = common.ERpcCmd.ListJobTransfers()
		Rpc(rpcCmd, lsRequest, &resp)
		PrintJobTransfers(resp)
	}
	return nil
}

// PrintJobTransfers prints the response of listOrder command when list Order command requested the list of specific transfer of an existing job
func PrintJobTransfers(listTransfersResponse common.ListJobTransfersResponse) {
	if listTransfersResponse.ErrorMsg != "" {
		fmt.Println(fmt.Sprintf("request failed with following message %s", listTransfersResponse.ErrorMsg))
		return
	}

	fmt.Println(fmt.Sprintf("----------- Transfers for JobId %s -----------", listTransfersResponse.JobID))
	for index := 0; index < len(listTransfersResponse.Details); index++ {
		fmt.Println(fmt.Sprintf("transfer--> source: %s destination: %s status %s", listTransfersResponse.Details[index].Src, listTransfersResponse.Details[index].Dst,
			listTransfersResponse.Details[index].TransferStatus))
	}
}

// PrintJobProgressSummary prints the response of listOrder command when listOrder command requested the progress summary of an existing job
func PrintJobProgressSummary(summary common.ListJobSummaryResponse) {
	if summary.ErrorMsg != "" {
		fmt.Println(fmt.Sprintf("list progress summary of job failed because %s", summary.ErrorMsg))
		return
	}
	fmt.Println(fmt.Sprintf("--------------- Progress Summary for Job %s ---------------", summary.JobID))
	fmt.Println("Total Number of Transfer ", summary.TotalTransfers)
	fmt.Println("Total Number of Transfer Completed ", summary.TransfersCompleted)
	fmt.Println("Total Number of Transfer Failed ", summary.TransfersFailed)
	fmt.Println("Has the final part been ordered ", summary.CompleteJobOrdered)
	fmt.Println("Job Status ", summary.JobStatus)
	//fmt.Println("Progress of Job in terms of Perecentage ", summary.PercentageProgress)
	for index := 0; index < len(summary.FailedTransfers); index++ {
		message := fmt.Sprintf("transfer-%d	source: %s	destination: %s", index, summary.FailedTransfers[index].Src, summary.FailedTransfers[index].Dst)
		fmt.Println(message)
	}
}
