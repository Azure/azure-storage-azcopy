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

type ListReq struct {
	JobID    common.JobID
	StatusOf common.TransferStatus
}

type ListResponse struct {
	ErrorMsg string
}

type ListExistingJobResponse struct {
}

type ListProgressSummaryResponse struct {
}

func init() {
	commandLineInput := common.ListRequest{}

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

			// if there is more than one argument passed, then it is taken as a jobId
			if len(args) > 0 {
				jobId, err := common.ParseJobID(args[0])
				if err != nil {
					fmt.Print("invalid job Id given ", args[0])
					return nil
				}
				commandLineInput.JobID = jobId
			}

			return nil
		},
		Run: func(cmd *cobra.Command, args []string) {
			HandleListCommand(commandLineInput)
		},
	}

	rootCmd.AddCommand(lsCmd)

	// define the flags relevant to the ls command

	// filters
	lsCmd.PersistentFlags().StringVar(&commandLineInput.OfStatus, "with-status", "", "Filter: list transfers of job only with this status")
}

// handles the list command
// dispatches the list order to theZiyi Wang storage engine
func HandleListCommand(listRequest common.ListRequest) {
	// check whether ofstatus transfer status is valid or not
	if listRequest.OfStatus != "" {
		/* TODO: Fix this: &&
		common.TransferStatusStringToCode(listRequest.OfStatus) == math.MaxUint32 */
		fmt.Println("invalid transfer status passed. Please provide the correct transfer status flag")
		return
	}

	rpcCmd := common.ERpcCmd.None()
	if listRequest.JobID.IsEmpty() {
		resp := common.ListJobsResponse{}
		rpcCmd = common.ERpcCmd.ListJobs()
		Rpc(rpcCmd, listRequest, &resp)
		PrintExistingJobIds(resp)
	} else if listRequest.OfStatus == "" {
		resp := common.ListJobSummaryResponse{}
		rpcCmd = common.ERpcCmd.ListJobSummary()
		Rpc(rpcCmd, &listRequest.JobID, &resp)
		PrintJobProgressSummary(resp)
	} else {
		resp := common.ListJobTransfersResponse{}
		rpcCmd = common.ERpcCmd.ListJobTransfers()
		Rpc(rpcCmd, listRequest, &resp)
		PrintJobTransfers(resp)
	}
}

// PrintExistingJobIds prints the response of listOrder command when listOrder command requested the list of existing jobs
func PrintExistingJobIds(listJobResponse common.ListJobsResponse) {
	if listJobResponse.ErrorMessage != "" {
		fmt.Println(fmt.Sprintf("request failed with following error message %s", listJobResponse.ErrorMessage))
		return
	}

	fmt.Println("Existing Jobs ")
	for index := 0; index < len(listJobResponse.JobIDs); index++ {
		fmt.Println(listJobResponse.JobIDs[index].String())
	}
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
	//fmt.Println("Progress of Job in terms of Perecentage ", summary.PercentageProgress)
	for index := 0; index < len(summary.FailedTransfers); index++ {
		message := fmt.Sprintf("transfer-%d	source: %s	destination: %s", index, summary.FailedTransfers[index].Src, summary.FailedTransfers[index].Dst)
		fmt.Println(message)
	}
}
