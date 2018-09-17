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

	"encoding/json"

	"github.com/Azure/azure-storage-azcopy/common"
	"github.com/spf13/cobra"
)

type ListReq struct {
	JobID    common.JobID
	OfStatus string
	Output   string
}

func init() {
	commandLineInput := ListReq{}

	// shJob represents the ls command
	shJob := &cobra.Command{
		Use:   "show [jobID]",
		Short: "Show detailed information for the given job ID",
		Long: `
Show detailed information for the given job ID: if only the job ID is supplied without flag, then the progress summary of the job is returned.
If the with-status flag is set, then the list of transfers in the job with the given value will be shown.`,
		Args: func(cmd *cobra.Command, args []string) error {

			// if there is any argument passed
			// it is an error
			if len(args) == 0 {
				return errors.New("showJob require at least the JobID")
			}
			// Parse the JobId
			jobId, err := common.ParseJobID(args[0])
			if err != nil {
				return errors.New("invalid jobId given " + args[0])
			}
			commandLineInput.JobID = jobId
			return nil
		},
		Run: func(cmd *cobra.Command, args []string) {
			listRequest := common.ListRequest{}
			listRequest.JobID = commandLineInput.JobID
			listRequest.OfStatus = commandLineInput.OfStatus
			err := listRequest.Output.Parse(commandLineInput.Output)
			if err != nil {
				glcm.Exit(fmt.Errorf("error parsing the given output format %s", commandLineInput.Output).Error(), common.EExitCode.Error())
			}
			err = HandleShowCommand(listRequest)
			if err == nil {
				glcm.Exit("", common.EExitCode.Success())
			} else {
				glcm.Exit(err.Error(), common.EExitCode.Error())
			}
		},
	}

	jobsCmd.AddCommand(shJob)

	// filters
	shJob.PersistentFlags().StringVar(&commandLineInput.OfStatus, "with-status", "", "only list the transfers of job with this status, available values: Started, Success, Failed")
	// filters
	shJob.PersistentFlags().StringVar(&commandLineInput.Output, "output", "text", "format of the command's output, the choices include: text, json")
}

// handles the list command
// dispatches the list order to the transfer engine
func HandleShowCommand(listRequest common.ListRequest) error {
	rpcCmd := common.ERpcCmd.None()
	if listRequest.OfStatus == "" {
		resp := common.ListJobSummaryResponse{}
		rpcCmd = common.ERpcCmd.ListJobSummary()
		Rpc(rpcCmd, &listRequest.JobID, &resp)
		PrintJobProgressSummary(listRequest.Output, resp)
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
		PrintJobTransfers(listRequest.Output, resp)
	}
	return nil
}

// PrintJobTransfers prints the response of listOrder command when list Order command requested the list of specific transfer of an existing job
func PrintJobTransfers(outputForamt common.OutputFormat, listTransfersResponse common.ListJobTransfersResponse) {
	if outputForamt == common.EOutputFormat.Json() {
		var exitCode = common.EExitCode.Success()
		if listTransfersResponse.ErrorMsg != "" {
			exitCode = common.EExitCode.Error()
		}
		//jsonOutput, err := json.MarshalIndent(listTransfersResponse, "", "  ")
		jsonOutput, err := json.Marshal(listTransfersResponse)
		common.PanicIfErr(err)
		glcm.Exit(string(jsonOutput), exitCode)
		return
	}
	if listTransfersResponse.ErrorMsg != "" {
		glcm.Exit("request failed with following message "+listTransfersResponse.ErrorMsg, common.EExitCode.Error())
		return
	}

	glcm.Info("----------- Transfers for JobId " + listTransfersResponse.JobID.String() + " -----------")
	for index := 0; index < len(listTransfersResponse.Details); index++ {
		glcm.Info("transfer--> source: " + listTransfersResponse.Details[index].Src + " destination: " +
			listTransfersResponse.Details[index].Dst + " status " + listTransfersResponse.Details[index].TransferStatus.String())
	}
}

// PrintJobProgressSummary prints the response of listOrder command when listOrder command requested the progress summary of an existing job
func PrintJobProgressSummary(outputFormat common.OutputFormat, summary common.ListJobSummaryResponse) {
	// Reset the bytes over the wire counter
	summary.BytesOverWire = 0

	// If the output format is Json, check the summary's error Message.
	// If there is an error message, then the exit code is error
	// else the exit code is success.
	// Marshal the summary and print in the Json format.
	if outputFormat == common.EOutputFormat.Json() {
		var exitCode = common.EExitCode.Success()
		if summary.ErrorMsg != "" {
			exitCode = common.EExitCode.Error()
		}
		//jsonOutput, err := json.MarshalIndent(summary, "", "  ")
		jsonOutput, err := json.Marshal(summary)
		common.PanicIfErr(err)
		glcm.Exit(string(jsonOutput), exitCode)
		return
	}

	if summary.ErrorMsg != "" {
		glcm.Exit("list progress summary of job failed because "+summary.ErrorMsg, common.EExitCode.Error())
	}
	glcm.Info(fmt.Sprintf(
		"\nJob %s summary\nTotal Number Of Transfers: %v\nNumber of Transfers Completed: %v\nNumber of Transfers Failed: %v\nNumber of Transfers Skipped: %v\nFinal Job Status: %v\n",
		summary.JobID.String(),
		summary.TotalTransfers,
		summary.TransfersCompleted,
		summary.TransfersFailed,
		summary.TransfersSkipped,
		summary.JobStatus,
	))
}
