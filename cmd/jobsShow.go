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
	"strings"

	"encoding/json"

	"github.com/Azure/azure-storage-azcopy/v10/common"
	"github.com/spf13/cobra"
)

type ListReq struct {
	JobID    common.JobID
	OfStatus string
}

func init() {
	commandLineInput := ListReq{}

	// shJob represents the ls command
	shJob := &cobra.Command{
		Use:   "show [jobID]",
		Short: showJobsCmdShortDescription,
		Long:  showJobsCmdLongDescription,
		Args: func(cmd *cobra.Command, args []string) error {
			if len(args) != 1 {
				return errors.New("show job command requires only the JobID")
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

			err := HandleShowCommand(listRequest)
			if err == nil {
				glcm.Exit(nil, common.EExitCode.Success())
			} else {
				glcm.Error(err.Error())
			}
		},
	}

	jobsCmd.AddCommand(shJob)

	// filters
	shJob.PersistentFlags().StringVar(&commandLineInput.OfStatus, "with-status", "", "Only list the transfers of job with this status, available values: Started, Success, Failed.")
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
		glcm.Error("request failed with following message " + listTransfersResponse.ErrorMsg)
	}

	glcm.Exit(func(format common.OutputFormat) string {
		if format == common.EOutputFormat.Json() {
			jsonOutput, err := json.Marshal(listTransfersResponse)
			common.PanicIfErr(err)
			return string(jsonOutput)
		}

		var sb strings.Builder
		sb.WriteString("----------- Transfers for JobId " + listTransfersResponse.JobID.String() + " -----------\n")
		for index := 0; index < len(listTransfersResponse.Details); index++ {
			folderChar := ""
			if listTransfersResponse.Details[index].IsFolderProperties {
				folderChar = "/"
			}
			sb.WriteString("transfer--> source: " + listTransfersResponse.Details[index].Src + folderChar + " destination: " +
				listTransfersResponse.Details[index].Dst + folderChar + " status " + listTransfersResponse.Details[index].TransferStatus.String() + "\n")
		}

		return sb.String()
	}, common.EExitCode.Success())
}

// PrintJobProgressSummary prints the response of listOrder command when listOrder command requested the progress summary of an existing job
func PrintJobProgressSummary(summary common.ListJobSummaryResponse) {
	if summary.ErrorMsg != "" {
		glcm.Error("list progress summary of job failed because " + summary.ErrorMsg)
	}

	// Reset the bytes over the wire counter
	summary.BytesOverWire = 0

	glcm.Exit(func(format common.OutputFormat) string {
		if format == common.EOutputFormat.Json() {
			jsonOutput, err := json.Marshal(summary) // see note below re % complete being approximate. We can't include "approx" in the JSON.
			common.PanicIfErr(err)
			return string(jsonOutput)
		}

		return fmt.Sprintf(
			"\nJob %s summary\nNumber of File Transfers: %v\nNumber of Folder Property Transfers: %v\nTotal Number Of Transfers: %v\nNumber of Transfers Completed: %v\nNumber of Transfers Failed: %v\nNumber of Transfers Skipped: %v\nPercent Complete (approx): %.1f\nFinal Job Status: %v\n",
			summary.JobID.String(),
			summary.FileTransfers,
			summary.FolderPropertyTransfers,
			summary.TotalTransfers,
			summary.TransfersCompleted,
			summary.TransfersFailed,
			summary.TransfersSkipped,
			summary.PercentComplete, // noted as approx in the format string because won't include in-flight files if this Show command is run from a different process
			summary.JobStatus,
		)
	}, common.EExitCode.Success())
}
