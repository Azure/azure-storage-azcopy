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
	"errors"
	"fmt"
	"strings"

	"github.com/Azure/azure-storage-azcopy/v10/azcopy"
	"github.com/Azure/azure-storage-azcopy/v10/common"
	"github.com/spf13/cobra"
)

type ListReq struct {
	JobID    common.JobID
	OfStatus string
}

func init() {
	commandLineInput := ListReq{}

	shJob := &cobra.Command{
		Use:   "show [jobID]",
		Short: showJobsCmdShortDescription,
		Long:  showJobsCmdLongDescription,
		Args: func(cmd *cobra.Command, args []string) error {
			if len(args) != 1 {
				return errors.New("show job command requires the JobID")
			}
			jobId, err := common.ParseJobID(args[0])
			if err != nil {
				return errors.New("invalid jobId given " + args[0])
			}
			commandLineInput.JobID = jobId
			return nil
		},
		Run: func(cmd *cobra.Command, args []string) {
			if commandLineInput.OfStatus == "" {
				resp, err := Client.GetJobSummary(azcopy.GetJobSummaryOptions{JobID: commandLineInput.JobID})
				if err != nil {
					glcm.Error(err.Error())
					return
				}
				PrintJobProgressSummary(common.ListJobSummaryResponse(resp))
			} else {
				var status common.TransferStatus
				err := status.Parse(commandLineInput.OfStatus)
				if err != nil {
					glcm.Error(fmt.Sprintf("cannot parse the given Transfer Status %s", commandLineInput.OfStatus))
					return
				}
				resp, err := Client.ListJobTransfers(azcopy.ListJobTransfersOptions{JobID: commandLineInput.JobID, WithStatus: &status})
				if err != nil {
					glcm.Error(err.Error())
					return
				}
				PrintJobTransfers(common.ListJobTransfersResponse(resp))
			}
			glcm.Exit(nil, EExitCode.Success())
		},
	}

	jobsCmd.AddCommand(shJob)

	shJob.PersistentFlags().StringVar(&commandLineInput.OfStatus, "with-status", "", "List only the transfers of job with the specified status. "+
		"\n Available values include: All, Started, Success, Failed.")
}

// PrintJobTransfers prints the response of listOrder command when list Order command requested the list of specific transfer of an existing job
func PrintJobTransfers(listTransfersResponse common.ListJobTransfersResponse) {
	if OutputFormat == EOutputFormat.Json() {
		glcm.Output(
			func(_ common.OutputFormat) string {
				buf, err := json.Marshal(listTransfersResponse)
				common.PanicIfErr(err)
				return string(buf)
			}, EOutputMessageType.ListJobTransfers())
	}
	glcm.Exit(func(format common.OutputFormat) string {
		if format == EOutputFormat.Json() {
			jsonOutput, err := json.Marshal(listTransfersResponse)
			common.PanicIfErr(err)
			return string(jsonOutput)
		}

		var sb strings.Builder
		sb.WriteString("----------- Transfers for JobId " + listTransfersResponse.JobID.String() + " -----------\n")
		for _, detail := range listTransfersResponse.Details {
			folderChar := ""
			if detail.IsFolderProperties {
				folderChar = "/"
			}
			sb.WriteString("transfer--> source: " + detail.Src + folderChar + " destination: " +
				detail.Dst + folderChar + " status " + detail.TransferStatus.String() + "\n")
		}
		return sb.String()
	}, EExitCode.Success())
}

// PrintJobProgressSummary formats a library job summary for CLI output.
func PrintJobProgressSummary(summary common.ListJobSummaryResponse) {
	azcopy.PrintJobProgressSummary(summary, OutputFormat, glcm)
}
