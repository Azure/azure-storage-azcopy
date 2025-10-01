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
	"github.com/Azure/azure-storage-azcopy/v10/azcopy"
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
				return errors.New("show job command requires the JobID")
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
			if commandLineInput.OfStatus == "" {
				resp, err := Client.GetJobSummary(azcopy.GetJobSummaryOptions{JobID: commandLineInput.JobID})
				if err != nil {
					glcm.Error(err.Error())
				}
				PrintJobProgressSummary(common.ListJobSummaryResponse(resp))
			} else {
				// Parse the given expected Transfer Status
				// If there is an error parsing, then kill return the error
				var status common.TransferStatus
				err := status.Parse(commandLineInput.OfStatus)
				if err != nil {
					glcm.Error(fmt.Sprintf("cannot parse the given Transfer Status %s", commandLineInput.OfStatus))
				}
				resp, err := Client.ListJobTransfers(azcopy.ListJobTransfersOptions{JobID: commandLineInput.JobID, WithStatus: &status})
				if err != nil {
					glcm.Error(err.Error())
				}
				PrintJobTransfers(common.ListJobTransfersResponse(resp))
			}
			glcm.Exit(nil, common.EExitCode.Success())
		},
	}

	jobsCmd.AddCommand(shJob)

	// filters
	shJob.PersistentFlags().StringVar(&commandLineInput.OfStatus, "with-status", "", "List only the transfers of job with the specified status. "+
		"\n Available values include: All, Started, Success, Failed.")
}

// PrintJobTransfers prints the response of listOrder command when list Order command requested the list of specific transfer of an existing job
func PrintJobTransfers(listTransfersResponse common.ListJobTransfersResponse) {
	if OutputFormat == common.EOutputFormat.Json() {
		glcm.Output(
			func(_ common.OutputFormat) string {
				buf, err := json.Marshal(listTransfersResponse)
				if err != nil {
					panic(err)
				}

				return string(buf)
			}, common.EOutputMessageType.ListJobTransfers())
	}
	glcm.Exit(func(format common.OutputFormat) string {
		if format == common.EOutputFormat.Json() {
			jsonOutput, err := json.Marshal(listTransfersResponse)
			common.PanicIfErr(err)
			return string(jsonOutput)
		} else {
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
		}
	}, common.EExitCode.Success())
}

// PrintJobProgressSummary prints the response of listOrder command when listOrder command requested the progress summary of an existing job
func PrintJobProgressSummary(summary common.ListJobSummaryResponse) {
	// Reset the bytes over the wire counter
	summary.BytesOverWire = 0

	if OutputFormat == common.EOutputFormat.Json() {
		glcm.Output(
			func(_ common.OutputFormat) string {
				buf, err := json.Marshal(summary)
				if err != nil {
					panic(err)
				}

				return string(buf)
			}, common.EOutputMessageType.GetJobSummary())
	}
	glcm.Exit(func(format common.OutputFormat) string {
		if format == common.EOutputFormat.Json() {
			jsonOutput, err := json.Marshal(summary) // see note below re % complete being approximate. We can't include "approx" in the JSON.
			common.PanicIfErr(err)
			return string(jsonOutput)
		}

		return fmt.Sprintf(
			`
Job %s summary
Number of File Transfers: %v
Number of Folder Property Transfers: %v
Number of Symlink Transfers: %v
Total Number of Transfers: %v
Number of File Transfers Completed: %v
Number of Folder Transfers Completed: %v
Number of File Transfers Failed: %v
Number of Folder Transfers Failed: %v
Number of File Transfers Skipped: %v
Number of Folder Transfers Skipped: %v
Total Number of Bytes Transferred: %v
Total Number of Bytes Skipped: %v
Percent Complete (approx): %.1f
Final Job Status: %v
`,
			summary.JobID.String(),
			summary.FileTransfers,
			summary.FolderPropertyTransfers,
			summary.SymlinkTransfers,
			summary.TotalTransfers,
			summary.TransfersCompleted-summary.FoldersCompleted,
			summary.FoldersCompleted,
			summary.TransfersFailed-summary.FoldersFailed,
			summary.FoldersFailed,
			summary.TransfersSkipped-summary.FoldersSkipped,
			summary.FoldersSkipped,
			summary.TotalBytesTransferred,
			summary.TotalBytesSkipped,
			summary.PercentComplete, // noted as approx in the format string because won't include in-flight files if this Show command is run from a different process
			summary.JobStatus,
		)
	}, common.EExitCode.Success())
}
