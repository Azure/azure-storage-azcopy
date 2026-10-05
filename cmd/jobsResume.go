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
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"

	"github.com/Azure/azure-storage-azcopy/v10/azcopy"
	"github.com/Azure/azure-storage-azcopy/v10/common"
	"github.com/spf13/cobra"
)

func init() {
	commandLineArgs := resumeCmdArgs{}
	resumeCmd := &cobra.Command{
		Use:        "resume [jobID]",
		SuggestFor: []string{"resme", "esume", "resue"},
		Short:      resumeJobsCmdShortDescription,
		Long:       resumeJobsCmdLongDescription,
		Args: func(cmd *cobra.Command, args []string) error {
			if len(args) != 1 {
				return errors.New("this command requires jobId to be passed as argument")
			}
			commandLineArgs.jobID = args[0]
			glcm.EnableInputWatcher()
			if cancelFromStdin {
				glcm.EnableCancelFromStdIn()
			}
			return nil
		},
		Run: func(cmd *cobra.Command, args []string) {
			if err := commandLineArgs.process(); err != nil {
				glcm.Error(fmt.Sprintf("failed to perform resume command due to error: %s", err))
				return
			}
			glcm.SurrenderControl()
		},
	}
	jobsCmd.AddCommand(resumeCmd)
	resumeCmd.PersistentFlags().StringVar(&commandLineArgs.includeTransfer, "include", "", "Deprecated: transfer selection lists are obsolete; nonempty values are rejected.")
	resumeCmd.PersistentFlags().StringVar(&commandLineArgs.excludeTransfer, "exclude", "", "Deprecated: transfer selection lists are obsolete; nonempty values are rejected.")
	resumeCmd.PersistentFlags().StringVar(&commandLineArgs.SourceSAS, "source-sas", "", "Source SAS token of the source for a given Job ID.")
	resumeCmd.PersistentFlags().StringVar(&commandLineArgs.DestinationSAS, "destination-sas", "", "Destination SAS token of the destination for a given Job ID.")
	AddSourceDestCredFlags(resumeCmd, &commandLineArgs.SrcCredName, &commandLineArgs.DstCredName)
}

type resumeCmdArgs struct {
	jobID           string
	includeTransfer string
	excludeTransfer string
	SourceSAS       string
	DestinationSAS  string
	SrcCredName     string
	DstCredName     string
}

func (rca resumeCmdArgs) process() error {
	jobID, err := common.ParseJobID(rca.jobID)
	if err != nil {
		return fmt.Errorf("error parsing the jobId %s. Failed with error %w", rca.jobID, err)
	}
	ctx, cancel := WithJobCancellation(context.Background())
	defer cancel()
	handler := &cliResumeHandler{}
	result, err := Client.ResumeJob(ctx, jobID, azcopy.ResumeJobOptions{
		SourceSAS:       rca.SourceSAS,
		DestinationSAS:  rca.DestinationSAS,
		SrcCredName:     rca.SrcCredName,
		DstCredName:     rca.DstCredName,
		IncludeTransfer: parseTransfers(rca.includeTransfer),
		ExcludeTransfer: parseTransfers(rca.excludeTransfer),
		Handler:         handler,
	})
	if err != nil {
		return fmt.Errorf("error resuming job %s: %w", jobID, err)
	}
	// Exit only after the library has completed its deferred cleanup.
	handler.printResult(result)
	return nil
}

type cliResumeHandler struct{}

func (c cliResumeHandler) OnStart(ctx azcopy.JobContext) {
	glcm.Init(GetStandardInitOutputBuilder(ctx.JobID.String(), ctx.LogPath, false, ""))
	StartSystemStatsMonitorForJobID(ctx.JobID)
}

func (c cliResumeHandler) OnTransferProgress(progress azcopy.ResumeJobProgress) {
	glcm.Progress(func(format common.OutputFormat) string {
		if format == EOutputFormat.Json() {
			jsonOutput, err := json.Marshal(progress.ListJobSummaryResponse)
			common.PanicIfErr(err)
			return string(jsonOutput)
		}
		return azcopy.GetCopyProgress(azcopy.CopyProgress(progress), false)
	})
}

func (c cliResumeHandler) OnComplete(azcopy.ResumeJobResult) {}

func (c cliResumeHandler) printResult(result azcopy.ResumeJobResult) {
	exitCode := EExitCode.Success()
	if result.TransfersFailed > 0 {
		exitCode = EExitCode.Error()
	}
	glcm.Exit(func(format common.OutputFormat) string {
		if format == EOutputFormat.Json() {
			jsonOutput, err := json.Marshal(result.ListJobSummaryResponse)
			common.PanicIfErr(err)
			return string(jsonOutput)
		}
		return fmt.Sprintf(
			`

Job %s summary
Elapsed Time (Minutes): %v
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
Final Job Status: %v
`,
			result.JobID.String(),
			azcopy.ToFixed(result.ElapsedTime.Minutes(), 4),
			result.FileTransfers,
			result.FolderPropertyTransfers,
			result.SymlinkTransfers,
			result.TotalTransfers,
			result.TransfersCompleted-result.FoldersCompleted,
			result.FoldersCompleted,
			result.TransfersFailed-result.FoldersFailed,
			result.FoldersFailed,
			result.TransfersSkipped-result.FoldersSkipped,
			result.FoldersSkipped,
			result.TotalBytesTransferred,
			result.JobStatus)
	}, exitCode)
}

func parseTransfers(arg string) map[string]int {
	transfers := make(map[string]int)
	for i, transfer := range strings.Split(arg, ";") {
		if transfer != "" {
			transfers[transfer] = i
		}
	}
	return transfers
}
