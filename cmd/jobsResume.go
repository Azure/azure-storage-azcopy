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
	"os"
	"os/signal"
	"strings"
	"syscall"

	"github.com/Azure/azure-storage-azcopy/v10/azcopy"
	"github.com/Azure/azure-storage-azcopy/v10/jobsAdmin"

	"github.com/Azure/azure-storage-azcopy/v10/common"
	"github.com/spf13/cobra"
)

func init() {
	commandLineArgs := resumeCmdArgs{}

	// resumeCmd represents the resume command
	resumeCmd := &cobra.Command{
		Use:        "resume [jobID]",
		SuggestFor: []string{"resme", "esume", "resue"},
		Short:      resumeJobsCmdShortDescription,
		Long:       resumeJobsCmdLongDescription,
		Args: func(cmd *cobra.Command, args []string) error {
			// the resume command requires necessarily to have an argument
			// resume jobId -- resumes all the parts of an existing job for given jobId

			// If no argument is passed then it is not valid
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
			includeTransfer := parseTransfers(commandLineArgs.includeTransfer)
			excludeTransfer := parseTransfers(commandLineArgs.excludeTransfer)
			if len(includeTransfer) > 0 || len(excludeTransfer) > 0 {
				panic("list of transfers is obsolete")
			}
			err := commandLineArgs.process()
			if err != nil {
				glcm.Error(fmt.Sprintf("failed to perform resume command due to error: %s", err.Error()))
			}
			glcm.Exit(nil, EExitCode.Success())
		},
	}

	jobsCmd.AddCommand(resumeCmd)
	resumeCmd.PersistentFlags().StringVar(&commandLineArgs.includeTransfer, "include", "", "Filter: Include only these failed transfer(s) when resuming the job. "+
		"Files should be separated by ';'.")
	resumeCmd.PersistentFlags().StringVar(&commandLineArgs.excludeTransfer, "exclude", "", "Filter: Exclude these failed transfer(s) when resuming the job. "+
		"Files should be separated by ';'.")
	// oauth options
	resumeCmd.PersistentFlags().StringVar(&commandLineArgs.SourceSAS, "source-sas", "", "Source SAS token of the source for a given Job ID.")
	resumeCmd.PersistentFlags().StringVar(&commandLineArgs.DestinationSAS, "destination-sas", "", "Destination SAS token of the destination for a given Job ID.")
}

type resumeCmdArgs struct {
	jobID           string
	includeTransfer string
	excludeTransfer string

	SourceSAS      string
	DestinationSAS string
}

// processes the resume command,
// dispatches the resume Job order to the storage engine.
func (rca resumeCmdArgs) process() error {
	// parsing the given JobId to validate its format correctness
	jobID, err := common.ParseJobID(rca.jobID)
	if err != nil {
		// If parsing gives an error, hence it is not a valid JobId format
		return fmt.Errorf("error parsing the jobId %s. Failed with error %w", rca.jobID, err)
	}

	resumeOptions := azcopy.ResumeJobOptions{
		SourceSAS:      rca.SourceSAS,
		DestinationSAS: rca.DestinationSAS,
	}
	// Create a context that can be cancelled by Ctrl-C
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Set up signal handling for graceful cancellation
	go func() {
		sigChan := make(chan os.Signal, 1)
		signal.Notify(sigChan, os.Interrupt, syscall.SIGTERM)
		<-sigChan
		cancel()
	}()

	summary, err := Client.ResumeJob(ctx, jobID, resumeOptions)
	if err != nil {
		return fmt.Errorf("error resuming job %s. failed with error: %v", jobID, err)
	}

	exitCode := EExitCode.Success()
	if summary.TransfersFailed > 0 {
		exitCode = EExitCode.Error()
	}
	glcm.Exit(func(format OutputFormat) string {
		if format == EOutputFormat.Json() {
			jsonOutput, err := json.Marshal(summary)
			common.PanicIfErr(err)
			return string(jsonOutput)
		} else {
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
				summary.JobID.String(),
				jobsAdmin.ToFixed(summary.ElapsedTime.Minutes(), 4),
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
				summary.JobStatus)
		}
	}, exitCode)

	return nil
}

type CLIResumeHandler struct {
}

func (C CLIResumeHandler) OnStart(ctx azcopy.JobContext) {
	glcm.Init(GetStandardInitOutputBuilder(ctx.JobID.String(), ctx.LogPath, false, ""))
}

func (C CLIResumeHandler) OnTransferProgress(progress azcopy.ResumeJobProgress) {
	builder := func(format OutputFormat) string {
		if format == EOutputFormat.Json() {
			jsonOutput, err := json.Marshal(progress.ListJobSummaryResponse)
			common.PanicIfErr(err)
			return string(jsonOutput)
		} else {
			// if json is not needed, then we generate a message that goes nicely on the same line
			// display a scanning keyword if the job is not completely ordered
			var scanningString = " (scanning...)"
			if progress.CompleteJobOrdered {
				scanningString = ""
			}

			throughput := progress.Throughput
			throughputString := fmt.Sprintf("2-sec Throughput (Mb/s): %v", jobsAdmin.ToFixed(throughput, 4))
			if throughput == 0 {
				// As there would be case when no bits sent from local, e.g. service side copy, when throughput = 0, hide it.
				throughputString = ""
			}

			// indicate whether constrained by disk or not
			perfString, diskString := getPerfDisplayText(progress.PerfStrings, progress.PerfConstraint, progress.ElapsedTime, false)

			return fmt.Sprintf("%.1f %%, %v Done, %v Failed, %v Pending, %v Skipped, %v Total%s, %s%s%s",
				progress.PercentComplete,
				progress.TransfersCompleted,
				progress.TransfersFailed,
				progress.TotalTransfers-(progress.TransfersCompleted+progress.TransfersFailed+progress.TransfersSkipped),
				progress.TransfersSkipped, progress.TotalTransfers, scanningString, perfString, throughputString, diskString)
		}
	}
	if jobsAdmin.JobsAdmin != nil {
		jobMan, exists := jobsAdmin.JobsAdmin.JobMgr(progress.JobID)
		if exists {
			jobMan.Log(common.LogInfo, builder(EOutputFormat.Text()))
		}
	}

	glcm.Progress(builder)
}

func parseTransfers(arg string) map[string]int {
	transfersMap := make(map[string]int)
	for i, t := range strings.Split(arg, ";") {
		if t == "" {
			continue // skip empty entries from misplaced ';'
		}
		transfersMap[t] = i
	}
	return transfersMap
}
