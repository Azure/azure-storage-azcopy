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
	"errors"
	"fmt"
	"os"
	"os/signal"
	"strings"
	"syscall"

	"github.com/Azure/azure-storage-azcopy/v10/azcopy"

	"github.com/Azure/azure-storage-azcopy/v10/common"
	"github.com/spf13/cobra"
)

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
				panic("List of transfers is obsolete.")
			}
			err := commandLineArgs.process()
			if err != nil {
				glcm.Error(fmt.Sprintf("failed to perform resume command due to error: %s", err.Error()))
			}
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

// TODO : (gapra) We need to wrap glcm since Golang does not support method overloading.
// We could consider naming the methods different per job type - then we can just pass glcm.
type CLIResumeHandler struct {
}

func (C CLIResumeHandler) OnStart(ctx common.JobContext) {
	glcm.OnStart(ctx)
}

func (C CLIResumeHandler) OnTransferProgress(progress azcopy.ResumeJobProgress) {
	glcm.OnTransferProgress(common.TransferProgress{
		ListJobSummaryResponse: progress.ListJobSummaryResponse,
		Throughput:             progress.Throughput,
		ElapsedTime:            progress.ElapsedTime,
		JobType:                common.EJobType.Resume(),
	})
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
	opts := azcopy.ResumeJobOptions{
		SourceSAS:      rca.SourceSAS,
		DestinationSAS: rca.DestinationSAS,
		Handler:        CLIResumeHandler{},
	}
	var resp azcopy.ResumeJobResult
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

	resp, err = Client.ResumeJob(ctx, jobID, opts)
	if err != nil {
		return fmt.Errorf("error resuming job %s. Failed with error %w", jobID, err)
	}

	exitCode := common.EExitCode.Success()
	if resp.TransfersFailed > 0 {
		exitCode = common.EExitCode.Error()
	}

	jobSummary := common.JobSummary{
		ExitCode:               exitCode,
		ListJobSummaryResponse: resp.ListJobSummaryResponse,
		ElapsedTime:            resp.ElapsedTime,
		JobType:                common.EJobType.Resume(),
	}
	glcm.OnComplete(jobSummary)

	return nil
}
