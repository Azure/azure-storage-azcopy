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
	"github.com/Azure/azure-storage-azcopy/v10/common"
	"github.com/Azure/azure-storage-azcopy/v10/jobsAdmin"
	"github.com/spf13/cobra"
)

// TODO should this command be removed? Previously AzCopy was supposed to have an independent backend (out of proc)
// TODO but that's not the plan anymore
type rawCancelCmdArgs struct {
	jobID                   string
	ignoreCompletedJobError bool
}

func (raw rawCancelCmdArgs) cook() (cookedCancelCmdArgs, error) {
	//parsing the given JobId to validate its format correctness
	jobID, err := common.ParseJobID(raw.jobID)
	if err != nil {
		// If parsing gives an error, hence it is not a valid JobId format
		return cookedCancelCmdArgs{}, fmt.Errorf("invalid jobId string passed: %q", raw.jobID)
	}

	return cookedCancelCmdArgs{jobID: jobID, ignoreCompletedJobError: raw.ignoreCompletedJobError}, nil
}

type cookedCancelCmdArgs struct {
	jobID                   common.JobID
	ignoreCompletedJobError bool
}

// handles the cancel command
// dispatches the cancel Job order to the storage engine
func (cca cookedCancelCmdArgs) process() error {
	cancelJobResponse := jobsAdmin.CancelPauseJobOrder(cca.jobID, common.EJobStatus.Cancelling(), glcm)
	if !cancelJobResponse.CancelledPauseResumed {
		if cca.ignoreCompletedJobError && cancelJobResponse.JobStatus == common.EJobStatus.Completed() {
			glcm.Info(cancelJobResponse.ErrorMsg)
			resp, err := Client.GetJobSummary(azcopy.GetJobSummaryOptions{JobID: cca.jobID})
			if err != nil {
				return err
			}
			PrintJobProgressSummary(common.ListJobSummaryResponse(resp))
			return nil
		}
		return errors.New(cancelJobResponse.ErrorMsg)
	}
	return nil
}

func init() {
	raw := rawCancelCmdArgs{}

	// cancelCmd represents the pause command
	cancelCmd := &cobra.Command{
		Use:        "cancel",
		SuggestFor: []string{"cancl", "ancl", "cacl"},
		Short:      "Stops an ongoing job with the given Job ID",
		Long:       "Stops an ongoing job with the given Job ID",
		Args: func(cmd *cobra.Command, args []string) error {
			// the cancel command requires a JobId argument
			// it then cancels all parts of the specified job

			// If no argument is passed then it is not valid
			if len(args) != 1 {
				return errors.New("this command requires only a jobID")
			}
			raw.jobID = args[0]
			return nil
		},
		Run: func(cmd *cobra.Command, args []string) {
			cooked, err := raw.cook()
			if err != nil {
				glcm.Error("failed to parse user input due to error " + err.Error())
			}

			err = cooked.process()
			if err != nil {
				glcm.Error("failed to perform cancel command due to error " + err.Error())
			}
			glcm.Exit(nil, EExitCode.Success())
		},
		// hide features not relevant to BFS
		// TODO remove after preview release.
		Hidden: true,
	}
	rootCmd.AddCommand(cancelCmd)

	cancelCmd.PersistentFlags().BoolVar(&raw.ignoreCompletedJobError, "ignore-error-if-completed", false, "")
}
