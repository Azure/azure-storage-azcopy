// Copyright © Microsoft <wastore@microsoft.com>
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
	"github.com/spf13/cobra"
)

func init() {
	type JobsRemoveReq struct {
		JobID common.JobID
	}

	commandLineInput := JobsRemoveReq{}

	// remove a single job's log and plan file
	jobsRemoveCmd := &cobra.Command{
		Use:     "remove [jobID]",
		Aliases: []string{"rm"},
		Short:   removeJobsCmdShortDescription,
		Long:    removeJobsCmdLongDescription,
		Example: removeJobsCmdExample,
		Args: func(cmd *cobra.Command, args []string) error {
			if len(args) != 1 {
				return errors.New("remove job command requires the JobID")
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
			_, err := Client.RemoveJob(azcopy.RemoveJobOptions{JobID: commandLineInput.JobID})
			if err == nil {
				glcm.Exit(func(format common.OutputFormat) string {
					return fmt.Sprintf("Successfully removed log and job plan files for job %s.", commandLineInput.JobID)
				}, common.EExitCode.Success())
			} else {
				glcm.Error(err.Error())
			}
		},
	}

	jobsCmd.AddCommand(jobsRemoveCmd)
}
