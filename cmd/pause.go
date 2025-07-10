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
	"github.com/Azure/azure-storage-azcopy/v10/jobsAdmin"

	"github.com/Azure/azure-storage-azcopy/v10/common"
	"github.com/spf13/cobra"
)

// TODO should this command be removed? Previously AzCopy was supposed to have an independent backend (out of proc)
// TODO but that's not the plan anymore
func init() {
	var commandLineInput = ""

	// pauseCmd represents the pause command
	pauseCmd := &cobra.Command{
		Use:        "pause",
		SuggestFor: []string{"pase", "ause", "paue"},
		Short:      "Pause the existing job with the given Job Id",
		Long:       `Pause the existing job with the given Job Id`,
		Args: func(cmd *cobra.Command, args []string) error {
			// the pause command requires necessarily to have an argument
			// pause jobId -- pause all the parts of an existing job for given jobId

			// If no argument is passed then it is not valid
			if len(args) != 1 {
				return errors.New("this command only requires jobId")
			}
			commandLineInput = (args[0])
			return nil
		},
		Run: func(cmd *cobra.Command, args []string) {
			HandlePauseCommand(commandLineInput)
			glcm.Exit(nil, common.EExitCode.Success())
		},
		// hide features not relevant to BFS
		// TODO remove after preview release
		Hidden: true,
	}
	rootCmd.AddCommand(pauseCmd)
}

// handles the pause command
// dispatches the pause Job order to the storage engine
func HandlePauseCommand(jobIdString string) {

	// parsing the given JobId to validate its format correctness
	jobID, err := common.ParseJobID(jobIdString)
	if err != nil {
		// If parsing gives an error, hence it is not a valid JobId format
		glcm.Error("invalid jobId string passed. Failed while parsing string to jobId")
	}

	// TODO : Why isnt the response here used?
	jobsAdmin.CancelPauseJobOrder(jobID, common.EJobStatus.Paused())
	glcm.Exit(func(format common.OutputFormat) string {
		return "Job " + jobID.String() + " paused successfully"
	}, common.EExitCode.Success())
}
