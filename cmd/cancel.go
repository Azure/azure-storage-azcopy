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
	"github.com/Azure/azure-storage-azcopy/common"
	"github.com/spf13/cobra"
)

func init() {
	var commandLineInput = ""

	// cancelCmd represents the pause command
	cancelCmd := &cobra.Command{
		Use:        "cancel",
		SuggestFor: []string{"cancl", "ancl", "cacl"},
		Short:      "cancels an existing job",
		Long:       "cancels an existing job",
		Args: func(cmd *cobra.Command, args []string) error {
			// the cancel command requires a JobId argument;
			// it then cancels all parts of the specified job.

			// If no argument is passed then it is not valid
			if len(args) != 1 {
				return errors.New("this command only requires jobId")
			}
			commandLineInput = args[0]
			return nil
		},
		Run: func(cmd *cobra.Command, args []string) {
			HandleCancelCommand(commandLineInput)
		},
	}
	rootCmd.AddCommand(cancelCmd)
}

// handles the cancel command
// dispatches the cancel Job order to the storage engine
func HandleCancelCommand(jobIdString string) {
	// parsing the given JobId to validate its format correctness
	jobID, err := common.ParseUUID(jobIdString)
	if err != nil {
		// If parsing gives an error, hence it is not a valid JobId format
		fmt.Println("invalid jobId string passed. Failed while parsing string to jobID")
		return
	}

	var cancelJobResponse common.CancelPauseResumeResponse
	if err := Rpc((common.RpcCmd{}).CancelJob(), jobID, &cancelJobResponse); err != nil {
		panic(err)
	}
	if !cancelJobResponse.CancelledPauseResumed {
		fmt.Println(fmt.Sprintf("job cannot be cancelled because %s", cancelJobResponse.ErrorMsg))
		return
	}
	fmt.Println(fmt.Sprintf("Job %s cancelled successfully", jobID))
}
