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
	"github.com/spf13/cobra"
	"github.com/Azure/azure-storage-azcopy/common"
	"fmt"
	"encoding/json"
)

func init() {
	var commandLineInput = ""

	// resumeCmd represents the resume command
	resumeCmd := &cobra.Command{
		Use:        "resume",
		SuggestFor: []string{"resme", "esume", "resue"},
		Short:      "resume resumes the existing job for given JobId.",
		Long:       `resume resumes the existing job for given JobId.`,
		Args: func(cmd *cobra.Command, args []string) error {
			// the resume command requires necessarily to have an argument
			// resume jobId -- resumes all the parts of an existing job for given jobId

			// If no argument is passed then it is not valid
			if len(args) != 1 {
				return errors.New("this command only requires jobId")
			}
			commandLineInput = args[0]
			return nil
		},
		Run: func(cmd *cobra.Command, args []string) {
			HandleResumeCommand(commandLineInput)
		},
	}
	rootCmd.AddCommand(resumeCmd)
}

// handles the resume command
// dispatches the resume Job order to the storage engine
func HandleResumeCommand(jobIdString string) {
	// parsing the given JobId to validate its format correctness
	jobId, err := common.ParseUUID(jobIdString)
	if err != nil {
		// If parsing gives an error, hence it is not a valid JobId format
		fmt.Println("invalid jobId string passed. Failed while parsing string to jobId")
		return
	}

	responseBytes, _ := common.Rpc("resume", jobId)
	var resumeJobResponse common.CancelPauseResumeResponse

	err = json.Unmarshal(responseBytes, &resumeJobResponse)
	if err != nil {
		panic(err)
	}
	if !resumeJobResponse.CancelledPauseResumed {
		fmt.Println(fmt.Sprintf("job cannot be resumed because %s", resumeJobResponse.ErrorMsg))
		return
	}
	fmt.Println(fmt.Sprintf("Job %s resume successfully", jobId))
}
