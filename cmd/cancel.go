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
	"bufio"
	"os"
	"strings"
)

// created a signal channel to receive the Interrupt and Kill signal send to OS
// this channel is shared by copy, resume, sync and an independent go routine reading stdin
// for cancel command
var cancelChannel = make(chan os.Signal, 1)

type rawCancelCmdArgs struct {
	jobID string
}

func (raw rawCancelCmdArgs) cook() (cookedCancelCmdArgs, error) {
	//parsing the given JobId to validate its format correctness
	jobID, err := common.ParseJobID(raw.jobID)
	if err != nil {
		// If parsing gives an error, hence it is not a valid JobId format
		return cookedCancelCmdArgs{}, fmt.Errorf("invalid jobId string passed: %q", raw.jobID)
	}

	return cookedCancelCmdArgs{jobID: jobID}, nil
}

type cookedCancelCmdArgs struct {
	jobID common.JobID
}

// handles the cancel command
// dispatches the cancel Job order to the storage engine
func (cca cookedCancelCmdArgs) process() error {
	var cancelJobResponse common.CancelPauseResumeResponse
	Rpc(common.ERpcCmd.CancelJob(), cca.jobID, &cancelJobResponse)
	if !cancelJobResponse.CancelledPauseResumed {
		return fmt.Errorf("job cannot be cancelled because %s", cancelJobResponse.ErrorMsg)
	}
	fmt.Println(fmt.Sprintf("Job %s cancelled successfully", cca.jobID))
	return nil
}

func init() {
	raw := rawCancelCmdArgs{}

	// cancelCmd represents the pause command
	cancelCmd := &cobra.Command{
		Use:        "cancel",
		SuggestFor: []string{"cancl", "ancl", "cacl"},
		Short:      "cancels an existing job",
		Long:       "cancels an existing job",
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
		RunE: func(cmd *cobra.Command, args []string) error {
			cooked, err := raw.cook()
			if err != nil {
				return fmt.Errorf("failed to parse user input due to error %s", err)
			}

			err = cooked.process()
			if err != nil {
				return fmt.Errorf("failed to perform copy command due to error %s", err)
			}

			return nil
		},
	}
	rootCmd.AddCommand(cancelCmd)
}

// ReadStandardInputToCancelJob is a function that reads the standard Input
// If Input given is "cancel", it cancels the current job.
func ReadStandardInputToCancelJob() {
	for {
		consoleReader := bufio.NewReader(os.Stdin)
		// ReadString reads input until the first occurrence of \n in the input,
		input, err := consoleReader.ReadString('\n')
		if err != nil {
			fmt.Println(err)
			os.Exit(1)
		}

		//remove the delimiter "\n"
		input = strings.Trim(input, "\n")
		// remove trailing white spaces
		input = strings.Trim(input, " ")
		// converting the input characters to lower case characters
		// this is done to avoid case sensitiveness.
		input = strings.ToLower(input)

		switch input {
		case "cancel":
			// send a kill signal to the cancel channel.
			cancelChannel <- os.Kill
		default:
			panic(fmt.Errorf("command %s not supported by azcopy", input))
		}
	}
}