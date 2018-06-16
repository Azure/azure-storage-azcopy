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
	"os"
	"os/signal"
	"strings"
	"time"
)

func init() {
	var commandLineInput = ""
	var includeTransfer = ""
	var excludeTransfer = ""
	rawResumeJobCommand := common.ResumeJob{}
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
			jobID, err := common.ParseJobID(commandLineInput)
			if err != nil {
				fmt.Println(fmt.Sprintf("error parsing the jobId %s. Failed with error %s", commandLineInput, err.Error()))
				os.Exit(1)
			}
			rawResumeJobCommand.JobID = jobID
			rawResumeJobCommand.IncludeTransfer = make(map[string]int)
			rawResumeJobCommand.ExcludeTransfer = make(map[string]int)

			// If the transfer has been provided with the include
			// parse the transfer list
			if len(includeTransfer) > 0 {
				// Split the Include Transfer using ';'
				transfers := strings.Split(includeTransfer, ";")
				for index := range transfers {
					if len(transfers[index]) == 0 {
						// If the transfer provided is empty
						// skip the transfer
						// This is to handle the misplaced ';'
						continue
					}
					rawResumeJobCommand.IncludeTransfer[transfers[index]] = index
				}
			}
			// If the transfer has been provided with the exclude
			// parse the transfer list
			if len(excludeTransfer) > 0 {
				// Split the Exclude Transfer using ';'
				transfers := strings.Split(excludeTransfer, ";")
				for index := range transfers {
					if len(transfers[index]) == 0 {
						// If the transfer provided is empty
						// skip the transfer
						// This is to handle the misplaced ';'
						continue
					}
					rawResumeJobCommand.ExcludeTransfer[transfers[index]] = index
				}
			}
			HandleResumeCommand(rawResumeJobCommand)
		},
	}
	rootCmd.AddCommand(resumeCmd)
	resumeCmd.PersistentFlags().StringVar(&includeTransfer, "include", "", "Filter: only include these failed transfer will be resumed while resuming the job "+
		"More than one file are separated by ';'")
	resumeCmd.PersistentFlags().StringVar(&excludeTransfer, "exclude", "", "Filter: exclude these failed transfer while resuming the job "+
		"More than one file are separated by ';'")
}

func waitUntilJobCompletion(jobID common.JobID) {

	// CancelChannel will be notified when os receives os.Interrupt and os.Kill signals
	signal.Notify(CancelChannel, os.Interrupt, os.Kill)

	// added an empty to provide a gap between the user given command and progress
	fmt.Println("")

	// waiting for signals from either CancelChannel or timeOut Channel.
	// if no signal received, will fetch/display a job status update then sleep for a bit
	startTime := time.Now()
	bytesTransferredInLastInterval := uint64(0)
	for {
		select {
		case <-CancelChannel:
			//fmt.Println("Cancelling Job")
			err := cookedCancelCmdArgs{jobID: jobID}.process()
			if err != nil {
				fmt.Println(fmt.Sprintf("error occurred while cancelling the job %s. Failed with error %s", jobID, err.Error()))
				os.Exit(1)
			}
		default:
			summary := copyHandlerUtil{}.fetchJobStatus(jobID, &startTime, &bytesTransferredInLastInterval, false)
			// happy ending to the front end
			if summary.JobStatus == common.EJobStatus.Completed() || summary.JobStatus == common.EJobStatus.Cancelled() {
				copyHandlerUtil{}.PrintFinalJobProgressSummary(summary)
				os.Exit(0)
			}

			// wait a bit before fetching job status again, as fetching has costs associated with it on the backend
			//time.Sleep(2 * time.Second)
			time.Sleep(2 * time.Second)
		}
	}
}

// handles the resume command
// dispatches the resume Job order to the storage engine
func HandleResumeCommand(resJobOrder common.ResumeJob) {

	var resumeJobResponse common.CancelPauseResumeResponse
	Rpc(common.ERpcCmd.ResumeJob(), resJobOrder, &resumeJobResponse)
	if !resumeJobResponse.CancelledPauseResumed {
		fmt.Println(resumeJobResponse.ErrorMsg)
		return
	}
	waitUntilJobCompletion(resJobOrder.JobID)
}
