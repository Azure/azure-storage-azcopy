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
	"fmt"
	"github.com/spf13/cobra"
	"github.com/Azure/azure-storage-azcopy/handlers"
	"github.com/Azure/azure-storage-azcopy/common"
)

// TODO check file size, max is 4.75TB
func init() {
	commandLineInput := common.ListCmdArgsAndFlags{}

	// lsCmd represents the ls command
	lsCmd := &cobra.Command{
		Use:   "list",
		Aliases: []string{"ls"},
		SuggestFor: []string{"lst", "lt", "ist"}, //TODO why does message appear twice on the console
		Short: "list(ls) lists the specifics of an existing job.",
		Long: `list(ls) lists the specifics of an existing job. The most common cases are:
  - lists all the existing jobs.
  - lists all the part numbers of an existing jobs.
  - lists all the transfers of an existing job.`,
		Args: func(cmd *cobra.Command, args []string) error {
			// the list command does not require necessarily to have an argument
			/*
			* list -- lists all the existing jobs
			* list jobId -- lists all the part numbers of an existing job
			* list jobId partNum -- lists all the transfers of an existing job part order
			*/
			jobId := ""
			partNum := ""

            if len(args) > 0{
            	jobId = args[0]
			}
			if len(args) > 1{
				partNum = args[1]
			}

			commandLineInput.JobId = jobId
			commandLineInput.PartNum = partNum
			return nil
		},
		Run: func(cmd *cobra.Command, args []string) {
			fmt.Println("copy job starting: ")
			handlers.HandleListCommand(commandLineInput)
			//fmt.Println("Job with id", jobId, "has started.")

			//// wait until job finishes
			//time.Sleep(600 * time.Second)
		},
	}

	rootCmd.AddCommand(lsCmd)

	// define the flags relevant to the ls command

	// filters
	lsCmd.PersistentFlags().BoolVar(&commandLineInput.ListOnlyActiveJobs, "listOnlyActiveJobs", false, "Filter: lists specifics of only active jobs.")
	lsCmd.PersistentFlags().BoolVar(&commandLineInput.ListOnlyActiveTransfers, "listOnlyActiveTransfers", false, "Filter: lists only active transfers of a job.")
	lsCmd.PersistentFlags().BoolVar(&commandLineInput.ListOnlyCompletedTransfers, "listOnlyCompletedTransfers", false, "Filter: lists only completed transfers of a job.")
	lsCmd.PersistentFlags().BoolVar(&commandLineInput.ListOnlyFailedTransfers, "listOnlyFailedTransfers", false, "Filter: lists only failed transfers of a job .")
}

