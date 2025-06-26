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
	"os"
	"path"
	"strings"

	"github.com/Azure/azure-storage-azcopy/v10/common"
	"github.com/spf13/cobra"
)

type JobsRemoveOptions struct {
	JobID common.JobID
}

func RunJobsRemove(opts JobsRemoveOptions) error {
	return handleRemoveSingleJob(opts.JobID)
}

func init() {
	commandLineInput := JobsRemoveOptions{}

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
			err := RunJobsRemove(commandLineInput)
			if err == nil {
				glcm.Exit(func(format common.OutputFormat) string {
					return fmt.Sprintf("Successfully removed log and job plan files for job %s.", commandLineInput.JobID)
				}, common.EExitCode.Success())
			} else {
				glcm.Error(fmt.Sprintf("Failed to remove log and job plan files for job %s due to error: %s.", commandLineInput.JobID, err))
			}
		},
	}

	jobsCmd.AddCommand(jobsRemoveCmd)
}

func handleRemoveSingleJob(jobID common.JobID) error {
	// get rid of the job plan files
	numPlanFileRemoved, err := removeFilesWithPredicate(common.AzcopyJobPlanFolder, func(s string) bool {
		if strings.Contains(s, jobID.String()) && strings.Contains(s, ".steV") {
			return true
		}
		return false
	})
	if err != nil {
		return err
	}

	// get rid of the logs
	// even though we only have 1 file right now, still scan the directory since we may change the
	// way we name the logs in the future (with suffix or whatnot)
	numLogFileRemoved, err := removeFilesWithPredicate(azcopyLogPathFolder, func(s string) bool {
		if strings.Contains(s, jobID.String()) && strings.HasSuffix(s, ".log") {
			return true
		}
		return false
	})
	if err != nil {
		return err
	}

	if numLogFileRemoved+numPlanFileRemoved == 0 {
		return errors.New("cannot find any log or job plan file with the specified ID")
	}

	return nil
}

// remove all files whose names are approved by the predicate in the targetFolder
func removeFilesWithPredicate(targetFolder string, predicate func(string) bool) (int, error) {
	count := 0
	files, err := os.ReadDir(targetFolder)
	if err != nil {
		return count, err
	}

	// go through the files and return if any of them fail to be removed
	for _, singleFile := range files {
		if predicate(singleFile.Name()) {
			err := os.Remove(path.Join(targetFolder, singleFile.Name()))
			if err != nil {
				return count, err
			}
			count += 1
		}
	}

	return count, nil
}
