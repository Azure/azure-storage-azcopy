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
	"strings"
	"time"

	"github.com/Azure/azure-storage-azcopy/common"
	"github.com/Azure/azure-storage-azcopy/ste"
	"github.com/spf13/cobra"
)

// TODO the behavior of the resume command should be double-checked
// TODO ex: does it output json??
type resumeJobController struct {
	// generated
	jobID common.JobID

	// variables used to calculate progress
	// intervalStartTime holds the last time value when the progress summary was fetched
	// the value of this variable is used to calculate the throughput
	// it gets updated every time the progress summary is fetched
	intervalStartTime        time.Time
	intervalBytesTransferred uint64

	// used to calculate job summary
	jobStartTime time.Time
}

// wraps call to lifecycle manager to wait for the job to complete
// if blocking is specified to true, then this method will never return
// if blocking is specified to false, then another goroutine spawns and wait out the job
func (cca *resumeJobController) waitUntilJobCompletion(blocking bool) {
	// print initial message to indicate that the job is starting
	glcm.Info("\nJob " + cca.jobID.String() + " has started\n")
	glcm.Info(fmt.Sprintf("%s.log file created in %s", cca.jobID, azcopyAppPathFolder))
	// initialize the times necessary to track progress
	cca.jobStartTime = time.Now()
	cca.intervalStartTime = time.Now()
	cca.intervalBytesTransferred = 0

	// hand over control to the lifecycle manager if blocking
	if blocking {
		glcm.InitiateProgressReporting(cca, true)
		glcm.SurrenderControl()
	} else {
		// non-blocking, return after spawning a go routine to watch the job
		glcm.InitiateProgressReporting(cca, true)
	}
}

func (cca *resumeJobController) Cancel(lcm common.LifecycleMgr) {
	err := cookedCancelCmdArgs{jobID: cca.jobID}.process()
	if err != nil {
		lcm.Exit("error occurred while cancelling the job "+cca.jobID.String()+". Failed with error "+err.Error(), common.EExitCode.Error())
	}
}

func (cca *resumeJobController) ReportProgressOrExit(lcm common.LifecycleMgr) {
	// fetch a job status
	var summary common.ListJobSummaryResponse
	Rpc(common.ERpcCmd.ListJobSummary(), &cca.jobID, &summary)
	jobDone := summary.JobStatus == common.EJobStatus.Completed() || summary.JobStatus == common.EJobStatus.Cancelled()

	// if json is not desired, and job is done, then we generate a special end message to conclude the job
	if jobDone {
		duration := time.Now().Sub(cca.jobStartTime) // report the total run time of the job
		exitCode := common.EExitCode.Success()
		if summary.TransfersFailed > 0 {
			exitCode = common.EExitCode.Error()
		}
		lcm.Exit(fmt.Sprintf(
			"\n\nJob %s summary\nElapsed Time (Minutes): %v\nTotal Number Of Transfers: %v\nNumber of Transfers Completed: %v\nNumber of Transfers Failed: %v\nNumber of Transfers Skipped: %v\nFinal Job Status: %v\n",
			summary.JobID.String(),
			ste.ToFixed(duration.Minutes(), 4),
			summary.TotalTransfers,
			summary.TransfersCompleted,
			summary.TransfersFailed,
			summary.TransfersSkipped,
			summary.JobStatus), exitCode)
	}

	// if json is not needed, and job is not done, then we generate a message that goes nicely on the same line
	// display a scanning keyword if the job is not completely ordered
	var scanningString = ""
	if !summary.CompleteJobOrdered {
		scanningString = "(scanning...)"
	}

	// reset the interval timer and byte count
	cca.intervalStartTime = time.Now()
	cca.intervalBytesTransferred = summary.BytesOverWire

	// As there would be case when no bits sent from local, e.g. service side copy, when throughput = 0, hide it.
	progressStr := fmt.Sprintf("%v Done, %v Failed, %v Skipped, %v Pending, %v Total%s",
		summary.TransfersCompleted,
		summary.TransfersFailed,
		summary.TransfersSkipped,
		summary.TotalTransfers-(summary.TransfersCompleted+summary.TransfersFailed),
		summary.TotalTransfers,
		scanningString)
	glcm.Progress(progressStr)
}

func init() {
	resumeCmdArgs := resumeCmdArgs{}

	// resumeCmd represents the resume command
	resumeCmd := &cobra.Command{
		Use:        "resume [jobID]",
		SuggestFor: []string{"resme", "esume", "resue"},
		Short:      "Resume the existing job with the given job ID",
		Long: `
Resume the existing job with the given job ID.`,
		Args: func(cmd *cobra.Command, args []string) error {
			// the resume command requires necessarily to have an argument
			// resume jobId -- resumes all the parts of an existing job for given jobId

			// If no argument is passed then it is not valid
			if len(args) != 1 {
				return errors.New("this command requires jobId to be passed as argument")
			}
			resumeCmdArgs.jobID = args[0]
			return nil
		},
		Run: func(cmd *cobra.Command, args []string) {
			err := resumeCmdArgs.process()
			if err != nil {
				glcm.Exit(fmt.Sprintf("failed to perform resume command due to error: %s", err.Error()), common.EExitCode.Error())
			}
		},
	}

	jobsCmd.AddCommand(resumeCmd)
	resumeCmd.PersistentFlags().StringVar(&resumeCmdArgs.includeTransfer, "include", "", "Filter: only include these failed transfer(s) when resuming the job. "+
		"Files should be separated by ';'.")
	resumeCmd.PersistentFlags().StringVar(&resumeCmdArgs.excludeTransfer, "exclude", "", "Filter: exclude these failed transfer(s) when resuming the job. "+
		"Files should be separated by ';'.")
	// oauth options
	resumeCmd.PersistentFlags().StringVar(&resumeCmdArgs.SourceSAS, "source-sas", "", "source sas of the source for given JobId")
	resumeCmd.PersistentFlags().StringVar(&resumeCmdArgs.DestinationSAS, "destination-sas", "", "destination sas of the destination for given JobId")
}

type resumeCmdArgs struct {
	jobID           string
	includeTransfer string
	excludeTransfer string

	SourceSAS      string
	DestinationSAS string
}

// getDestCredentialType gets the proper credential type for job resume command's copy destination.
func (rca resumeCmdArgs) getDestCredentialType() (credentialType common.CredentialType, err error) {
	// If SAS provided for destination, it's using Anonymous credential.
	if rca.DestinationSAS != "" {
		return common.ECredentialType.Anonymous(), nil
	}

	uotm := GetUserOAuthTokenManagerInstance()
	// If there is cached token or token passed from env var, it's using OAuthToken credential.
	if hasCachedToken, _ := uotm.HasCachedToken(); hasCachedToken || common.EnvVarOAuthTokenInfoExists() {
		return common.ECredentialType.OAuthToken(), nil
	}

	// TODO: Note BFS's case is special, SharedKey should be removed.

	// By default, use Anonymous credential.
	return common.ECredentialType.Anonymous(), nil
}

// processes the resume command,
// dispatches the resume Job order to the storage engine.
func (rca resumeCmdArgs) process() error {
	// parsing the given JobId to validate its format correctness
	jobID, err := common.ParseJobID(rca.jobID)
	if err != nil {
		// If parsing gives an error, hence it is not a valid JobId format
		return fmt.Errorf("error parsing the jobId %s. Failed with error %s", rca.jobID, err.Error())
	}

	includeTransfer := make(map[string]int)
	excludeTransfer := make(map[string]int)

	// If the transfer has been provided with the include, parse the transfer list.
	if len(rca.includeTransfer) > 0 {
		// Split the Include Transfer using ';'
		transfers := strings.Split(rca.includeTransfer, ";")
		for index := range transfers {
			if len(transfers[index]) == 0 {
				// If the transfer provided is empty
				// skip the transfer
				// This is to handle the misplaced ';'
				continue
			}
			includeTransfer[transfers[index]] = index
		}
	}
	// If the transfer has been provided with the exclude, parse the transfer list.
	if len(rca.excludeTransfer) > 0 {
		// Split the Exclude Transfer using ';'
		transfers := strings.Split(rca.excludeTransfer, ";")
		for index := range transfers {
			if len(transfers[index]) == 0 {
				// If the transfer provided is empty
				// skip the transfer
				// This is to handle the misplaced ';'
				continue
			}
			excludeTransfer[transfers[index]] = index
		}
	}

	// Initialize credential info.
	credentialInfo := common.CredentialInfo{}
	if credentialInfo.CredentialType, err = rca.getDestCredentialType(); err != nil {
		return err
	} else if credentialInfo.CredentialType == common.ECredentialType.OAuthToken() {
		// Message user that they are using Oauth token for authentication,
		// in case of silently using cached token without consciousness。
		glcm.Info("Resume is using OAuth token for authentication.")

		uotm := GetUserOAuthTokenManagerInstance()
		// Get token from env var or cache.
		if tokenInfo, err := uotm.GetTokenInfo(); err != nil {
			return err
		} else {
			credentialInfo.OAuthTokenInfo = *tokenInfo
		}
	}

	// Send resume job request.
	var resumeJobResponse common.CancelPauseResumeResponse
	Rpc(common.ERpcCmd.ResumeJob(),
		&common.ResumeJobRequest{
			JobID:           jobID,
			SourceSAS:       rca.SourceSAS,
			DestinationSAS:  rca.DestinationSAS,
			CredentialInfo:  credentialInfo,
			IncludeTransfer: includeTransfer,
			ExcludeTransfer: excludeTransfer,
		},
		&resumeJobResponse)

	if !resumeJobResponse.CancelledPauseResumed {
		glcm.Exit(resumeJobResponse.ErrorMsg, common.EExitCode.Error())
	}

	controller := resumeJobController{jobID: jobID}
	controller.waitUntilJobCompletion(true)

	return nil
}
