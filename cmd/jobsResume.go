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
		lcm.ExitWithError("error occurred while cancelling the job "+cca.jobID.String()+". Failed with error "+err.Error(), common.EExitCode.Error())
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

		lcm.ExitWithSuccess(fmt.Sprintf(
			"\n\nJob %s summary\nElapsed Time (Minutes): %v\nTotal Number Of Transfers: %v\nNumber of Transfers Completed: %v\nNumber of Transfers Failed: %v\nFinal Job Status: %v\n",
			summary.JobID.String(),
			ste.ToFixed(duration.Minutes(), 4),
			summary.TotalTransfers,
			summary.TransfersCompleted,
			summary.TransfersFailed,
			summary.JobStatus), common.EExitCode.Success())
	}

	// if json is not needed, and job is not done, then we generate a message that goes nicely on the same line
	// display a scanning keyword if the job is not completely ordered
	var scanningString = ""
	if !summary.CompleteJobOrdered {
		scanningString = "(scanning...)"
	}

	// compute the average throughput for the last time interval
	bytesInMB := float64(float64(summary.BytesOverWire-cca.intervalBytesTransferred) / float64(1024*1024))
	timeElapsed := time.Since(cca.intervalStartTime).Seconds()
	throughPut := common.Iffloat64(timeElapsed != 0, bytesInMB/timeElapsed, 0)

	// reset the interval timer and byte count
	cca.intervalStartTime = time.Now()
	cca.intervalBytesTransferred = summary.BytesOverWire

	// As there would be case when no bits sent from local, e.g. service side copy, when throughput = 0, hide it.
	progressStr := fmt.Sprintf("%v Done, %v Failed, %v Pending, %v Total%s",
		summary.TransfersCompleted,
		summary.TransfersFailed,
		summary.TotalTransfers-(summary.TransfersCompleted+summary.TransfersFailed),
		summary.TotalTransfers,
		scanningString)
	if throughPut != 0 {
		progressStr = fmt.Sprintf("%s, 2-sec Throughput (MB/s): %v", progressStr, ste.ToFixed(throughPut, 4))
	}

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
				glcm.ExitWithError(fmt.Sprintf("failed to perform resume command due to error: %s", err.Error()), common.EExitCode.Error())
			}
		},
	}

	jobsCmd.AddCommand(resumeCmd)
	resumeCmd.PersistentFlags().StringVar(&resumeCmdArgs.includeTransfer, "include", "", "Filter: only include these failed transfer(s) when resuming the job. "+
		"Files should be separated by ';'.")
	resumeCmd.PersistentFlags().StringVar(&resumeCmdArgs.excludeTransfer, "exclude", "", "Filter: exclude these failed transfer(s) when resuming the job. "+
		"Files should be separated by ';'.")
	// oauth options
	resumeCmd.PersistentFlags().BoolVar(&resumeCmdArgs.useInteractiveOAuthUserCredential, "oauth-user", false, "Use OAuth user credential and do interactive login.")
	resumeCmd.PersistentFlags().StringVar(&resumeCmdArgs.tenantID, "tenant-id", common.DefaultTenantID, "Tenant id to use for OAuth user interactive login.")
	resumeCmd.PersistentFlags().StringVar(&resumeCmdArgs.aadEndpoint, "aad-endpoint", common.DefaultActiveDirectoryEndpoint, "Azure active directory endpoint to use for OAuth user interactive login.")
	resumeCmd.PersistentFlags().StringVar(&resumeCmdArgs.SourceSAS, "source-sas", "", "source sas of the source for given JobId")
	resumeCmd.PersistentFlags().StringVar(&resumeCmdArgs.DestinationSAS, "destination-sas", "", "destination sas of the destination for given JobId")
}

type resumeCmdArgs struct {
	jobID           string
	includeTransfer string
	excludeTransfer string

	// oauth options
	useInteractiveOAuthUserCredential bool
	tenantID                          string
	aadEndpoint                       string

	SourceSAS      string
	DestinationSAS string
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
	credentialInfo := common.CredentialInfo{
		CredentialType: common.ECredentialType.Anonymous(),
	}
	// Check whether to use OAuthToken credential.
	// Scenario-1: interactive login per copy command
	// Scenario-Test: unattended testing with oauthTokenInfo set through environment variable
	// Scenario-2: session mode which get token from cache
	uotm := GetUserOAuthTokenManagerInstance()
	hasCachedToken, err := uotm.HasCachedToken()
	if rca.useInteractiveOAuthUserCredential || common.EnvVarOAuthTokenInfoExists() || hasCachedToken {
		credentialInfo.CredentialType = common.ECredentialType.OAuthToken()
		var oAuthTokenInfo *common.OAuthTokenInfo
		// For Scenario-1, create token with interactive login if necessary.
		if rca.useInteractiveOAuthUserCredential {
			oAuthTokenInfo, err = uotm.LoginWithADEndpoint(rca.tenantID, rca.aadEndpoint, false)
			if err != nil {
				return fmt.Errorf(
					"login failed with tenantID %q, using public Azure directory endpoint 'https://login.microsoftonline.com', due to error: %s",
					rca.tenantID,
					err.Error())
			}
		} else if oAuthTokenInfo, err = uotm.GetTokenInfoFromEnvVar(); err == nil || !common.IsErrorEnvVarOAuthTokenInfoNotSet(err) {
			// Scenario-Test
			glcm.Info(fmt.Sprintf("%v is set.", common.EnvVarOAuthTokenInfo))
			if err != nil { // this is the case when env var exists while get token info failed
				return err
			}
		} else { // Scenario-2
			oAuthTokenInfo, err = uotm.GetCachedTokenInfo()
			if err != nil {
				return err
			}
		}
		if oAuthTokenInfo == nil {
			return errors.New("cannot get valid oauth token")
		}
		credentialInfo.OAuthTokenInfo = *oAuthTokenInfo
	}
	//glcm.Info(fmt.Sprintf("Resume uses credential type %q.\n", credentialInfo.CredentialType))

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
		glcm.ExitWithError(resumeJobResponse.ErrorMsg, common.EExitCode.Error())
	}

	controller := resumeJobController{jobID: jobID}
	controller.waitUntilJobCompletion(true)

	return nil
}
