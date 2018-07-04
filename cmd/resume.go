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
	"os"
	"os/signal"
	"strings"
	"time"

	"github.com/Azure/azure-storage-azcopy/common"
	"github.com/spf13/cobra"
)

func init() {
	resumeCmdArgs := resumeCmdArgs{}

	// resumeCmd represents the resume command
	resumeCmd := &cobra.Command{
		Use:        "resume jobID",
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
		RunE: func(cmd *cobra.Command, args []string) error {
			err := resumeCmdArgs.process()
			if err != nil {
				return fmt.Errorf("failed to perform resume command due to error: %s", err.Error())
			}

			return nil
		},
	}

	rootCmd.AddCommand(resumeCmd)
	resumeCmd.PersistentFlags().StringVar(&resumeCmdArgs.includeTransfer, "include", "", "Filter: only include these failed transfer(s) when resuming the job. "+
		"Files should be separated by ';'.")
	resumeCmd.PersistentFlags().StringVar(&resumeCmdArgs.excludeTransfer, "exclude", "", "Filter: exclude these failed transfer(s) when resuming the job. "+
		"Files should be separated by ';'.")
	// oauth options
	resumeCmd.PersistentFlags().BoolVar(&resumeCmdArgs.useInteractiveOAuthUserCredential, "oauth-user", false, "Use OAuth user credential and do interactive login.")
	resumeCmd.PersistentFlags().StringVar(&resumeCmdArgs.tenantID, "tenant-id", common.DefaultTenantID, "Tenant id to use for OAuth user interactive login.")
	resumeCmd.PersistentFlags().StringVar(&resumeCmdArgs.aadEndpoint, "aad-endpoint", common.DefaultActiveDirectoryEndpoint, "Azure active directory endpoint to use for OAuth user interactive login.")
}

type resumeCmdArgs struct {
	jobID           string
	includeTransfer string
	excludeTransfer string

	// oauth options
	useInteractiveOAuthUserCredential bool
	tenantID                          string
	aadEndpoint                       string
}

func waitUntilJobCompletion(jobID common.JobID) {

	// CancelChannel will be notified when os receives os.Interrupt and os.Kill signals
	// waiting for signals from either CancelChannel or timeOut Channel.
	// if no signal received, will fetch/display a job status update then sleep for a bit
	signal.Notify(CancelChannel, os.Interrupt, os.Kill)

	// added an empty to provide a gap between the user given command and progress
	fmt.Println("")

	// throughputIntervalTime holds the last time value when the progress summary was fetched
	// The value of this variable is used to calculate the throughput
	// It gets updated every time the progress summary is fetched
	throughputIntervalTime := time.Now()
	// jobStartTime holds the time when Job was started
	// The value of this variable is used to calculate the elapsed time
	jobStartTime := throughputIntervalTime
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
			summary := copyHandlerUtil{}.fetchJobStatus(jobID, &throughputIntervalTime, &bytesTransferredInLastInterval, false)
			// happy ending to the front end
			if summary.JobStatus == common.EJobStatus.Completed() || summary.JobStatus == common.EJobStatus.Cancelled() {
				copyHandlerUtil{}.PrintFinalJobProgressSummary(summary, time.Now().Sub(jobStartTime))
				os.Exit(0)
			}

			// wait a bit before fetching job status again, as fetching has costs associated with it on the backend
			//time.Sleep(2 * time.Second)
			time.Sleep(2 * time.Second)
		}
	}
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
		} else if tokenInfo, err = uotm.GetTokenInfoFromEnvVar(); err == nil || !common.IsErrorEnvVarOAuthTokenInfoNotSet(err) {
			// Scenario-Test
			fmt.Printf("%v is set.\n", common.EnvVarOAuthTokenInfo) // TODO: Do logging what's the source of OAuth token with FE logging facilities.
			if err != nil {                                         // this is the case when env var exists while get token info failed
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
	fmt.Printf("Resume uses credential type %q.\n", credentialInfo.CredentialType) // TODO: use FE logging facility

	// Send resume job request.
	var resumeJobResponse common.CancelPauseResumeResponse
	Rpc(common.ERpcCmd.ResumeJob(),
		&common.ResumeJobRequest{
			JobID:           jobID,
			CredentialInfo:  credentialInfo,
			IncludeTransfer: includeTransfer,
			ExcludeTransfer: excludeTransfer,
		},
		&resumeJobResponse)

	if !resumeJobResponse.CancelledPauseResumed {
		return fmt.Errorf("resume failed due to error: %s", resumeJobResponse.ErrorMsg)
	}

	waitUntilJobCompletion(jobID)

	return nil
}
