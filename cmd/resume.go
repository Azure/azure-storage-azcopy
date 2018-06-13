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
	resumeCmd.PersistentFlags().StringVar(&resumeCmdArgs.includeTransfer, "include", "", "Filter: only include these failed transfer will be resumed while resuming the job "+
		"More than one file are separated by ';'")
	resumeCmd.PersistentFlags().StringVar(&resumeCmdArgs.excludeTransfer, "exclude", "", "Filter: exclude these failed transfer while resuming the job "+
		"More than one file are separated by ';'")
	// oauth options
	resumeCmd.PersistentFlags().BoolVar(&resumeCmdArgs.useInteractiveOAuthUserCredential, "oauth-user", false, "Use OAuth user credential and do interactive login.")
	resumeCmd.PersistentFlags().StringVar(&resumeCmdArgs.tenantID, "tenant-id", common.DefaultTenantID, "Tenant id to use for OAuth user interactive login.")
}

type resumeCmdArgs struct {
	jobID           string
	includeTransfer string
	excludeTransfer string

	// oauth options
	useInteractiveOAuthUserCredential bool
	tenantID                          string
}

func waitUntilJobCompletion(jobID common.JobID) {

	// CancelChannel will be notified when os receives os.Interrupt and os.Kill signals
	signal.Notify(CancelChannel, os.Interrupt, os.Kill)

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
			jobStatus := copyHandlerUtil{}.fetchJobStatus(jobID, &startTime, &bytesTransferredInLastInterval, false)

			// happy ending to the front end
			if jobStatus == common.EJobStatus.Completed() || jobStatus == common.EJobStatus.Cancelled() {
				os.Exit(0)
			}

			// wait a bit before fetching job status again, as fetching has costs associated with it on the backend
			//time.Sleep(2 * time.Second)
			time.Sleep(500 * time.Millisecond)
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

	// Check oauth related commandline switch, and create token with interactive login if necessary.
	credentialInfo := common.CredentialInfo{}
	if rca.useInteractiveOAuthUserCredential {
		credentialInfo.CredentialType = common.ECredentialType.OAuthToken()

		userOAuthTokenManager := GetUserOAuthTokenManagerInstance()
		oAuthTokenInfo, err := userOAuthTokenManager.LoginWithDefaultADEndpoint(rca.tenantID, false)
		if err != nil {
			return fmt.Errorf(
				"login failed with tenantID '%s', using public Azure directory endpoint 'https://login.microsoftonline.com', due to error: %s",
				rca.tenantID,
				err.Error())
		}
		credentialInfo.OAuthTokenInfo = *oAuthTokenInfo
	} else {
		credentialInfo.CredentialType = common.ECredentialType.Anonymous()
	}

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
