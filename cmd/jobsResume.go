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
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/Azure/azure-sdk-for-go/sdk/azcore"
	"github.com/Azure/azure-storage-azcopy/v10/jobsAdmin"

	"github.com/Azure/azure-storage-azcopy/v10/common"
	"github.com/Azure/azure-storage-azcopy/v10/ste"
	"github.com/spf13/cobra"
)

// TODO the behavior of the resume command should be double-checked
// TODO figure out how to merge resume job with copy
// TODO the progress reporting code is almost the same as the copy command, the copy-paste should be avoided
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
	// Output the log location if log-level is set to other then NONE
	var logPathFolder string
	if azcopyLogPathFolder != "" {
		logPathFolder = fmt.Sprintf("%s%s%s.log", azcopyLogPathFolder, common.OS_PATH_SEPARATOR, cca.jobID)
	}
	glcm.Init(common.GetStandardInitOutputBuilder(cca.jobID.String(), logPathFolder, false, ""))

	// initialize the times necessary to track progress
	cca.jobStartTime = time.Now()
	cca.intervalStartTime = time.Now()
	cca.intervalBytesTransferred = 0

	// hand over control to the lifecycle manager if blocking
	if blocking {
		glcm.InitiateProgressReporting(cca)
		glcm.SurrenderControl()
	} else {
		// non-blocking, return after spawning a go routine to watch the job
		glcm.InitiateProgressReporting(cca)
	}
}

func (cca *resumeJobController) Cancel(lcm common.LifecycleMgr) {
	err := cookedCancelCmdArgs{jobID: cca.jobID}.process()
	if err != nil {
		lcm.Error("error occurred while cancelling the job " + cca.jobID.String() + ". Failed with error " + err.Error())
	}
}

// TODO: can we combine this with the copy one (and the sync one?)
func (cca *resumeJobController) ReportProgressOrExit(lcm common.LifecycleMgr) (totalKnownCount uint32) {
	// fetch a job status
	var summary common.ListJobSummaryResponse
	Rpc(common.ERpcCmd.ListJobSummary(), &cca.jobID, &summary)
	glcmSwapOnce.Do(func() {
		Rpc(common.ERpcCmd.GetJobLCMWrapper(), &cca.jobID, &glcm)
	})
	jobDone := summary.JobStatus.IsJobDone()
	totalKnownCount = summary.TotalTransfers

	// if json is not desired, and job is done, then we generate a special end message to conclude the job
	duration := time.Since(cca.jobStartTime) // report the total run time of the job

	var computeThroughput = func() float64 {
		// compute the average throughput for the last time interval
		bytesInMb := float64(float64(summary.BytesOverWire-cca.intervalBytesTransferred) / float64(base10Mega))
		timeElapsed := time.Since(cca.intervalStartTime).Seconds()

		// reset the interval timer and byte count
		cca.intervalStartTime = time.Now()
		cca.intervalBytesTransferred = summary.BytesOverWire

		return common.Iff(timeElapsed != 0, bytesInMb/timeElapsed, 0) * 8
	}

	glcm.Progress(func(format common.OutputFormat) string {
		if format == common.EOutputFormat.Json() {
			jsonOutput, err := json.Marshal(summary)
			common.PanicIfErr(err)
			return string(jsonOutput)
		} else {
			// if json is not needed, then we generate a message that goes nicely on the same line
			// display a scanning keyword if the job is not completely ordered
			var scanningString = " (scanning...)"
			if summary.CompleteJobOrdered {
				scanningString = ""
			}

			throughput := computeThroughput()
			throughputString := fmt.Sprintf("2-sec Throughput (Mb/s): %v", jobsAdmin.ToFixed(throughput, 4))
			if throughput == 0 {
				// As there would be case when no bits sent from local, e.g. service side copy, when throughput = 0, hide it.
				throughputString = ""
			}

			// indicate whether constrained by disk or not
			perfString, diskString := getPerfDisplayText(summary.PerfStrings, summary.PerfConstraint, duration, false)

			return fmt.Sprintf("%.1f %%, %v Done, %v Failed, %v Pending, %v Skipped, %v Total%s, %s%s%s",
				summary.PercentComplete,
				summary.TransfersCompleted,
				summary.TransfersFailed,
				summary.TotalTransfers-(summary.TransfersCompleted+summary.TransfersFailed+summary.TransfersSkipped),
				summary.TransfersSkipped, summary.TotalTransfers, scanningString, perfString, throughputString, diskString)
		}
	})

	if jobDone {
		exitCode := common.EExitCode.Success()
		if summary.TransfersFailed > 0 {
			exitCode = common.EExitCode.Error()
		}

		lcm.Exit(func(format common.OutputFormat) string {
			if format == common.EOutputFormat.Json() {
				jsonOutput, err := json.Marshal(summary)
				common.PanicIfErr(err)
				return string(jsonOutput)
			} else {
				return fmt.Sprintf(
					`

Job %s summary
Elapsed Time (Minutes): %v
Number of File Transfers: %v
Number of Folder Property Transfers: %v
Number of Symlink Transfers: %v
Total Number of Transfers: %v
Number of File Transfers Completed: %v
Number of Folder Transfers Completed: %v
Number of File Transfers Failed: %v
Number of Folder Transfers Failed: %v
Number of File Transfers Skipped: %v
Number of Folder Transfers Skipped: %v
Number of Hardlinks Converted: %v
Total Number of Bytes Transferred: %v
Final Job Status: %v
`,
					summary.JobID.String(),
					jobsAdmin.ToFixed(duration.Minutes(), 4),
					summary.FileTransfers,
					summary.FolderPropertyTransfers,
					summary.SymlinkTransfers,
					summary.TotalTransfers,
					summary.TransfersCompleted-summary.FoldersCompleted,
					summary.FoldersCompleted,
					summary.TransfersFailed-summary.FoldersFailed,
					summary.FoldersFailed,
					summary.TransfersSkipped-summary.FoldersSkipped,
					summary.FoldersSkipped,
					summary.HardlinksConvertedCount,
					summary.TotalBytesTransferred,
					summary.JobStatus)
			}
		}, exitCode)
	}

	return
}

func init() {
	resumeCmdArgs := resumeCmdArgs{}

	// resumeCmd represents the resume command
	resumeCmd := &cobra.Command{
		Use:        "resume [jobID]",
		SuggestFor: []string{"resme", "esume", "resue"},
		Short:      resumeJobsCmdShortDescription,
		Long:       resumeJobsCmdLongDescription,
		Args: func(cmd *cobra.Command, args []string) error {
			// the resume command requires necessarily to have an argument
			// resume jobId -- resumes all the parts of an existing job for given jobId

			// If no argument is passed then it is not valid
			if len(args) != 1 {
				return errors.New("this command requires jobId to be passed as argument")
			}
			resumeCmdArgs.jobID = args[0]

			glcm.EnableInputWatcher()
			if cancelFromStdin {
				glcm.EnableCancelFromStdIn()
			}
			return nil
		},
		Run: func(cmd *cobra.Command, args []string) {
			err := resumeCmdArgs.process()
			if err != nil {
				glcm.Error(fmt.Sprintf("failed to perform resume command due to error: %s", err.Error()))
			}
			glcm.Exit(nil, common.EExitCode.Success())
		},
	}

	jobsCmd.AddCommand(resumeCmd)
	resumeCmd.PersistentFlags().StringVar(&resumeCmdArgs.includeTransfer, "include", "", "Filter: Include only these failed transfer(s) when resuming the job. "+
		"Files should be separated by ';'.")
	resumeCmd.PersistentFlags().StringVar(&resumeCmdArgs.excludeTransfer, "exclude", "", "Filter: Exclude these failed transfer(s) when resuming the job. "+
		"Files should be separated by ';'.")
	// oauth options
	resumeCmd.PersistentFlags().StringVar(&resumeCmdArgs.SourceSAS, "source-sas", "", "Source SAS token of the source for a given Job ID.")
	resumeCmd.PersistentFlags().StringVar(&resumeCmdArgs.DestinationSAS, "destination-sas", "", "Destination SAS token of the destination for a given Job ID.")
}

type resumeCmdArgs struct {
	jobID           string
	includeTransfer string
	excludeTransfer string

	SourceSAS      string
	DestinationSAS string
}

func (rca resumeCmdArgs) getSourceAndDestinationServiceClients(
	ctx context.Context,
	fromTo common.FromTo,
	source common.ResourceString,
	destination common.ResourceString,
) (*common.ServiceClient, *common.ServiceClient, error) {
	if len(rca.SourceSAS) > 0 && rca.SourceSAS[0] != '?' {
		rca.SourceSAS = "?" + rca.SourceSAS
	}
	if len(rca.DestinationSAS) > 0 && rca.DestinationSAS[0] != '?' {
		rca.DestinationSAS = "?" + rca.DestinationSAS
	}

	source.SAS = rca.SourceSAS
	destination.SAS = rca.DestinationSAS

	srcCredType, _, err := getCredentialTypeForLocation(ctx,
		fromTo.From(),
		source,
		true,
		common.CpkOptions{})
	if err != nil {
		return nil, nil, err
	}

	dstCredType, _, err := getCredentialTypeForLocation(ctx,
		fromTo.To(),
		destination,
		false,
		common.CpkOptions{})
	if err != nil {
		return nil, nil, err
	}

	var tc azcore.TokenCredential
	if srcCredType.IsAzureOAuth() || dstCredType.IsAzureOAuth() {
		uotm := GetUserOAuthTokenManagerInstance()
		// Get token from env var or cache.
		tokenInfo, err := uotm.GetTokenInfo(ctx)
		if err != nil {
			return nil, nil, err
		}

		tc, err = tokenInfo.GetTokenCredential()
		if err != nil {
			return nil, nil, err
		}
	}

	var reauthTok *common.ScopedAuthenticator
	if at, ok := tc.(common.AuthenticateToken); ok { // We don't need two different tokens here since it gets passed in just the same either way.
		// This will cause a reauth with StorageScope, which is fine, that's the original Authenticate call as it stands.
		reauthTok = (*common.ScopedAuthenticator)(common.NewScopedCredential(at, common.ECredentialType.OAuthToken()))
	}
	jobID, err := common.ParseJobID(rca.jobID)
	if err != nil {
		// Error for invalid JobId format
		return nil, nil, fmt.Errorf("error parsing the jobId %s. Failed with error %w", rca.jobID, err)
	}

	// But we don't want to supply a reauth token if we're not using OAuth. That could cause problems if say, a SAS is invalid.
	options := createClientOptions(common.AzcopyCurrentJobLogger, nil, common.Iff(srcCredType.IsAzureOAuth(), reauthTok, nil))
	var getJobDetailsResponse common.GetJobDetailsResponse
	// Get job details from the STE
	Rpc(common.ERpcCmd.GetJobDetails(),
		&common.GetJobDetailsRequest{JobID: jobID},
		&getJobDetailsResponse)
	if getJobDetailsResponse.ErrorMsg != "" {
		glcm.Error(getJobDetailsResponse.ErrorMsg)
	}

	var fileSrcClientOptions any
	if fromTo.From() == common.ELocation.File() {
		fileSrcClientOptions = &common.FileClientOptions{
			AllowTrailingDot: getJobDetailsResponse.TrailingDot.IsEnabled(), //Access the trailingDot option of the job
		}
	}
	srcServiceClient, err := common.GetServiceClientForLocation(fromTo.From(), source, srcCredType, tc, &options, fileSrcClientOptions)
	if err != nil {
		return nil, nil, err
	}

	var srcCred *common.ScopedToken
	if fromTo.IsS2S() && srcCredType.IsAzureOAuth() {
		srcCred = common.NewScopedCredential(tc, srcCredType)
	}
	options = createClientOptions(common.AzcopyCurrentJobLogger, srcCred, common.Iff(dstCredType.IsAzureOAuth(), reauthTok, nil))
	var fileClientOptions any
	if fromTo.To() == common.ELocation.File() {
		fileClientOptions = &common.FileClientOptions{
			AllowSourceTrailingDot: getJobDetailsResponse.TrailingDot.IsEnabled() && fromTo.From() == common.ELocation.File(),
			AllowTrailingDot:       getJobDetailsResponse.TrailingDot.IsEnabled(),
		}
	}
	dstServiceClient, err := common.GetServiceClientForLocation(fromTo.To(), destination, dstCredType, tc, &options, fileClientOptions)
	if err != nil {
		return nil, nil, err
	}
	return srcServiceClient, dstServiceClient, nil
}

// processes the resume command,
// dispatches the resume Job order to the storage engine.
func (rca resumeCmdArgs) process() error {
	// parsing the given JobId to validate its format correctness
	jobID, err := common.ParseJobID(rca.jobID)
	if err != nil {
		// If parsing gives an error, hence it is not a valid JobId format
		return fmt.Errorf("error parsing the jobId %s. Failed with error %w", rca.jobID, err)
	}

	// if no logging, set this empty so that we don't display the log location
	if azcopyLogVerbosity == common.LogNone {
		azcopyLogPathFolder = ""
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

	// Get fromTo info, so we can decide what's the proper credential type to use.
	var getJobFromToResponse common.GetJobDetailsResponse
	Rpc(common.ERpcCmd.GetJobDetails(),
		&common.GetJobDetailsRequest{JobID: jobID},
		&getJobFromToResponse)
	if getJobFromToResponse.ErrorMsg != "" {
		glcm.Error(getJobFromToResponse.ErrorMsg)
	}

	if getJobFromToResponse.FromTo.From() == common.ELocation.Benchmark() ||
		getJobFromToResponse.FromTo.To() == common.ELocation.Benchmark() {
		// Doesn't make sense to resume a benchmark job.
		// It's not tested, and wouldn't report progress correctly and wouldn't clean up after itself properly
		return errors.New("resuming benchmark jobs is not supported")
	}

	ctx := context.WithValue(context.TODO(), ste.ServiceAPIVersionOverride, ste.DefaultServiceApiVersion)
	// Initialize credential info.
	credentialInfo := common.CredentialInfo{}
	// TODO: Replace context with root context
	srcResourceString, err := SplitResourceString(getJobFromToResponse.Source, getJobFromToResponse.FromTo.From())
	_ = err // todo
	srcResourceString.SAS = rca.SourceSAS
	dstResourceString, err := SplitResourceString(getJobFromToResponse.Destination, getJobFromToResponse.FromTo.To())
	_ = err // todo
	dstResourceString.SAS = rca.DestinationSAS

	// we should stop using credentiaLInfo and use the clients instead. But before we fix
	// that there will be repeated calls to get Credential type for correctness.
	if credentialInfo.CredentialType, err = getCredentialType(ctx, rawFromToInfo{
		fromTo:      getJobFromToResponse.FromTo,
		source:      srcResourceString,
		destination: dstResourceString,
	}, common.CpkOptions{}); err != nil {
		return err
	}

	srcServiceClient, dstServiceClient, err := rca.getSourceAndDestinationServiceClients(
		ctx, getJobFromToResponse.FromTo,
		srcResourceString,
		dstResourceString,
	)
	if err != nil {
		return errors.New("could not create service clients " + err.Error())
	}
	// Send resume job request.
	var resumeJobResponse common.CancelPauseResumeResponse
	Rpc(common.ERpcCmd.ResumeJob(),
		&common.ResumeJobRequest{
			JobID:            jobID,
			SourceSAS:        rca.SourceSAS,
			DestinationSAS:   rca.DestinationSAS,
			SrcServiceClient: srcServiceClient,
			DstServiceClient: dstServiceClient,
			CredentialInfo:   credentialInfo,
			IncludeTransfer:  includeTransfer,
			ExcludeTransfer:  excludeTransfer,
		},
		&resumeJobResponse)

	if !resumeJobResponse.CancelledPauseResumed {
		glcm.Error(resumeJobResponse.ErrorMsg)
	}

	controller := resumeJobController{jobID: jobID}
	controller.waitUntilJobCompletion(true)

	return nil
}
