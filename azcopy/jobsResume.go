// Copyright © 2025 Microsoft <wastore@microsoft.com>
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

package azcopy

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/Azure/azure-sdk-for-go/sdk/azcore"
	"github.com/Azure/azure-storage-azcopy/v10/common"
	"github.com/Azure/azure-storage-azcopy/v10/jobsAdmin"
	"github.com/Azure/azure-storage-azcopy/v10/ste"
)

type ResumeJobOptions struct {
	SourceSAS      string
	DestinationSAS string
	Handler        ResumeJobHandler
}

type ResumeJobProgress struct {
	common.ListJobSummaryResponse
	Throughput  float64
	ElapsedTime time.Duration
}

type ResumeJobHandler interface {
	OnStart(ctx common.JobContext)
	OnTransferProgress(progress ResumeJobProgress)
}

type ResumeJobResult struct {
	common.ListJobSummaryResponse
	ElapsedTime time.Duration
}

// ResumeJob resumes a job with the specified JobID.
func (c *Client) ResumeJob(ctx context.Context, jobID common.JobID, opts ResumeJobOptions) (result ResumeJobResult, err error) {
	if jobID.IsEmpty() {
		return ResumeJobResult{}, errors.New("resume job requires the JobID")
	}
	c.CurrentJobID = jobID
	timeAtPrestart := time.Now()

	common.AzcopyCurrentJobLogger = common.NewJobLogger(c.CurrentJobID, c.GetLogLevel(), common.LogPathFolder, "")
	common.AzcopyCurrentJobLogger.OpenLog()
	defer common.AzcopyCurrentJobLogger.CloseLog()
	// Log a clear ISO 8601-formatted start time, so it can be read and use in the --include-after parameter
	// Subtract a few seconds, to ensure that this date DEFINITELY falls before the LMT of any file changed while this
	// job is running. I.e. using this later with --include-after is _guaranteed_ to pick up all files that changed during
	// or after this job
	adjustedTime := timeAtPrestart.Add(-5 * time.Second)
	startTimeMessage := fmt.Sprintf("ISO 8601 START TIME: to copy files that changed before or after this job started, use the parameter --%s=%s or --%s=%s",
		common.IncludeBeforeFlagName, FormatAsUTC(adjustedTime),
		common.IncludeAfterFlagName, FormatAsUTC(adjustedTime))
	common.LogToJobLogWithPrefix(startTimeMessage, common.LogInfo)

	// if no logging, set this empty so that we don't display the log location
	if c.GetLogLevel() == common.LogNone {
		common.LogPathFolder = ""
	}

	// Get fromTo info, so we can decide what's the proper credential type to use.
	jobDetails := jobsAdmin.GetJobDetails(common.GetJobDetailsRequest{JobID: jobID})
	if jobDetails.ErrorMsg != "" {
		return ResumeJobResult{}, errors.New(jobDetails.ErrorMsg)
	}
	if jobDetails.FromTo.From() == common.ELocation.Benchmark() ||
		jobDetails.FromTo.To() == common.ELocation.Benchmark() {
		// Doesn't make sense to resume a benchmark job.
		// It's not tested, and wouldn't report progress correctly and wouldn't clean up after itself properly
		return ResumeJobResult{}, errors.New("resuming benchmark jobs is not supported")
	}

	sourceSAS := normalizeSAS(opts.SourceSAS)
	destinationSAS := normalizeSAS(opts.DestinationSAS)

	srcResourceString, err := SplitResourceString(jobDetails.Source, jobDetails.FromTo.From())
	if err != nil {
		return ResumeJobResult{}, fmt.Errorf("error parsing source resource string: %w", err)
	}
	srcResourceString.SAS = sourceSAS
	dstResourceString, err := SplitResourceString(jobDetails.Destination, jobDetails.FromTo.To())
	if err != nil {
		return ResumeJobResult{}, fmt.Errorf("error parsing destination resource string: %w", err)
	}
	dstResourceString.SAS = destinationSAS

	// TODO: Replace context with root context
	ctx = context.WithValue(context.TODO(), ste.ServiceAPIVersionOverride, ste.DefaultServiceApiVersion)
	srcServiceClient, dstServiceClient, err := getSourceAndDestinationServiceClients(
		ctx,
		srcResourceString,
		dstResourceString,
		jobDetails,
		c.GetUserOAuthTokenManagerInstance(),
	)
	if err != nil {
		return ResumeJobResult{}, fmt.Errorf("cannot resume job with JobId %s, could not create service clients %v", jobID, err.Error())
	}

	// AzCopy CLI sets this globally before calling ResumeJob.
	// If in library mode, this will not be set and we will use the user-provided handler.
	// Note: It is not ideal that this is a global, but keeping it this way for now to avoid a larger refactor than this already is.
	resumeHandler := common.GetLifecycleMgr()
	if resumeHandler == nil {
		resumeHandler = common.NewJobUIHooks()
		common.SetUIHooks(resumeHandler)
	}

	mgr := NewJobLifecycleManager(resumeHandler)
	rpt := newResumeProgressTracker(jobID, opts.Handler)

	// Send resume job request.
	resumeJobResponse := jobsAdmin.ResumeJobOrder(common.ResumeJobRequest{
		JobID:            jobID,
		SourceSAS:        sourceSAS,
		DestinationSAS:   destinationSAS,
		SrcServiceClient: srcServiceClient,
		DstServiceClient: dstServiceClient,
		JobErrorHandler:  mgr,
	})

	if !resumeJobResponse.CancelledPauseResumed {
		return ResumeJobResult{}, errors.New(resumeJobResponse.ErrorMsg)
	}
	mgr.InitiateProgressReporting(ctx, rpt)

	err = mgr.Wait()
	if err != nil {
		return ResumeJobResult{}, err
	}

	// Get final job summary
	finalSummary := jobsAdmin.GetJobSummary(jobID)

	return ResumeJobResult{
		ListJobSummaryResponse: finalSummary,
		ElapsedTime:            rpt.GetElapsedTime(),
	}, nil
}

// normalizeSAS ensures the SAS token starts with "?" if non-empty.
func normalizeSAS(sas string) string {
	if sas != "" && sas[0] != '?' {
		return "?" + sas
	}
	return sas
}

func getSourceAndDestinationServiceClients(
	ctx context.Context,
	source common.ResourceString,
	destination common.ResourceString,
	jobDetails common.GetJobDetailsResponse,
	uotm *common.UserOAuthTokenManager,
) (*common.ServiceClient, *common.ServiceClient, error) {
	fromTo := jobDetails.FromTo
	srcCredType, isSrcPublic, err := GetCredentialTypeForLocation(ctx,
		fromTo.From(),
		source,
		true,
		uotm,
		common.CpkOptions{})
	if err != nil {
		return nil, nil, err
	}

	var errorMsg = ""

	// For an Azure source, if there is no SAS, the cred type is Anonymous and the resource is not Azure public blob, tell the user they need to pass a new SAS.
	if fromTo.From().IsAzure() && srcCredType == common.ECredentialType.Anonymous() && source.SAS == "" {
		if !(fromTo.From() == common.ELocation.Blob() && isSrcPublic) {
			errorMsg += "source-sas"
		}
	}

	dstCredType, isDstPublic, err := GetCredentialTypeForLocation(ctx,
		fromTo.To(),
		destination,
		false,
		uotm,
		common.CpkOptions{})
	if err != nil {
		return nil, nil, err
	}

	if fromTo.To().IsAzure() && dstCredType == common.ECredentialType.Anonymous() && destination.SAS == "" {
		if !(fromTo.To() == common.ELocation.Blob() && isDstPublic) {
			if errorMsg == "" {
				errorMsg = "destination-sas"
			} else {
				errorMsg += " and destination-sas"
			}
		}
	}
	if errorMsg != "" {
		return nil, nil, fmt.Errorf("the %s switch must be provided to resume the job", errorMsg)
	}

	var tc azcore.TokenCredential
	if srcCredType.IsAzureOAuth() || dstCredType.IsAzureOAuth() {
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
	// But we don't want to supply a reauth token if we're not using OAuth. That could cause problems if say, a SAS is invalid.
	options := CreateClientOptions(common.AzcopyCurrentJobLogger, nil, common.Iff(srcCredType.IsAzureOAuth(), reauthTok, nil))

	var fileSrcClientOptions any
	if fromTo.From() == common.ELocation.File() || fromTo.From() == common.ELocation.FileNFS() {
		fileSrcClientOptions = &common.FileClientOptions{
			AllowTrailingDot: jobDetails.TrailingDot.IsEnabled(), //Access the trailingDot option of the job
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
	options = CreateClientOptions(common.AzcopyCurrentJobLogger, srcCred, common.Iff(dstCredType.IsAzureOAuth(), reauthTok, nil))
	var fileClientOptions any
	if fromTo.To() == common.ELocation.File() || fromTo.To() == common.ELocation.FileNFS() {
		fileClientOptions = &common.FileClientOptions{
			AllowSourceTrailingDot: jobDetails.TrailingDot.IsEnabled() && fromTo.From() == common.ELocation.File(),
			AllowTrailingDot:       jobDetails.TrailingDot.IsEnabled(),
		}
	}
	dstServiceClient, err := common.GetServiceClientForLocation(fromTo.To(), destination, dstCredType, tc, &options, fileClientOptions)
	if err != nil {
		return nil, nil, err
	}
	return srcServiceClient, dstServiceClient, nil
}

type resumeProgressTracker struct {
	jobID   common.JobID
	handler ResumeJobHandler

	// variables used to calculate progress
	// intervalStartTime holds the last time value when the progress summary was fetched
	// the value of this variable is used to calculate the throughput
	// it gets updated every time the progress summary is fetched
	intervalStartTime        time.Time
	intervalBytesTransferred uint64

	// used to calculate job summary
	jobStartTime time.Time
}

func newResumeProgressTracker(jobID common.JobID, handler ResumeJobHandler) *resumeProgressTracker {
	return &resumeProgressTracker{
		jobID:   jobID,
		handler: handler,
	}
}

func (r *resumeProgressTracker) Start() {
	// initialize the times necessary to track progress
	r.jobStartTime = time.Now()
	r.intervalStartTime = time.Now()
	r.intervalBytesTransferred = 0

	var logPathFolder string
	if common.LogPathFolder != "" {
		logPathFolder = fmt.Sprintf("%s%s%s.log", common.LogPathFolder, common.OS_PATH_SEPARATOR, r.jobID)
	}
	r.handler.OnStart(common.JobContext{JobID: r.jobID, LogPath: logPathFolder})
}

func (r *resumeProgressTracker) CheckProgress() (uint32, bool) {
	summary := jobsAdmin.GetJobSummary(r.jobID)
	jobDone := summary.JobStatus.IsJobDone()
	totalKnownCount := summary.TotalTransfers
	duration := time.Since(r.jobStartTime)
	var computeThroughput = func() float64 {
		// compute the average throughput for the last time interval
		bytesInMb := float64(float64(summary.BytesOverWire-r.intervalBytesTransferred) / float64(common.Base10Mega))
		timeElapsed := time.Since(r.intervalStartTime).Seconds()

		// reset the interval timer and byte count
		r.intervalStartTime = time.Now()
		r.intervalBytesTransferred = summary.BytesOverWire

		return common.Iff(timeElapsed != 0, bytesInMb/timeElapsed, 0) * 8
	}
	throughput := computeThroughput()
	transferProgress := common.TransferProgress{
		ListJobSummaryResponse: summary,
		Throughput:             throughput,
		ElapsedTime:            duration,
		JobType:                common.EJobType.Resume(),
	}
	if common.AzcopyCurrentJobLogger != nil {
		common.AzcopyCurrentJobLogger.Log(common.LogInfo, common.GetProgressOutputBuilder(transferProgress)(common.EOutputFormat.Text()))
	}
	r.handler.OnTransferProgress(ResumeJobProgress{
		ListJobSummaryResponse: summary,
		Throughput:             throughput,
		ElapsedTime:            duration,
	})
	return totalKnownCount, jobDone
}

func (r *resumeProgressTracker) CompletedEnumeration() bool {
	return true // resume does not enumerate, so this is always true
}

func (r *resumeProgressTracker) GetJobID() common.JobID {
	return r.jobID
}

func (r *resumeProgressTracker) GetElapsedTime() time.Duration {
	return time.Since(r.jobStartTime)
}

var _ JobProgressTracker = &resumeProgressTracker{}
