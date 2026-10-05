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
	"strings"
	"time"

	"github.com/Azure/azure-storage-azcopy/v10/common"
	"github.com/Azure/azure-storage-azcopy/v10/common/buildmode"
	"github.com/Azure/azure-storage-azcopy/v10/common/cred"
	"github.com/Azure/azure-storage-azcopy/v10/common/enum"
	"github.com/Azure/azure-storage-azcopy/v10/common/ternary"
	"github.com/Azure/azure-storage-azcopy/v10/jobsAdmin"
	"github.com/Azure/azure-storage-azcopy/v10/ste"
	"github.com/Azure/azure-storage-azcopy/v10/traverser"
)

// ResumeJobOptions contains the optional parameters for resuming a job.
type ResumeJobOptions struct {
	SourceSAS      string
	DestinationSAS string
	Handler        ResumeJobHandler
	SrcCredName    string
	DstCredName    string
	// Retained for compatibility; nonempty transfer selections are rejected as obsolete.
	IncludeTransfer map[string]int
	ExcludeTransfer map[string]int
}

// ResumeJobProgress contains the progress information for a resumed job.
type ResumeJobProgress CopyProgress

// ResumeJobHandler defines the interface for handling resume job events.
type ResumeJobHandler interface {
	OnStart(ctx JobContext)
	OnTransferProgress(progress ResumeJobProgress)
	OnComplete(result ResumeJobResult)
}

// ResumeJobResult contains the result of a resumed job.
type ResumeJobResult CopyResult

// ResumeJob resumes a job with the specified JobID.
func (c *Client) ResumeJob(ctx context.Context, jobID common.JobID, opts ResumeJobOptions) (result ResumeJobResult, err error) {
	if jobID.IsEmpty() {
		return ResumeJobResult{}, errors.New("resume job requires the JobID")
	}
	if len(opts.IncludeTransfer) != 0 || len(opts.ExcludeTransfer) != 0 {
		return ResumeJobResult{}, errors.New("include/exclude transfer lists are obsolete and cannot be used when resuming a job")
	}
	ctx, cancelOperation := context.WithCancel(ctx)
	defer cancelOperation()
	// Initialization of logs
	c.CurrentJobID = jobID
	timeAtPrestart := time.Now()

	logger := common.NewJobLogger(c.CurrentJobID, c.GetLogLevel(), common.LogPathFolder, "")
	common.AzcopyCurrentJobLogger = logger
	logger.OpenLog()
	closeLogger := true
	defer func() {
		if closeLogger {
			logger.CloseLog()
		}
	}()
	// Log a clear ISO 8601-formatted start time, so it can be read and use in the --include-after parameter
	// Subtract a few seconds, to ensure that this date DEFINITELY falls before the LMT of any file changed while this
	// job is running. I.e. using this later with --include-after is _guaranteed_ to pick up all files that changed during
	// or after this job
	adjustedTime := timeAtPrestart.Add(-5 * time.Second)
	startTimeMessage := fmt.Sprintf("ISO 8601 START TIME: to copy files that changed before or after this job started, use the parameter --%s=%s or --%s=%s",
		common.IncludeBeforeFlagName, traverser.IncludeBeforeDateFilter{}.FormatAsUTC(adjustedTime),
		common.IncludeAfterFlagName, traverser.IncludeAfterDateFilter{}.FormatAsUTC(adjustedTime))
	common.LogToJobLogWithPrefix(startTimeMessage, common.LogInfo)

	// Get fromTo info, so we can decide what's the proper credential type to use.
	jobDetails := jobsAdmin.GetJobDetails(common.GetJobDetailsRequest{JobID: jobID})
	if jobDetails.ErrorMsg != "" {
		return ResumeJobResult{}, errors.New(jobDetails.ErrorMsg)
	}

	// Validate that the job is resumable
	if jobDetails.FromTo.From() == common.ELocation.Benchmark() ||
		jobDetails.FromTo.To() == common.ELocation.Benchmark() {
		// Doesn't make sense to resume a benchmark job.
		// It's not tested, and wouldn't report progress correctly and wouldn't clean up after itself properly
		return ResumeJobResult{}, errors.New("resuming benchmark jobs is not supported")
	}

	// Prepare source and destination resource strings with updated SAS tokens
	srcResourceString, err := traverser.SplitResourceString(jobDetails.Source, jobDetails.FromTo.From())
	if err != nil {
		return ResumeJobResult{}, fmt.Errorf("error parsing source resource string: %w", err)
	}
	srcResourceString.SAS = normalizeSAS(opts.SourceSAS)
	dstResourceString, err := traverser.SplitResourceString(jobDetails.Destination, jobDetails.FromTo.To())
	if err != nil {
		return ResumeJobResult{}, fmt.Errorf("error parsing destination resource string: %w", err)
	}
	dstResourceString.SAS = normalizeSAS(opts.DestinationSAS)

	ctx = context.WithValue(ctx, ste.ServiceAPIVersionOverride, ste.DefaultServiceApiVersion)

	srcServiceClient, dstServiceClient, srcCredInfo, dstCredInfo, err := getSourceAndDestinationServiceClients(
		ctx,
		srcResourceString,
		dstResourceString,
		jobDetails,
		c.GetCredentialManager(),
		opts,
	)
	if err != nil {
		return ResumeJobResult{}, fmt.Errorf("cannot resume job with JobId %s, could not create service clients %v", jobID, err)
	}

	resumeHandler := common.GetLifecycleMgr()
	mgr := NewJobLifecycleManager(resumeHandler, logger)
	rpt := newResumeProgressTracker(jobID, opts.Handler)
	if c.GetLogLevel() != common.LogNone && common.LogPathFolder != "" {
		rpt.logPath = fmt.Sprintf("%s%s%s.log", common.LogPathFolder, common.OS_PATH_SEPARATOR, jobID)
	}
	finalized := false
	finalize := func(summary *common.ListJobSummaryResponse) error {
		if finalized {
			return nil
		}
		finalized = true
		cancelOperation()
		cancellationErr := mgr.RequestCancellation(jobID)
		cleanupCtx, cancelCleanup := context.WithTimeout(context.Background(), time.Minute)
		defer cancelCleanup()
		if drainErr := errors.Join(cancellationErr, mgr.CancelAndDrain(cleanupCtx, jobID)); drainErr != nil {
			closeLogger = false
			return fmt.Errorf("could not drain resumed job %s; job resources and logger retained: %w", jobID, drainErr)
		}
		if summary != nil {
			*summary = jobsAdmin.GetJobSummary(jobID, !buildmode.IsMover)
		}
		if !buildmode.IsMover {
			jobsAdmin.JobsAdmin.JobMgrCleanUp(jobID)
		}
		return nil
	}
	defer func() {
		if cleanupErr := finalize(nil); cleanupErr != nil {
			err = errors.Join(err, cleanupErr)
		}
	}()

	// Send resume job request.
	resumeJobResponse := jobsAdmin.ResumeJobOrder(common.ResumeJobRequest{
		JobID:                   jobID,
		SourceSAS:               srcResourceString.SAS,
		DestinationSAS:          dstResourceString.SAS,
		SrcServiceClient:        srcServiceClient,
		DstServiceClient:        dstServiceClient,
		JobErrorHandler:         mgr,
		IncludeTransfer:         opts.IncludeTransfer,
		ExcludeTransfer:         opts.ExcludeTransfer,
		Provider:                srcCredInfo.S3CredentialInfo.Provider,
		TargetCredentialType:    ternary.Iff(jobDetails.FromTo.IsDownload(), srcCredInfo.CredentialType, dstCredInfo.CredentialType),
		S2SSourceCredentialType: ternary.Iff(jobDetails.FromTo.IsS2S(), srcCredInfo.CredentialType, enum.ECredentialType.Anonymous()),
	})
	if !resumeJobResponse.CancelledPauseResumed {
		return ResumeJobResult{}, errors.New(resumeJobResponse.ErrorMsg)
	}
	mgr.InitiateProgressReporting(ctx, rpt)

	err = mgr.Wait()
	if err != nil {
		return ResumeJobResult{}, err
	}

	// Snapshot final counters after the STE drain and before releasing the job.
	var finalSummary common.ListJobSummaryResponse
	if err := finalize(&finalSummary); err != nil {
		return result, err
	}

	result = ResumeJobResult{
		ListJobSummaryResponse: finalSummary,
		ElapsedTime:            rpt.GetElapsedTime(),
	}

	if opts.Handler != nil {
		opts.Handler.OnComplete(result)
	}

	return result, nil
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
	manager cred.Manager,
	opts ResumeJobOptions,
) (*common.ServiceClient, *common.ServiceClient, cred.CredentialInfo, cred.CredentialInfo, error) {
	fromTo := jobDetails.FromTo

	srcCredInfo, err := GetTargetCredInfo(source, fromTo.From(), GetTargetCredInfoOptions{
		Context: ctx, CanBePublic: true, PreferredTokenName: opts.SrcCredName, TokenManager: manager,
	})
	if err != nil {
		return nil, nil, cred.CredentialInfo{}, cred.CredentialInfo{}, err
	}
	dstCredInfo, err := GetTargetCredInfo(destination, fromTo.To(), GetTargetCredInfoOptions{
		Context: ctx, CanBePublic: false, PreferredTokenName: opts.DstCredName, TokenManager: manager,
	})
	if err != nil {
		return nil, nil, cred.CredentialInfo{}, cred.CredentialInfo{}, err
	}
	var missingSAS []string
	if fromTo.From().IsAzure() && source.SAS == "" {
		if srcCredInfo.CredentialType == enum.ECredentialType.Unknown() {
			missingSAS = append(missingSAS, "source-sas")
		} else if srcCredInfo.CredentialType == enum.ECredentialType.Anonymous() {
			public := false
			if fromTo.From() == common.ELocation.Blob() {
				public, err = IsPublic(ctx, source.Value, common.CpkOptions{})
				if err != nil {
					return nil, nil, cred.CredentialInfo{}, cred.CredentialInfo{}, err
				}
			}
			if !public {
				missingSAS = append(missingSAS, "source-sas")
			}
		}
	}
	if fromTo.To().IsAzure() && destination.SAS == "" &&
		(dstCredInfo.CredentialType == enum.ECredentialType.Unknown() || dstCredInfo.CredentialType == enum.ECredentialType.Anonymous()) {
		missingSAS = append(missingSAS, "destination-sas")
	}
	if len(missingSAS) > 0 {
		return nil, nil, cred.CredentialInfo{}, cred.CredentialInfo{}, fmt.Errorf("the %s switch must be provided to resume the job", strings.Join(missingSAS, " and "))
	}
	srcOptions := CreateClientOptions(common.AzcopyCurrentJobLogger, nil, srcCredInfo.TokenCredential)

	var fileSrcClientOptions any
	if fromTo.From().IsFile() {
		fileSrcClientOptions = &common.FileClientOptions{
			AllowTrailingDot: jobDetails.TrailingDot.IsEnabled(), //Access the trailingDot option of the job
		}
	}
	srcServiceClient, err := common.GetServiceClientForLocation(fromTo.From(), source, srcCredInfo.CredentialType, srcCredInfo.TokenCredential, &srcOptions, fileSrcClientOptions)
	if err != nil {
		return nil, nil, cred.CredentialInfo{}, cred.CredentialInfo{}, err
	}
	dstOptions := CreateClientOptions(common.AzcopyCurrentJobLogger, srcCredInfo.TokenCredential, dstCredInfo.TokenCredential)
	var fileClientOptions any
	if fromTo.To().IsFile() {
		fileClientOptions = &common.FileClientOptions{
			AllowSourceTrailingDot: jobDetails.TrailingDot.IsEnabled() && fromTo.From().IsFile(),
			AllowTrailingDot:       jobDetails.TrailingDot.IsEnabled(),
		}
	}
	dstServiceClient, err := common.GetServiceClientForLocation(fromTo.To(), destination, dstCredInfo.CredentialType, dstCredInfo.TokenCredential, &dstOptions, fileClientOptions)
	if err != nil {
		return nil, nil, cred.CredentialInfo{}, cred.CredentialInfo{}, err
	}
	return srcServiceClient, dstServiceClient, srcCredInfo, dstCredInfo, nil
}

type resumeProgressTracker struct {
	jobID   common.JobID
	handler ResumeJobHandler
	logPath string

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

	if r.handler != nil {
		r.handler.OnStart(JobContext{JobID: r.jobID, LogPath: r.logPath})
	}
}

func (r *resumeProgressTracker) CheckProgress() (uint32, bool) {
	summary := jobsAdmin.GetJobSummary(r.jobID, !buildmode.IsMover)
	jobDone := summary.JobStatus.IsJobDone()
	totalKnownCount := summary.TotalTransfers
	duration := time.Since(r.jobStartTime)
	var computeThroughput = func() float64 {
		// compute the average throughput for the last time interval
		bytesInMb := float64(float64(summary.BytesOverWire-r.intervalBytesTransferred) / float64(Base10Mega))
		timeElapsed := time.Since(r.intervalStartTime).Seconds()

		// reset the interval timer and byte count
		r.intervalStartTime = time.Now()
		r.intervalBytesTransferred = summary.BytesOverWire

		return ternary.Iff(timeElapsed != 0, bytesInMb/timeElapsed, 0) * 8
	}
	throughput := computeThroughput()
	progress := CopyProgress{
		ListJobSummaryResponse: summary,
		Throughput:             throughput,
		ElapsedTime:            duration,
	}
	if jobsAdmin.JobsAdmin != nil {
		if manager, ok := jobsAdmin.JobsAdmin.JobMgr(r.jobID); ok {
			manager.Log(common.LogInfo, GetCopyProgress(progress, false))
		}
	}
	if r.handler != nil {
		r.handler.OnTransferProgress(ResumeJobProgress(progress))
	}
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

var _ jobProgressTracker = &resumeProgressTracker{}
