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

// This file records each job attempt and maps how it ended to the job.finished terminal stage and error fields.

package azcopy

import (
	"context"
	"errors"
	"net"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/Azure/azure-sdk-for-go/sdk/azcore"
	"github.com/Azure/azure-storage-azcopy/v10/common"
	"github.com/Azure/azure-storage-azcopy/v10/telemetry"
)

// attemptStage is the TerminalStage sent on job.finished.
type attemptStage uint8

const (
	attemptStageInitialization attemptStage = iota
	attemptStageEnumeration
	attemptStageTransfer
	attemptStageCompletion
	attemptStageCompleted
)

var attemptStageNames = [...]string{
	attemptStageInitialization: "initialization",
	attemptStageEnumeration:    "enumeration",
	attemptStageTransfer:       "transfer",
	attemptStageCompletion:     "completion",
	attemptStageCompleted:      "completed",
}

func (s attemptStage) String() string {
	if int(s) < len(attemptStageNames) {
		return attemptStageNames[s]
	}
	return ""
}

type attemptOutcome uint8

const (
	outcomeCompleted attemptOutcome = iota
	outcomeCompletedWithErrors
	outcomeFailed
	outcomeCancelled
)

// jobErrorCategory is the JobErrorCategory sent on job.finished.
type jobErrorCategory uint8

const (
	errorCategoryNone jobErrorCategory = iota
	errorCategoryInitialization
	errorCategoryEnumeration
	errorCategoryTransfer
	errorCategoryCompletion
	errorCategoryAuthentication
	errorCategoryAuthorization
	errorCategoryNotFound
	errorCategoryConflict
	errorCategoryThrottling
	errorCategoryTimeout
	errorCategoryRequest
	errorCategoryService
	errorCategoryNetwork
	errorCategoryLocalIO
	errorCategoryAzCopy
	errorCategoryUnknown
)

var jobErrorCategoryNames = [...]string{
	errorCategoryNone:           "",
	errorCategoryInitialization: "initialization",
	errorCategoryEnumeration:    "enumeration",
	errorCategoryTransfer:       "transfer",
	errorCategoryCompletion:     "completion",
	errorCategoryAuthentication: "authentication",
	errorCategoryAuthorization:  "authorization",
	errorCategoryNotFound:       "not-found",
	errorCategoryConflict:       "conflict",
	errorCategoryThrottling:     "throttling",
	errorCategoryTimeout:        "timeout",
	errorCategoryRequest:        "request",
	errorCategoryService:        "service",
	errorCategoryNetwork:        "network",
	errorCategoryLocalIO:        "local-io",
	errorCategoryAzCopy:         "azcopy",
	errorCategoryUnknown:        "unknown",
}

func (c jobErrorCategory) String() string {
	if int(c) < len(jobErrorCategoryNames) {
		return jobErrorCategoryNames[c]
	}
	return ""
}

// attemptTelemetryRecorder records one job attempt and reports its job.started and at most one job.finished.
type attemptTelemetryRecorder struct {
	agent        *telemetryAgent
	dimensions   telemetry.JobDimensions
	runID        string
	invocationID string
	start        time.Time
	stage        attemptStage
	finished     bool

	summaryFn            func() (common.ListJobSummaryResponse, bool)
	enumerationElapsedFn func() time.Duration
	transferElapsedFn    func() time.Duration
	shapeFn              func() sourceShapeSummary
	finalSummary         *common.ListJobSummaryResponse
}

func newAttemptTelemetryRecorder(agent *telemetryAgent, dimensions telemetry.JobDimensions, runID, invocationID string, start time.Time) *attemptTelemetryRecorder {
	return &attemptTelemetryRecorder{
		agent:        agent,
		dimensions:   dimensions,
		runID:        runID,
		invocationID: invocationID,
		start:        start,
		stage:        attemptStageInitialization,
	}
}

func (r *attemptTelemetryRecorder) startEvent() {
	if r == nil {
		return
	}
	r.agent.reportStarted(r.dimensions, r.runID, r.invocationID, r.start)
}

func (a *telemetryAgent) reportInitializationFailure(dimensions func() telemetry.JobDimensions, jobID, invocationID string, start time.Time, attemptErr error) {
	recorder := a.newAttempt(dimensions, jobID, invocationID, start)
	recorder.startEvent()
	recorder.finish(attemptErr)
}

func (a *telemetryAgent) newAttempt(dimensions func() telemetry.JobDimensions, jobID, invocationID string, start time.Time) (recorder *attemptTelemetryRecorder) {
	recorder = newAttemptTelemetryRecorder(nil, telemetry.JobDimensions{}, jobID, invocationID, start)
	if !a.isActive() {
		return recorder
	}
	defer func() {
		if recover() != nil {
			common.LogToJobLogWithPrefix("telemetry: skipped attempt after collection panic", common.LogWarning)
		}
	}()
	recorder.dimensions = dimensions()
	recorder.agent = a
	return recorder
}

func (r *attemptTelemetryRecorder) setStage(stage attemptStage) {
	if r != nil {
		r.stage = stage
	}
}

func (r *attemptTelemetryRecorder) setFinalSummary(summary common.ListJobSummaryResponse) {
	if r != nil {
		r.finalSummary = &summary
	}
}

func (r *attemptTelemetryRecorder) finish(attemptErr error) {
	if r == nil || r.finished {
		return
	}
	r.finished = true
	if !r.agent.isActive() {
		return
	}
	defer func() {
		if recover() != nil {
			common.LogToJobLogWithPrefix("telemetry: dropped event after collection panic", common.LogWarning)
		}
	}()

	summary := common.ListJobSummaryResponse{}
	if r.finalSummary != nil {
		summary = *r.finalSummary
	} else if r.summaryFn != nil {
		if liveSummary, ok := r.summaryFn(); ok {
			summary = liveSummary
		}
	}
	terminalStatus, outcome := terminalAttemptStatus(summary.JobStatus, attemptErr)
	summary.JobStatus = terminalStatus
	terminalStage := r.stage
	if outcome == outcomeCompleted || outcome == outcomeCompletedWithErrors {
		terminalStage = attemptStageCompleted
	}

	enumerationElapsed := durationOrZero(r.enumerationElapsedFn)
	transferElapsed := durationOrZero(r.transferElapsedFn)
	shape := sourceShapeSummary{}
	if r.shapeFn != nil {
		shape = r.shapeFn()
	}
	event := buildFinishedEvent(r.agent.resource, r.dimensions, r.runID, r.invocationID, time.Now(), summary, time.Since(r.start), enumerationElapsed, transferElapsed, shape)
	event.TerminalStage = terminalStage.String()
	errorCategory, errorCode := jobErrorAttributes(attemptErr, outcome, terminalStage)
	event.JobErrorCategory, event.JobErrorCode = errorCategory.String(), errorCode
	r.agent.reportFinished(event)
}

func durationOrZero(fn func() time.Duration) time.Duration {
	if fn == nil {
		return 0
	}
	return fn()
}

func terminalAttemptStatus(status common.JobStatus, attemptErr error) (common.JobStatus, attemptOutcome) {
	if status == common.EJobStatus.Cancelled() || errors.Is(attemptErr, context.Canceled) {
		return common.EJobStatus.Cancelled(), outcomeCancelled
	}
	if attemptErr != nil {
		return common.EJobStatus.Failed(), outcomeFailed
	}
	switch status {
	case common.EJobStatus.CompletedWithErrors(), common.EJobStatus.CompletedWithErrorsAndSkipped():
		return status, outcomeCompletedWithErrors
	case common.EJobStatus.CompletedWithSkipped(), common.EJobStatus.Completed():
		return status, outcomeCompleted
	case common.EJobStatus.Failed():
		return status, outcomeFailed
	default:
		return common.EJobStatus.Completed(), outcomeCompleted
	}
}

// jobErrorAttributes maps how an attempt ended to a bounded error category and code; raw error text is never sent.
func jobErrorAttributes(attemptErr error, outcome attemptOutcome, terminalStage attemptStage) (jobErrorCategory, string) {
	switch outcome {
	case outcomeCompleted, outcomeCancelled:
		return errorCategoryNone, ""
	case outcomeCompletedWithErrors:
		return errorCategoryTransfer, "transfer-failures"
	}

	var responseErr *azcore.ResponseError
	if errors.As(attemptErr, &responseErr) {
		code := sanitizeJobErrorCode(responseErr.ErrorCode)
		if code == "" && responseErr.StatusCode > 0 {
			code = "http-" + strconv.Itoa(responseErr.StatusCode)
		}
		if code == "" {
			code = "storage-service-error"
		}
		return responseErrorCategory(responseErr.ErrorCode, responseErr.StatusCode), code
	}

	if errors.Is(attemptErr, context.DeadlineExceeded) {
		return errorCategoryTimeout, "context-deadline-exceeded"
	}
	var pathErr *os.PathError
	if errors.As(attemptErr, &pathErr) {
		return errorCategoryLocalIO, "local-path-error"
	}
	var networkErr net.Error
	if errors.As(attemptErr, &networkErr) {
		if networkErr.Timeout() {
			return errorCategoryTimeout, "network-timeout"
		}
		return errorCategoryNetwork, "network-error"
	}
	var azErr common.AzError
	if errors.As(attemptErr, &azErr) {
		return errorCategoryAzCopy, "azcopy-" + strconv.FormatUint(azErr.ErrorCode(), 10)
	}

	switch terminalStage {
	case attemptStageInitialization:
		return errorCategoryInitialization, "initialization-error"
	case attemptStageEnumeration:
		return errorCategoryEnumeration, "enumeration-error"
	case attemptStageTransfer:
		return errorCategoryTransfer, "transfer-error"
	case attemptStageCompletion:
		return errorCategoryCompletion, "completion-error"
	default:
		return errorCategoryUnknown, "job-failed"
	}
}

func responseErrorCategory(errorCode string, statusCode int) jobErrorCategory {
	switch strings.ToLower(errorCode) {
	case "authenticationfailed", "invalidauthenticationinfo", "noauthenticationinformation":
		return errorCategoryAuthentication
	case "authorizationfailure", "authorizationpermissionmismatch":
		return errorCategoryAuthorization
	case "serverbusy":
		return errorCategoryThrottling
	case "operationtimedout":
		return errorCategoryTimeout
	}

	switch statusCode {
	case 401:
		return errorCategoryAuthentication
	case 403:
		return errorCategoryAuthorization
	case 404:
		return errorCategoryNotFound
	case 408:
		return errorCategoryTimeout
	case 409, 412:
		return errorCategoryConflict
	case 429, 503:
		return errorCategoryThrottling
	}
	if statusCode >= 400 && statusCode < 500 {
		return errorCategoryRequest
	}
	return errorCategoryService
}

func sanitizeJobErrorCode(code string) string {
	code = strings.TrimSpace(code)
	if code == "" || len(code) > 64 {
		return ""
	}
	for _, char := range code {
		if (char >= 'a' && char <= 'z') || (char >= 'A' && char <= 'Z') ||
			(char >= '0' && char <= '9') || char == '_' || char == '-' || char == '.' {
			continue
		}
		return ""
	}
	return code
}
