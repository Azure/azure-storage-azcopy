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

package azcopy

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"net"
	"net/url"
	"os"
	"path/filepath"
	"runtime"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/Azure/azure-sdk-for-go/sdk/azcore"
	"github.com/Azure/azure-storage-azcopy/v10/common"
	"github.com/Azure/azure-storage-azcopy/v10/telemetry"
)

const (
	telemetrySchemaVersion = "3"
)

// telemetryConnectionString defaults to the test Application Insights component.
// AZCOPY_TELEMETRY_CONNECTION_STRING overrides it at runtime.
var telemetryConnectionString = "InstrumentationKey=09115a66-cd5e-4f48-b9b6-f71c883eed46;IngestionEndpoint=https://eastus-8.in.applicationinsights.azure.com/;LiveEndpoint=https://eastus.livediagnostics.monitor.azure.com/;ApplicationId=e43bbaa9-f24c-4613-b098-9e80a2a8216a"

// telemetryDisabledByFlag is set from the --disable-telemetry CLI flag (wired
// through ClientOptions.DisableTelemetry). When true, telemetry is disabled
// regardless of the connection string.
var telemetryDisabledByFlag bool

const (
	// envTelemetryConnectionString overrides the embedded connection string.
	envTelemetryConnectionString = "AZCOPY_TELEMETRY_CONNECTION_STRING"
	// envDisableTelemetry, when set to "true", disables telemetry entirely.
	envDisableTelemetry = "AZCOPY_DISABLE_TELEMETRY"
	// envE2ETelemetryRunID optionally correlates telemetry emitted by one E2E
	// pipeline matrix leg. It is unset in normal AzCopy usage.
	envE2ETelemetryRunID = "AZCOPY_E2E_TELEMETRY_RUN_ID"
	// Individual sends are bounded so an ordered started/finished pair fits
	// within the process-wide shutdown drain.
	telemetrySendTimeout = time.Second
	// telemetryFlushTimeout is the maximum telemetry may add to process exit.
	telemetryFlushTimeout    = 2 * time.Second
	telemetryMaxPendingSends = 4
	// installationIDFileName stores the anonymous, per-install identifier.
	installationIDFileName       = "installation_id"
	installationIDLockRetryDelay = 10 * time.Millisecond
	installationIDLockAttempts   = 100
)

// telemetryAgent owns the (optional) telemetry reporter plus the cached,
// process-wide resource attributes. All methods are safe to call when the agent
// is nil or disabled, in which case they are no-ops.
type telemetryAgent struct {
	enabled      bool
	stopped      atomic.Bool
	reporter     *telemetry.Reporter
	resource     telemetry.ResourceAttributes
	startedSends sync.Map
	dispatchMu   sync.Mutex
	pending      map[chan struct{}]context.CancelFunc
	idle         chan struct{}
	sendTimeout  time.Duration
}

var (
	telemetryOnce   sync.Once
	telemetryInst   *telemetryAgent
	telemetryInstMu sync.RWMutex
)

// getTelemetryAgent lazily builds the process-wide telemetry agent.
func getTelemetryAgent() *telemetryAgent {
	telemetryOnce.Do(func() {
		agent := initializeTelemetryAgent(newTelemetryAgent)
		telemetryInstMu.Lock()
		telemetryInst = agent
		telemetryInstMu.Unlock()
	})
	telemetryInstMu.RLock()
	defer telemetryInstMu.RUnlock()
	return telemetryInst
}

func initializeTelemetryAgent(create func() *telemetryAgent) (agent *telemetryAgent) {
	agent = &telemetryAgent{}
	defer func() {
		if recover() != nil {
			agent = &telemetryAgent{}
		}
	}()
	return create()
}

// FlushTelemetry gives outstanding best-effort telemetry one bounded chance to
// complete before process exit.
func FlushTelemetry() {
	telemetryInstMu.RLock()
	agent := telemetryInst
	telemetryInstMu.RUnlock()
	if agent != nil {
		agent.flush(telemetryFlushTimeout)
	}
}

func newTelemetryAgent() *telemetryAgent {
	a := &telemetryAgent{}
	if telemetryDisabledByFlag || strings.EqualFold(os.Getenv(envDisableTelemetry), "true") {
		return a
	}
	conn := configuredTelemetryConnectionString(os.Getenv)
	if conn == "" {
		return a
	}
	a.reporter = telemetry.NewReporter(telemetry.Config{
		Backend:          telemetry.BackendAppInsights,
		ConnectionString: conn,
	})
	a.resource = buildResourceAttributes()
	a.enabled = true
	return a
}

func configuredTelemetryConnectionString(getenv func(string) string) string {
	conn := strings.TrimSpace(getenv(envTelemetryConnectionString))
	if conn == "" {
		conn = strings.TrimSpace(telemetryConnectionString)
	}
	if conn == "" || strings.Contains(strings.ToLower(conn), "instrumentationkey=00000000-0000-0000-0000-000000000000") {
		return ""
	}
	return conn
}

// ReportCommandInvoked emits a single command.invoked telemetry event for the
// given canonical command path. It is intended for commands that do not emit
// paired job-attempt start/finish events. It is best-effort and a no-op when
// telemetry is disabled.
func ReportCommandInvoked(command, runID string, options telemetry.OptionAttributes) {
	getTelemetryAgent().reportCommand(command, runID, newTelemetryInvocationID(), options)
}

// reportStarted emits a job.started event asynchronously (best-effort). It never
// blocks the caller and never surfaces errors to the user.
func (a *telemetryAgent) reportStarted(dims telemetry.JobDimensions, runID, invocationID string, start time.Time) {
	if !a.isActive() {
		return
	}
	evt := telemetry.JobStartedEvent{
		Resource:     a.resource,
		Dimensions:   dims,
		JobID:        runID,
		InvocationID: invocationID,
		Timestamp:    start,
		StartedCount: 1,
	}
	key := startedSendKey(runID, invocationID)
	sendComplete := a.dispatch(evt, nil)
	if sendComplete == nil {
		return
	}
	a.startedSends.Store(key, sendComplete)
	go func() {
		<-sendComplete
		a.startedSends.CompareAndDelete(key, sendComplete)
	}()
}

// reportFinished queues job.finished after its matching job.started send. The
// process-wide flush provides the bounded delivery opportunity before exit.
func (a *telemetryAgent) reportFinished(evt telemetry.JobFinishedEvent) {
	if !a.isActive() {
		return
	}
	var startedComplete <-chan struct{}
	if sendComplete, ok := a.startedSends.LoadAndDelete(startedSendKey(evt.JobID, evt.InvocationID)); ok {
		startedComplete = sendComplete.(chan struct{})
	}
	a.dispatch(evt, startedComplete)
}

func startedSendKey(jobID, invocationID string) string {
	return jobID + "\x00" + invocationID
}

type attemptTelemetryFinalizer struct {
	agent        *telemetryAgent
	dimensions   telemetry.JobDimensions
	runID        string
	invocationID string
	start        time.Time
	stage        string
	finished     bool

	summaryFn            func() (common.ListJobSummaryResponse, bool)
	enumerationElapsedFn func() time.Duration
	transferElapsedFn    func() time.Duration
	shapeFn              func() sourceShapeSummary
	finalSummary         *common.ListJobSummaryResponse
}

func newAttemptTelemetryFinalizer(agent *telemetryAgent, dimensions telemetry.JobDimensions, runID, invocationID string, start time.Time) *attemptTelemetryFinalizer {
	return &attemptTelemetryFinalizer{
		agent:        agent,
		dimensions:   dimensions,
		runID:        runID,
		invocationID: invocationID,
		start:        start,
		stage:        "initialization",
	}
}

func (f *attemptTelemetryFinalizer) startEvent() {
	if f == nil {
		return
	}
	f.agent.reportStarted(f.dimensions, f.runID, f.invocationID, f.start)
}

func (a *telemetryAgent) reportInitializationFailure(dimensions func() telemetry.JobDimensions, jobID, invocationID string, start time.Time, attemptErr error) {
	finalizer := a.newAttempt(dimensions, jobID, invocationID, start)
	finalizer.startEvent()
	finalizer.finish(attemptErr)
}

func (a *telemetryAgent) newAttempt(dimensions func() telemetry.JobDimensions, jobID, invocationID string, start time.Time) (finalizer *attemptTelemetryFinalizer) {
	finalizer = newAttemptTelemetryFinalizer(nil, telemetry.JobDimensions{}, jobID, invocationID, start)
	if !a.isActive() {
		return finalizer
	}
	defer func() { _ = recover() }()
	collected := dimensions()
	finalizer.dimensions = collected
	finalizer.agent = a
	return finalizer
}

func (f *attemptTelemetryFinalizer) setStage(stage string) {
	if f != nil {
		f.stage = stage
	}
}

func (f *attemptTelemetryFinalizer) setFinalSummary(summary common.ListJobSummaryResponse) {
	if f != nil {
		copy := summary
		f.finalSummary = &copy
	}
}

func (f *attemptTelemetryFinalizer) finish(attemptErr error) {
	if f == nil || f.finished {
		return
	}
	f.finished = true
	if !f.agent.isActive() {
		return
	}
	defer func() {
		if recover() != nil {
			common.LogToJobLogWithPrefix("telemetry: dropped event after collection panic", common.LogWarning)
		}
	}()

	summary := common.ListJobSummaryResponse{}
	if f.finalSummary != nil {
		summary = *f.finalSummary
	} else if f.summaryFn != nil {
		if liveSummary, ok := f.summaryFn(); ok {
			summary = liveSummary
		}
	}
	terminalStatus, terminalReason := terminalAttemptStatus(summary.JobStatus, attemptErr)
	summary.JobStatus = terminalStatus
	terminalStage := f.stage
	if terminalReason == "completed" || terminalReason == "completed-with-errors" {
		terminalStage = "completed"
	}

	enumerationElapsed := durationOrZero(f.enumerationElapsedFn)
	transferElapsed := durationOrZero(f.transferElapsedFn)
	shape := sourceShapeSummary{}
	if f.shapeFn != nil {
		shape = f.shapeFn()
	}
	resource := telemetry.ResourceAttributes{}
	if f.agent != nil {
		resource = f.agent.resource
	}
	event := buildFinishedEvent(resource, f.dimensions, f.runID, f.invocationID, f.start, time.Now(), summary, time.Since(f.start), enumerationElapsed, transferElapsed, shape)
	event.TerminalStage = terminalStage
	event.JobErrorCategory, event.JobErrorCode = jobErrorAttributes(attemptErr, terminalReason, terminalStage)
	f.agent.reportFinished(event)
}

func durationOrZero(fn func() time.Duration) time.Duration {
	if fn == nil {
		return 0
	}
	return fn()
}

func terminalAttemptStatus(status common.JobStatus, attemptErr error) (common.JobStatus, string) {
	if status == common.EJobStatus.Cancelled() || errors.Is(attemptErr, context.Canceled) {
		return common.EJobStatus.Cancelled(), "cancelled"
	}
	if attemptErr != nil {
		return common.EJobStatus.Failed(), "failed"
	}
	switch status {
	case common.EJobStatus.CompletedWithErrors(), common.EJobStatus.CompletedWithErrorsAndSkipped():
		return status, "completed-with-errors"
	case common.EJobStatus.CompletedWithSkipped(), common.EJobStatus.Completed():
		return status, "completed"
	case common.EJobStatus.Failed():
		return status, "failed"
	default:
		return common.EJobStatus.Completed(), "completed"
	}
}

func jobErrorAttributes(attemptErr error, terminalReason, terminalStage string) (string, string) {
	switch terminalReason {
	case "completed", "cancelled":
		return "", ""
	case "completed-with-errors":
		return "transfer", "transfer-failures"
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
		return "timeout", "context-deadline-exceeded"
	}
	var pathErr *os.PathError
	if errors.As(attemptErr, &pathErr) {
		return "local-io", "local-path-error"
	}
	var urlErr *url.Error
	if errors.As(attemptErr, &urlErr) {
		if urlErr.Timeout() {
			return "timeout", "network-timeout"
		}
		return "network", "network-error"
	}
	var networkErr net.Error
	if errors.As(attemptErr, &networkErr) {
		if networkErr.Timeout() {
			return "timeout", "network-timeout"
		}
		return "network", "network-error"
	}
	var azErr common.AzError
	if errors.As(attemptErr, &azErr) {
		return "azcopy", "azcopy-" + strconv.FormatUint(azErr.ErrorCode(), 10)
	}

	switch terminalStage {
	case "initialization", "enumeration", "transfer", "completion":
		return terminalStage, terminalStage + "-error"
	default:
		return "unknown", "job-failed"
	}
}

func responseErrorCategory(errorCode string, statusCode int) string {
	switch strings.ToLower(errorCode) {
	case "authenticationfailed", "invalidauthenticationinfo", "noauthenticationinformation":
		return "authentication"
	case "authorizationfailure", "authorizationpermissionmismatch":
		return "authorization"
	case "serverbusy":
		return "throttling"
	case "operationtimedout":
		return "timeout"
	}

	switch statusCode {
	case 401:
		return "authentication"
	case 403:
		return "authorization"
	case 404:
		return "not-found"
	case 408:
		return "timeout"
	case 409, 412:
		return "conflict"
	case 429, 503:
		return "throttling"
	}
	if statusCode >= 500 {
		return "service"
	}
	return "service"
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

// reportCommand queues a single command.invoked event without delaying command
// execution. Best-effort; no-op when disabled.
func (a *telemetryAgent) reportCommand(command, runID, invocationID string, options telemetry.OptionAttributes) {
	if !a.isActive() {
		return
	}
	a.dispatch(telemetry.CommandInvokedEvent{
		Resource:     a.resource,
		Command:      command,
		Options:      options.Clone(),
		JobID:        runID,
		InvocationID: invocationID,
		Timestamp:    time.Now(),
		InvokedCount: 1,
	}, nil)
}

func newTelemetryInvocationID() string {
	buf := make([]byte, 16)
	if _, err := rand.Read(buf); err != nil {
		return ""
	}
	return hex.EncodeToString(buf)
}

func (a *telemetryAgent) shouldCollectSourceShape() bool {
	return a.isActive()
}

func (a *telemetryAgent) isActive() bool {
	return a != nil && a.enabled && !a.stopped.Load()
}

func (a *telemetryAgent) stopAfterDeliveryFailure() bool {
	a.dispatchMu.Lock()
	defer a.dispatchMu.Unlock()
	if !a.stopped.CompareAndSwap(false, true) {
		return false
	}
	for _, cancel := range a.pending {
		cancel()
	}
	return true
}

func (a *telemetryAgent) dispatch(evt telemetry.MetricEvent, waitFor <-chan struct{}) chan struct{} {
	if !a.isActive() {
		return nil
	}
	a.dispatchMu.Lock()
	if !a.isActive() || len(a.pending) >= telemetryMaxPendingSends {
		a.dispatchMu.Unlock()
		return nil
	}
	if a.pending == nil {
		a.pending = make(map[chan struct{}]context.CancelFunc)
	}
	if len(a.pending) == 0 {
		a.idle = make(chan struct{})
	}
	timeout := a.sendTimeout
	if timeout <= 0 {
		timeout = telemetrySendTimeout
	}
	dispatchTimeout := timeout
	if waitFor != nil {
		dispatchTimeout += timeout
	}
	ctx, cancel := context.WithTimeout(context.Background(), dispatchTimeout)
	complete := make(chan struct{})
	a.pending[complete] = cancel
	a.dispatchMu.Unlock()
	go func() {
		defer func() {
			cancel()
			a.dispatchMu.Lock()
			close(complete)
			delete(a.pending, complete)
			if len(a.pending) == 0 {
				close(a.idle)
			}
			a.dispatchMu.Unlock()
		}()
		if waitFor != nil {
			select {
			case <-waitFor:
			case <-ctx.Done():
				return
			}
		}
		if ctx.Err() == nil {
			sendContext, sendCancel := context.WithTimeout(ctx, timeout)
			defer sendCancel()
			a.sendSafely(sendContext, evt)
		}
	}()
	return complete
}

func (a *telemetryAgent) flush(timeout time.Duration) {
	if a == nil || !a.enabled {
		return
	}
	a.dispatchMu.Lock()
	if len(a.pending) == 0 {
		a.dispatchMu.Unlock()
		return
	}
	complete := a.idle
	a.dispatchMu.Unlock()
	timer := time.NewTimer(timeout)
	defer timer.Stop()
	select {
	case <-complete:
	case <-timer.C:
		a.dispatchMu.Lock()
		for _, cancel := range a.pending {
			cancel()
		}
		a.dispatchMu.Unlock()
	}
}

func (a *telemetryAgent) sendSafely(ctx context.Context, evt telemetry.MetricEvent) {
	defer func() {
		if recover() != nil {
			common.LogToJobLogWithPrefix("telemetry: dropped event after send panic", common.LogWarning)
		}
	}()
	if !a.isActive() || ctx.Err() != nil {
		return
	}
	if err := a.reporter.ReportEvent(ctx, evt); err != nil {
		if telemetry.IsDeliveryFailure(err) {
			if a.stopAfterDeliveryFailure() {
				common.LogToJobLogWithPrefix(fmt.Sprintf("telemetry: disabled for this process after delivery failure sending %s: %v", evt.EventName(), err), common.LogWarning)
			}
			return
		}
		common.LogToJobLogWithPrefix(fmt.Sprintf("telemetry: failed to send %s: %v", evt.EventName(), err), common.LogWarning)
	}
}

// ---------------------------------------------------------------------------
// Resource attributes (host probe)
// ---------------------------------------------------------------------------

func buildResourceAttributes() telemetry.ResourceAttributes {
	hw := probeHostHardware()
	imds := probeIMDS()

	return telemetry.ResourceAttributes{
		AzCopyVersion:      common.AzcopyVersion,
		SchemaVersion:      telemetrySchemaVersion,
		E2ETestRunID:       strings.TrimSpace(os.Getenv(envE2ETelemetryRunID)),
		OSType:             runtime.GOOS,
		OSVersion:          hw.osVersion,
		HostArch:           runtime.GOARCH,
		HostNumCPU:         runtime.NumCPU(),
		HostCPUModel:       hw.cpuModel,
		HostMemoryTotalGB:  hw.memoryTotalGB,
		HostNICSpeedMbps:   hw.nicMbps,
		HostNICSpeedBucket: nicSpeedBucket(hw.nicMbps),
		AzureVMDetected:    imds.isAzureVM,
		InstallationID:     installationID(),
		InvocationContext:  detectInvocationContext(os.Getenv),
	}
}

func nicSpeedBucket(speedMbps int) string {
	switch {
	case speedMbps < 0:
		return "unknown"
	case speedMbps < 1000:
		return "<1gbps"
	case speedMbps < 10000:
		return "1-<10gbps"
	case speedMbps < 40000:
		return "10-<40gbps"
	default:
		return ">=40gbps"
	}
}

// installationID returns a stable, anonymous per-install identifier. It is a
// random 128-bit value persisted with AzCopy's application data. It is NOT
// derived from any machine identity and contains no PII.
func installationID() string {
	return installationIDInDir(common.GetAzCopyAppPath())
}

func installationIDInDir(appDataDir string) string {
	if appDataDir == "" {
		return ""
	}
	if err := os.MkdirAll(appDataDir, 0700); err != nil {
		return ""
	}

	p := filepath.Join(appDataDir, installationIDFileName)
	lockPath := p + ".lock"
	if id := readInstallationID(p); id != "" {
		return id
	}
	lockFile, err := os.OpenFile(lockPath, os.O_RDWR|os.O_CREATE, 0600)
	if err != nil {
		return ""
	}
	defer lockFile.Close()
	for attempt := 0; attempt < installationIDLockAttempts; attempt++ {
		if id := readInstallationID(p); id != "" {
			return id
		}

		locked, err := tryInstallationLock(lockFile)
		if err != nil {
			return ""
		}
		if locked {
			return createInstallationID(p, appDataDir)
		}
		time.Sleep(installationIDLockRetryDelay)
	}
	return ""
}

func readInstallationID(path string) string {
	b, err := os.ReadFile(path)
	if err != nil {
		return ""
	}
	id := strings.TrimSpace(string(b))
	if len(id) != 32 {
		return ""
	}
	if _, err = hex.DecodeString(id); err != nil {
		return ""
	}
	return id
}

func createInstallationID(path, appDataDir string) string {
	if id := readInstallationID(path); id != "" {
		return id
	}

	buf := make([]byte, 16)
	if _, err := rand.Read(buf); err != nil {
		return ""
	}
	id := hex.EncodeToString(buf)

	tempFile, err := os.CreateTemp(appDataDir, "."+installationIDFileName+"-*")
	if err != nil {
		return ""
	}
	tempPath := tempFile.Name()
	defer os.Remove(tempPath)

	if _, err = tempFile.WriteString(id); err != nil {
		_ = tempFile.Close()
		return ""
	}
	if err = tempFile.Close(); err != nil {
		return ""
	}

	for attempt := 0; attempt < installationIDLockAttempts; attempt++ {
		if err = os.Rename(tempPath, path); err == nil {
			return id
		}
		time.Sleep(installationIDLockRetryDelay)
	}
	return ""
}

// detectInvocationContext infers how AzCopy was invoked. getenv is injected for
// testability.
func detectInvocationContext(getenv func(string) string) string {
	for _, k := range []string{"TF_BUILD", "GITHUB_ACTIONS", "CI", "JENKINS_URL", "GITLAB_CI", "BUILD_BUILDID"} {
		if getenv(k) != "" {
			return "ci"
		}
	}
	return "interactive"
}

// ---------------------------------------------------------------------------
// Job dimensions
// ---------------------------------------------------------------------------

func baseJobDimensions(command string, fromTo common.FromTo, srcCredType, dstCredType common.CredentialType) telemetry.JobDimensions {
	return telemetry.JobDimensions{
		Command:             command,
		FromTo:              fromTo.String(),
		SourceType:          fromTo.From().String(),
		DestType:            fromTo.To().String(),
		SourceProtocol:      protocolForLocation(fromTo.From()),
		SourceMountType:     mountTypeForLocation(fromTo.From()),
		DestProtocol:        protocolForLocation(fromTo.To()),
		SourceAuthMechanism: srcCredType.String(),
		DestAuthMechanism:   dstCredType.String(),
	}
}

func resumeJobDimensions(jobDetails common.GetJobDetailsResponse, source, destination common.ResourceString, srcCredType, dstCredType common.CredentialType, options telemetry.OptionAttributes) telemetry.JobDimensions {
	d := baseJobDimensions("jobs.resume", jobDetails.FromTo, srcCredType, dstCredType)
	d.SummaryCounterScope = "job-cumulative"
	d.SourceMountType = sourceMountType(jobDetails.FromTo.From(), source.Value)
	d.SourceStorageAccount = storageAccountName(source, jobDetails.FromTo.From())
	d.SourceScope = scopeForLocation(source, jobDetails.FromTo.From(), true)
	d.DestStorageAccount = storageAccountName(destination, jobDetails.FromTo.To())
	d.DestScope = scopeForLocation(destination, jobDetails.FromTo.To(), false)
	d.SourceAuthMechanism = authMechanism(srcCredType, source, jobDetails.FromTo.From())
	d.DestAuthMechanism = authMechanism(dstCredType, destination, jobDetails.FromTo.To())
	d.DestEndpointKind = endpointKind(destination, jobDetails.FromTo.To())
	d.SourceCloudType = endpointCloudType(source, jobDetails.FromTo.From())
	d.DestCloudType = endpointCloudType(destination, jobDetails.FromTo.To())
	d.Options = options.Clone()
	return d
}

// protocolForLocation maps a transfer endpoint location to the wire/access
// protocol used to reach it.
func protocolForLocation(loc common.Location) string {
	switch loc {
	case common.ELocation.Local():
		return "local"
	case common.ELocation.Blob(), common.ELocation.BlobFS(), common.ELocation.File():
		return "https"
	case common.ELocation.FileNFS():
		return "nfs"
	case common.ELocation.S3():
		return "s3"
	case common.ELocation.GCP():
		return "gcs"
	default:
		return ""
	}
}

// mountTypeForLocation classifies the storage backing an endpoint location at a
// coarse level (no path inspection). For local paths it reports "local-disk";
// callers that have the concrete local path should prefer sourceMountType to
// distinguish NAS (SMB/NFS) mounts.
func mountTypeForLocation(loc common.Location) string {
	switch {
	case loc == common.ELocation.Local():
		return "local-disk"
	case loc.IsAzure():
		return "cloud-azure"
	case loc == common.ELocation.S3():
		return "cloud-s3"
	case loc == common.ELocation.GCP():
		return "cloud-gcs"
	default:
		return ""
	}
}

// sourceMountType refines mountTypeForLocation for local sources by inspecting
// the OS mount table to distinguish network-attached storage from local disk:
// "nas-nfs" | "nas-smb" | "local-disk". For remote locations it defers to the
// coarse classification. localPath is ignored for non-local sources.
func sourceMountType(loc common.Location, localPath string) string {
	if loc != common.ELocation.Local() {
		return mountTypeForLocation(loc)
	}
	if mt := localMountType(localPath); mt != "" {
		return mt
	}
	return "local-disk"
}

func storageAccountName(resource common.ResourceString, location common.Location) string {
	if !location.IsAzure() {
		return ""
	}
	endpoint, err := url.Parse(resource.Value)
	if err != nil || (endpoint.Scheme != "http" && endpoint.Scheme != "https") || endpoint.User != nil {
		return ""
	}
	host := strings.TrimSuffix(strings.ToLower(endpoint.Hostname()), ".")
	if cloudTypeFromHost(host) == "" {
		return ""
	}
	account, _, found := strings.Cut(host, ".")
	if !found || len(account) < 3 || len(account) > 24 {
		return ""
	}
	for _, character := range account {
		if (character < 'a' || character > 'z') && (character < '0' || character > '9') {
			return ""
		}
	}
	return account
}

func authMechanism(credType common.CredentialType, resource common.ResourceString, location common.Location) string {
	if location.IsLocal() || location == common.ELocation.Pipe() || location == common.ELocation.Benchmark() || location == common.ELocation.None() {
		return "NotApplicable"
	}
	if resource.SAS != "" {
		return "SAS"
	}
	if credType == common.ECredentialType.Anonymous() {
		return "PublicAnonymous"
	}
	return credType.String()
}

func scopeForLocation(resource common.ResourceString, location common.Location, source bool) string {
	switch location {
	case common.ELocation.Pipe():
		return "stream"
	case common.ELocation.Benchmark():
		return "benchmark"
	case common.ELocation.None():
		return "none"
	}
	level, err := DetermineLocationLevel(resource.Value, location, source)
	if err != nil {
		return "unknown"
	}
	if location.IsLocal() {
		if level == ELocationLevel.Container() {
			return "local-directory"
		}
		return "local-object"
	}
	switch level {
	case ELocationLevel.Service():
		return "service"
	case ELocationLevel.Object():
		return "object-or-prefix"
	case ELocationLevel.Container():
		switch location {
		case common.ELocation.File(), common.ELocation.FileNFS():
			return "share"
		case common.ELocation.S3(), common.ELocation.GCP():
			return "bucket"
		default:
			return "container"
		}
	default:
		return "unknown"
	}
}

// hostOf returns the lower-cased hostname of a resource URL, or "" when it
// cannot be parsed or has no host.
func hostOf(r common.ResourceString) string {
	u, err := url.Parse(r.Value)
	if err != nil || u.Host == "" {
		return ""
	}
	return strings.ToLower(u.Hostname())
}

// endpointKind reports whether the destination is reached via an Azure Private
// Endpoint ("private-endpoint") or the public endpoint ("public"). It returns ""
// for non-Azure destinations or when the host cannot be parsed.
func endpointKind(r common.ResourceString, loc common.Location) string {
	if !loc.IsAzure() {
		return ""
	}
	host := hostOf(r)
	if host == "" {
		return ""
	}
	if strings.Contains(host, ".privatelink.") {
		return "private-endpoint"
	}
	return "public"
}

func endpointCloudType(resource common.ResourceString, location common.Location) string {
	if !location.IsAzure() {
		return ""
	}
	if cloud := cloudTypeFromHost(hostOf(resource)); cloud != "" {
		return cloud
	}
	return "unknown"
}

// cloudTypeFromHost maps an Azure storage host suffix to a cloud environment.
func cloudTypeFromHost(host string) string {
	switch {
	case host == "":
		return ""
	case strings.HasSuffix(host, ".core.usgovcloudapi.net"):
		return "gov"
	case strings.HasSuffix(host, ".core.chinacloudapi.cn"):
		return "china"
	case strings.HasSuffix(host, ".core.cloudapi.de"):
		return "germany"
	case strings.HasSuffix(host, ".core.windows.net"), strings.HasSuffix(host, ".storage.azure.net"):
		return "public"
	default:
		return ""
	}
}

func copyJobDimensions(o *CookedTransferOptions, srcCredType, dstCredType common.CredentialType) telemetry.JobDimensions {
	d := baseJobDimensions("copy", o.fromTo, srcCredType, dstCredType)
	d.SourceMountType = sourceMountType(o.fromTo.From(), o.source.Value)
	d.SourceStorageAccount = storageAccountName(o.source, o.fromTo.From())
	d.SourceScope = scopeForLocation(o.source, o.fromTo.From(), true)
	d.DestStorageAccount = storageAccountName(o.destination, o.fromTo.To())
	d.DestScope = scopeForLocation(o.destination, o.fromTo.To(), false)
	d.SourceAuthMechanism = authMechanism(srcCredType, o.source, o.fromTo.From())
	d.DestAuthMechanism = authMechanism(dstCredType, o.destination, o.fromTo.To())
	d.DestEndpointKind = endpointKind(o.destination, o.fromTo.To())
	d.SourceCloudType = endpointCloudType(o.source, o.fromTo.From())
	d.DestCloudType = endpointCloudType(o.destination, o.fromTo.To())
	d.Options = o.telemetryOptions.Clone()
	if o.benchmarkTelemetry != nil {
		d.Command = "bench"
		d.BenchmarkMode = o.benchmarkTelemetry.mode
		d.BenchmarkFileCount = o.benchmarkTelemetry.fileCount
		d.BenchmarkFileSizeBytes = o.benchmarkTelemetry.fileSizeBytes
		d.BenchmarkFolderCount = o.benchmarkTelemetry.folderCount
		d.BenchmarkCleanupRequested = o.benchmarkTelemetry.cleanupRequested
		d.BenchmarkIsCleanup = o.benchmarkTelemetry.isCleanup
	}
	return d
}

func shouldEmitCopyTelemetry(o *CookedTransferOptions) bool {
	return o != nil && !o.dryrun && (o.benchmarkTelemetry == nil || !o.benchmarkTelemetry.isCleanup)
}

func syncJobDimensions(o *cookedSyncOptions, srcCredType, dstCredType common.CredentialType) telemetry.JobDimensions {
	d := baseJobDimensions("sync", o.fromTo, srcCredType, dstCredType)
	d.SourceMountType = sourceMountType(o.fromTo.From(), o.source.Value)
	d.SourceStorageAccount = storageAccountName(o.source, o.fromTo.From())
	d.SourceScope = scopeForLocation(o.source, o.fromTo.From(), true)
	d.DestStorageAccount = storageAccountName(o.destination, o.fromTo.To())
	d.DestScope = scopeForLocation(o.destination, o.fromTo.To(), false)
	d.SourceAuthMechanism = authMechanism(srcCredType, o.source, o.fromTo.From())
	d.DestAuthMechanism = authMechanism(dstCredType, o.destination, o.fromTo.To())
	d.DestEndpointKind = endpointKind(o.destination, o.fromTo.To())
	d.SourceCloudType = endpointCloudType(o.source, o.fromTo.From())
	d.DestCloudType = endpointCloudType(o.destination, o.fromTo.To())
	d.Options = o.telemetryOptions.Clone()
	return d
}

// ---------------------------------------------------------------------------
// Finished event
// ---------------------------------------------------------------------------

func buildFinishedEvent(resource telemetry.ResourceAttributes, dims telemetry.JobDimensions, runID, invocationID string, start, end time.Time, summary common.ListJobSummaryResponse, elapsed, enumerationElapsed, transferElapsed time.Duration, shape sourceShapeSummary) telemetry.JobFinishedEvent {
	jobDurationSeconds := elapsed.Seconds()
	enumerationPhaseDurationSeconds := enumerationElapsed.Seconds()
	transferPhaseDurationSeconds := transferElapsed.Seconds()
	failureErrorCodes, failureErrorOtherCount := aggregateErrorCodesWithOther(summary.FailedTransfers)
	performanceConstraint, adviceCodes := performanceAdviceAttributes(summary.PerfConstraint, summary.PerformanceAdvice)
	return telemetry.JobFinishedEvent{
		Resource:                        resource,
		Dimensions:                      dims,
		JobID:                           runID,
		InvocationID:                    invocationID,
		StartTimestamp:                  start,
		EndTimestamp:                    end,
		FinishedCount:                   1,
		JobStatus:                       summary.JobStatus.String(),
		BytesEnumerated:                 int64(summary.TotalBytesEnumerated),
		BytesExpected:                   int64(summary.TotalBytesExpected),
		BytesTransferred:                int64(summary.TotalBytesTransferred),
		BytesOverWire:                   int64(summary.BytesOverWire),
		ObjectsScheduled:                countExcludingFolders(summary.TotalTransfers, summary.FolderPropertyTransfers),
		RegularFilesScheduled:           int64(summary.FileTransfers),
		SymlinksScheduled:               int64(summary.SymlinkTransfers),
		HardlinksConvertedScheduled:     int64(summary.HardlinksConvertedCount),
		FolderPropertiesScheduled:       int64(summary.FolderPropertyTransfers),
		ObjectsCompleted:                countExcludingFolders(summary.TransfersCompleted, summary.FoldersCompleted),
		ObjectsFailed:                   countExcludingFolders(summary.TransfersFailed, summary.FoldersFailed),
		ObjectsSkipped:                  countExcludingFolders(summary.TransfersSkipped, summary.FoldersSkipped),
		FolderPropertiesCompleted:       int64(summary.FoldersCompleted),
		FolderPropertiesFailed:          int64(summary.FoldersFailed),
		FolderPropertiesSkipped:         int64(summary.FoldersSkipped),
		SourceObjectsScanned:            shape.ObjectsScanned,
		SourceBytesScanned:              shape.BytesScanned,
		SourceAverageObjectSizeBytes:    shape.AverageObjectSizeBytes,
		SourceObjectSizeP50BytesApprox:  shape.ObjectSizeP50BytesApprox,
		SourceObjectSizeP90BytesApprox:  shape.ObjectSizeP90BytesApprox,
		SourceObjectSizeP95BytesApprox:  shape.ObjectSizeP95BytesApprox,
		SourceObjectsUnder1MiB:          shape.ObjectsUnder1MiB,
		SourceObjectsUnder1MiBRatioPct:  shape.ObjectsUnder1MiBRatioPct,
		SourceMaxDirectoryDepth:         shape.MaxDirectoryDepth,
		ContainersScanned:               shape.ContainersScanned,
		ContainersTouched:               shape.ContainersTouched,
		BucketsScanned:                  shape.BucketsScanned,
		BucketsTouched:                  shape.BucketsTouched,
		TransfersCompleted:              int64(summary.TransfersCompleted),
		TransfersFailed:                 int64(summary.TransfersFailed),
		TransfersSkipped:                int64(summary.TransfersSkipped),
		TransfersTotal:                  int64(summary.TotalTransfers),
		JobDurationSeconds:              jobDurationSeconds,
		EnumerationPhaseDurationSeconds: enumerationPhaseDurationSeconds,
		TransferPhaseDurationSeconds:    transferPhaseDurationSeconds,
		JobThroughputMbps:               throughputMbps(int64(summary.TotalBytesTransferred), jobDurationSeconds),
		TransferPhaseThroughputMbps:     throughputMbps(int64(summary.TotalBytesTransferred), transferPhaseDurationSeconds),
		AverageStorageHTTPAttemptE2EMs:  int64(summary.AverageE2EMilliseconds),
		AvgIOPS:                         int64(summary.AverageIOPS),
		StorageHTTPAttemptCount:         summary.StorageHTTPAttemptCount,
		NetworkErrorAttemptCount:        summary.NetworkErrorAttemptCount,
		ServerBusy503Count:              summary.ServerBusy503Count,
		ServerBusyThroughputCount:       summary.ServerBusyThroughputCount,
		ServerBusyIOPSCount:             summary.ServerBusyIOPSCount,
		ServerBusyOtherCount:            summary.ServerBusyOtherCount,
		ServerBusyPct:                   float64(summary.ServerBusyPercentage),
		NetworkErrorPct:                 float64(summary.NetworkErrorPercentage),
		PercentComplete:                 float64(summary.PercentComplete),
		FailureErrorCodes:               failureErrorCodes,
		FailureErrorOtherCount:          failureErrorOtherCount,
		PerformanceConstraint:           performanceConstraint,
		PerformanceAdviceCodes:          adviceCodes,
	}
}

func countExcludingFolders(total, folders uint32) int64 {
	if folders >= total {
		return 0
	}
	return int64(total - folders)
}

// maxErrorCodeBuckets bounds how many distinct error codes are reported so a job
// with many different failure codes cannot create an unbounded dimension value.
const maxErrorCodeBuckets = 10

func aggregateErrorCodesWithOther(failed []common.TransferDetail) (string, int64) {
	if len(failed) == 0 {
		return "", 0
	}
	counts := make(map[int32]int)
	for _, t := range failed {
		counts[t.ErrorCode]++
	}
	type bucket struct {
		code  int32
		count int
	}
	buckets := make([]bucket, 0, len(counts))
	for code, count := range counts {
		buckets = append(buckets, bucket{code, count})
	}
	sort.Slice(buckets, func(i, j int) bool {
		if buckets[i].count != buckets[j].count {
			return buckets[i].count > buckets[j].count
		}
		return buckets[i].code < buckets[j].code
	})
	var otherCount int64
	if len(buckets) > maxErrorCodeBuckets {
		for _, bucket := range buckets[maxErrorCodeBuckets:] {
			otherCount += int64(bucket.count)
		}
		buckets = buckets[:maxErrorCodeBuckets]
	}
	parts := make([]string, 0, len(buckets))
	for _, b := range buckets {
		parts = append(parts, strconv.Itoa(int(b.code))+":"+strconv.Itoa(b.count))
	}
	return strings.Join(parts, ","), otherCount
}

const maxPerformanceAdviceCodes = 8

func performanceAdviceAttributes(constraint common.PerfConstraint, advice []common.PerformanceAdvice) (string, []string) {
	constraintValue := ""
	if constraint != common.EPerfConstraint.Unknown() {
		constraintValue = constraint.String()
	}

	seen := make(map[string]struct{})
	codes := make([]string, 0, len(advice))
	for _, item := range advice {
		code := sanitizeAdviceCode(item.Code)
		if code == "" {
			continue
		}
		if _, exists := seen[code]; exists || len(codes) == maxPerformanceAdviceCodes {
			continue
		}
		seen[code] = struct{}{}
		codes = append(codes, code)
	}
	return constraintValue, codes
}

func sanitizeAdviceCode(code string) string {
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

func throughputMbps(bytes int64, durationSeconds float64) float64 {
	if durationSeconds <= 0 {
		return 0
	}
	return float64(bytes) * 8 / 1e6 / durationSeconds
}
