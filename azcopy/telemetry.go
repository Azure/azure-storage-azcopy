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

// This file assembles shared telemetry metadata and invocation context, persists
// the installation ID across runs, and generates fresh invocation IDs.

package azcopy

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
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

	"github.com/Azure/azure-storage-azcopy/v10/common"
	"github.com/Azure/azure-storage-azcopy/v10/telemetry"
)

const (
	telemetrySchemaVersion = "1"
)

// telemetryConnectionString is empty until a later integration layer configures it.
// AZCOPY_TELEMETRY_CONNECTION_STRING overrides it at runtime.
const telemetryConnectionString = ""

const (
	// envTelemetryConnectionString overrides the embedded connection string.
	envTelemetryConnectionString = "AZCOPY_TELEMETRY_CONNECTION_STRING"
	// envDisableTelemetry, when set to "true", disables telemetry entirely.
	envDisableTelemetry = "AZCOPY_DISABLE_TELEMETRY"
	// envE2ETelemetryRunID optionally correlates telemetry emitted by one E2E
	// pipeline matrix leg. It is unset in normal AzCopy usage.
	envE2ETelemetryRunID = "AZCOPY_E2E_TELEMETRY_RUN_ID"
	// The exit flush can cancel a send before this deadline.
	telemetrySendTimeout = 5 * time.Second
	// telemetryFlushTimeout is the maximum telemetry may add to process exit.
	telemetryFlushTimeout = 5 * time.Second
	// installationIDFileName stores the anonymous, per-install identifier.
	installationIDFileName         = "installation_id"
	installationIDRenameRetryDelay = 10 * time.Millisecond
	installationIDRenameAttempts   = 100
)

// telemetryAgent owns the optional telemetry reporter and cached resource
// attributes. Its report and flush methods are no-ops when the agent is nil or disabled.
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

// clientTelemetry builds a client's telemetry agent on first use.
type clientTelemetry struct {
	enabled bool
	once    sync.Once
	agent   atomic.Pointer[telemetryAgent]
}

// get returns the client's agent; a nil handle yields a disabled agent.
func (t *clientTelemetry) get() *telemetryAgent {
	if t == nil {
		return &telemetryAgent{}
	}
	t.once.Do(func() {
		t.agent.Store(initializeTelemetryAgent(func() *telemetryAgent { return newTelemetryAgent(t.enabled) }))
	})
	return t.agent.Load()
}

func initializeTelemetryAgent(create func() *telemetryAgent) (agent *telemetryAgent) {
	defer func() {
		if recover() != nil {
			common.LogToJobLogWithPrefix("telemetry: disabled for this process after initialization panic", common.LogWarning)
			agent = &telemetryAgent{}
		}
	}()
	return create()
}

// FlushTelemetry gives outstanding best-effort telemetry one bounded chance to
// complete before process exit.
func (c *Client) FlushTelemetry() {
	if c.telemetry != nil {
		c.telemetry.agent.Load().flush(telemetryFlushTimeout)
	}
}

func newTelemetryAgent(enabled bool) *telemetryAgent {
	a := &telemetryAgent{}
	if !enabled || strings.EqualFold(os.Getenv(envDisableTelemetry), "true") {
		return a
	}
	conn := configuredTelemetryConnectionString(os.Getenv, telemetryConnectionString)
	if conn == "" {
		return a
	}
	a.reporter = telemetry.NewReporter(telemetry.Config{
		ConnectionString: conn,
	})
	a.resource = buildResourceAttributes()
	a.enabled = true
	return a
}

func configuredTelemetryConnectionString(getenv func(string) string, embedded string) string {
	conn := strings.TrimSpace(getenv(envTelemetryConnectionString))
	if conn == "" {
		conn = strings.TrimSpace(embedded)
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
func (c *Client) ReportCommandInvoked(command, runID string, options telemetry.OptionAttributes) {
	c.telemetry.get().reportCommand(command, runID, newTelemetryInvocationID(), options)
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
	}, nil)
}

func newTelemetryInvocationID() string {
	buf := make([]byte, 16)
	// crypto/rand.Read never returns an error since Go 1.24, so this branch never runs.
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
	if !a.isActive() {
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
		// The exit flush or an earlier delivery failure cancelled this send.
		if errors.Is(ctx.Err(), context.Canceled) {
			return
		}
		if telemetry.IsDeliveryFailure(err) {
			if a.stopAfterDeliveryFailure() {
				common.LogToJobLogWithPrefix(fmt.Sprintf("telemetry: disabled for this process after delivery failure sending %s: %v", evt.EventName(), err), common.LogWarning)
			}
			return
		}
		common.LogToJobLogWithPrefix(fmt.Sprintf("telemetry: failed to send %s: %v", evt.EventName(), err), common.LogWarning)
	}
}

func buildResourceAttributes() telemetry.ResourceAttributes {
	hw := probeHostHardware()

	return telemetry.ResourceAttributes{
		AzCopyVersion:     common.AzcopyVersion,
		SchemaVersion:     telemetrySchemaVersion,
		E2ETestRunID:      strings.TrimSpace(os.Getenv(envE2ETelemetryRunID)),
		OSType:            runtime.GOOS,
		OSVersion:         hw.osVersion,
		HostArch:          runtime.GOARCH,
		HostNumCPU:        runtime.NumCPU(),
		HostCPUModel:      hw.cpuModel,
		HostMemoryTotalGB: hw.memoryTotalGB,
		AzureVMDetected:   probeAzureVM(),
		InstallationID:    installationID(),
		InvocationContext: detectInvocationContext(os.Getenv),
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

	return createInstallationID(filepath.Join(appDataDir, installationIDFileName), appDataDir)
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
	// crypto/rand.Read never returns an error since Go 1.24, so this branch never runs.
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

	for attempt := 0; attempt < installationIDRenameAttempts; attempt++ {
		if err = os.Rename(tempPath, path); err == nil {
			return id
		}
		time.Sleep(installationIDRenameRetryDelay)
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
	d := endpointJobDimensions("jobs.resume", jobDetails.FromTo, source, destination, srcCredType, dstCredType, options)
	d.SummaryCounterScope = "job-cumulative"
	return d
}

func endpointJobDimensions(command string, fromTo common.FromTo, source, destination common.ResourceString, srcCredType, dstCredType common.CredentialType, options telemetry.OptionAttributes) telemetry.JobDimensions {
	src, dst := fromTo.From(), fromTo.To()
	d := baseJobDimensions(command, fromTo, srcCredType, dstCredType)
	d.SourceMountType = sourceMountType(src, source.Value)
	d.SourceStorageAccount = storageAccountName(source, src)
	d.SourceScope = scopeForLocation(source, src, true)
	d.SourceEndpointKind = endpointKind(source, src)
	d.SourceAuthMechanism = authMechanism(srcCredType, source, src)
	d.SourceCloudType = endpointCloudType(source, src)
	d.DestStorageAccount = storageAccountName(destination, dst)
	d.DestScope = scopeForLocation(destination, dst, false)
	d.DestEndpointKind = endpointKind(destination, dst)
	d.DestAuthMechanism = authMechanism(dstCredType, destination, dst)
	d.DestCloudType = endpointCloudType(destination, dst)
	d.Options = options.Clone()
	return d
}

// protocolForLocation reports how AzCopy reaches an endpoint. Azure Files uses
// REST over HTTPS for both SMB and NFS shares.
func protocolForLocation(loc common.Location) string {
	switch loc {
	case common.ELocation.Local():
		return "local"
	case common.ELocation.Blob(), common.ELocation.BlobFS(), common.ELocation.File(), common.ELocation.FileNFS():
		return "https"
	case common.ELocation.S3():
		return "s3"
	case common.ELocation.GCP():
		return "gcs"
	default:
		return ""
	}
}

// mountTypeForLocation classifies the storage backing an endpoint location at a
// coarse level (no path inspection). For local paths it reports "local-disk".
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
	host := storageHost(resource)
	// Sovereign-cloud account names stay inside their cloud; telemetry ingestion is in the public cloud.
	if cloudTypeFromHost(host) != "public" {
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

// storageHost returns the lower-cased host of an http(s) URL without user info, or "".
func storageHost(resource common.ResourceString) string {
	endpoint, err := url.Parse(resource.Value)
	if err != nil || (endpoint.Scheme != "http" && endpoint.Scheme != "https") || endpoint.User != nil {
		return ""
	}
	return strings.TrimSuffix(strings.ToLower(endpoint.Hostname()), ".")
}

// endpointKind classifies an Azure hostname, not DNS resolution or network routing.
// Non-Azure endpoints return an empty value.
func endpointKind(r common.ResourceString, loc common.Location) string {
	if !loc.IsAzure() {
		return ""
	}
	host := storageHost(r)
	switch {
	case cloudTypeFromHost(host) == "":
		return "unknown"
	case strings.Contains(host, ".privatelink."):
		return "private-endpoint"
	default:
		return "public"
	}
}

func endpointCloudType(resource common.ResourceString, location common.Location) string {
	if !location.IsAzure() {
		return ""
	}
	if cloud := cloudTypeFromHost(storageHost(resource)); cloud != "" {
		return cloud
	}
	return "unknown"
}

// cloudTypeFromHost maps an Azure storage host suffix to a cloud environment.
func cloudTypeFromHost(host string) string {
	switch {
	case host == "":
		return ""
	case strings.HasSuffix(host, ".core.windows.net"), strings.HasSuffix(host, ".storage.azure.net"):
		return "public"
	case strings.HasSuffix(host, ".core.usgovcloudapi.net"):
		return "usgov"
	case strings.HasSuffix(host, ".core.chinacloudapi.cn"):
		return "china"
	case strings.HasSuffix(host, ".core.microsoft.scloud"):
		return "ussec"
	case strings.HasSuffix(host, ".core.eaglex.ic.gov"):
		return "usnat"
	case strings.HasSuffix(host, ".core.sovcloud-api.fr"):
		return "bleu"
	case strings.HasSuffix(host, ".core.sovcloud-api.de"):
		return "delos"
	case strings.HasSuffix(host, ".core.sovcloud-api.sg"):
		return "govsg"
	default:
		return ""
	}
}

func copyJobDimensions(o *CookedTransferOptions, srcCredType, dstCredType common.CredentialType) telemetry.JobDimensions {
	return endpointJobDimensions("copy", o.fromTo, o.source, o.destination, srcCredType, dstCredType, o.telemetryOptions)
}

func shouldEmitCopyTelemetry(o *CookedTransferOptions) bool {
	return o != nil && !o.dryrun
}

func syncJobDimensions(o *cookedSyncOptions, srcCredType, dstCredType common.CredentialType) telemetry.JobDimensions {
	return endpointJobDimensions("sync", o.fromTo, o.source, o.destination, srcCredType, dstCredType, o.telemetryOptions)
}

func buildFinishedEvent(resource telemetry.ResourceAttributes, dims telemetry.JobDimensions, runID, invocationID string, end time.Time, summary common.ListJobSummaryResponse, elapsed, enumerationElapsed, transferElapsed time.Duration, shape sourceShapeSummary) telemetry.JobFinishedEvent {
	jobDurationSeconds := elapsed.Seconds()
	enumerationPhaseDurationSeconds := enumerationElapsed.Seconds()
	transferPhaseDurationSeconds := transferElapsed.Seconds()
	failureErrorCodes, failureErrorOtherCount := aggregateErrorCodesWithOther(summary.FailedTransferErrorCodeCounts)
	performanceConstraint, adviceCodes := performanceAdviceAttributes(summary.PerfConstraint, summary.PerformanceAdvice)
	return telemetry.JobFinishedEvent{
		Resource:               resource,
		Dimensions:             dims,
		JobID:                  runID,
		InvocationID:           invocationID,
		EndTimestamp:           end,
		JobStatus:              summary.JobStatus.String(),
		FailureErrorCodes:      failureErrorCodes,
		PerformanceConstraint:  performanceConstraint,
		PerformanceAdviceCodes: adviceCodes,
		Measurements: telemetry.JobMeasurements{
			FailureErrorOtherCount:          failureErrorOtherCount,
			BytesEnumerated:                 summaryInt64(summary.TotalBytesEnumerated),
			BytesExpected:                   summaryInt64(summary.TotalBytesExpected),
			BytesTransferred:                summaryInt64(summary.TotalBytesTransferred),
			BytesOverWire:                   summaryInt64(summary.BytesOverWire),
			ObjectsScheduled:                countExcludingFolders(summary.TotalTransfers, summary.FolderPropertyTransfers),
			RegularFilesScheduled:           int64(summary.FileTransfers),
			SymlinksScheduled:               int64(summary.SymlinkTransfers),
			HardlinksConvertedScheduled:     int64(summary.HardlinksConvertedCount),
			HardlinksPreservedScheduled:     int64(summary.HardlinksTransferCount),
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
			JobThroughputMbps:               throughputMbps(summaryInt64(summary.TotalBytesTransferred), jobDurationSeconds),
			TransferPhaseThroughputMbps:     throughputMbps(summaryInt64(summary.TotalBytesTransferred), transferPhaseDurationSeconds),
			AverageStorageHTTPAttemptE2EMs:  int64(summary.AverageE2EMilliseconds),
			AvgIOPS:                         int64(summary.AverageIOPS),
			StorageHTTPAttemptCount:         summary.StorageHTTPAttemptCount,
			NetworkErrorAttemptCount:        summary.NetworkErrorAttemptCount,
			ServerBusy503Count:              summary.ServerBusy503Count,
			ServerBusyThroughputCount:       summary.ServerBusyThroughputCount,
			ServerBusyIOPSCount:             summary.ServerBusyIOPSCount,
			ServerBusyOtherCount:            summary.ServerBusyOtherCount,
			ServerBusyPct:                   summaryFloat64(summary.ServerBusyPercentage),
			NetworkErrorPct:                 summaryFloat64(summary.NetworkErrorPercentage),
			PercentComplete:                 summaryFloat64(summary.PercentComplete),
		},
	}
}

func countExcludingFolders(total, folders uint32) int64 {
	if folders >= total {
		return 0
	}
	return int64(total - folders)
}

// Values above MaxInt64 are reported as -1 (unavailable), like source statistics.
func summaryInt64(value uint64) int64 {
	if converted := int64(value); converted >= 0 {
		return converted
	}
	return -1
}

// Uses the shortest float32 decimal, so 33.3 is sent as 33.3 rather than 33.29999923706055.
func summaryFloat64(value float32) float64 {
	converted, _ := strconv.ParseFloat(strconv.FormatFloat(float64(value), 'g', -1, 32), 64)
	return converted
}

// maxErrorCodeBuckets bounds how many distinct error codes are reported so a job
// with many different failure codes cannot create an unbounded dimension value.
const maxErrorCodeBuckets = 10

// Codes are HTTP status codes; 0 means the transfer failed without an HTTP error response.
func aggregateErrorCodesWithOther(counts map[int32]uint32) (string, int64) {
	if len(counts) == 0 {
		return "", 0
	}
	type bucket struct {
		code  int32
		count uint32
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
		parts = append(parts, strconv.Itoa(int(b.code))+":"+strconv.FormatUint(uint64(b.count), 10))
	}
	return strings.Join(parts, ","), otherCount
}

const maxPerformanceAdviceCodes = 8

// Eight codes of this length plus separators fit the 512-byte telemetry property limit.
const maxPerformanceAdviceCodeLen = 48

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
	if code == "" || len(code) > maxPerformanceAdviceCodeLen {
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
	if bytes < 0 {
		return -1
	}
	if durationSeconds <= 0 {
		return 0
	}
	return float64(bytes) * 8 / 1e6 / durationSeconds
}
