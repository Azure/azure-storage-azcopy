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

// Package telemetry defines the AzCopy telemetry event model and the reporter
// used to send those events to Azure Monitor (Application Insights).
//
// Two events mirror what AzCopy emits per run:
//
//	Event 1 (azcopy.job.started)  - emitted at job start.
//	Event 2 (azcopy.job.finished) - emitted at job completion, with measurements.
package telemetry

import (
	"strconv"
	"strings"
	"time"
	"unicode/utf8"
)

// ---------------------------------------------------------------------------
// Resource attributes - machine/system facts, constant for the process.
// Attached to BOTH metric events.
// ---------------------------------------------------------------------------

type ResourceAttributes struct {
	AzCopyVersion      string // "10.32.2"
	SchemaVersion      string
	SamplingRate       float64
	SamplerVersion     string
	E2ETestRunID       string // optional correlation ID used only by AzCopy E2E validation
	OSType             string // runtime.GOOS
	OSVersion          string // uname / RtlGetVersion
	HostArch           string // runtime.GOARCH
	HostNumCPU         int    // runtime.NumCPU()
	HostCPUModel       string // /proc/cpuinfo, WMI, sysctl
	HostMemoryTotalGB  int    // total physical memory
	HostNICSpeedMbps   int    // best-effort; -1 when unavailable
	HostNICSpeedBucket string // unknown | <1gbps | 1-<10gbps | 10-<40gbps | >=40gbps
	AzureVMDetected    bool   // true when Azure Instance Metadata Service responds
	InstallationID     string // anonymous, stable per-install identifier (no PII)
	InvocationContext  string // "interactive" | "ci" | "sdk" | "unknown"
}

func (ra ResourceAttributes) props() map[string]string {
	props := map[string]string{
		"AzCopyVersion":      ra.AzCopyVersion,
		"SchemaVersion":      ra.SchemaVersion,
		"SamplingRate":       strconv.FormatFloat(ra.SamplingRate, 'f', -1, 64),
		"SamplerVersion":     ra.SamplerVersion,
		"OSType":             ra.OSType,
		"OSVersion":          ra.OSVersion,
		"HostArch":           ra.HostArch,
		"HostNumCPU":         strconv.Itoa(ra.HostNumCPU),
		"HostCPUModel":       ra.HostCPUModel,
		"HostMemoryTotalGB":  strconv.Itoa(ra.HostMemoryTotalGB),
		"HostNICSpeedMbps":   strconv.Itoa(ra.HostNICSpeedMbps),
		"HostNICSpeedBucket": ra.HostNICSpeedBucket,
		"AzureVMDetected":    strconv.FormatBool(ra.AzureVMDetected),
		"InstallationID":     ra.InstallationID,
		"InvocationContext":  ra.InvocationContext,
	}
	if ra.E2ETestRunID != "" {
		props["E2ETestRunID"] = ra.E2ETestRunID
	}
	return props
}

// ---------------------------------------------------------------------------
// Job dimension attributes - job-specific facts attached to each data point.
// Present on BOTH events.
// ---------------------------------------------------------------------------

type OptionAttributes struct {
	FlagsSet   []string
	EnvVarsSet []string
	Values     map[string]string
}

func (o OptionAttributes) Clone() OptionAttributes {
	clone := OptionAttributes{
		FlagsSet:   append([]string(nil), o.FlagsSet...),
		EnvVarsSet: append([]string(nil), o.EnvVarsSet...),
	}
	if len(o.Values) > 0 {
		clone.Values = make(map[string]string, len(o.Values))
		for key, value := range o.Values {
			clone.Values[key] = value
		}
	}
	return clone
}

func (o OptionAttributes) addTo(props map[string]string) {
	if len(o.FlagsSet) > 0 {
		props["OptFlagsSet"] = truncateValue(strings.Join(o.FlagsSet, ","))
	}
	if len(o.EnvVarsSet) > 0 {
		props["OptEnvVarsSet"] = truncateValue(strings.Join(o.EnvVarsSet, ","))
	}
	for key, value := range o.Values {
		if value != "" {
			props[key] = truncateValue(value)
		}
	}
}

type JobDimensions struct {
	Command                   string // "copy" | "sync" | "remove"
	SummaryCounterScope       string // job-cumulative for resume summaries; empty otherwise
	FromTo                    string // "LocalBlob", "BlobLocal", "S3Blob", ...
	SourceType                string // "Local" | "Blob" | "File" | "S3" | ...
	DestType                  string
	SourceProtocol            string // "smb" | "nfs" | "local" | "https" | "s3" | "gcs"
	SourceMountType           string // "nas-smb" | "nas-nfs" | "local-disk" | "cloud-azure" | ...
	SourceScope               string // service | container | share | bucket | object-or-prefix | local-* | stream | benchmark
	DestProtocol              string
	DestScope                 string
	DestEndpointKind          string // "public" | "private-endpoint"
	SourceCloudType           string // Azure environment: "public" | "gov" | "china" | "germany"; empty for non-Azure
	DestCloudType             string // Azure environment: "public" | "gov" | "china" | "germany"; empty for non-Azure
	SourceAuthMechanism       string // "OAuthToken" | "Anonymous" | "SharedKey" | ...
	DestAuthMechanism         string // "OAuthToken" | "Anonymous" | "SharedKey" | ...
	BenchmarkMode             string // upload | download
	BenchmarkFileCount        int64
	BenchmarkFileSizeBytes    int64
	BenchmarkFolderCount      int64
	BenchmarkCleanupRequested bool
	BenchmarkIsCleanup        bool
	Options                   OptionAttributes
}

func (jd JobDimensions) props() map[string]string {
	props := map[string]string{
		"Command":             jd.Command,
		"FromTo":              jd.FromTo,
		"SourceType":          jd.SourceType,
		"DestType":            jd.DestType,
		"SourceProtocol":      jd.SourceProtocol,
		"SourceMountType":     jd.SourceMountType,
		"SourceScope":         jd.SourceScope,
		"DestProtocol":        jd.DestProtocol,
		"DestScope":           jd.DestScope,
		"DestEndpointKind":    jd.DestEndpointKind,
		"SourceCloudType":     jd.SourceCloudType,
		"DestCloudType":       jd.DestCloudType,
		"SourceAuthMechanism": jd.SourceAuthMechanism,
		"DestAuthMechanism":   jd.DestAuthMechanism,
	}
	if jd.SummaryCounterScope != "" {
		props["SummaryCounterScope"] = jd.SummaryCounterScope
	}
	if jd.Command == "bench" {
		props["BenchmarkMode"] = jd.BenchmarkMode
		props["BenchmarkFileCount"] = strconv.FormatInt(jd.BenchmarkFileCount, 10)
		props["BenchmarkFileSizeBytes"] = strconv.FormatInt(jd.BenchmarkFileSizeBytes, 10)
		props["BenchmarkFolderCount"] = strconv.FormatInt(jd.BenchmarkFolderCount, 10)
		props["BenchmarkCleanupRequested"] = strconv.FormatBool(jd.BenchmarkCleanupRequested)
		props["BenchmarkIsCleanup"] = strconv.FormatBool(jd.BenchmarkIsCleanup)
	}
	jd.Options.addTo(props)
	return props
}

// ---------------------------------------------------------------------------
// Event 1: job.started
// ---------------------------------------------------------------------------

type JobStartedEvent struct {
	Resource   ResourceAttributes
	Dimensions JobDimensions
	// JobID correlates the original attempt and all resume attempts. A paired
	// job.started and job.finished event also share an InvocationID.
	JobID        string
	InvocationID string
	Timestamp    time.Time
	StartedCount int64 // monotonic counter increment, always 1
}

// ---------------------------------------------------------------------------
// Event 2: job.finished (+ measurements)
// ---------------------------------------------------------------------------

type JobFinishedEvent struct {
	Resource       ResourceAttributes
	Dimensions     JobDimensions
	JobID          string
	InvocationID   string
	StartTimestamp time.Time
	EndTimestamp   time.Time

	FinishedCount    int64  // monotonic counter increment, always 1
	JobStatus        string // "Completed" | "CompletedWithErrors" | "Failed" | "Cancelled" | ...
	TerminalStage    string // initialization | enumeration | transfer | completion | completed
	JobErrorCategory string // authentication | authorization | throttling | timeout | network | local-io | conflict | not-found | service | azcopy | initialization | enumeration | transfer | completion | unknown
	JobErrorCode     string // bounded stable code; never raw error text

	// FailureErrorCodes is a compact, bounded histogram of the error codes seen
	// across failed transfers, e.g. "403:5,500:2" (ordered by descending count).
	// Empty when there were no failures. Contains no PII (only numeric codes).
	FailureErrorCodes      string
	FailureErrorOtherCount int64
	PerformanceConstraint  string
	PerformanceAdviceCodes []string

	// Measurements (from ListJobSummaryResponse + ElapsedTime)
	BytesEnumerated                 int64 // Source sizes in scheduled job-plan entries after filters and copy/sync comparison; not all bytes scanned
	BytesExpected                   int64 // Current successful + still-expected payload bytes
	BytesTransferred                int64 // Logical successful payload bytes; no retry duplication
	BytesOverWire                   int64 // Physical payload traffic; includes retries and failed-transfer traffic
	ObjectsScheduled                int64 // File-like payload transfers: regular files/objects, symlinks, and converted hardlinks
	RegularFilesScheduled           int64 // Regular file/object transfers only
	SymlinksScheduled               int64 // Preserved symlink transfers
	HardlinksConvertedScheduled     int64 // Hardlinks scheduled as converted file transfers
	FolderPropertiesScheduled       int64 // Folder existence/property transfers, not contained files
	ObjectsCompleted                int64 // Completed payload objects, excluding folder-property transfers
	ObjectsFailed                   int64 // Failed payload objects, excluding folder-property transfers
	ObjectsSkipped                  int64 // Skipped payload objects, excluding folder-property transfers
	FolderPropertiesCompleted       int64
	FolderPropertiesFailed          int64
	FolderPropertiesSkipped         int64
	SourceObjectsScanned            int64
	SourceBytesScanned              int64
	SourceAverageObjectSizeBytes    float64
	SourceObjectSizeP50BytesApprox  int64
	SourceObjectSizeP90BytesApprox  int64
	SourceObjectSizeP95BytesApprox  int64
	SourceObjectsUnder1MiB          int64
	SourceObjectsUnder1MiBRatioPct  float64
	SourceMaxDirectoryDepth         int64
	ContainersScanned               int64
	ContainersTouched               int64
	BucketsScanned                  int64
	BucketsTouched                  int64
	TransfersCompleted              int64   // TransfersCompleted
	TransfersFailed                 int64   // TransfersFailed
	TransfersSkipped                int64   // TransfersSkipped
	TransfersTotal                  int64   // TotalTransfers
	JobDurationSeconds              float64 // Entire job attempt, including enumeration/finalization
	EnumerationPhaseDurationSeconds float64 // Tracker start through final job part dispatch; overlaps transfer
	TransferPhaseDurationSeconds    float64 // First part ordered through terminal wait; may overlap enumeration
	JobThroughputMbps               float64 // BytesTransferred * 8 / 1e6 / JobDurationSeconds
	TransferPhaseThroughputMbps     float64 // BytesTransferred * 8 / 1e6 / TransferPhaseDurationSeconds
	AverageStorageHTTPAttemptE2EMs  int64   // Mean Storage HTTP attempt duration; retries are separate attempts
	AvgIOPS                         int64
	StorageHTTPAttemptCount         int64
	NetworkErrorAttemptCount        int64
	ServerBusy503Count              int64
	ServerBusyThroughputCount       int64
	ServerBusyIOPSCount             int64
	ServerBusyOtherCount            int64
	ServerBusyPct                   float64
	NetworkErrorPct                 float64
	PercentComplete                 float64 // PercentComplete (0-100); useful especially for cancelled jobs
}

// namedMetric is a single numeric measurement to be sent to a backend.
type namedMetric struct {
	Name  string
	Value float64
	Count int
}

// MetricEvent is implemented by every telemetry event the telemetry package can
// process and send. A single Reporter can therefore handle either event type.
type MetricEvent interface {
	// EventName returns the metric/event name (e.g. "azcopy.job.started").
	EventName() string
	// attributes returns the flattened resource + dimension properties.
	attributes() map[string]string
	// measurements returns the numeric data points to send.
	measurements() []namedMetric
	// timestamp returns the event time to stamp on the telemetry.
	timestamp() time.Time
}

func (JobStartedEvent) EventName() string  { return "azcopy.job.started" }
func (JobFinishedEvent) EventName() string { return "azcopy.job.finished" }

func (e JobStartedEvent) timestamp() time.Time  { return e.Timestamp }
func (e JobFinishedEvent) timestamp() time.Time { return e.EndTimestamp }

func (e JobStartedEvent) attributes() map[string]string {
	attrs := mergeProps(e.Resource.props(), e.Dimensions.props())
	attrs["JobID"] = e.JobID
	if e.InvocationID != "" {
		attrs["InvocationID"] = e.InvocationID
	}
	return boundProperties(attrs)
}

func (e JobFinishedEvent) attributes() map[string]string {
	attrs := mergeProps(e.Resource.props(), e.Dimensions.props())
	attrs["JobID"] = e.JobID
	if e.InvocationID != "" {
		attrs["InvocationID"] = e.InvocationID
	}
	attrs["JobStatus"] = e.JobStatus
	attrs["TerminalStage"] = e.TerminalStage
	if e.JobErrorCategory != "" {
		attrs["JobErrorCategory"] = e.JobErrorCategory
	}
	if e.JobErrorCode != "" {
		attrs["JobErrorCode"] = e.JobErrorCode
	}
	if e.FailureErrorCodes != "" {
		attrs["FailureErrorCodes"] = e.FailureErrorCodes
	}
	if e.PerformanceConstraint != "" {
		attrs["PerformanceConstraint"] = e.PerformanceConstraint
	}
	if len(e.PerformanceAdviceCodes) > 0 {
		attrs["PerformanceAdviceCodes"] = truncateValue(strings.Join(e.PerformanceAdviceCodes, ","))
	}
	return boundProperties(attrs)
}

func (e JobStartedEvent) measurements() []namedMetric {
	return []namedMetric{
		{Name: "azcopy.job.started", Value: float64(e.StartedCount), Count: 1},
	}
}

func (e JobFinishedEvent) measurements() []namedMetric {
	return []namedMetric{
		{Name: "azcopy.job.finished", Value: float64(e.FinishedCount), Count: 1},
		{Name: "azcopy.failure_error_other_count", Value: float64(e.FailureErrorOtherCount), Count: 1},
		{Name: "azcopy.bytes_enumerated", Value: float64(e.BytesEnumerated), Count: 1},
		{Name: "azcopy.bytes_expected", Value: float64(e.BytesExpected), Count: 1},
		{Name: "azcopy.bytes_transferred", Value: float64(e.BytesTransferred), Count: 1},
		{Name: "azcopy.bytes_over_wire", Value: float64(e.BytesOverWire), Count: 1},
		{Name: "azcopy.objects_scheduled", Value: float64(e.ObjectsScheduled), Count: 1},
		{Name: "azcopy.regular_files_scheduled", Value: float64(e.RegularFilesScheduled), Count: 1},
		{Name: "azcopy.symlinks_scheduled", Value: float64(e.SymlinksScheduled), Count: 1},
		{Name: "azcopy.hardlinks_converted_scheduled", Value: float64(e.HardlinksConvertedScheduled), Count: 1},
		{Name: "azcopy.folder_properties_scheduled", Value: float64(e.FolderPropertiesScheduled), Count: 1},
		{Name: "azcopy.objects_completed", Value: float64(e.ObjectsCompleted), Count: 1},
		{Name: "azcopy.objects_failed", Value: float64(e.ObjectsFailed), Count: 1},
		{Name: "azcopy.objects_skipped", Value: float64(e.ObjectsSkipped), Count: 1},
		{Name: "azcopy.folder_properties_completed", Value: float64(e.FolderPropertiesCompleted), Count: 1},
		{Name: "azcopy.folder_properties_failed", Value: float64(e.FolderPropertiesFailed), Count: 1},
		{Name: "azcopy.folder_properties_skipped", Value: float64(e.FolderPropertiesSkipped), Count: 1},
		{Name: "azcopy.source_objects_scanned", Value: float64(e.SourceObjectsScanned), Count: 1},
		{Name: "azcopy.source_bytes_scanned", Value: float64(e.SourceBytesScanned), Count: 1},
		{Name: "azcopy.source_average_object_size_bytes", Value: e.SourceAverageObjectSizeBytes, Count: 1},
		{Name: "azcopy.source_object_size_p50_bytes_approx", Value: float64(e.SourceObjectSizeP50BytesApprox), Count: 1},
		{Name: "azcopy.source_object_size_p90_bytes_approx", Value: float64(e.SourceObjectSizeP90BytesApprox), Count: 1},
		{Name: "azcopy.source_object_size_p95_bytes_approx", Value: float64(e.SourceObjectSizeP95BytesApprox), Count: 1},
		{Name: "azcopy.source_objects_under_1_mib", Value: float64(e.SourceObjectsUnder1MiB), Count: 1},
		{Name: "azcopy.source_objects_under_1_mib_ratio_pct", Value: e.SourceObjectsUnder1MiBRatioPct, Count: 1},
		{Name: "azcopy.source_max_directory_depth", Value: float64(e.SourceMaxDirectoryDepth), Count: 1},
		{Name: "azcopy.containers_scanned", Value: float64(e.ContainersScanned), Count: 1},
		{Name: "azcopy.containers_touched", Value: float64(e.ContainersTouched), Count: 1},
		{Name: "azcopy.buckets_scanned", Value: float64(e.BucketsScanned), Count: 1},
		{Name: "azcopy.buckets_touched", Value: float64(e.BucketsTouched), Count: 1},
		{Name: "azcopy.transfers_completed", Value: float64(e.TransfersCompleted), Count: 1},
		{Name: "azcopy.transfers_failed", Value: float64(e.TransfersFailed), Count: 1},
		{Name: "azcopy.transfers_skipped", Value: float64(e.TransfersSkipped), Count: 1},
		{Name: "azcopy.transfers_total", Value: float64(e.TransfersTotal), Count: 1},
		{Name: "azcopy.job_duration_seconds", Value: e.JobDurationSeconds, Count: 1},
		{Name: "azcopy.enumeration_phase_duration_seconds", Value: e.EnumerationPhaseDurationSeconds, Count: 1},
		{Name: "azcopy.transfer_phase_duration_seconds", Value: e.TransferPhaseDurationSeconds, Count: 1},
		{Name: "azcopy.job_throughput_mbps", Value: e.JobThroughputMbps, Count: 1},
		{Name: "azcopy.transfer_phase_throughput_mbps", Value: e.TransferPhaseThroughputMbps, Count: 1},
		{Name: "azcopy.average_storage_http_attempt_e2e_ms", Value: float64(e.AverageStorageHTTPAttemptE2EMs), Count: 1},
		{Name: "azcopy.avg_iops", Value: float64(e.AvgIOPS), Count: 1},
		{Name: "azcopy.storage_http_attempt_count", Value: float64(e.StorageHTTPAttemptCount), Count: 1},
		{Name: "azcopy.network_error_attempt_count", Value: float64(e.NetworkErrorAttemptCount), Count: 1},
		{Name: "azcopy.server_busy_503_count", Value: float64(e.ServerBusy503Count), Count: 1},
		{Name: "azcopy.server_busy_throughput_count", Value: float64(e.ServerBusyThroughputCount), Count: 1},
		{Name: "azcopy.server_busy_iops_count", Value: float64(e.ServerBusyIOPSCount), Count: 1},
		{Name: "azcopy.server_busy_other_count", Value: float64(e.ServerBusyOtherCount), Count: 1},
		{Name: "azcopy.server_busy_pct", Value: e.ServerBusyPct, Count: 1},
		{Name: "azcopy.network_error_pct", Value: e.NetworkErrorPct, Count: 1},
		{Name: "azcopy.percent_complete", Value: e.PercentComplete, Count: 1},
	}
}

// ---------------------------------------------------------------------------
// Event 3: command.invoked
//
// Emitted once for commands that do not emit paired job-attempt start/finish
// events. It is a lightweight usage signal carrying only the resource
// attributes plus the canonical full command path, so we can answer which
// subcommands customers use without conflating nested commands or aliases.
// ---------------------------------------------------------------------------

type CommandInvokedEvent struct {
	Resource ResourceAttributes
	Command  string // "login" | "jobs.list" | "remove" | ...
	Options  OptionAttributes
	// JobID is present when the invocation has an AzCopy JobID.
	JobID        string
	InvocationID string
	Timestamp    time.Time
	InvokedCount int64 // monotonic counter increment, always 1
}

func (CommandInvokedEvent) EventName() string { return "azcopy.command.invoked" }

func (e CommandInvokedEvent) timestamp() time.Time { return e.Timestamp }

func (e CommandInvokedEvent) attributes() map[string]string {
	attrs := e.Resource.props()
	attrs["Command"] = e.Command
	e.Options.addTo(attrs)
	if e.JobID != "" {
		attrs["JobID"] = e.JobID
	}
	if e.InvocationID != "" {
		attrs["InvocationID"] = e.InvocationID
	}
	return boundProperties(attrs)
}

func (e CommandInvokedEvent) measurements() []namedMetric {
	return []namedMetric{
		{Name: "azcopy.command.invoked", Value: float64(e.InvokedCount), Count: 1},
	}
}

const (
	maxPropValueLen         = 1024
	maxIdentifierValueLen   = 128
	maxHostValueLen         = 256
	maxOptionValueLen       = 512
	maxCompactListValueLen  = 512
	truncatedPropertyMarker = "...(truncated)"
)

var propertyValueLimits = map[string]int{
	"AzCopyVersion":             64,
	"SchemaVersion":             32,
	"SamplingRate":              32,
	"SamplerVersion":            64,
	"E2ETestRunID":              maxIdentifierValueLen,
	"OSType":                    32,
	"OSVersion":                 maxHostValueLen,
	"HostArch":                  32,
	"HostNumCPU":                32,
	"HostCPUModel":              maxHostValueLen,
	"HostMemoryTotalGB":         32,
	"HostNICSpeedMbps":          32,
	"HostNICSpeedBucket":        32,
	"AzureVMDetected":           5,
	"InstallationID":            maxIdentifierValueLen,
	"InvocationContext":         32,
	"Command":                   64,
	"SummaryCounterScope":       32,
	"FromTo":                    64,
	"SourceType":                64,
	"DestType":                  64,
	"SourceProtocol":            32,
	"SourceMountType":           64,
	"SourceScope":               64,
	"DestProtocol":              32,
	"DestScope":                 64,
	"DestEndpointKind":          64,
	"SourceCloudType":           32,
	"DestCloudType":             32,
	"SourceAuthMechanism":       64,
	"DestAuthMechanism":         64,
	"BenchmarkMode":             32,
	"BenchmarkFileCount":        32,
	"BenchmarkFileSizeBytes":    32,
	"BenchmarkFolderCount":      32,
	"BenchmarkCleanupRequested": 5,
	"BenchmarkIsCleanup":        5,
	"JobID":                     maxIdentifierValueLen,
	"InvocationID":              maxIdentifierValueLen,
	"JobStatus":                 maxIdentifierValueLen,
	"TerminalStage":             maxIdentifierValueLen,
	"JobErrorCategory":          maxIdentifierValueLen,
	"JobErrorCode":              maxIdentifierValueLen,
	"FailureErrorCodes":         maxCompactListValueLen,
	"PerformanceConstraint":     maxIdentifierValueLen,
	"PerformanceAdviceCodes":    maxCompactListValueLen,
	"OptFlagsSet":               maxPropValueLen,
	"OptEnvVarsSet":             maxOptionValueLen,
	"OptExcludeBlobTypes":       maxPropValueLen,
}

// truncateValue caps a property value at maxPropValueLen, appending a marker
// when truncation occurs.
func truncateValue(v string) string {
	return truncateValueTo(v, maxPropValueLen)
}

func truncateValueTo(value string, maxBytes int) string {
	value = strings.ToValidUTF8(value, "\uFFFD")
	if maxBytes <= 0 {
		return ""
	}
	if len(value) <= maxBytes {
		return value
	}

	marker := truncatedPropertyMarker
	end := maxBytes - len(marker)
	if end <= 0 {
		marker = ""
		end = maxBytes
	}
	for end > 0 && !utf8.RuneStart(value[end]) {
		end--
	}
	return value[:end] + marker
}

func propertyValueLimit(name string) int {
	if limit, ok := propertyValueLimits[name]; ok {
		return limit
	}
	if strings.HasPrefix(name, "Opt") {
		return maxOptionValueLen
	}
	return maxPropValueLen
}

func boundProperties(properties map[string]string) map[string]string {
	for name, value := range properties {
		properties[name] = truncateValueTo(value, propertyValueLimit(name))
	}
	return properties
}

// mergeProps merges the given property maps into a single map. Later maps win
// on key collisions.
func mergeProps(maps ...map[string]string) map[string]string {
	out := make(map[string]string)
	for _, m := range maps {
		for k, v := range m {
			out[k] = v
		}
	}
	return out
}
