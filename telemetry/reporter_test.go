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

package telemetry

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"strings"
	"testing"
	"time"
	"unicode/utf8"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const testConnString = "InstrumentationKey=11111111-2222-3333-4444-555555555555;IngestionEndpoint=https://eastus.example.com/"

// stubClient is an httpDoer that records the last request it received and
// returns a canned response (or error), so we can assert on telemetry traffic
// without making real network calls.
type stubClient struct {
	lastReq  *http.Request
	lastBody []byte
	status   int
	respBody string
	err      error
	calls    int
}

func (c *stubClient) Do(req *http.Request) (*http.Response, error) {
	c.calls++
	c.lastReq = req
	if req.Body != nil {
		c.lastBody, _ = io.ReadAll(req.Body)
	}
	if c.err != nil {
		return nil, c.err
	}
	status := c.status
	if status == 0 {
		status = http.StatusOK
	}
	return &http.Response{
		StatusCode: status,
		Body:       io.NopCloser(bytes.NewReader([]byte(c.respBody))),
		Header:     make(http.Header),
	}, nil
}

func sampleStarted() JobStartedEvent {
	ts := time.Date(2026, 6, 23, 10, 0, 0, 0, time.UTC)
	return JobStartedEvent{
		Resource: ResourceAttributes{
			AzCopyVersion:    "10.32.2",
			SchemaVersion:    "1",
			OSType:           "linux",
			HostArch:         "amd64",
			HostNumCPU:       8,
			HostNICSpeedMbps: -1,
			AzureVMDetected:  true,
			InstallationID:   "abc123",
		},
		Dimensions: JobDimensions{
			Command:         "copy",
			FromTo:          "LocalBlob",
			SourceType:      "Local",
			DestType:        "Blob",
			SourceCloudType: "",
			DestCloudType:   "public",
			Options: OptionAttributes{
				FlagsSet: []string{"recursive", "put-md5"},
				Values: map[string]string{
					"OptRecursive":   "true",
					"OptBlockSizeMB": "8",
				},
			},
		},
		JobID:        "job-1234",
		InvocationID: "invocation-1234",
		Timestamp:    ts,
		StartedCount: 1,
	}
}

func sampleFinished() JobFinishedEvent {
	start := time.Date(2026, 6, 23, 10, 0, 0, 0, time.UTC)
	return JobFinishedEvent{
		Resource:                        sampleStarted().Resource,
		Dimensions:                      sampleStarted().Dimensions,
		JobID:                           "job-1234",
		InvocationID:                    "invocation-1234",
		StartTimestamp:                  start,
		EndTimestamp:                    start.Add(time.Minute),
		FinishedCount:                   1,
		JobStatus:                       "CompletedWithErrors",
		TerminalStage:                   "completed",
		JobErrorCategory:                "transfer",
		JobErrorCode:                    "transfer-failures",
		FailureErrorOtherCount:          2,
		PerformanceConstraint:           "Service",
		PerformanceAdviceCodes:          []string{"NetworkErrors", "AccountIOPS"},
		BytesEnumerated:                 2048,
		BytesExpected:                   1536,
		BytesTransferred:                1024,
		BytesOverWire:                   1100,
		ObjectsScheduled:                9,
		RegularFilesScheduled:           6,
		SymlinksScheduled:               2,
		HardlinksConvertedScheduled:     1,
		FolderPropertiesScheduled:       4,
		ObjectsCompleted:                7,
		ObjectsFailed:                   1,
		ObjectsSkipped:                  1,
		FolderPropertiesCompleted:       3,
		FolderPropertiesFailed:          0,
		FolderPropertiesSkipped:         1,
		SourceObjectsScanned:            20,
		SourceBytesScanned:              40960,
		SourceAverageObjectSizeBytes:    2048,
		SourceObjectSizeP50BytesApprox:  1024,
		SourceObjectSizeP90BytesApprox:  16 * 1024 * 1024,
		SourceObjectSizeP95BytesApprox:  256 * 1024 * 1024,
		SourceObjectsUnder1MiB:          12,
		SourceObjectsUnder1MiBRatioPct:  60,
		SourceMaxDirectoryDepth:         5,
		ContainersScanned:               3,
		ContainersTouched:               2,
		TransfersCompleted:              10,
		TransfersFailed:                 1,
		TransfersSkipped:                2,
		TransfersTotal:                  13,
		JobDurationSeconds:              60,
		EnumerationPhaseDurationSeconds: 40,
		TransferPhaseDurationSeconds:    50,
		JobThroughputMbps:               0.0001365,
		TransferPhaseThroughputMbps:     0.00016384,
		AverageStorageHTTPAttemptE2EMs:  42,
		AvgIOPS:                         100,
		StorageHTTPAttemptCount:         1000,
		NetworkErrorAttemptCount:        2,
		ServerBusy503Count:              15,
		ServerBusyThroughputCount:       10,
		ServerBusyIOPSCount:             3,
		ServerBusyOtherCount:            2,
		ServerBusyPct:                   1.5,
		NetworkErrorPct:                 0.2,
		PercentComplete:                 100,
	}
}

func TestParseConnectionString(t *testing.T) {
	m := parseConnectionString(testConnString)
	assert.Equal(t, "11111111-2222-3333-4444-555555555555", m["instrumentationkey"])
	assert.Equal(t, "https://eastus.example.com/", m["ingestionendpoint"])

	// Keys are case-insensitive; whitespace and malformed segments are tolerated.
	m = parseConnectionString(" A = 1 ; bogus ; b=2")
	assert.Equal(t, "1", m["a"])
	assert.Equal(t, "2", m["b"])
	_, ok := m["bogus"]
	assert.False(t, ok)
}

func TestEndpointAndKey(t *testing.T) {
	const ikey = "11111111-2222-3333-4444-555555555555"
	tests := []struct {
		name           string
		connection     string
		wantEndpoint   string
		wantErrContain string
	}{
		{
			name:         "explicit endpoint",
			connection:   testConnString,
			wantEndpoint: "https://eastus.example.com",
		},
		{
			name:         "explicit endpoint takes precedence",
			connection:   "InstrumentationKey=" + ikey + ";IngestionEndpoint=https://proxy.example.test/custom/;EndpointSuffix=ai.contoso.com;Location=westus2;Authorization=IKEY",
			wantEndpoint: "https://proxy.example.test/custom",
		},
		{
			name:         "case insensitive keys",
			connection:   " instrumentationkey=" + ikey + "; ingestionendpoint = https://example.test/ ",
			wantEndpoint: "https://example.test",
		},
		{
			name:         "endpoint suffix",
			connection:   "InstrumentationKey=" + ikey + ";EndpointSuffix=applicationinsights.azure.cn",
			wantEndpoint: "https://dc.applicationinsights.azure.cn",
		},
		{
			name:         "endpoint suffix with location",
			connection:   "InstrumentationKey=" + ikey + ";EndpointSuffix=ai.contoso.com;Location=westus2",
			wantEndpoint: "https://westus2.dc.ai.contoso.com",
		},
		{
			name:           "missing instrumentation key",
			connection:     "IngestionEndpoint=https://example.test",
			wantErrContain: "InstrumentationKey",
		},
		{
			name:           "missing endpoint configuration",
			connection:     "InstrumentationKey=" + ikey,
			wantErrContain: "IngestionEndpoint or EndpointSuffix",
		},
		{
			name:           "unsupported authorization",
			connection:     "InstrumentationKey=" + ikey + ";IngestionEndpoint=https://example.test;Authorization=AAD",
			wantErrContain: "unsupported",
		},
		{
			name:           "malformed endpoint",
			connection:     "InstrumentationKey=" + ikey + ";IngestionEndpoint=example.test",
			wantErrContain: "invalid",
		},
		{
			name:           "endpoint with query",
			connection:     "InstrumentationKey=" + ikey + ";IngestionEndpoint=https://example.test?route=ingestion",
			wantErrContain: "invalid",
		},
		{
			name:           "malformed endpoint suffix",
			connection:     "InstrumentationKey=" + ikey + ";EndpointSuffix=https://example.test",
			wantErrContain: "EndpointSuffix",
		},
		{
			name:           "malformed location",
			connection:     "InstrumentationKey=" + ikey + ";EndpointSuffix=ai.contoso.com;Location=west/us",
			wantErrContain: "Location",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			endpoint, gotKey, err := NewReporter(Config{ConnectionString: tt.connection}).endpointAndKey()
			if tt.wantErrContain != "" {
				require.Error(t, err)
				assert.Contains(t, err.Error(), tt.wantErrContain)
				return
			}
			require.NoError(t, err)
			assert.Equal(t, tt.wantEndpoint, endpoint)
			assert.Equal(t, ikey, gotKey)
		})
	}
}

func TestEventNamesAndTimestamps(t *testing.T) {
	s := sampleStarted()
	f := sampleFinished()
	assert.Equal(t, "azcopy.job.started", s.EventName())
	assert.Equal(t, "azcopy.job.finished", f.EventName())
	assert.Equal(t, s.Timestamp, s.timestamp())
	assert.Equal(t, f.EndTimestamp, f.timestamp())
}

func TestStartedMeasurements(t *testing.T) {
	m := sampleStarted().measurements()
	require.Len(t, m, 1)
	assert.Equal(t, "azcopy.job.started", m[0].Name)
	assert.Equal(t, float64(1), m[0].Value)
}

func TestFinishedMeasurements(t *testing.T) {
	m := sampleFinished().measurements()
	byName := map[string]float64{}
	for _, nm := range m {
		byName[nm.Name] = nm.Value
	}
	assert.Equal(t, float64(1024), byName["azcopy.bytes_transferred"])
	assert.Equal(t, float64(2), byName["azcopy.failure_error_other_count"])
	assert.Equal(t, float64(2048), byName["azcopy.bytes_enumerated"])
	assert.Equal(t, float64(1536), byName["azcopy.bytes_expected"])
	assert.Equal(t, float64(1100), byName["azcopy.bytes_over_wire"])
	assert.Equal(t, float64(9), byName["azcopy.objects_scheduled"])
	assert.Equal(t, float64(6), byName["azcopy.regular_files_scheduled"])
	assert.Equal(t, float64(2), byName["azcopy.symlinks_scheduled"])
	assert.Equal(t, float64(1), byName["azcopy.hardlinks_converted_scheduled"])
	assert.Equal(t, float64(4), byName["azcopy.folder_properties_scheduled"])
	assert.Equal(t, float64(7), byName["azcopy.objects_completed"])
	assert.Equal(t, float64(1), byName["azcopy.objects_failed"])
	assert.Equal(t, float64(1), byName["azcopy.objects_skipped"])
	assert.Equal(t, float64(3), byName["azcopy.folder_properties_completed"])
	assert.Equal(t, float64(0), byName["azcopy.folder_properties_failed"])
	assert.Equal(t, float64(1), byName["azcopy.folder_properties_skipped"])
	assert.Equal(t, float64(20), byName["azcopy.source_objects_scanned"])
	assert.Equal(t, float64(40960), byName["azcopy.source_bytes_scanned"])
	assert.Equal(t, float64(2048), byName["azcopy.source_average_object_size_bytes"])
	assert.Equal(t, float64(1024), byName["azcopy.source_object_size_p50_bytes_approx"])
	assert.Equal(t, float64(16*1024*1024), byName["azcopy.source_object_size_p90_bytes_approx"])
	assert.Equal(t, float64(256*1024*1024), byName["azcopy.source_object_size_p95_bytes_approx"])
	assert.Equal(t, float64(12), byName["azcopy.source_objects_under_1_mib"])
	assert.Equal(t, float64(60), byName["azcopy.source_objects_under_1_mib_ratio_pct"])
	assert.Equal(t, float64(5), byName["azcopy.source_max_directory_depth"])
	assert.Equal(t, float64(3), byName["azcopy.containers_scanned"])
	assert.Equal(t, float64(2), byName["azcopy.containers_touched"])
	assert.Equal(t, float64(10), byName["azcopy.transfers_completed"])
	assert.Equal(t, float64(1), byName["azcopy.transfers_failed"])
	assert.Equal(t, float64(2), byName["azcopy.transfers_skipped"])
	assert.Equal(t, float64(13), byName["azcopy.transfers_total"])
	assert.Equal(t, float64(60), byName["azcopy.job_duration_seconds"])
	assert.Equal(t, float64(40), byName["azcopy.enumeration_phase_duration_seconds"])
	assert.Equal(t, float64(50), byName["azcopy.transfer_phase_duration_seconds"])
	assert.Equal(t, 0.0001365, byName["azcopy.job_throughput_mbps"])
	assert.Equal(t, 0.00016384, byName["azcopy.transfer_phase_throughput_mbps"])
	assert.Equal(t, float64(42), byName["azcopy.average_storage_http_attempt_e2e_ms"])
	assert.Equal(t, float64(100), byName["azcopy.avg_iops"])
	assert.Equal(t, float64(1000), byName["azcopy.storage_http_attempt_count"])
	assert.Equal(t, float64(2), byName["azcopy.network_error_attempt_count"])
	assert.Equal(t, float64(15), byName["azcopy.server_busy_503_count"])
	assert.Equal(t, float64(10), byName["azcopy.server_busy_throughput_count"])
	assert.Equal(t, float64(3), byName["azcopy.server_busy_iops_count"])
	assert.Equal(t, float64(2), byName["azcopy.server_busy_other_count"])
	assert.InDelta(t, 1.5, byName["azcopy.server_busy_pct"], 1e-9)
	assert.InDelta(t, 0.2, byName["azcopy.network_error_pct"], 1e-9)
	assert.Equal(t, float64(100), byName["azcopy.percent_complete"])
	assert.Len(t, m, 50)
}

func TestCommandInvokedEvent(t *testing.T) {
	ts := time.Date(2026, 6, 23, 10, 0, 0, 0, time.UTC)
	e := CommandInvokedEvent{
		Resource: sampleStarted().Resource,
		Command:  "login",
		Options: OptionAttributes{
			FlagsSet: []string{"method", "tenant"},
			Values:   map[string]string{"OptLoginType": "device"},
		},
		JobID:        "job-9999",
		InvocationID: "invocation-9999",
		Timestamp:    ts,
		InvokedCount: 1,
	}
	assert.Equal(t, "azcopy.command.invoked", e.EventName())
	assert.Equal(t, ts, e.timestamp())

	m := e.measurements()
	require.Len(t, m, 1)
	assert.Equal(t, "azcopy.command.invoked", m[0].Name)
	assert.Equal(t, float64(1), m[0].Value)

	attrs := e.attributes()
	assert.Equal(t, "login", attrs["Command"])
	assert.Equal(t, "method,tenant", attrs["OptFlagsSet"])
	assert.Equal(t, "device", attrs["OptLoginType"])
	assert.Equal(t, "job-9999", attrs["JobID"])
	assert.Equal(t, "invocation-9999", attrs["InvocationID"])
	// Resource attributes are included.
	assert.Equal(t, "10.32.2", attrs["AzCopyVersion"])
	_, hasServiceName := attrs["ServiceName"]
	assert.False(t, hasServiceName)
	_, hasServiceVersion := attrs["ServiceVersion"]
	assert.False(t, hasServiceVersion)
	// No job dimensions on a command.invoked event.
	_, hasFromTo := attrs["FromTo"]
	assert.False(t, hasFromTo)

	// Empty JobID is omitted.
	e.JobID = ""
	_, hasJobID := e.attributes()["JobID"]
	assert.False(t, hasJobID)
}

func TestAttributesIncludeResourceAndDimensions(t *testing.T) {
	attrs := sampleFinished().attributes()
	assert.Equal(t, "10.32.2", attrs["AzCopyVersion"])
	_, hasServiceName := attrs["ServiceName"]
	assert.False(t, hasServiceName)
	_, hasServiceVersion := attrs["ServiceVersion"]
	assert.False(t, hasServiceVersion)
	assert.Equal(t, "1", attrs["SchemaVersion"])
	assert.Equal(t, "true", attrs["AzureVMDetected"])
	_, hasHostVirtualization := attrs["HostVirtualization"]
	assert.False(t, hasHostVirtualization)
	_, hasNetworkRunContext := attrs["NetworkRunContext"]
	assert.False(t, hasNetworkRunContext)
	assert.Equal(t, "copy", attrs["Command"])
	_, hasAttemptType := attrs["AttemptType"]
	assert.False(t, hasAttemptType)
	_, hasMeasurementScope := attrs["MeasurementScope"]
	assert.False(t, hasMeasurementScope)
	assert.Equal(t, "true", attrs["OptRecursive"])
	assert.Equal(t, "8", attrs["OptBlockSizeMB"])
	assert.Equal(t, "recursive,put-md5", attrs["OptFlagsSet"])
	// JobStatus is only present on the finished event.
	assert.Equal(t, "CompletedWithErrors", attrs["JobStatus"])
	_, hasTerminalReason := attrs["TerminalReason"]
	assert.False(t, hasTerminalReason)
	assert.Equal(t, "completed", attrs["TerminalStage"])
	assert.Equal(t, "transfer", attrs["JobErrorCategory"])
	assert.Equal(t, "transfer-failures", attrs["JobErrorCode"])
	assert.Equal(t, "Service", attrs["PerformanceConstraint"])
	_, hasPrimaryAdvice := attrs["PrimaryPerformanceAdviceCode"]
	assert.False(t, hasPrimaryAdvice)
	assert.Equal(t, "NetworkErrors,AccountIOPS", attrs["PerformanceAdviceCodes"])
	_, hasStatus := sampleStarted().attributes()["JobStatus"]
	assert.False(t, hasStatus)
	_, hasE2ETestRunID := attrs["E2ETestRunID"]
	assert.False(t, hasE2ETestRunID)
}

func TestE2ETestRunIDIsIncludedAndBoundedWhenConfigured(t *testing.T) {
	event := sampleFinished()
	event.Resource.E2ETestRunID = strings.Repeat("r", maxIdentifierValueLen+100)

	attrs := event.attributes()
	assert.Len(t, attrs["E2ETestRunID"], maxIdentifierValueLen)
	assert.True(t, strings.HasSuffix(attrs["E2ETestRunID"], truncatedPropertyMarker))

	command := CommandInvokedEvent{
		Resource:     event.Resource,
		Command:      "jobs.list",
		Timestamp:    time.Now(),
		InvokedCount: 1,
	}
	assert.Equal(t, attrs["E2ETestRunID"], command.attributes()["E2ETestRunID"])
}

func TestBenchmarkDimensionsProperties(t *testing.T) {
	properties := JobDimensions{
		Command:                   "bench",
		BenchmarkMode:             "upload",
		BenchmarkFileCount:        100,
		BenchmarkFileSizeBytes:    256 * 1024 * 1024,
		BenchmarkFolderCount:      10,
		BenchmarkCleanupRequested: true,
		BenchmarkIsCleanup:        false,
	}.props()
	assert.Equal(t, "upload", properties["BenchmarkMode"])
	assert.Equal(t, "100", properties["BenchmarkFileCount"])
	assert.Equal(t, "268435456", properties["BenchmarkFileSizeBytes"])
	assert.Equal(t, "10", properties["BenchmarkFolderCount"])
	assert.Equal(t, "true", properties["BenchmarkCleanupRequested"])
	assert.Equal(t, "false", properties["BenchmarkIsCleanup"])
}

func TestResumeDimensionsProperties(t *testing.T) {
	properties := JobDimensions{
		Command:             "jobs.resume",
		SummaryCounterScope: "job-cumulative",
	}.props()
	assert.Equal(t, "jobs.resume", properties["Command"])
	assert.Equal(t, "job-cumulative", properties["SummaryCounterScope"])
}

func TestJobIDCorrelatesEvents(t *testing.T) {
	// The started and finished events for an attempt share JobID and InvocationID.
	started := sampleStarted().attributes()
	finished := sampleFinished().attributes()
	assert.Equal(t, "job-1234", started["JobID"])
	assert.Equal(t, "job-1234", finished["JobID"])
	assert.Equal(t, started["JobID"], finished["JobID"])
	assert.Equal(t, "invocation-1234", started["InvocationID"])
	assert.Equal(t, started["InvocationID"], finished["InvocationID"])
}

func TestOptFlagsSetTruncation(t *testing.T) {
	// A run with many flags must not produce an OptFlagsSet value larger than
	// the cap, so the telemetry payload stays bounded.
	flags := make([]string, 0, 500)
	for i := 0; i < 500; i++ {
		flags = append(flags, "--some-long-flag-name")
	}
	attrs := JobDimensions{Options: OptionAttributes{FlagsSet: flags}}.props()
	val := attrs["OptFlagsSet"]
	assert.LessOrEqual(t, len(val), maxPropValueLen)
	assert.True(t, strings.HasSuffix(val, "...(truncated)"))

	// A short flag set is left untouched.
	short := JobDimensions{Options: OptionAttributes{FlagsSet: []string{"recursive", "put-md5"}}}.props()
	assert.Equal(t, "recursive,put-md5", short["OptFlagsSet"])
	assert.NotContains(t, short["OptFlagsSet"], "truncated")
}

func TestBoundPropertiesAppliesDefaultAndSpecificLimits(t *testing.T) {
	oversized := strings.Repeat("x", maxPropValueLen+100)
	properties := map[string]string{
		"FutureProperty": oversized,
		"OptFutureValue": oversized,
	}
	for name := range propertyValueLimits {
		properties[name] = oversized
	}

	boundProperties(properties)
	for name, value := range properties {
		assert.LessOrEqual(t, len(value), propertyValueLimit(name), name)
		if propertyValueLimit(name) > len(truncatedPropertyMarker) {
			assert.True(t, strings.HasSuffix(value, truncatedPropertyMarker), name)
		}
	}
	assert.Len(t, properties["OptFutureValue"], maxOptionValueLen)
	assert.Len(t, properties["FutureProperty"], maxPropValueLen)
}

func TestTruncateValueToPreservesUTF8(t *testing.T) {
	value := strings.Repeat("界", maxPropValueLen)
	truncated := truncateValueTo(value, maxPropValueLen)

	assert.LessOrEqual(t, len(truncated), maxPropValueLen)
	assert.True(t, utf8.ValidString(truncated))
	assert.True(t, strings.HasSuffix(truncated, truncatedPropertyMarker))
}

func TestEventPropertiesAreBoundedBeforeExport(t *testing.T) {
	oversized := strings.Repeat("x", maxPropValueLen+100)
	event := sampleFinished()
	event.Resource.OSVersion = oversized
	event.Resource.HostCPUModel = oversized
	event.Dimensions.Options = OptionAttributes{
		FlagsSet: []string{oversized, oversized},
		Values:   map[string]string{"OptFutureValue": oversized},
	}
	event.JobID = oversized
	event.InvocationID = oversized
	event.JobStatus = oversized
	event.FailureErrorCodes = oversized

	envelope := eventToEnvelopes("ikey-1", event)[0]
	for name, value := range envelope.Data.BaseData.Properties {
		assert.LessOrEqual(t, len(value), propertyValueLimit(name), name)
	}
	assert.Len(t, envelope.Data.BaseData.Properties["OSVersion"], maxHostValueLen)
	assert.Len(t, envelope.Data.BaseData.Properties["JobID"], maxIdentifierValueLen)
	assert.Len(t, envelope.Data.BaseData.Properties["OptFutureValue"], maxOptionValueLen)

	command := CommandInvokedEvent{
		Resource:     event.Resource,
		Command:      oversized,
		Options:      event.Dimensions.Options,
		JobID:        oversized,
		InvocationID: oversized,
		Timestamp:    time.Now(),
		InvokedCount: 1,
	}
	for name, value := range command.attributes() {
		assert.LessOrEqual(t, len(value), propertyValueLimit(name), name)
	}
}

func TestUnsetOptionsAreOmitted(t *testing.T) {
	attrs := JobDimensions{Command: "copy"}.props()
	for _, key := range []string{"OptFlagsSet", "OptEnvVarsSet", "OptRecursive", "OptBlockSizeMB", "OptConcurrency"} {
		_, exists := attrs[key]
		assert.False(t, exists, key)
	}
}

func TestMergeProps(t *testing.T) {
	out := mergeProps(
		map[string]string{"a": "1", "b": "1"},
		map[string]string{"b": "2", "c": "3"},
	)
	assert.Equal(t, map[string]string{"a": "1", "b": "2", "c": "3"}, out)
}

func TestEventToEnvelopes(t *testing.T) {
	envelopes := eventToEnvelopes("ikey-1", sampleFinished())
	require.Len(t, envelopes, 1)
	envelope := envelopes[0]
	assert.Equal(t, "Microsoft.ApplicationInsights.Event", envelope.Name)
	assert.Equal(t, "ikey-1", envelope.IKey)
	assert.Equal(t, "EventData", envelope.Data.BaseType)
	assert.Equal(t, 2, envelope.Data.BaseData.Version)
	assert.Equal(t, "azcopy.job.finished", envelope.Data.BaseData.Name)
	assert.Len(t, envelope.Data.BaseData.Measurements, 50)
	assert.Equal(t, float64(1), envelope.Data.BaseData.Measurements["azcopy.job.finished"])
	assert.Equal(t, float64(1024), envelope.Data.BaseData.Measurements["azcopy.bytes_transferred"])
	assert.Equal(t, float64(100), envelope.Data.BaseData.Measurements["azcopy.percent_complete"])
	assert.Equal(t, "copy", envelope.Data.BaseData.Properties["Command"])
	assert.Equal(t, "", envelope.Data.BaseData.Properties["SourceCloudType"])
	assert.Equal(t, "public", envelope.Data.BaseData.Properties["DestCloudType"])
	_, hasSourceEndpointIdentity := envelope.Data.BaseData.Properties["SourceEndpointIdentity"]
	assert.False(t, hasSourceEndpointIdentity)
	_, hasDestEndpointIdentity := envelope.Data.BaseData.Properties["DestEndpointIdentity"]
	assert.False(t, hasDestEndpointIdentity)
	_, hasSourceStorageAccount := envelope.Data.BaseData.Properties["SourceStorageAccount"]
	assert.False(t, hasSourceStorageAccount)
	_, hasDestStorageAccount := envelope.Data.BaseData.Properties["DestStorageAccount"]
	assert.False(t, hasDestStorageAccount)
	_, hasCloudType := envelope.Data.BaseData.Properties["CloudType"]
	assert.False(t, hasCloudType)
}

func TestReportEventAppInsights(t *testing.T) {
	client := &stubClient{status: http.StatusOK}
	r := NewReporter(Config{
		Backend:          BackendAppInsights,
		ConnectionString: testConnString,
		HTTPClient:       client,
	})

	err := r.ReportEvent(context.Background(), sampleFinished())
	require.NoError(t, err)
	require.Equal(t, 1, client.calls)

	// Validate the request targets the track endpoint with JSON content.
	assert.Equal(t, http.MethodPost, client.lastReq.Method)
	assert.Equal(t, "https://eastus.example.com/v2.1/track", client.lastReq.URL.String())
	assert.Equal(t, "application/json", client.lastReq.Header.Get("Content-Type"))

	var envs []appInsightsEnvelope
	require.NoError(t, json.Unmarshal(client.lastBody, &envs))
	require.Len(t, envs, 1)
	assert.Equal(t, "Microsoft.ApplicationInsights.Event", envs[0].Name)
	assert.Equal(t, "EventData", envs[0].Data.BaseType)
	assert.Equal(t, "azcopy.job.finished", envs[0].Data.BaseData.Name)
	assert.Len(t, envs[0].Data.BaseData.Measurements, 50)
	assert.Equal(t, float64(100), envs[0].Data.BaseData.Measurements["azcopy.percent_complete"])
}

func TestReportEventAppInsightsServerError(t *testing.T) {
	client := &stubClient{status: http.StatusInternalServerError, respBody: "boom"}
	r := NewReporter(Config{
		Backend:          BackendAppInsights,
		ConnectionString: testConnString,
		HTTPClient:       client,
	})
	err := r.ReportEvent(context.Background(), sampleStarted())
	require.Error(t, err)
	assert.Contains(t, err.Error(), "500")
}

func TestReportEventAppInsightsRejectsPartialAcceptance(t *testing.T) {
	client := &stubClient{
		status:   http.StatusPartialContent,
		respBody: `{"itemsReceived":2,"itemsAccepted":1,"errors":[{"index":1,"statusCode":400,"message":"invalid field"}]}`,
	}
	r := NewReporter(Config{
		Backend:          BackendAppInsights,
		ConnectionString: testConnString,
		HTTPClient:       client,
	})

	err := r.ReportEvent(context.Background(), sampleStarted())
	require.Error(t, err)
	assert.ErrorContains(t, err, "partially accepted telemetry")
	assert.ErrorContains(t, err, "index=1 status=400")
}

func TestReportEventAppInsightsRejectsInvalidPartialResponses(t *testing.T) {
	tests := []struct {
		name string
		body string
		want string
	}{
		{name: "empty", body: "", want: "invalid response"},
		{name: "malformed", body: "{", want: "invalid response"},
		{name: "invalid counts", body: `{"itemsReceived":1,"itemsAccepted":2}`, want: "invalid item counts"},
		{name: "missing errors", body: `{"itemsReceived":2,"itemsAccepted":1}`, want: "inconsistent rejection details"},
		{name: "invalid index", body: `{"itemsReceived":2,"itemsAccepted":1,"errors":[{"index":2}]}`, want: "invalid rejected item index"},
		{name: "no rejection", body: `{"itemsReceived":1,"itemsAccepted":1}`, want: "inconsistent rejection details"},
		{name: "trailing content", body: `{"itemsReceived":2,"itemsAccepted":1,"errors":[{"index":1,"statusCode":400}]} trailing`, want: "trailing content"},
		{name: "duplicate index", body: `{"itemsReceived":3,"itemsAccepted":1,"errors":[{"index":1,"statusCode":400},{"index":1,"statusCode":400}]}`, want: "duplicate rejected item index"},
		{name: "invalid status", body: `{"itemsReceived":2,"itemsAccepted":1,"errors":[{"index":1,"statusCode":0}]}`, want: "invalid rejection status"},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			client := &stubClient{status: http.StatusPartialContent, respBody: test.body}
			r := NewReporter(Config{
				Backend:          BackendAppInsights,
				ConnectionString: testConnString,
				HTTPClient:       client,
			})

			err := r.ReportEvent(context.Background(), sampleStarted())
			require.Error(t, err)
			assert.ErrorContains(t, err, test.want)
		})
	}
}

func TestReportEventTransportError(t *testing.T) {
	client := &stubClient{err: errors.New("network down")}
	r := NewReporter(Config{
		Backend:          BackendAppInsights,
		ConnectionString: testConnString,
		HTTPClient:       client,
	})
	err := r.ReportEvent(context.Background(), sampleStarted())
	require.Error(t, err)
	assert.Contains(t, err.Error(), "network down")
}

func TestReportEventOTel(t *testing.T) {
	client := &stubClient{status: http.StatusOK}
	r := NewReporter(Config{
		Backend:          BackendOTel,
		ConnectionString: testConnString,
		HTTPClient:       client,
	})

	err := r.ReportEvent(context.Background(), sampleFinished())
	require.NoError(t, err)
	require.Equal(t, 1, client.calls)
	assert.Equal(t, "https://eastus.example.com/v2.1/track", client.lastReq.URL.String())

	var envs []appInsightsEnvelope
	require.NoError(t, json.Unmarshal(client.lastBody, &envs))
	require.Len(t, envs, 1)
	assert.Equal(t, "Microsoft.ApplicationInsights.Event", envs[0].Name)
	assert.Equal(t, "EventData", envs[0].Data.BaseType)
	assert.Equal(t, "azcopy.job.finished", envs[0].Data.BaseData.Name)
	assert.Len(t, envs[0].Data.BaseData.Measurements, 50)
	assert.Equal(t, float64(100), envs[0].Data.BaseData.Measurements["azcopy.percent_complete"])
	assert.Equal(t, "copy", envs[0].Data.BaseData.Properties["Command"])
}

func TestReportEventBackendsBoundProperties(t *testing.T) {
	for _, backend := range []Backend{BackendAppInsights, BackendOTel} {
		t.Run(string(backend), func(t *testing.T) {
			oversized := strings.Repeat("x", maxPropValueLen+100)
			event := sampleFinished()
			event.Resource.OSVersion = oversized
			event.Dimensions.Options.Values = map[string]string{"OptFutureValue": oversized}
			event.JobID = oversized

			client := &stubClient{status: http.StatusOK}
			reporter := NewReporter(Config{
				Backend:          backend,
				ConnectionString: testConnString,
				HTTPClient:       client,
			})
			require.NoError(t, reporter.ReportEvent(context.Background(), event))

			var envelopes []appInsightsEnvelope
			require.NoError(t, json.Unmarshal(client.lastBody, &envelopes))
			require.Len(t, envelopes, 1)
			for name, value := range envelopes[0].Data.BaseData.Properties {
				assert.LessOrEqual(t, len(value), propertyValueLimit(name), name)
			}
			assert.Len(t, envelopes[0].Data.BaseData.Properties["OSVersion"], maxHostValueLen)
			assert.Len(t, envelopes[0].Data.BaseData.Properties["JobID"], maxIdentifierValueLen)
			assert.Len(t, envelopes[0].Data.BaseData.Properties["OptFutureValue"], maxOptionValueLen)
		})
	}
}

func TestReportEvents_StopsOnFirstError(t *testing.T) {
	client := &stubClient{status: http.StatusInternalServerError}
	r := NewReporter(Config{
		Backend:          BackendAppInsights,
		ConnectionString: testConnString,
		HTTPClient:       client,
	})
	err := r.ReportEvents(context.Background(), sampleStarted(), sampleFinished())
	require.Error(t, err)
	assert.Equal(t, 1, client.calls) // stopped after the first failed send
}

func TestReportEventUnknownBackend(t *testing.T) {
	r := NewReporter(Config{Backend: "nope", ConnectionString: testConnString})
	err := r.ReportEvent(context.Background(), sampleStarted())
	require.Error(t, err)
	assert.Contains(t, err.Error(), "unknown telemetry backend")
}

func TestReportEventMissingConnectionString(t *testing.T) {
	r := NewReporter(Config{Backend: BackendAppInsights, ConnectionString: ""})
	err := r.ReportEvent(context.Background(), sampleStarted())
	require.Error(t, err)
}
