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
	"github.com/stretchr/testify/assert"
	"strings"
	"testing"
	"time"
	"unicode/utf8"
)

func sampleStarted() JobStartedEvent {
	ts := time.Date(2026, 6, 23, 10, 0, 0, 0, time.UTC)
	return JobStartedEvent{
		Resource: ResourceAttributes{
			AzCopyVersion:    "10.32.2",
			SchemaVersion:    "1",
			OSType:           "linux",
			HostArch:         "amd64",
			HostNumCPU:       8,
			AzureVMDetected:  true,
			InstallationID:   "abc123",
		},
		Dimensions: JobDimensions{
			Command:            "copy",
			FromTo:             "LocalBlob",
			SourceType:         "Local",
			DestType:           "Blob",
			SourceCloudType:    "",
			DestCloudType:      "public",
			DestStorageAccount: "account",
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
	}
}

func sampleFinished() JobFinishedEvent {
	start := time.Date(2026, 6, 23, 10, 0, 0, 0, time.UTC)
	return JobFinishedEvent{
		Resource:               sampleStarted().Resource,
		Dimensions:             sampleStarted().Dimensions,
		JobID:                  "job-1234",
		InvocationID:           "invocation-1234",
		EndTimestamp:           start.Add(time.Minute),
		JobStatus:              "CompletedWithErrors",
		TerminalStage:          "completed",
		JobErrorCategory:       "transfer",
		JobErrorCode:           "transfer-failures",
		PerformanceConstraint:  "Service",
		PerformanceAdviceCodes: []string{"NetworkErrors", "AccountIOPS"},
		Measurements: JobMeasurements{
			FailureErrorOtherCount:          2,
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
		},
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
	assert.Empty(t, m)
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
	assert.Len(t, m, 49)
	assert.NotContains(t, byName, "azcopy.job.finished")
}

func TestFinishedMeasurementOmissionRules(t *testing.T) {
	for _, test := range []struct {
		name       string
		cumulative bool
		incomplete bool
	}{
		{name: "available"},
		{name: "cumulative", cumulative: true},
		{name: "incomplete", incomplete: true},
		{name: "cumulative and incomplete", cumulative: true, incomplete: true},
	} {
		t.Run(test.name, func(t *testing.T) {
			event := sampleFinished()
			event.Measurements.BucketsScanned = 4
			event.Measurements.BucketsTouched = 2
			if test.cumulative {
				event.Dimensions.SummaryCounterScope = "job-cumulative"
			}
			if test.incomplete {
				event.Measurements.ContainersScanned = -1
				event.Measurements.ContainersTouched = -1
				event.Measurements.BucketsScanned = -1
				event.Measurements.BucketsTouched = -1
			}
			metrics := map[string]float64{}
			for _, metric := range event.measurements() {
				metrics[metric.Name] = metric.Value
			}
			assert.NotContains(t, metrics, "azcopy.job.finished")
			assert.EqualValues(t, 1024, metrics["azcopy.bytes_transferred"])
			assert.EqualValues(t, 2, metrics["azcopy.failure_error_other_count"])
			for _, name := range []string{"azcopy.job_throughput_mbps", "azcopy.transfer_phase_throughput_mbps"} {
				_, present := metrics[name]
				assert.Equal(t, !test.cumulative, present, name)
			}
			for _, name := range []string{"azcopy.containers_scanned", "azcopy.containers_touched", "azcopy.buckets_scanned", "azcopy.buckets_touched"} {
				_, present := metrics[name]
				assert.Equal(t, !test.incomplete, present, name)
			}
			properties := event.properties()
			assert.Equal(t, "CompletedWithErrors", properties["JobStatus"])
			if test.cumulative {
				assert.Equal(t, "unavailable-cumulative-summary", properties["ThroughputStatus"])
			} else {
				assert.NotContains(t, properties, "ThroughputStatus")
			}
			if test.incomplete {
				assert.Equal(t, "incomplete", properties["SourceScopeCountsStatus"])
			} else {
				assert.NotContains(t, properties, "SourceScopeCountsStatus")
			}
		})
	}
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
	}
	assert.Equal(t, "azcopy.command.invoked", e.EventName())
	assert.Equal(t, ts, e.timestamp())

	m := e.measurements()
	assert.Empty(t, m)

	attrs := e.properties()
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
	_, hasJobID := e.properties()["JobID"]
	assert.False(t, hasJobID)
}

func TestAttributesIncludeResourceAndDimensions(t *testing.T) {
	attrs := sampleFinished().properties()
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
	_, hasStatus := sampleStarted().properties()["JobStatus"]
	assert.False(t, hasStatus)
	_, hasE2ETestRunID := attrs["E2ETestRunID"]
	assert.False(t, hasE2ETestRunID)
}

func TestE2ETestRunIDIsIncludedAndBoundedWhenConfigured(t *testing.T) {
	event := sampleFinished()
	event.Resource.E2ETestRunID = strings.Repeat("r", maxIdentifierValueLen+100)

	attrs := event.properties()
	assert.Len(t, attrs["E2ETestRunID"], maxIdentifierValueLen)
	assert.True(t, strings.HasSuffix(attrs["E2ETestRunID"], truncatedPropertyMarker))

	command := CommandInvokedEvent{
		Resource:  event.Resource,
		Command:   "jobs.list",
		Timestamp: time.Now(),
	}
	assert.Equal(t, attrs["E2ETestRunID"], command.properties()["E2ETestRunID"])
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
	started := sampleStarted().properties()
	finished := sampleFinished().properties()
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
	attrs := JobStartedEvent{Dimensions: JobDimensions{Options: OptionAttributes{FlagsSet: flags}}}.properties()
	val := attrs["OptFlagsSet"]
	assert.LessOrEqual(t, len(val), maxPropValueLen)
	assert.True(t, strings.HasSuffix(val, "...(truncated)"))

	// A short flag set is left untouched.
	short := JobDimensions{Options: OptionAttributes{FlagsSet: []string{"recursive", "put-md5"}}}.props()
	assert.Equal(t, "recursive,put-md5", short["OptFlagsSet"])
	assert.NotContains(t, short["OptFlagsSet"], "truncated")
}

func TestOptionValuesCannotOverwriteContractProperties(t *testing.T) {
	attributes := JobStartedEvent{
		Dimensions: JobDimensions{
			Command: "copy",
			Options: OptionAttributes{
				FlagsSet:   []string{"recursive"},
				EnvVarsSet: []string{"AZCOPY_CONCURRENCY_VALUE"},
				Values: map[string]string{
					"Command":       "sync",
					"OptFlagsSet":   "overwrite",
					"OptEnvVarsSet": "overwrite",
					"Opt-Invalid":   "invalid",
					"OptSafeValue":  "preserved",
				},
			},
		},
	}.properties()

	assert.Equal(t, "copy", attributes["Command"])
	assert.Equal(t, "recursive", attributes["OptFlagsSet"])
	assert.Equal(t, "AZCOPY_CONCURRENCY_VALUE", attributes["OptEnvVarsSet"])
	assert.Equal(t, "preserved", attributes["OptSafeValue"])
	assert.NotContains(t, attributes, "Opt-Invalid")
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

func TestTruncateValueToHandlesSmallLimits(t *testing.T) {
	for _, test := range []struct {
		name     string
		value    string
		maxBytes int
		want     string
	}{
		{name: "zero", value: "value", maxBytes: 0, want: ""},
		{name: "negative", value: "value", maxBytes: -1, want: ""},
		{name: "shorter than marker", value: "abcdef", maxBytes: 2, want: "ab"},
		{name: "no partial rune", value: "界界", maxBytes: 2, want: ""},
	} {
		t.Run(test.name, func(t *testing.T) {
			actual := truncateValueTo(test.value, test.maxBytes)
			assert.Equal(t, test.want, actual)
			assert.True(t, utf8.ValidString(actual))
			assert.LessOrEqual(t, len(actual), max(test.maxBytes, 0))
		})
	}
}
