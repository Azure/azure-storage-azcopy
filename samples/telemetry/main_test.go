package main

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	aztelemetry "github.com/Azure/azure-storage-azcopy/v10/telemetry"
)

func TestBuildSampleEvents(t *testing.T) {
	events := buildSampleEvents(time.Date(2026, 7, 21, 12, 0, 0, 0, time.UTC))
	require.Len(t, events, 21)

	counts := map[string]map[string]int{
		"copy": {},
		"sync": {},
	}
	fromToCounts := make(map[string]int)
	commandEvents := 0
	for _, event := range events {
		switch typed := event.(type) {
		case aztelemetry.CommandInvokedEvent:
			commandEvents++
			assert.Equal(t, "jobs.list", typed.Command)
			assert.NotEmpty(t, typed.JobID)
			assert.NotEmpty(t, typed.InvocationID)
		case aztelemetry.JobStartedEvent:
			counts[typed.Dimensions.Command]["started"]++
			fromToCounts[typed.Dimensions.FromTo]++
			assert.False(t, typed.Resource.AzureVMDetected)
			assert.Equal(t, "telemetry-sample-v1", typed.Resource.SamplerVersion)
			assert.Equal(t, "3", typed.Resource.SchemaVersion)
			assert.Equal(t, 1.0, typed.Resource.SamplingRate)
			assert.NotEmpty(t, typed.JobID)
			assert.NotEmpty(t, typed.InvocationID)
		case aztelemetry.JobFinishedEvent:
			counts[typed.Dimensions.Command]["finished"]++
			assert.Positive(t, typed.BytesEnumerated)
			assert.Positive(t, typed.ObjectsScheduled)
			assert.Positive(t, typed.StorageHTTPAttemptCount)
			assert.Positive(t, typed.JobDurationSeconds)
			assert.NotEmpty(t, typed.TerminalStage)
			assert.LessOrEqual(t, typed.BytesTransferred, typed.BytesExpected)
			assert.LessOrEqual(t, typed.BytesExpected, typed.BytesEnumerated)
			assert.Greater(t, typed.BytesOverWire, typed.BytesTransferred)
			assert.GreaterOrEqual(t, typed.SourceBytesScanned, typed.BytesEnumerated)
			if typed.ObjectsCompleted == typed.ObjectsScheduled {
				assert.Equal(t, typed.BytesEnumerated, typed.BytesExpected)
				assert.Equal(t, typed.BytesEnumerated, typed.BytesTransferred)
			} else {
				assert.Equal(t, typed.BytesTransferred, typed.BytesExpected)
			}
			assert.Equal(t, typed.ObjectsScheduled, typed.RegularFilesScheduled+typed.SymlinksScheduled+typed.HardlinksConvertedScheduled)
			assert.Equal(t, typed.ObjectsScheduled, typed.ObjectsCompleted+typed.ObjectsFailed+typed.ObjectsSkipped)
			assert.Equal(t, typed.FolderPropertiesScheduled, typed.FolderPropertiesCompleted+typed.FolderPropertiesFailed+typed.FolderPropertiesSkipped)
			assert.Equal(t, typed.ObjectsCompleted+typed.FolderPropertiesCompleted, typed.TransfersCompleted)
			assert.Equal(t, typed.ObjectsFailed+typed.FolderPropertiesFailed, typed.TransfersFailed)
			assert.Equal(t, typed.ObjectsSkipped+typed.FolderPropertiesSkipped, typed.TransfersSkipped)
			assert.Equal(t, typed.ObjectsScheduled+typed.FolderPropertiesScheduled, typed.TransfersTotal)
			assert.Equal(t, typed.TransfersTotal, typed.TransfersCompleted+typed.TransfersFailed+typed.TransfersSkipped)
			assert.InDelta(t, 100*float64(typed.NetworkErrorAttemptCount)/float64(typed.StorageHTTPAttemptCount), typed.NetworkErrorPct, 0.0001)
			assert.Equal(t, typed.ServerBusyThroughputCount+typed.ServerBusyIOPSCount+typed.ServerBusyOtherCount, typed.ServerBusy503Count)
			assert.InDelta(t, 100*float64(typed.ServerBusy503Count)/float64(typed.StorageHTTPAttemptCount), typed.ServerBusyPct, 0.0001)
			assert.InDelta(t, float64(typed.BytesTransferred)*8/1_000_000/typed.JobDurationSeconds, typed.JobThroughputMbps, 0.0001)
			assert.InDelta(t, float64(typed.BytesTransferred)*8/1_000_000/typed.TransferPhaseDurationSeconds, typed.TransferPhaseThroughputMbps, 0.0001)
			switch typed.Dimensions.SourceType {
			case "Local":
				assert.Zero(t, typed.ContainersScanned)
				assert.Zero(t, typed.ContainersTouched)
				assert.Zero(t, typed.BucketsScanned)
				assert.Zero(t, typed.BucketsTouched)
			case "Blob", "BlobFS", "File":
				assert.Positive(t, typed.ContainersScanned)
				assert.Positive(t, typed.ContainersTouched)
				assert.LessOrEqual(t, typed.ContainersTouched, typed.ContainersScanned)
				assert.Zero(t, typed.BucketsScanned)
				assert.Zero(t, typed.BucketsTouched)
			case "S3", "GCP":
				assert.Zero(t, typed.ContainersScanned)
				assert.Zero(t, typed.ContainersTouched)
				assert.Equal(t, int64(1), typed.BucketsScanned)
				assert.Equal(t, int64(1), typed.BucketsTouched)
			}
		default:
			t.Fatalf("unexpected event type %T", event)
		}
	}

	assert.Equal(t, 1, commandEvents)
	assert.Equal(t, jobsPerCommand, counts["copy"]["started"])
	assert.Equal(t, jobsPerCommand, counts["copy"]["finished"])
	assert.Equal(t, jobsPerCommand, counts["sync"]["started"])
	assert.Equal(t, jobsPerCommand, counts["sync"]["finished"])
	assert.Equal(t, map[string]int{"LocalBlob": 2, "BlobLocal": 2, "BlobBlob": 2, "S3Blob": 2, "GCPBlob": 2}, fromToCounts)
}

func TestSampleDimensionVariants(t *testing.T) {
	tests := []struct {
		index         int
		fromTo        string
		sourceCloud   string
		destCloud     string
		sourceAccount string
	}{
		{0, "LocalBlob", "", "public", ""},
		{1, "BlobLocal", "public", "", "azcopytelemetrysource"},
		{2, "BlobBlob", "public", "public", "azcopytelemetrysource"},
		{3, "S3Blob", "", "public", ""},
		{4, "GCPBlob", "", "gov", ""},
	}

	for _, test := range tests {
		dimensions := sampleDimensions("copy", test.index)
		assert.Equal(t, test.fromTo, dimensions.FromTo)
		assert.Equal(t, test.sourceCloud, dimensions.SourceCloudType)
		assert.Equal(t, test.destCloud, dimensions.DestCloudType)
		assert.Equal(t, test.sourceAccount, dimensions.SourceStorageAccount)
		assert.NotEmpty(t, dimensions.SourceProtocol)
		assert.NotEmpty(t, dimensions.DestProtocol)
	}
}

func TestBuildSampleEventsVariesByRun(t *testing.T) {
	first := buildSampleEvents(time.Date(2026, 7, 21, 12, 0, 0, 1, time.UTC))
	second := buildSampleEvents(time.Date(2026, 7, 21, 12, 0, 0, 2, time.UTC))

	firstFinished := first[2].(aztelemetry.JobFinishedEvent)
	secondFinished := second[2].(aztelemetry.JobFinishedEvent)
	assert.NotEqual(t, firstFinished.BytesEnumerated, secondFinished.BytesEnumerated)
	assert.NotEqual(t, firstFinished.JobDurationSeconds, secondFinished.JobDurationSeconds)
	assert.NotEqual(t, firstFinished.AvgIOPS, secondFinished.AvgIOPS)
}

func TestPairedEventsShareCorrelationIDs(t *testing.T) {
	events := buildSampleEvents(time.Date(2026, 7, 21, 12, 0, 0, 0, time.UTC))

	started := make(map[string]string)
	for _, event := range events {
		switch typed := event.(type) {
		case aztelemetry.JobStartedEvent:
			started[typed.JobID] = typed.InvocationID
		case aztelemetry.JobFinishedEvent:
			require.Contains(t, started, typed.JobID)
			assert.Equal(t, started[typed.JobID], typed.InvocationID)
			assert.Equal(t, typed.StartTimestamp, typed.EndTimestamp.Add(-time.Duration(typed.JobDurationSeconds)*time.Second))
		}
	}
}

func TestWritePayloadSamples(t *testing.T) {
	events := buildSampleEvents(time.Date(2026, 7, 21, 12, 0, 0, 0, time.UTC))
	outputPath := filepath.Join(t.TempDir(), "payloads.md")
	require.NoError(t, writePayloadSamples(outputPath, events))

	contents, err := os.ReadFile(outputPath)
	require.NoError(t, err)
	markdown := string(contents)
	for _, section := range []string{
		"## Command Invoked",
		"## Copy Job Started",
		"## Copy Job Finished",
		"## Sync Job Started",
		"## Sync Job Finished",
	} {
		assert.Contains(t, markdown, section)
	}
	assert.Equal(t, 5, strings.Count(markdown, "```json"))
	assert.Equal(t, 5, strings.Count(markdown, "\"name\": \"Microsoft.ApplicationInsights.Event\""))
	assert.Equal(t, 2, strings.Count(markdown, "\"name\": \"azcopy.job.finished\""))
	assert.Contains(t, markdown, "\"azcopy.bytes_transferred\":")
	assert.Contains(t, markdown, "\"measurements\": {")
	assert.Contains(t, markdown, "\"AzureVMDetected\": \"false\"")
	for _, removed := range []string{
		"AttemptType", "HostVirtualization", "MeasurementScope", "NetworkRunContext",
		"PrimaryPerformanceAdviceCode", "RunID", "SamplingUnit", "TerminalReason", "TransferDirection", "TransferTopology",
	} {
		assert.NotContains(t, markdown, removed)
	}
	assert.Contains(t, markdown, "\"JobID\":")
	assert.NotContains(t, markdown, "IngestionEndpoint")
	assert.NotContains(t, markdown, "LiveEndpoint")
	assert.NotContains(t, markdown, "ApplicationId")
}
