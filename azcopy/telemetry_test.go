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
	"encoding/json"
	"github.com/Azure/azure-storage-azcopy/v10/common"
	"github.com/Azure/azure-storage-azcopy/v10/telemetry"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestBaseJobDimensions(t *testing.T) {
	a := assert.New(t)
	d := baseJobDimensions("copy", common.EFromTo.LocalBlob(), common.ECredentialType.OAuthToken(), common.ECredentialType.SharedKey())
	a.Equal("copy", d.Command)
	a.Empty(d.SummaryCounterScope)
	a.Equal(common.EFromTo.LocalBlob().String(), d.FromTo)
	a.Equal(common.ELocation.Local().String(), d.SourceType)
	a.Equal(common.ELocation.Blob().String(), d.DestType)
	a.Equal("local", d.SourceProtocol)
	a.Equal("local-disk", d.SourceMountType)
	a.Equal("https", d.DestProtocol)
	a.Equal(common.ECredentialType.OAuthToken().String(), d.SourceAuthMechanism)
	a.Equal(common.ECredentialType.SharedKey().String(), d.DestAuthMechanism)
}

func TestProtocolForLocation(t *testing.T) {
	a := assert.New(t)
	a.Equal("local", protocolForLocation(common.ELocation.Local()))
	a.Equal("https", protocolForLocation(common.ELocation.Blob()))
	a.Equal("https", protocolForLocation(common.ELocation.BlobFS()))
	a.Equal("https", protocolForLocation(common.ELocation.File()))
	a.Equal("nfs", protocolForLocation(common.ELocation.FileNFS()))
	a.Equal("s3", protocolForLocation(common.ELocation.S3()))
	a.Equal("gcs", protocolForLocation(common.ELocation.GCP()))
}

func TestMountTypeForLocation(t *testing.T) {
	a := assert.New(t)
	a.Equal("local-disk", mountTypeForLocation(common.ELocation.Local()))
	a.Equal("cloud-azure", mountTypeForLocation(common.ELocation.Blob()))
	a.Equal("cloud-azure", mountTypeForLocation(common.ELocation.FileNFS()))
	a.Equal("cloud-s3", mountTypeForLocation(common.ELocation.S3()))
	a.Equal("cloud-gcs", mountTypeForLocation(common.ELocation.GCP()))
}

func TestBuildFinishedEvent(t *testing.T) {
	a := assert.New(t)
	start := time.Now()
	end := start.Add(2 * time.Second)
	summary := common.ListJobSummaryResponse{
		JobStatus:                 common.EJobStatus.Completed(),
		TotalBytesEnumerated:      1500000,
		TotalBytesExpected:        1200000,
		TotalBytesTransferred:     1000000,
		BytesOverWire:             1100000,
		FileTransfers:             6,
		FolderPropertyTransfers:   4,
		SymlinkTransfers:          2,
		HardlinksConvertedCount:   1,
		FoldersCompleted:          3,
		FoldersFailed:             0,
		FoldersSkipped:            1,
		TransfersCompleted:        10,
		TransfersFailed:           1,
		TransfersSkipped:          2,
		TotalTransfers:            13,
		AverageE2EMilliseconds:    50,
		AverageIOPS:               7,
		ServerBusyPercentage:      1.5,
		NetworkErrorPercentage:    0.5,
		StorageHTTPAttemptCount:   200,
		NetworkErrorAttemptCount:  1,
		ServerBusy503Count:        3,
		ServerBusyThroughputCount: 2,
		ServerBusyIOPSCount:       1,
		ServerBusyOtherCount:      0,
		PerfConstraint:            common.EPerfConstraint.Service(),
		PerformanceAdvice: []common.PerformanceAdvice{
			{Code: "NetworkErrors", PriorityAdvice: true},
			{Code: "ConcurrencyHitUpperLimit"},
		},
		PercentComplete: 100,
		FailedTransfers: []common.TransferDetail{
			{ErrorCode: 403}, {ErrorCode: 500}, {ErrorCode: 403}, {ErrorCode: 403},
		},
	}
	dims := baseJobDimensions("copy", common.EFromTo.LocalBlob(), common.ECredentialType.OAuthToken(), common.ECredentialType.SharedKey())
	shape := sourceShapeSummary{
		ObjectsScanned:           20,
		BytesScanned:             40960,
		AverageObjectSizeBytes:   2048,
		ObjectSizeP50BytesApprox: 1024,
		ObjectSizeP90BytesApprox: 16 * 1024 * 1024,
		ObjectSizeP95BytesApprox: 256 * 1024 * 1024,
		ObjectsUnder1MiB:         12,
		ObjectsUnder1MiBRatioPct: 60,
		MaxDirectoryDepth:        5,
		ContainersScanned:        3,
		ContainersTouched:        2,
	}
	evt := buildFinishedEvent(telemetryResourceForTest(), dims, "job-1234", "invocation-1234", end, summary, 2*time.Second, 1500*time.Millisecond, time.Second, shape)

	a.Equal("job-1234", evt.JobID)
	a.Equal("invocation-1234", evt.InvocationID)
	a.Equal(common.EJobStatus.Completed().String(), evt.JobStatus)
	a.Equal("403:3,500:1", evt.FailureErrorCodes)
	a.Equal(common.EPerfConstraint.Service().String(), evt.PerformanceConstraint)
	a.Equal([]string{"NetworkErrors", "ConcurrencyHitUpperLimit"}, evt.PerformanceAdviceCodes)
	measurements := evt.Measurements
	a.Equal(int64(0), measurements.FailureErrorOtherCount)
	a.Equal(100.0, measurements.PercentComplete)
	a.Equal(int64(1500000), measurements.BytesEnumerated)
	a.Equal(int64(1200000), measurements.BytesExpected)
	a.Equal(int64(1000000), measurements.BytesTransferred)
	a.Equal(int64(1100000), measurements.BytesOverWire)
	a.Equal(int64(9), measurements.ObjectsScheduled)
	a.Equal(int64(6), measurements.RegularFilesScheduled)
	a.Equal(int64(2), measurements.SymlinksScheduled)
	a.Equal(int64(1), measurements.HardlinksConvertedScheduled)
	a.Equal(int64(4), measurements.FolderPropertiesScheduled)
	a.Equal(int64(7), measurements.ObjectsCompleted)
	a.Equal(int64(1), measurements.ObjectsFailed)
	a.Equal(int64(1), measurements.ObjectsSkipped)
	a.Equal(int64(3), measurements.FolderPropertiesCompleted)
	a.Equal(int64(0), measurements.FolderPropertiesFailed)
	a.Equal(int64(1), measurements.FolderPropertiesSkipped)
	a.Equal(int64(20), measurements.SourceObjectsScanned)
	a.Equal(int64(40960), measurements.SourceBytesScanned)
	a.Equal(2048.0, measurements.SourceAverageObjectSizeBytes)
	a.Equal(int64(1024), measurements.SourceObjectSizeP50BytesApprox)
	a.Equal(int64(16*1024*1024), measurements.SourceObjectSizeP90BytesApprox)
	a.Equal(int64(256*1024*1024), measurements.SourceObjectSizeP95BytesApprox)
	a.Equal(int64(12), measurements.SourceObjectsUnder1MiB)
	a.Equal(60.0, measurements.SourceObjectsUnder1MiBRatioPct)
	a.Equal(int64(5), measurements.SourceMaxDirectoryDepth)
	a.Equal(int64(3), measurements.ContainersScanned)
	a.Equal(int64(2), measurements.ContainersTouched)
	a.Equal(int64(10), measurements.TransfersCompleted)
	a.Equal(int64(1), measurements.TransfersFailed)
	a.Equal(int64(2), measurements.TransfersSkipped)
	a.Equal(int64(13), measurements.TransfersTotal)
	a.Equal(2.0, measurements.JobDurationSeconds)
	a.Equal(1.5, measurements.EnumerationPhaseDurationSeconds)
	a.Equal(1.0, measurements.TransferPhaseDurationSeconds)
	a.InDelta(4.0, measurements.JobThroughputMbps, 1e-9)           // 1e6 bytes * 8 / 1e6 / 2s
	a.InDelta(8.0, measurements.TransferPhaseThroughputMbps, 1e-9) // 1e6 bytes * 8 / 1e6 / 1s
	a.Equal(int64(50), measurements.AverageStorageHTTPAttemptE2EMs)
	a.Equal(int64(7), measurements.AvgIOPS)
	a.Equal(int64(200), measurements.StorageHTTPAttemptCount)
	a.Equal(int64(1), measurements.NetworkErrorAttemptCount)
	a.Equal(int64(3), measurements.ServerBusy503Count)
	a.Equal(int64(2), measurements.ServerBusyThroughputCount)
	a.Equal(int64(1), measurements.ServerBusyIOPSCount)
	a.Equal(int64(0), measurements.ServerBusyOtherCount)
}

func TestTelemetryCountersExcludedFromCustomerJSON(t *testing.T) {
	summary := common.ListJobSummaryResponse{
		TotalTransfers:            13,
		AverageIOPS:               7,
		StorageHTTPAttemptCount:   200,
		NetworkErrorAttemptCount:  1,
		ServerBusy503Count:        6,
		ServerBusyThroughputCount: 1,
		ServerBusyIOPSCount:       2,
		ServerBusyOtherCount:      3,
	}
	for _, test := range []struct {
		name  string
		value any
	}{
		{name: "job summary", value: summary},
		{name: "jobs show response", value: JobSummaryResponse(summary)},
		{name: "copy progress", value: CopyProgress{ListJobSummaryResponse: summary}},
		{name: "copy or resume result", value: CopyResult{ListJobSummaryResponse: summary}},
		{name: "sync progress", value: SyncProgress{ListJobSummaryResponse: summary}},
		{name: "sync result", value: SyncResult{ListJobSummaryResponse: summary}},
		{name: "sync summary", value: common.ListSyncJobSummaryResponse{ListJobSummaryResponse: summary}},
		{name: "zero telemetry counters", value: common.ListJobSummaryResponse{TotalTransfers: 13, AverageIOPS: 7}},
	} {
		t.Run(test.name, func(t *testing.T) {
			encoded, err := json.Marshal(test.value)
			require.NoError(t, err)
			var fields map[string]json.RawMessage
			require.NoError(t, json.Unmarshal(encoded, &fields))
			for _, name := range []string{
				"StorageHTTPAttemptCount", "NetworkErrorAttemptCount", "ServerBusy503Count",
				"ServerBusyThroughputCount", "ServerBusyIOPSCount", "ServerBusyOtherCount",
			} {
				_, exposed := fields[name]
				assert.False(t, exposed, "%s must not appear in customer JSON", name)
			}
			assert.JSONEq(t, `"13"`, string(fields["TotalTransfers"]))
			assert.JSONEq(t, `"7"`, string(fields["AverageIOPS"]))
		})
	}

	event := buildFinishedEvent(telemetryResourceForTest(), telemetry.JobDimensions{}, "job", "invocation", time.Now(), summary, time.Second, 0, time.Second, sourceShapeSummary{})
	assert.Equal(t, summary.StorageHTTPAttemptCount, event.Measurements.StorageHTTPAttemptCount)
	assert.Equal(t, summary.NetworkErrorAttemptCount, event.Measurements.NetworkErrorAttemptCount)
	assert.Equal(t, summary.ServerBusy503Count, event.Measurements.ServerBusy503Count)
	assert.Equal(t, summary.ServerBusyThroughputCount, event.Measurements.ServerBusyThroughputCount)
	assert.Equal(t, summary.ServerBusyIOPSCount, event.Measurements.ServerBusyIOPSCount)
	assert.Equal(t, summary.ServerBusyOtherCount, event.Measurements.ServerBusyOtherCount)
}

func TestCountExcludingFolders(t *testing.T) {
	assert.Equal(t, int64(7), countExcludingFolders(10, 3))
	assert.Zero(t, countExcludingFolders(3, 3))
	assert.Zero(t, countExcludingFolders(2, 3))
}

func TestAggregateErrorCodes(t *testing.T) {
	a := assert.New(t)
	// No failures -> empty.
	histogram, other := aggregateErrorCodesWithOther(nil)
	a.Empty(histogram)
	a.Zero(other)

	histogram, other = aggregateErrorCodesWithOther([]common.TransferDetail{})
	a.Empty(histogram)
	a.Zero(other)
	// Ordered by descending count, then ascending code.
	histogram, other = aggregateErrorCodesWithOther([]common.TransferDetail{
		{ErrorCode: 500}, {ErrorCode: 403}, {ErrorCode: 403}, {ErrorCode: 403},
	})
	a.Equal("403:3,500:1", histogram)
	a.Zero(other)
	// Tie on count -> lower code first.
	histogram, other = aggregateErrorCodesWithOther([]common.TransferDetail{
		{ErrorCode: 409}, {ErrorCode: 404},
	})
	a.Equal("404:1,409:1", histogram)
	a.Zero(other)
	// Bounded to maxErrorCodeBuckets distinct codes.
	many := make([]common.TransferDetail, 0, maxErrorCodeBuckets+5)
	for i := 0; i < maxErrorCodeBuckets+5; i++ {
		many = append(many, common.TransferDetail{ErrorCode: int32(600 + i)})
	}
	histogram, other = aggregateErrorCodesWithOther(many)
	a.Equal(maxErrorCodeBuckets, strings.Count(histogram, ":"))
	a.Equal(int64(5), other)
}

func TestAggregateErrorCodesWithOther(t *testing.T) {
	failed := make([]common.TransferDetail, 0, 12)
	for code := int32(400); code < 412; code++ {
		failed = append(failed, common.TransferDetail{ErrorCode: code})
	}
	histogram, other := aggregateErrorCodesWithOther(failed)
	assert.Equal(t, "400:1,401:1,402:1,403:1,404:1,405:1,406:1,407:1,408:1,409:1", histogram)
	assert.Equal(t, int64(2), other)
}

func TestPerformanceAdviceAttributes(t *testing.T) {
	advice := []common.PerformanceAdvice{
		{Code: "NetworkErrors", PriorityAdvice: true},
		{Code: "NetworkErrors"},
		{Code: "invalid value"},
		{Code: "AccountIOPS"},
	}
	constraint, codes := performanceAdviceAttributes(common.EPerfConstraint.Service(), advice)
	assert.Equal(t, common.EPerfConstraint.Service().String(), constraint)
	assert.Equal(t, []string{"NetworkErrors", "AccountIOPS"}, codes)
}

func TestThroughputMbps(t *testing.T) {
	a := assert.New(t)
	a.Equal(0.0, throughputMbps(1000, 0))
	a.Equal(0.0, throughputMbps(1000, -1))
	a.InDelta(8.0, throughputMbps(1_000_000, 1), 1e-9)
}
func TestDetectInvocationContext(t *testing.T) {
	a := assert.New(t)
	a.Equal("interactive", detectInvocationContext(func(string) string { return "" }))
	a.Equal("ci", detectInvocationContext(func(k string) string {
		if k == "GITHUB_ACTIONS" {
			return "true"
		}
		return ""
	}))
}

func TestInstallationIDPersistsOutsideJobPlanFolder(t *testing.T) {
	rootDir := t.TempDir()
	appDataDir := filepath.Join(rootDir, ".azcopy")
	jobPlanDir := filepath.Join(rootDir, "plans")
	require.NoError(t, os.Mkdir(jobPlanDir, 0700))

	first := installationIDInDir(appDataDir)
	second := installationIDInDir(appDataDir)

	assert.Len(t, first, 32)
	assert.Equal(t, first, second)
	assert.FileExists(t, filepath.Join(appDataDir, installationIDFileName))
	assert.NoFileExists(t, filepath.Join(appDataDir, installationIDFileName+".lock"))

	entries, err := os.ReadDir(jobPlanDir)
	require.NoError(t, err)
	assert.Empty(t, entries)
}

func TestInstallationIDIsStableAcrossConcurrentReads(t *testing.T) {
	appDataDir := filepath.Join(t.TempDir(), ".azcopy")
	first := installationIDInDir(appDataDir)
	require.Len(t, first, 32)
	const callCount = 16
	results := make(chan string, callCount)

	for i := 0; i < callCount; i++ {
		go func() {
			results <- installationIDInDir(appDataDir)
		}()
	}

	for i := 0; i < callCount; i++ {
		assert.Equal(t, first, <-results)
	}
}

func TestInstallationIDRecoversMalformedFile(t *testing.T) {
	appDataDir := filepath.Join(t.TempDir(), ".azcopy")
	require.NoError(t, os.Mkdir(appDataDir, 0700))
	require.NoError(t, os.WriteFile(filepath.Join(appDataDir, installationIDFileName), []byte("partial"), 0600))

	first := installationIDInDir(appDataDir)
	require.Len(t, first, 32)
	assert.Equal(t, first, readInstallationID(filepath.Join(appDataDir, installationIDFileName)))
	assert.Equal(t, first, installationIDInDir(appDataDir))
}

func TestInstallationIDIgnoresLegacyLockPath(t *testing.T) {
	for _, malformed := range []bool{false, true} {
		appDataDir := t.TempDir()
		path := filepath.Join(appDataDir, installationIDFileName)
		require.NoError(t, os.Mkdir(path+".lock", 0700))
		if malformed {
			require.NoError(t, os.WriteFile(path, []byte("partial"), 0600))
		}
		identity := installationIDInDir(appDataDir)
		require.Len(t, identity, 32)
		require.Equal(t, identity, readInstallationID(path))
		require.Equal(t, identity, installationIDInDir(appDataDir))
		entries, err := os.ReadDir(appDataDir)
		require.NoError(t, err)
		require.Len(t, entries, 2)
	}
}

func TestNewTelemetryInvocationID(t *testing.T) {
	first := newTelemetryInvocationID()
	second := newTelemetryInvocationID()
	assert.Len(t, first, 32)
	assert.Len(t, second, 32)
	assert.NotEqual(t, first, second)
}

func TestBuildResourceAttributesSchemaVersion(t *testing.T) {
	resource := buildResourceAttributes()
	assert.Equal(t, "1", resource.SchemaVersion)
}

func TestBuildResourceAttributesE2ETestRunID(t *testing.T) {
	t.Setenv(envE2ETelemetryRunID, "pipeline-run-123")
	assert.Equal(t, "pipeline-run-123", buildResourceAttributes().E2ETestRunID)

	t.Setenv(envE2ETelemetryRunID, "   ")
	assert.Empty(t, buildResourceAttributes().E2ETestRunID)
}

func telemetryResourceForTest() telemetry.ResourceAttributes {
	return telemetry.ResourceAttributes{
		AzCopyVersion: common.AzcopyVersion,
	}
}
