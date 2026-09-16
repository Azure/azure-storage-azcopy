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
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/Azure/azure-sdk-for-go/sdk/azcore"
	"github.com/Azure/azure-storage-azcopy/v10/common"
	"github.com/Azure/azure-storage-azcopy/v10/telemetry"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
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

func TestSourceMountType(t *testing.T) {
	a := assert.New(t)
	// Remote sources use the coarse cloud classification (no path inspection).
	a.Equal("cloud-azure", sourceMountType(common.ELocation.Blob(), ""))
	a.Equal("cloud-s3", sourceMountType(common.ELocation.S3(), ""))
	a.Equal("cloud-gcs", sourceMountType(common.ELocation.GCP(), ""))
	// A local path that cannot be classified falls back to local-disk (never empty).
	a.Equal("local-disk", sourceMountType(common.ELocation.Local(), "this-path-does-not-exist-xyz"))
}

func TestProtocolForLocation(t *testing.T) {
	a := assert.New(t)
	a.Equal("local", protocolForLocation(common.ELocation.Local()))
	a.Equal("https", protocolForLocation(common.ELocation.Blob()))
	a.Equal("https", protocolForLocation(common.ELocation.BlobFS()))
	a.Equal("https", protocolForLocation(common.ELocation.File()))
	a.Equal("https", protocolForLocation(common.ELocation.FileNFS()))
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

func TestEndpointKind(t *testing.T) {
	a := assert.New(t)
	a.Equal("public", endpointKind(
		common.ResourceString{Value: "https://acct.blob.core.windows.net/c"},
		common.ELocation.Blob()))
	a.Equal("private-endpoint", endpointKind(
		common.ResourceString{Value: "https://acct.privatelink.blob.core.windows.net/c"},
		common.ELocation.Blob()))
	a.Equal("public", endpointKind(
		common.ResourceString{Value: "https://acct.blob.core.sovcloud-api.fr/c"},
		common.ELocation.Blob()))
	a.Equal("private-endpoint", endpointKind(
		common.ResourceString{Value: "https://acct.privatelink.blob.core.usgovcloudapi.net/c"},
		common.ELocation.Blob()))
	for _, host := range []string{"http://127.0.0.1:10000/devstoreaccount1", "https://acct.blob.example.com/c", "https://%zz/c", ""} {
		a.Equal("unknown", endpointKind(common.ResourceString{Value: host}, common.ELocation.Blob()), host)
	}
	// Non-Azure destinations have no endpoint kind.
	a.Equal("", endpointKind(
		common.ResourceString{Value: "/local/path"},
		common.ELocation.Local()))
}

func TestEndpointCloudType(t *testing.T) {
	a := assert.New(t)
	for host, want := range map[string]string{
		"acct.blob.core.windows.net":          "public",
		"acct.z12.blob.storage.azure.net":     "public",
		"acct.blob.core.usgovcloudapi.net":    "usgov",
		"acct.blob.core.chinacloudapi.cn":     "china",
		"acct.blob.core.microsoft.scloud":     "ussec",
		"acct.blob.core.eaglex.ic.gov":        "usnat",
		"acct.blob.core.sovcloud-api.fr":      "bleu",
		"acct.blob.core.sovcloud-api.de":      "delos",
		"acct.dfs.core.sovcloud-api.sg":       "govsg",
		"acct.blob.core.windows.net.":         "public",
		"acct.blob.core.cloudapi.de":          "unknown",
		"acct.blob.example.com":               "unknown",
		"acct.blob.core.sovcloud-api.fr.evil": "unknown",
	} {
		a.Equal(want, endpointCloudType(common.ResourceString{Value: "https://" + host + "/c"}, common.ELocation.Blob()), host)
	}
	a.Empty(endpointCloudType(
		common.ResourceString{Value: "https://s3.amazonaws.com/bucket"}, common.ELocation.S3()))
	a.Empty(endpointCloudType(
		common.ResourceString{Value: "/local/path"}, common.ELocation.Local()))
}

func TestStorageAccountName(t *testing.T) {
	tests := []struct {
		name     string
		resource string
		location common.Location
		want     string
	}{
		{"blob", "https://account.blob.core.windows.net/container/object?sig=secret", common.ELocation.Blob(), "account"},
		{"dfs", "https://account.dfs.core.windows.net/filesystem/path", common.ELocation.BlobFS(), "account"},
		{"file", "https://account.file.core.windows.net/share/path", common.ELocation.File(), "account"},
		{"nfs", "https://account.file.core.windows.net/share/path", common.ELocation.FileNFS(), "account"},
		{"private link", "https://account.privatelink.blob.core.windows.net/container", common.ELocation.Blob(), "account"},
		{"us government", "https://account.blob.core.usgovcloudapi.net/container", common.ELocation.Blob(), ""},
		{"china", "https://account.blob.core.chinacloudapi.cn/container", common.ELocation.Blob(), ""},
		{"ussec", "https://account.blob.core.microsoft.scloud/container", common.ELocation.Blob(), ""},
		{"usnat", "https://account.blob.core.eaglex.ic.gov/container", common.ELocation.Blob(), ""},
		{"bleu", "https://account.blob.core.sovcloud-api.fr/container", common.ELocation.Blob(), ""},
		{"delos", "https://account.blob.core.sovcloud-api.de/container", common.ELocation.Blob(), ""},
		{"govsg", "https://account.blob.core.sovcloud-api.sg/container", common.ELocation.Blob(), ""},
		{"retired germany cloud", "https://account.blob.core.cloudapi.de/container", common.ELocation.Blob(), ""},
		{"dns zone", "https://account.z12.blob.storage.azure.net/container", common.ELocation.Blob(), "account"},
		{"case and port", "https://Account123.Blob.Core.Windows.Net:443/container", common.ELocation.Blob(), "account123"},
		{"trailing dot", "https://account.blob.core.windows.net./container", common.ELocation.Blob(), "account"},
		{"http", "http://account.blob.core.windows.net/container", common.ELocation.Blob(), "account"},
		{"local", "/local/private/path", common.ELocation.Local(), ""},
		{"local mount", "\\\\account.file.core.windows.net\\share", common.ELocation.Local(), ""},
		{"s3", "https://bucket.s3.amazonaws.com/object", common.ELocation.S3(), ""},
		{"gcp", "https://storage.googleapis.com/bucket/object", common.ELocation.GCP(), ""},
		{"non Azure type", "https://account.blob.core.windows.net/container", common.ELocation.S3(), ""},
		{"custom host", "https://account.example.com/container", common.ELocation.Blob(), ""},
		{"suffix spoof", "https://account.blob.core.windows.net.example.com/container", common.ELocation.Blob(), ""},
		{"emulator", "http://127.0.0.1:10000/devstoreaccount1/container", common.ELocation.Blob(), ""},
		{"empty", "", common.ELocation.Blob(), ""},
		{"malformed", "https://%zz/container", common.ELocation.Blob(), ""},
		{"missing account", "https://.blob.core.windows.net/container", common.ELocation.Blob(), ""},
		{"short account", "https://ab.blob.core.windows.net/container", common.ELocation.Blob(), ""},
		{"long account", "https://" + strings.Repeat("a", 25) + ".blob.core.windows.net/container", common.ELocation.Blob(), ""},
		{"invalid account", "https://account-name.blob.core.windows.net/container", common.ELocation.Blob(), ""},
		{"userinfo", "https://user:secret@account.blob.core.windows.net/container", common.ELocation.Blob(), ""},
		{"unsupported scheme", "ftp://account.blob.core.windows.net/container", common.ELocation.Blob(), ""},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			resource := common.ResourceString{Value: test.resource, SAS: "sig=another-secret", ExtraQuery: "versionid=private-version"}
			assert.Equal(t, test.want, storageAccountName(resource, test.location))
		})
	}
}

func TestStorageAccountDimensions(t *testing.T) {
	source := common.ResourceString{Value: "https://sourceaccount.blob.core.windows.net/source/private-object", SAS: "sig=source-secret"}
	destination := common.ResourceString{Value: "https://targetaccount.blob.core.windows.net/target/private-object", SAS: "sig=target-secret"}
	credential := common.ECredentialType.Anonymous()
	fromTo := common.EFromTo.BlobBlob()
	dimensions := map[string]telemetry.JobDimensions{
		"copy":   copyJobDimensions(&CookedTransferOptions{fromTo: fromTo, source: source, destination: destination}, credential, credential),
		"sync":   syncJobDimensions(&cookedSyncOptions{fromTo: fromTo, source: source, destination: destination}, credential, credential),
		"resume": resumeJobDimensions(common.GetJobDetailsResponse{FromTo: fromTo}, source, destination, credential, credential, telemetry.OptionAttributes{}),
	}
	for name, dimension := range dimensions {
		t.Run(name, func(t *testing.T) {
			assert.Equal(t, "sourceaccount", dimension.SourceStorageAccount)
			assert.Equal(t, "targetaccount", dimension.DestStorageAccount)
		})
	}
}

func TestCopyJobDimensions(t *testing.T) {
	a := assert.New(t)
	o := &CookedTransferOptions{
		fromTo:      common.EFromTo.LocalBlob(),
		source:      common.ResourceString{Value: "local"},
		destination: common.ResourceString{Value: "https://account.blob.core.windows.net/container"},
		telemetryOptions: telemetry.OptionAttributes{
			FlagsSet: []string{"block-size-mb", "put-md5", "recursive"},
			Values: map[string]string{
				"OptBlockSizeMB": "8",
				"OptPutMD5":      "true",
				"OptRecursive":   "true",
			},
		},
	}
	d := copyJobDimensions(o, common.ECredentialType.Anonymous(), common.ECredentialType.OAuthToken())
	a.Equal("copy", d.Command)
	a.Equal("NotApplicable", d.SourceAuthMechanism)
	a.Equal(common.ECredentialType.OAuthToken().String(), d.DestAuthMechanism)
	a.Empty(d.SourceCloudType)
	a.Equal("public", d.DestCloudType)
	a.Empty(d.SourceStorageAccount)
	a.Equal("account", d.DestStorageAccount)
	a.Equal([]string{"block-size-mb", "put-md5", "recursive"}, d.Options.FlagsSet)
	a.Equal("8", d.Options.Values["OptBlockSizeMB"])
	o.telemetryOptions.FlagsSet[0] = "mutated"
	o.telemetryOptions.Values["OptBlockSizeMB"] = "mutated"
	a.Equal([]string{"block-size-mb", "put-md5", "recursive"}, d.Options.FlagsSet)
	a.Equal("8", d.Options.Values["OptBlockSizeMB"])
}

func TestCopyJobDimensionsS3ToAzureGovernment(t *testing.T) {
	o := &CookedTransferOptions{
		fromTo:      common.EFromTo.S3Blob(),
		source:      common.ResourceString{Value: "https://s3.amazonaws.com/source-bucket"},
		destination: common.ResourceString{Value: "https://account.blob.core.usgovcloudapi.net/container"},
	}
	dimensions := copyJobDimensions(o, common.ECredentialType.S3AccessKey(), common.ECredentialType.OAuthToken())
	assert.Equal(t, "S3Blob", dimensions.FromTo)
	assert.Empty(t, dimensions.SourceCloudType)
	assert.Equal(t, "usgov", dimensions.DestCloudType)
	assert.Empty(t, dimensions.SourceStorageAccount)
	assert.Empty(t, dimensions.DestStorageAccount, "account names are only reported for the public cloud")
	assert.Equal(t, "public", dimensions.DestEndpointKind)
}

func TestShouldEmitCopyTelemetry(t *testing.T) {
	assert.True(t, shouldEmitCopyTelemetry(&CookedTransferOptions{}))
	assert.False(t, shouldEmitCopyTelemetry(&CookedTransferOptions{dryrun: true}))
}

func TestSyncJobDimensions(t *testing.T) {
	a := assert.New(t)
	o := &cookedSyncOptions{
		fromTo:      common.EFromTo.LocalBlob(),
		source:      common.ResourceString{Value: "local"},
		destination: common.ResourceString{Value: "https://account.blob.core.windows.net/container"},
		telemetryOptions: telemetry.OptionAttributes{
			FlagsSet: []string{"delete-destination", "mirror-mode", "recursive"},
			Values: map[string]string{
				"OptDeleteDestination": "true",
				"OptMirrorMode":        "true",
				"OptRecursive":         "false",
			},
		},
	}
	d := syncJobDimensions(o, common.ECredentialType.SharedKey(), common.ECredentialType.Anonymous())
	a.Equal("sync", d.Command)
	a.Empty(d.SourceCloudType)
	a.Equal("public", d.DestCloudType)
	a.Empty(d.SourceStorageAccount)
	a.Equal("account", d.DestStorageAccount)
	a.Equal([]string{"delete-destination", "mirror-mode", "recursive"}, d.Options.FlagsSet)
	a.Equal("false", d.Options.Values["OptRecursive"])
}

func TestResumeJobDimensions(t *testing.T) {
	options := telemetry.OptionAttributes{
		FlagsSet: []string{"include"},
		Values:   map[string]string{"OptExample": "value"},
	}
	dimensions := resumeJobDimensions(
		common.GetJobDetailsResponse{FromTo: common.EFromTo.LocalBlob()},
		common.ResourceString{Value: "local"},
		common.ResourceString{Value: "https://account.blob.core.windows.net/container", SAS: "?sig=redacted"},
		common.ECredentialType.Anonymous(),
		common.ECredentialType.Anonymous(),
		options,
	)
	assert.Equal(t, "jobs.resume", dimensions.Command)
	assert.Equal(t, "job-cumulative", dimensions.SummaryCounterScope)
	assert.Equal(t, "NotApplicable", dimensions.SourceAuthMechanism)
	assert.Equal(t, "SAS", dimensions.DestAuthMechanism)
	assert.Empty(t, dimensions.SourceCloudType)
	assert.Equal(t, "public", dimensions.DestCloudType)
	assert.Empty(t, dimensions.SourceStorageAccount)
	assert.Equal(t, "account", dimensions.DestStorageAccount)
	assert.Equal(t, []string{"include"}, dimensions.Options.FlagsSet)
	options.FlagsSet[0] = "mutated"
	options.Values["OptExample"] = "mutated"
	assert.Equal(t, []string{"include"}, dimensions.Options.FlagsSet)
	assert.Equal(t, "value", dimensions.Options.Values["OptExample"])
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
		FileTransfers:             3,
		FolderPropertyTransfers:   4,
		SymlinkTransfers:          2,
		HardlinksConvertedCount:   1,
		HardlinksTransferCount:    3,
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
		NetworkErrorPercentage:    0.2,
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
		// Per-poll list; the event must use the cumulative counts instead.
		FailedTransfers:               []common.TransferDetail{{ErrorCode: 404}},
		FailedTransferErrorCodeCounts: map[int32]uint32{403: 3, 500: 1},
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
	a.Equal(1.5, measurements.ServerBusyPct)
	a.Equal(0.2, measurements.NetworkErrorPct)
	a.Equal(int64(1500000), measurements.BytesEnumerated)
	a.Equal(int64(1200000), measurements.BytesExpected)
	a.Equal(int64(1000000), measurements.BytesTransferred)
	a.Equal(int64(1100000), measurements.BytesOverWire)
	a.Equal(int64(9), measurements.ObjectsScheduled)
	a.Equal(int64(3), measurements.RegularFilesScheduled)
	a.Equal(int64(2), measurements.SymlinksScheduled)
	a.Equal(int64(1), measurements.HardlinksConvertedScheduled)
	a.Equal(int64(3), measurements.HardlinksPreservedScheduled)
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
	summary.FailedTransferErrorCodeCounts = map[int32]uint32{404: 1}
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
				"FailedTransferErrorCodeCounts",
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
	assert.Equal(t, "404:1", event.FailureErrorCodes)
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

	histogram, other = aggregateErrorCodesWithOther(map[int32]uint32{})
	a.Empty(histogram)
	a.Zero(other)
	// Ordered by descending count, then ascending code.
	histogram, other = aggregateErrorCodesWithOther(map[int32]uint32{500: 1, 403: 3})
	a.Equal("403:3,500:1", histogram)
	a.Zero(other)
	// Tie on count -> lower code first.
	histogram, other = aggregateErrorCodesWithOther(map[int32]uint32{409: 1, 404: 1})
	a.Equal("404:1,409:1", histogram)
	a.Zero(other)
	// Bounded to maxErrorCodeBuckets distinct codes.
	many := make(map[int32]uint32, maxErrorCodeBuckets+5)
	for i := 0; i < maxErrorCodeBuckets+5; i++ {
		many[int32(600+i)] = 1
	}
	histogram, other = aggregateErrorCodesWithOther(many)
	a.Equal(maxErrorCodeBuckets, strings.Count(histogram, ":"))
	a.Equal(int64(5), other)
}

func TestAggregateErrorCodesWithOther(t *testing.T) {
	failed := make(map[int32]uint32, 12)
	for code := int32(400); code < 412; code++ {
		failed[code] = 1
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

	longest := strings.Repeat("A", maxPerformanceAdviceCodeLen)
	_, codes = performanceAdviceAttributes(common.EPerfConstraint.Unknown(), []common.PerformanceAdvice{{Code: longest}, {Code: longest + "B"}})
	assert.Equal(t, []string{longest}, codes)
	assert.LessOrEqual(t, maxPerformanceAdviceCodes*(maxPerformanceAdviceCodeLen+1)-1, 512)
}

func TestAuthMechanism(t *testing.T) {
	resourceWithSAS := common.ResourceString{Value: "https://account.blob.core.windows.net/container", SAS: "?sig=secret"}
	assert.Equal(t, "SAS", authMechanism(common.ECredentialType.Anonymous(), resourceWithSAS, common.ELocation.Blob()))
	assert.Equal(t, "PublicAnonymous", authMechanism(common.ECredentialType.Anonymous(), common.ResourceString{}, common.ELocation.Blob()))
	assert.Equal(t, common.ECredentialType.OAuthToken().String(), authMechanism(common.ECredentialType.OAuthToken(), common.ResourceString{}, common.ELocation.Blob()))
	assert.Equal(t, "NotApplicable", authMechanism(common.ECredentialType.Anonymous(), common.ResourceString{}, common.ELocation.Local()))
}

func TestScopeForLocation(t *testing.T) {
	assert.Equal(t, "service", scopeForLocation(common.ResourceString{Value: "https://account.blob.core.windows.net"}, common.ELocation.Blob(), true))
	assert.Equal(t, "container", scopeForLocation(common.ResourceString{Value: "https://account.blob.core.windows.net/container"}, common.ELocation.Blob(), true))
	assert.Equal(t, "share", scopeForLocation(common.ResourceString{Value: "https://account.file.core.windows.net/share"}, common.ELocation.File(), true))
	assert.Equal(t, "bucket", scopeForLocation(common.ResourceString{Value: "https://s3.amazonaws.com/bucket"}, common.ELocation.S3(), true))
	assert.Equal(t, "object-or-prefix", scopeForLocation(common.ResourceString{Value: "https://account.blob.core.windows.net/container/path"}, common.ELocation.Blob(), true))
	assert.Equal(t, "stream", scopeForLocation(common.ResourceString{}, common.ELocation.Pipe(), true))
	assert.Equal(t, "benchmark", scopeForLocation(common.ResourceString{}, common.ELocation.Benchmark(), true))
}

func TestThroughputMbps(t *testing.T) {
	a := assert.New(t)
	a.Equal(0.0, throughputMbps(1000, 0))
	a.Equal(0.0, throughputMbps(1000, -1))
	a.InDelta(8.0, throughputMbps(1_000_000, 1), 1e-9)
	a.Equal(-1.0, throughputMbps(-1, 1))
}

func TestSummaryInt64(t *testing.T) {
	a := assert.New(t)
	const maxInt64 = uint64(1)<<63 - 1
	a.Equal(int64(0), summaryInt64(0))
	a.Equal(int64(maxInt64), summaryInt64(maxInt64))
	a.Equal(int64(-1), summaryInt64(maxInt64+1))
	a.Equal(int64(-1), summaryInt64(^uint64(0)))
}

func TestSummaryFloat64(t *testing.T) {
	a := assert.New(t)
	a.Equal(33.3, summaryFloat64(33.3))
	a.Equal(0.2, summaryFloat64(0.2))
	a.Equal(100.0, summaryFloat64(100))
	a.Equal(0.0, summaryFloat64(0))
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

func TestShouldCollectSourceShape(t *testing.T) {
	assert.False(t, (*telemetryAgent)(nil).shouldCollectSourceShape())
	assert.False(t, (&telemetryAgent{}).shouldCollectSourceShape())
	assert.True(t, (&telemetryAgent{enabled: true}).shouldCollectSourceShape())
	stopped := &telemetryAgent{enabled: true}
	stopped.stopped.Store(true)
	assert.False(t, stopped.shouldCollectSourceShape())
}

func TestBuildResourceAttributesSchemaVersion(t *testing.T) {
	t.Setenv(common.EEnvironmentVariable.UserDir().Name, t.TempDir())
	resource := buildResourceAttributes()
	assert.Equal(t, "1", resource.SchemaVersion)
	assert.Equal(t, probeAzureVM(), resource.AzureVMDetected)
}

func TestBuildResourceAttributesE2ETestRunID(t *testing.T) {
	t.Setenv(common.EEnvironmentVariable.UserDir().Name, t.TempDir())
	t.Setenv(envE2ETelemetryRunID, "pipeline-run-123")
	assert.Equal(t, "pipeline-run-123", buildResourceAttributes().E2ETestRunID)

	t.Setenv(envE2ETelemetryRunID, "   ")
	assert.Empty(t, buildResourceAttributes().E2ETestRunID)
}

func TestConfiguredTelemetryConnectionString(t *testing.T) {
	assert.Empty(t, telemetryConnectionString)
	noEnv := func(string) string { return "" }
	assert.Equal(t, telemetryConnectionString, configuredTelemetryConnectionString(noEnv, telemetryConnectionString))
	assert.Equal(t, telemetryConnectionString, configuredTelemetryConnectionString(func(string) string { return " \t " }, telemetryConnectionString))
	assert.Empty(t, configuredTelemetryConnectionString(func(string) string {
		return "InstrumentationKey=00000000-0000-0000-0000-000000000000"
	}, telemetryConnectionString))
	assert.Equal(t, "InstrumentationKey=override;IngestionEndpoint=https://example.test/", configuredTelemetryConnectionString(func(string) string {
		return " InstrumentationKey=override;IngestionEndpoint=https://example.test/ "
	}, telemetryConnectionString))

	assert.Empty(t, configuredTelemetryConnectionString(noEnv, ""))
	assert.Empty(t, configuredTelemetryConnectionString(noEnv, "InstrumentationKey=00000000-0000-0000-0000-000000000000"))
	assert.Equal(t, "InstrumentationKey=build-time", configuredTelemetryConnectionString(noEnv, "InstrumentationKey=build-time"))
	assert.Equal(t, "InstrumentationKey=runtime", configuredTelemetryConnectionString(func(name string) string {
		if name == envTelemetryConnectionString {
			return "InstrumentationKey=runtime"
		}
		return ""
	}, "InstrumentationKey=build-time"))
}

func TestTelemetryDisabledByDefault(t *testing.T) {
	t.Setenv(envTelemetryConnectionString, "InstrumentationKey=test;IngestionEndpoint=https://example.test/")
	t.Setenv(envDisableTelemetry, "false")

	agent := (&clientTelemetry{enabled: ClientOptions{}.EnableTelemetry}).get()
	assert.False(t, agent.enabled)
	assert.Nil(t, agent.reporter)
	assert.Empty(t, agent.resource.InstallationID)
}

func TestTelemetryEnabledByClientOption(t *testing.T) {
	t.Setenv(common.EEnvironmentVariable.UserDir().Name, t.TempDir())
	t.Setenv(envTelemetryConnectionString, "InstrumentationKey=test;IngestionEndpoint=https://example.test/")
	t.Setenv(envDisableTelemetry, "false")

	agent := (&clientTelemetry{enabled: true}).get()
	assert.True(t, agent.enabled)
	assert.NotNil(t, agent.reporter)
}

func TestTelemetryEmbeddedDefaultHonorsOptOut(t *testing.T) {
	t.Setenv(envTelemetryConnectionString, "")
	t.Setenv(envDisableTelemetry, "true")
	agent := newTelemetryAgent(true)
	assert.False(t, agent.enabled)
	assert.Nil(t, agent.reporter)
	assert.Empty(t, agent.resource.InstallationID)

	t.Setenv(envDisableTelemetry, "false")
	agent = newTelemetryAgent(false)
	assert.False(t, agent.enabled)
	assert.Nil(t, agent.reporter)
	assert.Empty(t, agent.resource.InstallationID)
}

func TestTelemetryBuildsUseEmbeddedDefault(t *testing.T) {
	for _, path := range []string{
		"../azurePipelineTemplates/build_linux.yml",
		"../azurePipelineTemplates/build_windows.yml",
		"../azurePipelineTemplates/build_macos.yml",
		"../.github/workflows/build_m1.yml",
	} {
		contents, err := os.ReadFile(path)
		require.NoError(t, err)
		assert.Contains(t, string(contents), "go build", path)
		assert.NotContains(t, string(contents), "azcopy.telemetryConnectionString", path)
		assert.NotContains(t, string(contents), "AZCOPY_TELEMETRY_CONNECTION_STRING_PROD", path)
	}
}

func TestTerminalAttemptStatus(t *testing.T) {
	tests := []struct {
		name        string
		status      common.JobStatus
		err         error
		wantStatus  common.JobStatus
		wantOutcome attemptOutcome
	}{
		{"completed", common.EJobStatus.Completed(), nil, common.EJobStatus.Completed(), outcomeCompleted},
		{"completed with errors", common.EJobStatus.CompletedWithErrors(), nil, common.EJobStatus.CompletedWithErrors(), outcomeCompletedWithErrors},
		{"failed error", common.EJobStatus.InProgress(), errors.New("failed"), common.EJobStatus.Failed(), outcomeFailed},
		{"cancelled context", common.EJobStatus.InProgress(), context.Canceled, common.EJobStatus.Cancelled(), outcomeCancelled},
		{"cancelled summary", common.EJobStatus.Cancelled(), nil, common.EJobStatus.Cancelled(), outcomeCancelled},
		{"missing success summary", common.EJobStatus.InProgress(), nil, common.EJobStatus.Completed(), outcomeCompleted},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			status, outcome := terminalAttemptStatus(test.status, test.err)
			assert.Equal(t, test.wantStatus, status)
			assert.Equal(t, test.wantOutcome, outcome)
		})
	}
}

func TestJobErrorAttributes(t *testing.T) {
	tests := []struct {
		name         string
		err          error
		outcome      attemptOutcome
		stage        attemptStage
		wantCategory string
		wantCode     string
	}{
		{"success", nil, outcomeCompleted, attemptStageCompleted, "", ""},
		{"cancelled", context.Canceled, outcomeCancelled, attemptStageEnumeration, "", ""},
		{"partial success", nil, outcomeCompletedWithErrors, attemptStageCompleted, "transfer", "transfer-failures"},
		{"authentication", &azcore.ResponseError{ErrorCode: "AuthenticationFailed", StatusCode: 403}, outcomeFailed, attemptStageEnumeration, "authentication", "AuthenticationFailed"},
		{"authorization", &azcore.ResponseError{ErrorCode: "AuthorizationPermissionMismatch", StatusCode: 403}, outcomeFailed, attemptStageTransfer, "authorization", "AuthorizationPermissionMismatch"},
		{"throttling", &azcore.ResponseError{ErrorCode: "ServerBusy", StatusCode: 503}, outcomeFailed, attemptStageTransfer, "throttling", "ServerBusy"},
		{"http fallback", &azcore.ResponseError{StatusCode: 404}, outcomeFailed, attemptStageEnumeration, "not-found", "http-404"},
		{"invalid request", &azcore.ResponseError{ErrorCode: "InvalidQueryParameterValue", StatusCode: 400}, outcomeFailed, attemptStageEnumeration, "request", "InvalidQueryParameterValue"},
		{"deadline", context.DeadlineExceeded, outcomeFailed, attemptStageTransfer, "timeout", "context-deadline-exceeded"},
		{"local path", &os.PathError{Op: "open", Path: "secret", Err: os.ErrNotExist}, outcomeFailed, attemptStageEnumeration, "local-io", "local-path-error"},
		{"network", &url.Error{Op: "Get", URL: "https://secret", Err: errors.New("connection refused")}, outcomeFailed, attemptStageTransfer, "network", "network-error"},
		{"azcopy", common.EAzError.InvalidServiceClient(), outcomeFailed, attemptStageInitialization, "azcopy", "azcopy-4"},
		{"stage fallback", errors.New("contains sensitive text"), outcomeFailed, attemptStageEnumeration, "enumeration", "enumeration-error"},
		{"unknown fallback", errors.New("contains sensitive text"), outcomeFailed, attemptStageCompleted, "unknown", "job-failed"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			category, code := jobErrorAttributes(test.err, test.outcome, test.stage)
			assert.Equal(t, test.wantCategory, category.String())
			assert.Equal(t, test.wantCode, code)
		})
	}
}

func TestAttemptTelemetryWireValues(t *testing.T) {
	assert.Equal(t, [...]string{"initialization", "enumeration", "transfer", "completion", "completed"}, attemptStageNames)
	assert.Equal(t, [...]string{"", "initialization", "enumeration", "transfer", "completion", "authentication", "authorization",
		"not-found", "conflict", "throttling", "timeout", "request", "service", "network", "local-io", "azcopy", "unknown"}, jobErrorCategoryNames)
	assert.Empty(t, attemptStage(len(attemptStageNames)).String())
	assert.Empty(t, jobErrorCategory(len(jobErrorCategoryNames)).String())
}

func TestSanitizeJobErrorCode(t *testing.T) {
	assert.Equal(t, "AuthorizationPermissionMismatch", sanitizeJobErrorCode(" AuthorizationPermissionMismatch "))
	assert.Empty(t, sanitizeJobErrorCode("code with spaces"))
	assert.Empty(t, sanitizeJobErrorCode(strings.Repeat("a", 65)))
}

func TestDisabledAgentIsNoop(t *testing.T) {
	client := &failurePolicyClient{}
	disabled := failureTestAgent(client)
	disabled.enabled = false
	stopped := failureTestAgent(client)
	stopped.stopped.Store(true)
	assert.NotPanics(t, func() {
		for _, agent := range []*telemetryAgent{nil, disabled, stopped} {
			agent.reportStarted(telemetry.JobDimensions{}, "id", "invocation", time.Now())
			agent.reportFinished(telemetry.JobFinishedEvent{JobID: "id", InvocationID: "invocation"})
			agent.reportCommand("list", "id", "invocation", telemetry.OptionAttributes{})
			agent.flush(time.Second)
		}
		var zero Client
		zero.ReportCommandInvoked("list", "id", telemetry.OptionAttributes{})
		zero.FlushTelemetry()
	})
	assert.Nil(t, disabled.pending)
	assert.Nil(t, stopped.pending)
	client.mu.Lock()
	defer client.mu.Unlock()
	assert.Empty(t, client.bodies)
}

type orderedTelemetryClient struct {
	mu           sync.Mutex
	eventNames   []string
	firstEntered chan struct{}
	releaseFirst chan struct{}
}

func (c *orderedTelemetryClient) Do(req *http.Request) (*http.Response, error) {
	body, err := io.ReadAll(req.Body)
	if err != nil {
		return nil, err
	}

	eventName := ""
	for _, candidate := range []string{"azcopy.job.started", "azcopy.job.finished"} {
		if strings.Contains(string(body), candidate) {
			eventName = candidate
			break
		}
	}

	c.mu.Lock()
	c.eventNames = append(c.eventNames, eventName)
	callNumber := len(c.eventNames)
	c.mu.Unlock()
	if callNumber == 1 {
		close(c.firstEntered)
		<-c.releaseFirst
	}

	return &http.Response{
		StatusCode: http.StatusOK,
		Body:       io.NopCloser(strings.NewReader("")),
		Header:     make(http.Header),
	}, nil
}

func TestFinishedTelemetryWaitsForStartedDelivery(t *testing.T) {
	client := &orderedTelemetryClient{
		firstEntered: make(chan struct{}),
		releaseFirst: make(chan struct{}),
	}
	agent := &telemetryAgent{
		enabled: true,
		reporter: telemetry.NewReporter(telemetry.Config{
			ConnectionString: "InstrumentationKey=00000000-0000-0000-0000-000000000001;IngestionEndpoint=https://example.test",
			HTTPClient:       client,
		}),
	}
	const jobID = "job"
	const invocationID = "invocation"
	agent.reportStarted(telemetry.JobDimensions{}, jobID, invocationID, time.Now())
	<-client.firstEntered

	finishedReturned := make(chan struct{})
	go func() {
		agent.reportFinished(telemetry.JobFinishedEvent{
			JobID:        jobID,
			InvocationID: invocationID,
			EndTimestamp: time.Now(),
		})
		close(finishedReturned)
	}()
	select {
	case <-finishedReturned:
	case <-time.After(250 * time.Millisecond):
		t.Fatal("reportFinished blocked on telemetry delivery")
	}

	client.mu.Lock()
	assert.Equal(t, []string{"azcopy.job.started"}, client.eventNames)
	client.mu.Unlock()

	close(client.releaseFirst)
	agent.flush(time.Second)

	client.mu.Lock()
	assert.Equal(t, []string{"azcopy.job.started", "azcopy.job.finished"}, client.eventNames)
	client.mu.Unlock()
}

type blockingTelemetryClient struct {
	entered chan struct{}
	release chan struct{}
	once    sync.Once
}

func (c *blockingTelemetryClient) Do(*http.Request) (*http.Response, error) {
	c.once.Do(func() { close(c.entered) })
	<-c.release
	return &http.Response{
		StatusCode: http.StatusOK,
		Body:       io.NopCloser(strings.NewReader("")),
		Header:     make(http.Header),
	}, nil
}

func newBlockingTelemetryAgent(client *blockingTelemetryClient) *telemetryAgent {
	return &telemetryAgent{
		enabled: true,
		reporter: telemetry.NewReporter(telemetry.Config{
			ConnectionString: "InstrumentationKey=00000000-0000-0000-0000-000000000001;IngestionEndpoint=https://example.test",
			HTTPClient:       client,
		}),
	}
}

func TestCommandTelemetryDoesNotDelayCommandExecution(t *testing.T) {
	client := &blockingTelemetryClient{
		entered: make(chan struct{}),
		release: make(chan struct{}),
	}
	agent := newBlockingTelemetryAgent(client)

	commandReturned := make(chan struct{})
	go func() {
		agent.reportCommand("list", "job", "invocation", telemetry.OptionAttributes{})
		close(commandReturned)
	}()
	select {
	case <-commandReturned:
	case <-time.After(250 * time.Millisecond):
		t.Fatal("reportCommand blocked on telemetry delivery")
	}
	<-client.entered

	close(client.release)
	agent.flush(time.Second)
}

func TestTelemetryFlushHasSingleBoundedExitBudget(t *testing.T) {
	client := &blockingTelemetryClient{
		entered: make(chan struct{}),
		release: make(chan struct{}),
	}
	agent := newBlockingTelemetryAgent(client)
	agent.reportStarted(telemetry.JobDimensions{}, "job", "invocation", time.Now())
	<-client.entered
	agent.reportFinished(telemetry.JobFinishedEvent{
		JobID:        "job",
		InvocationID: "invocation",
		EndTimestamp: time.Now(),
	})

	startedAt := time.Now()
	agent.flush(40 * time.Millisecond)
	elapsed := time.Since(startedAt)
	assert.GreaterOrEqual(t, elapsed, 30*time.Millisecond)
	assert.Less(t, elapsed, 200*time.Millisecond)

	close(client.release)
	agent.flush(time.Second)
}

func telemetryResourceForTest() telemetry.ResourceAttributes {
	return telemetry.ResourceAttributes{
		AzCopyVersion: common.AzcopyVersion,
	}
}
