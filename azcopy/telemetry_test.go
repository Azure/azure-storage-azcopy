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
	"errors"
	"fmt"
	"math"
	"net/url"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/Azure/azure-sdk-for-go/sdk/azcore"
	"github.com/Azure/azure-storage-azcopy/v10/common"
	"github.com/Azure/azure-storage-azcopy/v10/telemetry"
	"github.com/Azure/azure-storage-azcopy/v10/traverser"
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

func TestClassifyFSType(t *testing.T) {
	a := assert.New(t)
	a.Equal("nas-nfs", classifyFSType("nfs"))
	a.Equal("nas-nfs", classifyFSType("nfs4"))
	a.Equal("nas-smb", classifyFSType("cifs"))
	a.Equal("nas-smb", classifyFSType("smb3"))
	a.Equal("nas-smb", classifyFSType("smbfs"))
	a.Equal("local-disk", classifyFSType("ext4"))
	a.Equal("local-disk", classifyFSType("xfs"))
	a.Equal("", classifyFSType(""))
}

func TestParseMountinfoLine(t *testing.T) {
	a := assert.New(t)
	mp, fs, ok := parseMountinfoLine("36 35 98:0 / /mnt/nas rw,noatime - nfs4 1.2.3.4:/export rw")
	a.True(ok)
	a.Equal("/mnt/nas", mp)
	a.Equal("nfs4", fs)

	mp, fs, ok = parseMountinfoLine("22 30 0:21 / / rw,relatime shared:1 - ext4 /dev/root rw")
	a.True(ok)
	a.Equal("/", mp)
	a.Equal("ext4", fs)

	_, _, ok = parseMountinfoLine("garbage line without separator")
	a.False(ok)
}

func TestPathHasMountPrefix(t *testing.T) {
	a := assert.New(t)
	a.True(pathHasMountPrefix("/mnt/nas/data", "/mnt/nas"))
	a.True(pathHasMountPrefix("/mnt/nas", "/mnt/nas"))
	a.True(pathHasMountPrefix("/anything", "/"))
	a.False(pathHasMountPrefix("/mnt/nasextra", "/mnt/nas")) // not a path-segment prefix
	a.False(pathHasMountPrefix("/home/user", "/mnt/nas"))
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

func TestEndpointKind(t *testing.T) {
	a := assert.New(t)
	a.Equal("public", endpointKind(
		common.ResourceString{Value: "https://acct.blob.core.windows.net/c"},
		common.ELocation.Blob()))
	a.Equal("private-endpoint", endpointKind(
		common.ResourceString{Value: "https://acct.privatelink.blob.core.windows.net/c"},
		common.ELocation.Blob()))
	// Non-Azure destinations have no endpoint kind.
	a.Equal("", endpointKind(
		common.ResourceString{Value: "/local/path"},
		common.ELocation.Local()))
}

func TestEndpointCloudType(t *testing.T) {
	a := assert.New(t)
	a.Equal("public", endpointCloudType(
		common.ResourceString{Value: "https://acct.blob.core.windows.net/c"}, common.ELocation.Blob()))
	a.Equal("gov", endpointCloudType(
		common.ResourceString{Value: "https://acct.blob.core.usgovcloudapi.net/c"}, common.ELocation.Blob()))
	a.Equal("china", endpointCloudType(
		common.ResourceString{Value: "https://acct.blob.core.chinacloudapi.cn/c"}, common.ELocation.Blob()))
	a.Equal("germany", endpointCloudType(
		common.ResourceString{Value: "https://acct.blob.core.cloudapi.de/c"}, common.ELocation.Blob()))
	a.Equal("unknown", endpointCloudType(
		common.ResourceString{Value: "https://acct.blob.example.com/c"}, common.ELocation.Blob()))
	a.Empty(endpointCloudType(
		common.ResourceString{Value: "https://s3.amazonaws.com/bucket"}, common.ELocation.S3()))
	a.Empty(endpointCloudType(
		common.ResourceString{Value: "/local/path"}, common.ELocation.Local()))
}

func TestNICSpeedBucket(t *testing.T) {
	assert.Equal(t, "unknown", nicSpeedBucket(-1))
	assert.Equal(t, "<1gbps", nicSpeedBucket(100))
	assert.Equal(t, "1-<10gbps", nicSpeedBucket(1000))
	assert.Equal(t, "10-<40gbps", nicSpeedBucket(10000))
	assert.Equal(t, ">=40gbps", nicSpeedBucket(40000))
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
	assert.Equal(t, "gov", dimensions.DestCloudType)
}

func TestCopyJobDimensionsBenchmark(t *testing.T) {
	o := &CookedTransferOptions{
		fromTo:      common.EFromTo.BenchmarkBlob(),
		source:      common.ResourceString{Value: "https://benchmark"},
		destination: common.ResourceString{Value: "https://account.blob.core.windows.net/container/benchmark-job"},
		benchmarkTelemetry: &benchmarkTelemetryOptions{
			mode:             "upload",
			fileCount:        100,
			fileSizeBytes:    256 * 1024 * 1024,
			folderCount:      10,
			cleanupRequested: true,
		},
	}
	dimensions := copyJobDimensions(o, common.ECredentialType.Anonymous(), common.ECredentialType.OAuthToken())
	assert.Equal(t, "bench", dimensions.Command)
	assert.Equal(t, "upload", dimensions.BenchmarkMode)
	assert.Equal(t, int64(100), dimensions.BenchmarkFileCount)
	assert.Equal(t, int64(256*1024*1024), dimensions.BenchmarkFileSizeBytes)
	assert.Equal(t, int64(10), dimensions.BenchmarkFolderCount)
	assert.True(t, dimensions.BenchmarkCleanupRequested)
	assert.False(t, dimensions.BenchmarkIsCleanup)
}

func TestShouldEmitCopyTelemetry(t *testing.T) {
	assert.True(t, shouldEmitCopyTelemetry(&CookedTransferOptions{}))
	assert.False(t, shouldEmitCopyTelemetry(&CookedTransferOptions{dryrun: true}))
	assert.True(t, shouldEmitCopyTelemetry(&CookedTransferOptions{benchmarkTelemetry: &benchmarkTelemetryOptions{}}))
	assert.False(t, shouldEmitCopyTelemetry(&CookedTransferOptions{benchmarkTelemetry: &benchmarkTelemetryOptions{isCleanup: true}}))
}

func TestBenchmarkTelemetrySurvivesOptionCooking(t *testing.T) {
	options := CopyOptions{
		FromTo:    common.EFromTo.BenchmarkBlob(),
		Recursive: true,
	}
	options.SetBenchmarkTelemetry("upload", 25, 4*1024*1024, 5, true, false)
	source := traverser.BenchmarkSourceHelper{}.ToUrl(25, 4*1024*1024, 5)
	cooked, err := newCookedCopyOptions(source, "https://account.blob.core.windows.net/container/benchmark-job", options)
	require.NoError(t, err)
	dimensions := copyJobDimensions(cooked, common.ECredentialType.Anonymous(), common.ECredentialType.OAuthToken())
	assert.Equal(t, "bench", dimensions.Command)
	assert.Equal(t, "upload", dimensions.BenchmarkMode)
	assert.Equal(t, int64(25), dimensions.BenchmarkFileCount)
	assert.Equal(t, int64(4*1024*1024), dimensions.BenchmarkFileSizeBytes)
	assert.Equal(t, int64(5), dimensions.BenchmarkFolderCount)
	assert.True(t, dimensions.BenchmarkCleanupRequested)
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
	assert.Equal(t, "account", dimensions.DestStorageAccount)
	assert.Empty(t, dimensions.SourceCloudType)
	assert.Equal(t, "public", dimensions.DestCloudType)
	assert.Equal(t, []string{"include"}, dimensions.Options.FlagsSet)
	options.FlagsSet[0] = "mutated"
	options.Values["OptExample"] = "mutated"
	assert.Equal(t, []string{"include"}, dimensions.Options.FlagsSet)
	assert.Equal(t, "value", dimensions.Options.Values["OptExample"])
}

func TestResumeProgressTrackerElapsedTimeBeforeStart(t *testing.T) {
	assert.Zero(t, (&resumeProgressTracker{}).GetElapsedTime())
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
	evt := buildFinishedEvent(telemetryResourceForTest(), dims, "job-1234", "invocation-1234", start, end, summary, 2*time.Second, 1500*time.Millisecond, time.Second, shape)

	a.Equal("job-1234", evt.JobID)
	a.Equal("invocation-1234", evt.InvocationID)
	a.Equal(int64(1), evt.FinishedCount)
	a.Equal(common.EJobStatus.Completed().String(), evt.JobStatus)
	a.Equal("403:3,500:1", evt.FailureErrorCodes)
	a.Equal(int64(0), evt.FailureErrorOtherCount)
	a.Equal(common.EPerfConstraint.Service().String(), evt.PerformanceConstraint)
	a.Equal([]string{"NetworkErrors", "ConcurrencyHitUpperLimit"}, evt.PerformanceAdviceCodes)
	a.Equal(100.0, evt.PercentComplete)
	a.Equal(int64(1500000), evt.BytesEnumerated)
	a.Equal(int64(1200000), evt.BytesExpected)
	a.Equal(int64(1000000), evt.BytesTransferred)
	a.Equal(int64(1100000), evt.BytesOverWire)
	a.Equal(int64(9), evt.ObjectsScheduled)
	a.Equal(int64(6), evt.RegularFilesScheduled)
	a.Equal(int64(2), evt.SymlinksScheduled)
	a.Equal(int64(1), evt.HardlinksConvertedScheduled)
	a.Equal(int64(4), evt.FolderPropertiesScheduled)
	a.Equal(int64(7), evt.ObjectsCompleted)
	a.Equal(int64(1), evt.ObjectsFailed)
	a.Equal(int64(1), evt.ObjectsSkipped)
	a.Equal(int64(3), evt.FolderPropertiesCompleted)
	a.Equal(int64(0), evt.FolderPropertiesFailed)
	a.Equal(int64(1), evt.FolderPropertiesSkipped)
	a.Equal(int64(20), evt.SourceObjectsScanned)
	a.Equal(int64(40960), evt.SourceBytesScanned)
	a.Equal(2048.0, evt.SourceAverageObjectSizeBytes)
	a.Equal(int64(1024), evt.SourceObjectSizeP50BytesApprox)
	a.Equal(int64(16*1024*1024), evt.SourceObjectSizeP90BytesApprox)
	a.Equal(int64(256*1024*1024), evt.SourceObjectSizeP95BytesApprox)
	a.Equal(int64(12), evt.SourceObjectsUnder1MiB)
	a.Equal(60.0, evt.SourceObjectsUnder1MiBRatioPct)
	a.Equal(int64(5), evt.SourceMaxDirectoryDepth)
	a.Equal(int64(3), evt.ContainersScanned)
	a.Equal(int64(2), evt.ContainersTouched)
	a.Equal(int64(10), evt.TransfersCompleted)
	a.Equal(int64(1), evt.TransfersFailed)
	a.Equal(int64(2), evt.TransfersSkipped)
	a.Equal(int64(13), evt.TransfersTotal)
	a.Equal(2.0, evt.JobDurationSeconds)
	a.Equal(1.5, evt.EnumerationPhaseDurationSeconds)
	a.Equal(1.0, evt.TransferPhaseDurationSeconds)
	a.InDelta(4.0, evt.JobThroughputMbps, 1e-9)           // 1e6 bytes * 8 / 1e6 / 2s
	a.InDelta(8.0, evt.TransferPhaseThroughputMbps, 1e-9) // 1e6 bytes * 8 / 1e6 / 1s
	a.Equal(int64(50), evt.AverageStorageHTTPAttemptE2EMs)
	a.Equal(int64(7), evt.AvgIOPS)
	a.Equal(int64(200), evt.StorageHTTPAttemptCount)
	a.Equal(int64(1), evt.NetworkErrorAttemptCount)
	a.Equal(int64(3), evt.ServerBusy503Count)
	a.Equal(int64(2), evt.ServerBusyThroughputCount)
	a.Equal(int64(1), evt.ServerBusyIOPSCount)
	a.Equal(int64(0), evt.ServerBusyOtherCount)
}

func TestCountExcludingFolders(t *testing.T) {
	assert.Equal(t, int64(7), countExcludingFolders(10, 3))
	assert.Zero(t, countExcludingFolders(3, 3))
	assert.Zero(t, countExcludingFolders(2, 3))
}

func TestAggregateErrorCodes(t *testing.T) {
	a := assert.New(t)
	// No failures -> empty.
	a.Equal("", aggregateErrorCodes(nil))
	a.Equal("", aggregateErrorCodes([]common.TransferDetail{}))
	// Ordered by descending count, then ascending code.
	a.Equal("403:3,500:1", aggregateErrorCodes([]common.TransferDetail{
		{ErrorCode: 500}, {ErrorCode: 403}, {ErrorCode: 403}, {ErrorCode: 403},
	}))
	// Tie on count -> lower code first.
	a.Equal("404:1,409:1", aggregateErrorCodes([]common.TransferDetail{
		{ErrorCode: 409}, {ErrorCode: 404},
	}))
	// Bounded to maxErrorCodeBuckets distinct codes.
	many := make([]common.TransferDetail, 0, maxErrorCodeBuckets+5)
	for i := 0; i < maxErrorCodeBuckets+5; i++ {
		many = append(many, common.TransferDetail{ErrorCode: int32(600 + i)})
	}
	res := aggregateErrorCodes(many)
	a.Equal(maxErrorCodeBuckets, strings.Count(res, ":"))
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

func TestStorageAccountName(t *testing.T) {
	a := assert.New(t)
	// Azure remote: first DNS label is the account name.
	a.Equal("myaccount", storageAccountName(
		common.ResourceString{Value: "https://myaccount.blob.core.windows.net/container/path"},
		common.ELocation.Blob()))
	a.Equal("acct2", storageAccountName(
		common.ResourceString{Value: "https://acct2.dfs.core.windows.net/fs"},
		common.ELocation.BlobFS()))
	a.Equal("privateacct", storageAccountName(
		common.ResourceString{Value: "https://privateacct.privatelink.blob.core.windows.net/container"},
		common.ELocation.Blob()))
	// Azure-typed custom endpoints do not emit a hostname-derived identity.
	a.Equal("", storageAccountName(
		common.ResourceString{Value: "https://acct.blob.example.com/container"},
		common.ELocation.Blob()))
	// Local locations return empty.
	a.Equal("", storageAccountName(
		common.ResourceString{Value: "/local/path"},
		common.ELocation.Local()))
	// Non-Azure providers never emit bucket names.
	a.Equal("", storageAccountName(
		common.ResourceString{Value: "https://bucket.s3.amazonaws.com/key"},
		common.ELocation.S3()))
	a.Equal("", storageAccountName(
		common.ResourceString{Value: "https://storage.cloud.google.com/mybucket/object"},
		common.ELocation.GCP()))
	// Hostless values return empty.
	a.Equal("", storageAccountName(
		common.ResourceString{Value: "relative/path"},
		common.ELocation.Blob()))
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

func TestNewTelemetryInvocationID(t *testing.T) {
	first := newTelemetryInvocationID()
	second := newTelemetryInvocationID()
	assert.Len(t, first, 32)
	assert.Len(t, second, 32)
	assert.NotEqual(t, first, second)
}

func TestShouldSampleTelemetry(t *testing.T) {
	assert.False(t, shouldSampleTelemetry("", 1))
	assert.False(t, shouldSampleTelemetry("job", 0))
	assert.True(t, shouldSampleTelemetry("job", 1))

	includedAtOnePercent := 0
	for index := 0; index < 10000; index++ {
		jobID := fmt.Sprintf("job-%d", index)
		atOnePercent := shouldSampleTelemetry(jobID, 0.01)
		assert.Equal(t, atOnePercent, shouldSampleTelemetry(jobID, 0.01))
		if atOnePercent {
			includedAtOnePercent++
			assert.True(t, shouldSampleTelemetry(jobID, 0.10), jobID)
		}
	}
	assert.Greater(t, includedAtOnePercent, 70)
	assert.Less(t, includedAtOnePercent, 130)
}

func TestBuildResourceAttributesSamplingMetadata(t *testing.T) {
	resource := buildResourceAttributes()
	assert.Equal(t, telemetrySchemaVersion, resource.SchemaVersion)
	assert.Equal(t, telemetrySamplingRate, resource.SamplingRate)
	assert.Equal(t, telemetrySamplerVersion, resource.SamplerVersion)
}

func TestBuildResourceAttributesE2ETestRunID(t *testing.T) {
	t.Setenv(envE2ETelemetryRunID, "pipeline-run-123")
	assert.Equal(t, "pipeline-run-123", buildResourceAttributes().E2ETestRunID)

	t.Setenv(envE2ETelemetryRunID, "   ")
	assert.Empty(t, buildResourceAttributes().E2ETestRunID)
}

func TestConfigureTelemetrySamplingRate(t *testing.T) {
	original := telemetrySamplingRate
	t.Cleanup(func() { telemetrySamplingRate = original })

	assert.NoError(t, configureTelemetrySamplingRate(nil))
	assert.Equal(t, defaultTelemetrySamplingRate, telemetrySamplingRate)

	for _, rate := range []float64{0, 0.25, 1} {
		rate := rate
		t.Run(fmt.Sprintf("rate-%g", rate), func(t *testing.T) {
			assert.NoError(t, configureTelemetrySamplingRate(&rate))
			assert.Equal(t, rate, telemetrySamplingRate)
			assert.Equal(t, rate, buildResourceAttributes().SamplingRate)
		})
	}

	for _, rate := range []float64{-0.01, 1.01, math.NaN(), math.Inf(1), math.Inf(-1)} {
		rate := rate
		t.Run(fmt.Sprintf("invalid-%v", rate), func(t *testing.T) {
			assert.Error(t, configureTelemetrySamplingRate(&rate))
		})
	}
}

func TestConfiguredTelemetryConnectionString(t *testing.T) {
	original := telemetryConnectionString
	t.Cleanup(func() { telemetryConnectionString = original })

	telemetryConnectionString = ""
	assert.Empty(t, configuredTelemetryConnectionString(func(string) string { return "" }))

	telemetryConnectionString = "InstrumentationKey=00000000-0000-0000-0000-000000000000"
	assert.Empty(t, configuredTelemetryConnectionString(func(string) string { return "" }))

	telemetryConnectionString = "InstrumentationKey=build-time"
	assert.Equal(t, "InstrumentationKey=build-time", configuredTelemetryConnectionString(func(string) string { return "" }))
	assert.Equal(t, "InstrumentationKey=runtime", configuredTelemetryConnectionString(func(name string) string {
		if name == envTelemetryConnectionString {
			return "InstrumentationKey=runtime"
		}
		return ""
	}))
}

func TestTerminalAttemptStatus(t *testing.T) {
	tests := []struct {
		name       string
		status     common.JobStatus
		err        error
		wantStatus common.JobStatus
		wantReason string
	}{
		{"completed", common.EJobStatus.Completed(), nil, common.EJobStatus.Completed(), "completed"},
		{"completed with errors", common.EJobStatus.CompletedWithErrors(), nil, common.EJobStatus.CompletedWithErrors(), "completed-with-errors"},
		{"failed error", common.EJobStatus.InProgress(), errors.New("failed"), common.EJobStatus.Failed(), "failed"},
		{"cancelled context", common.EJobStatus.InProgress(), context.Canceled, common.EJobStatus.Cancelled(), "cancelled"},
		{"cancelled summary", common.EJobStatus.Cancelled(), nil, common.EJobStatus.Cancelled(), "cancelled"},
		{"missing success summary", common.EJobStatus.InProgress(), nil, common.EJobStatus.Completed(), "completed"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			status, reason := terminalAttemptStatus(test.status, test.err)
			assert.Equal(t, test.wantStatus, status)
			assert.Equal(t, test.wantReason, reason)
		})
	}
}

func TestAttemptTelemetryFinalizerIsIdempotent(t *testing.T) {
	finalizer := newAttemptTelemetryFinalizer(nil, telemetry.JobDimensions{}, "job", "invocation", time.Now())
	finalizer.finish(errors.New("first"))
	assert.True(t, finalizer.finished)
	finalizer.finish(errors.New("second"))
	assert.True(t, finalizer.finished)
}

func TestJobErrorAttributes(t *testing.T) {
	tests := []struct {
		name         string
		err          error
		reason       string
		stage        string
		wantCategory string
		wantCode     string
	}{
		{"success", nil, "completed", "completed", "", ""},
		{"cancelled", context.Canceled, "cancelled", "enumeration", "", ""},
		{"partial success", nil, "completed-with-errors", "completed", "transfer", "transfer-failures"},
		{"authentication", &azcore.ResponseError{ErrorCode: "AuthenticationFailed", StatusCode: 403}, "failed", "enumeration", "authentication", "AuthenticationFailed"},
		{"authorization", &azcore.ResponseError{ErrorCode: "AuthorizationPermissionMismatch", StatusCode: 403}, "failed", "transfer", "authorization", "AuthorizationPermissionMismatch"},
		{"throttling", &azcore.ResponseError{ErrorCode: "ServerBusy", StatusCode: 503}, "failed", "transfer", "throttling", "ServerBusy"},
		{"http fallback", &azcore.ResponseError{StatusCode: 404}, "failed", "enumeration", "not-found", "http-404"},
		{"deadline", context.DeadlineExceeded, "failed", "transfer", "timeout", "context-deadline-exceeded"},
		{"local path", &os.PathError{Op: "open", Path: "secret", Err: os.ErrNotExist}, "failed", "enumeration", "local-io", "local-path-error"},
		{"network", &url.Error{Op: "Get", URL: "https://secret", Err: errors.New("connection refused")}, "failed", "transfer", "network", "network-error"},
		{"azcopy", common.EAzError.InvalidServiceClient(), "failed", "initialization", "azcopy", "azcopy-4"},
		{"stage fallback", errors.New("contains sensitive text"), "failed", "enumeration", "enumeration", "enumeration-error"},
		{"unknown fallback", errors.New("contains sensitive text"), "failed", "", "unknown", "job-failed"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			category, code := jobErrorAttributes(test.err, test.reason, test.stage)
			assert.Equal(t, test.wantCategory, category)
			assert.Equal(t, test.wantCode, code)
		})
	}
}

func TestSanitizeJobErrorCode(t *testing.T) {
	assert.Equal(t, "AuthorizationPermissionMismatch", sanitizeJobErrorCode(" AuthorizationPermissionMismatch "))
	assert.Empty(t, sanitizeJobErrorCode("code with spaces"))
	assert.Empty(t, sanitizeJobErrorCode(strings.Repeat("a", 65)))
}

func TestTransferPhaseElapsedTime(t *testing.T) {
	copyTracker := &transferProgressTracker{}
	syncTracker := &syncProgressTracker{}
	assert.Zero(t, copyTracker.GetTransferElapsedTime())
	assert.Zero(t, syncTracker.GetTransferElapsedTime())
	assert.Zero(t, copyTracker.GetEnumerationElapsedTime())
	assert.Zero(t, syncTracker.GetEnumerationElapsedTime())

	now := time.Now()
	started := now.Add(-2 * time.Second).UnixNano()
	copyTracker.atomicTransferStartUnixNano = started
	syncTracker.atomicTransferStartUnixNano = started
	copyTracker.jobStartTime = now.Add(-3 * time.Second)
	syncTracker.jobStartTime = now.Add(-3 * time.Second)
	copyTracker.atomicEnumerationEndUnixNano = now.Add(-time.Second).UnixNano()
	syncTracker.atomicEnumerationEndUnixNano = now.Add(-time.Second).UnixNano()
	assert.InDelta(t, 2*time.Second, copyTracker.GetTransferElapsedTime(), float64(250*time.Millisecond))
	assert.InDelta(t, 2*time.Second, syncTracker.GetTransferElapsedTime(), float64(250*time.Millisecond))
	assert.InDelta(t, 2*time.Second, copyTracker.GetEnumerationElapsedTime(), float64(250*time.Millisecond))
	assert.InDelta(t, 2*time.Second, syncTracker.GetEnumerationElapsedTime(), float64(250*time.Millisecond))
}

func TestDisabledAgentIsNoop(t *testing.T) {
	a := assert.New(t)
	var agent *telemetryAgent
	// nil agent must not panic.
	agent.reportStarted(telemetry.JobDimensions{}, "id", "invocation", time.Now())
	agent.reportFinished(telemetry.JobFinishedEvent{})

	disabled := &telemetryAgent{enabled: false}
	disabled.reportStarted(telemetry.JobDimensions{}, "id", "invocation", time.Now())
	disabled.reportFinished(telemetry.JobFinishedEvent{})
	a.False(disabled.enabled)
}

func telemetryResourceForTest() telemetry.ResourceAttributes {
	return telemetry.ResourceAttributes{
		AzCopyVersion: common.AzcopyVersion,
	}
}
