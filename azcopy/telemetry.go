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
	"crypto/rand"
	"encoding/hex"
	"github.com/Azure/azure-storage-azcopy/v10/common"
	"github.com/Azure/azure-storage-azcopy/v10/telemetry"
	"os"
	"path/filepath"
	"runtime"
	"sort"
	"strconv"
	"strings"
	"time"
)

const (
	telemetrySchemaVersion = "1"
)

const (
	// envTelemetryConnectionString overrides the embedded connection string.
	envTelemetryConnectionString = "AZCOPY_TELEMETRY_CONNECTION_STRING"
	// envDisableTelemetry, when set to "true", disables telemetry entirely.
	envDisableTelemetry = "AZCOPY_DISABLE_TELEMETRY"
	// envE2ETelemetryRunID optionally correlates telemetry emitted by one E2E
	// pipeline matrix leg. It is unset in normal AzCopy usage.
	envE2ETelemetryRunID = "AZCOPY_E2E_TELEMETRY_RUN_ID"
	// The shorter shutdown drain can cancel an in-flight send before this deadline.
	telemetrySendTimeout = 5 * time.Second
	// telemetryFlushTimeout is the maximum telemetry may add to process exit.
	telemetryFlushTimeout    = 4 * time.Second
	telemetryMaxPendingSends = 4
	// installationIDFileName stores the anonymous, per-install identifier.
	installationIDFileName         = "installation_id"
	installationIDRenameRetryDelay = 10 * time.Millisecond
	installationIDRenameAttempts   = 100
)

func newTelemetryInvocationID() string {
	buf := make([]byte, 16)
	if _, err := rand.Read(buf); err != nil {
		return ""
	}
	return hex.EncodeToString(buf)
}

func buildResourceAttributes() telemetry.ResourceAttributes {
	hw := probeHostHardware()

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
		AzureVMDetected:    probeAzureVM(),
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
