// Copyright © 2025 Microsoft <wastore@microsoft.com>
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
	"fmt"
	"math"
	"runtime"
	"strings"
	"time"

	"github.com/Azure/azure-storage-azcopy/v10/common"
)

// Copy Output

// GetCopyProgress returns the copy progress formatted as a string
func GetCopyProgress(progress CopyProgress, isBenchmark bool) string {
	// abbreviated output for cleanup jobs
	cleanupStatusString := fmt.Sprintf("Cleanup %v/%v", progress.TransfersCompleted, progress.TotalTransfers)
	if progress.IsCleanupJob {
		return cleanupStatusString
	}

	// if json is not needed, then we generate a message that goes nicely on the same line
	// display a scanning keyword if the job is not completely ordered
	var scanningString = " (scanning...)"
	if progress.CompleteJobOrdered {
		scanningString = ""
	}

	throughputString := fmt.Sprintf("2-sec Throughput (Mb/s): %v", ToFixed(progress.Throughput, 4))
	if progress.Throughput == 0 {
		// As there would be case when no bits sent from local, e.g. service side copy, when throughput = 0, hide it.
		throughputString = ""
	}

	// indicate whether constrained by disk or not
	perfString, diskString := GetPerfDisplayText(progress.PerfStrings, progress.PerfConstraint, progress.ElapsedTime, isBenchmark)
	return fmt.Sprintf("%.1f %%, %v Done, %v Failed, %v Pending, %v Skipped, %v Total%s, %s%s%s",
		progress.PercentComplete,
		progress.TransfersCompleted,
		progress.TransfersFailed,
		progress.TotalTransfers-(progress.TransfersCompleted+progress.TransfersFailed+progress.TransfersSkipped),
		progress.TransfersSkipped+progress.SkippedSymlinkCount+progress.SkippedSpecialFileCount,
		progress.TotalTransfers, scanningString, perfString, throughputString, diskString)
}

// GetCopyResult returns the copy result formatted as a string
// showStats is true during benchmark or when logging
func GetCopyResult(result CopyResult, showStats bool) string {
	stats := FormatExtraStats(result.AverageIOPS, result.AverageE2EMilliseconds, result.NetworkErrorPercentage, result.ServerBusyPercentage)

	output := fmt.Sprintf(
		`

Job %s summary
Elapsed Time (Minutes): %v
Number of File Transfers: %v
Number of Folder Property Transfers: %v
Number of Symlink Transfers: %v
Total Number of Transfers: %v
Number of File Transfers Completed: %v
Number of Folder Transfers Completed: %v
Number of File Transfers Failed: %v
Number of Folder Transfers Failed: %v
Number of File Transfers Skipped: %v
Number of Folder Transfers Skipped: %v
Number of Symbolic Links Skipped: %v
Number of Hardlinks Converted: %v
Number of Hardlinks Transferred: %v
Number of Hardlinks Skipped: %v
Number of Special Files Skipped: %v
Total Number of Bytes Transferred: %v
Final Job Status: %v%s%s
`,
		result.JobID.String(),
		ToFixed(result.ElapsedTime.Minutes(), 4),
		result.FileTransfers,
		result.FolderPropertyTransfers,
		result.SymlinkTransfers,
		result.TotalTransfers,
		result.TransfersCompleted-result.FoldersCompleted,
		result.FoldersCompleted,
		result.TransfersFailed-result.FoldersFailed,
		result.FoldersFailed,
		result.TransfersSkipped-result.FoldersSkipped,
		result.FoldersSkipped,
		result.SkippedSymlinkCount,
		result.HardlinksConvertedCount,
		result.HardlinksTransferCount,
		result.SkippedHardlinkCount,
		result.SkippedSpecialFileCount,
		result.TotalBytesTransferred,
		result.JobStatus,
		common.Iff(showStats, stats, ""),
		FormatPerfAdvice(result.PerformanceAdvice))

	// abbreviated output for cleanup jobs
	if result.IsCleanupJob {
		cleanupStatusString := fmt.Sprintf("Cleanup %v/%v", result.TransfersCompleted, result.TotalTransfers)
		output = fmt.Sprintf("%s: %s)", cleanupStatusString, result.JobStatus)
	}
	return output
}

// Sync Output

// GetSyncProgress returns the sync progress formatted as a string
func GetSyncProgress(progress SyncProgress) string {
	// indicate whether constrained by disk or not
	perfString, diskString := GetPerfDisplayText(progress.PerfStrings, progress.PerfConstraint, progress.ElapsedTime, false)

	return fmt.Sprintf("%.1f %%, %v Done, %v Failed, %v Pending, %v Total%s, 2-sec Throughput (Mb/s): %v%s",
		progress.PercentComplete,
		progress.TransfersCompleted,
		progress.TransfersFailed,
		progress.TotalTransfers-progress.TransfersCompleted-progress.TransfersFailed,
		progress.TotalTransfers, perfString, ToFixed(progress.Throughput, 4), diskString)
}

// GetSyncResult returns the sync result formatted as a string
func GetSyncResult(result SyncResult, showStats bool) string {
	stats := FormatExtraStats(result.AverageIOPS, result.AverageE2EMilliseconds, result.NetworkErrorPercentage, result.ServerBusyPercentage)

	return fmt.Sprintf(
		`
Job %s Summary
Files Scanned at Source: %v
Files Scanned at Destination: %v
Elapsed Time (Minutes): %v
Number of Copy Transfers for Files: %v
Number of Copy Transfers for Folder Properties: %v
Number of Symlink Transfers: %v
Total Number of Copy Transfers: %v
Number of Copy Transfers Completed: %v
Number of Copy Transfers Failed: %v
Number of Deletions at Destination: %v
Number of Symbolic Links Skipped: %v
Number of Special Files Skipped: %v
Number of Hardlinks Converted: %v
Number of Hardlinks Skipped: %v
Total Number of Bytes Transferred: %v
Total Number of Bytes Enumerated: %v
Final Job Status: %v%s%s
`,
		result.JobID.String(),
		result.SourceFilesScanned,
		result.DestinationFilesScanned,
		ToFixed(result.ElapsedTime.Minutes(), 4),
		result.FileTransfers,
		result.FolderPropertyTransfers,
		result.SymlinkTransfers,
		result.TotalTransfers,
		result.TransfersCompleted,
		result.TransfersFailed,
		result.DeleteTransfersCompleted,
		result.SkippedSymlinkCount,
		result.SkippedSpecialFileCount,
		result.HardlinksConvertedCount,
		result.SkippedHardlinkCount,
		result.TotalBytesTransferred,
		result.TotalBytesEnumerated,
		result.JobStatus,
		common.Iff(showStats, stats, ""),
		FormatPerfAdvice(result.PerformanceAdvice))
}

// GetPerfDisplayText
// Is disk speed looking like a constraint on throughput?  Ignore the first little-while,
// to give an (arbitrary) amount of time for things to reach steady-state.
func GetPerfDisplayText(perfDiagnosticStrings []string, constraint common.PerfConstraint, durationOfJob time.Duration, isBench bool) (perfString string, diskString string) {
	perfString = ""
	if shouldDisplayPerfStates() {
		perfString = "[States: " + strings.Join(perfDiagnosticStrings, ", ") + "], "
	}

	haveBeenRunningLongEnoughToStabilize := durationOfJob.Seconds() > 30                                    // this duration is an arbitrary guesstimate
	if constraint != common.EPerfConstraint.Unknown() && haveBeenRunningLongEnoughToStabilize && !isBench { // don't display when benchmarking, because we got some spurious slow "disk" constraint reports there - which would be confusing given there is no disk in release 1 of benchmarking
		diskString = fmt.Sprintf(" (%s may be limiting speed)", constraint)
	} else {
		diskString = ""
	}
	return
}

func shouldDisplayPerfStates() bool {
	return common.GetEnvironmentVariable(common.EEnvironmentVariable.ShowPerfStates()) != ""
}

// round api rounds up the float number after the decimal point.
func round(num float64) int {
	return int(num + math.Copysign(0.5, num))
}

// ToFixed api returns the float number precised up to given decimal places.
func ToFixed(num float64, precision int) float64 {
	output := math.Pow(10, float64(precision))
	return float64(round(num*output)) / output
}

// format extra stats to include in the log.  If benchmarking, also output them on screen (but not to screen in normal
// usage because too cluttered)
func FormatExtraStats(avgIOPS int, avgE2EMilliseconds int, networkErrorPercent float32, serverBusyPercent float32) string {
	return fmt.Sprintf(
		`

Diagnostic stats:
IOPS: %v
End-to-end ms per request: %v
Network Errors: %.2f%%
Server Busy: %.2f%%`,
		avgIOPS, avgE2EMilliseconds, networkErrorPercent, serverBusyPercent)
}

func FormatPerfAdvice(advice []common.PerformanceAdvice) string {
	if len(advice) == 0 {
		return ""
	}
	b := strings.Builder{}
	b.WriteString("\n\n") // two newlines to separate the perf results from everything else
	b.WriteString("Performance benchmark results: \n")
	b.WriteString("Note: " + common.BenchmarkPreviewNotice + "\n")
	for _, a := range advice {
		b.WriteString("\n")
		pri := "Main"
		if !a.PriorityAdvice {
			pri = "Additional"
		}
		b.WriteString(pri + " Result:\n")
		b.WriteString("  Code:   " + a.Code + "\n")
		b.WriteString("  Desc:   " + a.Title + "\n")
		b.WriteString("  Reason: " + a.Reason + "\n")
	}
	b.WriteString("\n")
	b.WriteString(common.BenchmarkFinalDisclaimer)
	if runtime.GOOS == "linux" {
		b.WriteString(common.BenchmarkLinuxExtraDisclaimer)
	}
	return b.String()
}
