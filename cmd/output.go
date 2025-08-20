package cmd

import (
	"encoding/json"
	"fmt"
	"math"
	"runtime"
	"strings"
	"time"

	"github.com/Azure/azure-storage-azcopy/v10/common"
)

// defines the output and how it should be handled
type outputMessage struct {
	msgContent    string
	msgType       common.OutputMessageType
	exitCode      common.ExitCode // only for when the application is meant to exit after printing (i.e. Error or Final)
	inputChannel  chan<- string   // support getting a response from the user
	promptDetails common.PromptDetails
}

func (m outputMessage) shouldExitProcess() bool {
	return m.msgType == common.EOutputMessageType.Error() ||
		(m.msgType == common.EOutputMessageType.EndOfJob() && !(m.exitCode == common.EExitCode.NoExit()))
}

// -------------------------------------- JSON templates -------------------------------------- //
// used to help formatting of JSON outputs

func GetJsonStringFromTemplate(template interface{}) string {
	jsonOutput, err := json.Marshal(template)
	common.PanicIfErr(err)

	return string(jsonOutput)
}

// defines the general output template when the format is set to json
type JsonOutputTemplate struct {
	TimeStamp      time.Time
	MessageType    string
	MessageContent string // a simple string for INFO and ERROR, a serialized JSON for INIT, PROGRESS, EXIT
	PromptDetails  common.PromptDetails
}

func newJsonOutputTemplate(messageType common.OutputMessageType, messageContent string, promptDetails common.PromptDetails) *JsonOutputTemplate {
	return &JsonOutputTemplate{TimeStamp: time.Now(), MessageType: messageType.String(),
		MessageContent: messageContent, PromptDetails: promptDetails}
}

// Ideally this is just JobContext, but we probably shouldn't break the json output format
type InitMsgJsonTemplate struct {
	LogFileLocation string
	JobID           string
	IsCleanupJob    bool
}

const cleanUpJobMessage = "Running cleanup job to delete files created during benchmarking"

func GetStandardInitOutputBuilder(ctx common.JobContext) common.OutputBuilder {
	return func(format common.OutputFormat) string {
		if format == common.EOutputFormat.Json() {
			return GetJsonStringFromTemplate(InitMsgJsonTemplate{
				JobID:           ctx.JobID.String(),
				LogFileLocation: ctx.LogPath,
				IsCleanupJob:    ctx.IsCleanup,
			})
		}

		var sb strings.Builder
		if ctx.IsCleanup {
			cleanupHeader := "(" + cleanUpJobMessage + " with cleanup jobID " + ctx.JobID.String()
			sb.WriteString(strings.Repeat("-", len(cleanupHeader)) + "\n")
			sb.WriteString(cleanupHeader)
		} else {
			sb.WriteString("\nJob " + ctx.JobID.String() + " has started\n")
			if ctx.LogPath != "" {
				sb.WriteString("Log file is located at: " + ctx.LogPath)
			}
			sb.WriteString("\n")
		}
		return sb.String()
	}
}

// Ideally this is just ScanProgress, but we probably shouldn't break the json output format
type scanningProgressJsonTemplate struct {
	FilesScannedAtSource      uint64
	FilesScannedAtDestination uint64
}

func GetScanProgressOutputBuilder(progress common.ScanProgress) common.OutputBuilder {
	return func(format common.OutputFormat) string {
		if format == common.EOutputFormat.Json() {
			jsonOutputTemplate := scanningProgressJsonTemplate{
				FilesScannedAtSource:      progress.Source,
				FilesScannedAtDestination: progress.Destination,
			}
			outputString, err := json.Marshal(jsonOutputTemplate)
			common.PanicIfErr(err)
			return string(outputString)
		}

		// text output
		throughputString := ""
		if progress.TransferThroughput != nil {
			throughputString = fmt.Sprintf(", 2-sec Throughput (Mb/s): %v", ToFixed(*progress.TransferThroughput, 4))
		}
		return fmt.Sprintf("%v Files Scanned at Source, %v Files Scanned at Destination %s",
			progress.Source, progress.Destination, throughputString)
	}
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

// Is disk speed looking like a constraint on throughput?  Ignore the first little-while,
// to give an (arbitrary) amount of time for things to reach steady-state.
func getPerfDisplayText(perfDiagnosticStrings []string, constraint common.PerfConstraint, durationOfJob time.Duration, isBench bool) (perfString string, diskString string) {
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

func GetProgressOutputBuilder(progress common.TransferProgress) common.OutputBuilder {
	return func(format common.OutputFormat) string {
		if format == common.EOutputFormat.Json() {
			switch progress.JobType {
			case common.EJobType.Copy(), common.EJobType.Resume(), common.EJobType.Benchmark():
				jsonOutput, err := json.Marshal(progress.ListJobSummaryResponse)
				common.PanicIfErr(err)
				return string(jsonOutput)
			case common.EJobType.Sync():
				wrapped := common.ListSyncJobSummaryResponse{ListJobSummaryResponse: progress.ListJobSummaryResponse}
				wrapped.DeleteTotalTransfers = progress.DeleteTotalTransfers
				wrapped.DeleteTransfersCompleted = progress.DeleteTransfersCompleted
				jsonOutput, err := json.Marshal(wrapped)
				common.PanicIfErr(err)
				return string(jsonOutput)
			default:
				return ""
			}
		} else {
			if progress.IsCleanupJob {
				return fmt.Sprintf("Cleanup %v/%v", progress.TransfersCompleted, progress.TotalTransfers)
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
			perfString, diskString := getPerfDisplayText(progress.PerfStrings, progress.PerfConstraint, progress.ElapsedTime, progress.JobType == common.EJobType.Benchmark())

			switch progress.JobType {
			case common.EJobType.Copy(), common.EJobType.Resume(), common.EJobType.Benchmark():
				return fmt.Sprintf("%.1f %%, %v Done, %v Failed, %v Pending, %v Skipped, %v Total%s, %s%s%s",
					progress.PercentComplete,
					progress.TransfersCompleted,
					progress.TransfersFailed,
					progress.TotalTransfers-(progress.TransfersCompleted+progress.TransfersFailed+progress.TransfersSkipped),
					progress.TransfersSkipped,
					progress.TotalTransfers,
					scanningString,
					perfString,
					throughputString,
					diskString)
			case common.EJobType.Sync():
				return fmt.Sprintf("%.1f %%, %v Done, %v Failed, %v Pending, %v Total%s, %s%s",
					progress.PercentComplete,
					progress.TransfersCompleted,
					progress.TransfersFailed,
					progress.TotalTransfers-progress.TransfersCompleted-progress.TransfersFailed,
					progress.TotalTransfers,
					perfString,
					throughputString,
					diskString)
			default:
				return ""
			}
		}
	}
}

func formatPerfAdvice(advice []common.PerformanceAdvice) string {
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

// format extra stats to include in the log.  If benchmarking, also output them on screen (but not to screen in normal
// usage because too cluttered)
func FormatExtraStats(jobType common.JobType, avgIOPS int, avgE2EMilliseconds int, networkErrorPercent float32, serverBusyPercent float32) (screenStats, logStats string) {
	logStats = fmt.Sprintf(
		`

Diagnostic stats:
IOPS: %v
End-to-end ms per request: %v
Network Errors: %.2f%%
Server Busy: %.2f%%`,
		avgIOPS, avgE2EMilliseconds, networkErrorPercent, serverBusyPercent)

	if jobType == common.EJobType.Benchmark() {
		screenStats = logStats
		logStats = "" // since will display in the screen stats, and they get logged too
	}

	return
}

func GetJobSummaryOutputBuilder(summary common.JobSummary) common.OutputBuilder {
	return func(format common.OutputFormat) string {
		if format == common.EOutputFormat.Json() {
			switch summary.JobType {
			case common.EJobType.Copy(), common.EJobType.Resume(), common.EJobType.Benchmark():
				jsonOutput, err := json.Marshal(summary.ListJobSummaryResponse)
				common.PanicIfErr(err)
				return string(jsonOutput)
			case common.EJobType.Sync():
				wrapped := common.ListSyncJobSummaryResponse{ListJobSummaryResponse: summary.ListJobSummaryResponse}
				wrapped.DeleteTotalTransfers = summary.DeleteTotalTransfers
				wrapped.DeleteTransfersCompleted = summary.DeleteTransfersCompleted
				jsonOutput, err := json.Marshal(wrapped)
				common.PanicIfErr(err)
				return string(jsonOutput)
			default:
				return ""
			}
		} else {
			switch summary.JobType {
			case common.EJobType.Copy(), common.EJobType.Benchmark():
				screenStats, _ := FormatExtraStats(summary.JobType, summary.AverageIOPS, summary.AverageE2EMilliseconds, summary.NetworkErrorPercentage, summary.ServerBusyPercentage)

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
Number of Special Files Skipped: %v
Total Number of Bytes Transferred: %v
Final Job Status: %v%s%s
`,
					summary.JobID.String(),
					ToFixed(summary.ElapsedTime.Minutes(), 4),
					summary.FileTransfers,
					summary.FolderPropertyTransfers,
					summary.SymlinkTransfers,
					summary.TotalTransfers,
					summary.TransfersCompleted-summary.FoldersCompleted,
					summary.FoldersCompleted,
					summary.TransfersFailed-summary.FoldersFailed,
					summary.FoldersFailed,
					summary.TransfersSkipped-summary.FoldersSkipped,
					summary.FoldersSkipped,
					summary.SkippedSymlinkCount,
					summary.HardlinksConvertedCount,
					summary.SkippedSpecialFileCount,
					summary.TotalBytesTransferred,
					summary.JobStatus,
					screenStats,
					formatPerfAdvice(summary.PerformanceAdvice))

				// abbreviated output for cleanup jobs
				if summary.IsCleanupJob {
					cleanupStatusString := fmt.Sprintf("Cleanup %v/%v", summary.TransfersCompleted, summary.TotalTransfers)
					output = fmt.Sprintf("%s: %s)", cleanupStatusString, summary.JobStatus)
				}
				return output
			case common.EJobType.Resume():
				return fmt.Sprintf(
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
Total Number of Bytes Transferred: %v
Final Job Status: %v
`,
					summary.JobID.String(),
					ToFixed(summary.ElapsedTime.Minutes(), 4),
					summary.FileTransfers,
					summary.FolderPropertyTransfers,
					summary.SymlinkTransfers,
					summary.TotalTransfers,
					summary.TransfersCompleted-summary.FoldersCompleted,
					summary.FoldersCompleted,
					summary.TransfersFailed-summary.FoldersFailed,
					summary.FoldersFailed,
					summary.TransfersSkipped-summary.FoldersSkipped,
					summary.FoldersSkipped,
					summary.TotalBytesTransferred,
					summary.JobStatus)
			case common.EJobType.Sync():
				screenStats, _ := FormatExtraStats(summary.JobType, summary.AverageIOPS, summary.AverageE2EMilliseconds, summary.NetworkErrorPercentage, summary.ServerBusyPercentage)

				output := fmt.Sprintf(
					`
Job %s Summary
Files Scanned at Source: %v
Files Scanned at Destination: %v
Elapsed Time (Minutes): %v
Number of Copy Transfers for Files: %v
Number of Copy Transfers for Folder Properties: %v 
Total Number of Copy Transfers: %v
Number of Copy Transfers Completed: %v
Number of Copy Transfers Failed: %v
Number of Deletions at Destination: %v
Number of Symbolic Links Skipped: %v
Number of Special Files Skipped: %v
Number of Hardlinks Converted: %v
Total Number of Bytes Transferred: %v
Total Number of Bytes Enumerated: %v
Final Job Status: %v%s%s
`,
					summary.JobID.String(),
					summary.SourceFilesScanned,
					summary.DestinationFilesScanned,
					ToFixed(summary.ElapsedTime.Minutes(), 4),
					summary.FileTransfers,
					summary.FolderPropertyTransfers,
					summary.TotalTransfers,
					summary.TransfersCompleted,
					summary.TransfersFailed,
					summary.DeleteTransfersCompleted,
					summary.SkippedSymlinkCount,
					summary.SkippedSpecialFileCount,
					summary.HardlinksConvertedCount,
					summary.TotalBytesTransferred,
					summary.TotalBytesEnumerated,
					summary.JobStatus,
					screenStats,
					formatPerfAdvice(summary.PerformanceAdvice))

				return output
			default:
				return ""
			}
		}
	}
}
