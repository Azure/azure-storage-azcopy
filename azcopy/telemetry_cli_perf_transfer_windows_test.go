//go:build telemetrylive && telemetryperf

package azcopy

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"sort"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/Azure/azure-storage-azcopy/v10/common"
	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
)

type telemetryCLIPerfTrial struct {
	telemetryCLIProcessMetrics
	Workload            string
	Mode                string
	Pair                int
	Order               int
	Warmup              bool
	JobID               string
	Bytes               uint64
	Files               int
	TelemetryState      string
	TelemetryStopReason string
}

type telemetryCLIPerfComparison struct {
	Metric              string
	Unit                string
	DisabledMedian      float64
	EnabledMedian       float64
	PairedDeltaMedian   float64
	DeltaLower95        float64
	DeltaUpper95        float64
	PairedPercentMedian float64
	PercentLower95      float64
	PercentUpper95      float64
}

func telemetryCLIPerfCompare(metric, unit string, disabled, enabled []float64) telemetryCLIPerfComparison {
	median := func(values []float64) float64 {
		ordered := append([]float64(nil), values...)
		sort.Float64s(ordered)
		return (ordered[(len(ordered)-1)/2] + ordered[len(ordered)/2]) / 2
	}
	deltas, percentages := make([]float64, len(disabled)), make([]float64, len(disabled))
	for index := range disabled {
		deltas[index] = enabled[index] - disabled[index]
		percentages[index] = 100 * (enabled[index]/disabled[index] - 1)
	}
	delta := telemetryPerfInterval(metric, deltas, 0)
	percent := telemetryPerfInterval(metric, percentages, 0)
	return telemetryCLIPerfComparison{
		Metric: metric, Unit: unit, DisabledMedian: median(disabled), EnabledMedian: median(enabled),
		PairedDeltaMedian: delta.Median, DeltaLower95: delta.Lower95, DeltaUpper95: delta.Upper95,
		PairedPercentMedian: percent.Median, PercentLower95: percent.Lower95, PercentUpper95: percent.Upper95,
	}
}

func TestTelemetryCLIPerformanceAnalysis(t *testing.T) {
	result := telemetryCLIPerfCompare("time", "s", []float64{1, 2, 3, 4}, []float64{1.1, 2.2, 3.3, 4.4})
	require.Equal(t, 2.5, result.DisabledMedian)
	require.InDelta(t, 2.75, result.EnabledMedian, 0.000001)
	require.InDelta(t, 0.25, result.PairedDeltaMedian, 0.000001)
	require.InDelta(t, 10, result.PairedPercentMedian, 0.000001)
	require.InDelta(t, 10, result.PercentLower95, 0.000001)
	require.InDelta(t, 10, result.PercentUpper95, 0.000001)
}

func TestTelemetryCLIPerformance(t *testing.T) {
	if os.Getenv("AZCOPY_RUN_CLI_TELEMETRY_PERF") != "1" || os.Getenv("AZCOPY_RUN_LIVE_TELEMETRY") != "1" {
		t.Skip("manual full CLI performance comparison; use telemetry-live.ps1 -Scenario cli-performance")
	}
	pairs, err := strconv.Atoi(os.Getenv("AZCOPY_CLI_TELEMETRY_PERF_PAIRS"))
	require.NoError(t, err)
	require.GreaterOrEqual(t, pairs, 3)
	require.LessOrEqual(t, pairs, 15)
	executable, err := filepath.Abs(os.Getenv("AZCOPY_LIVE_TELEMETRY_EXECUTABLE"))
	require.NoError(t, err)
	info, err := os.Stat(executable)
	require.NoError(t, err)
	require.False(t, info.IsDir())
	account := os.Getenv("AZCOPY_LIVE_TELEMETRY_STORAGE_ACCOUNT")
	require.Regexp(t, `^[a-z0-9]{3,24}$`, account)
	output := os.Getenv("AZCOPY_LIVE_TELEMETRY_OUTPUT")
	require.NotEmpty(t, output)
	output = filepath.Join(output, "comparison-"+uuid.NewString())
	require.NoError(t, os.MkdirAll(output, 0700))
	t.Logf("performance evidence: %s", output)
	binary, err := os.ReadFile(executable)
	require.NoError(t, err)
	binaryHash := sha256.Sum256(binary)
	binary = nil
	metadata := map[string]any{
		"StartedUTC": time.Now().UTC(), "OS": runtime.GOOS, "Architecture": runtime.GOARCH,
		"GoVersion": runtime.Version(), "LogicalCPUs": runtime.NumCPU(), "GOMAXPROCS": 8, "TransferConcurrency": 16,
		"BinarySHA256": hex.EncodeToString(binaryHash[:]), "PairsPerWorkload": pairs, "Order": "alternating AB/BA",
		"WallTimeScope": "CLI launch through process exit, including authentication, enumeration, transfers and telemetry flush",
		"CPUScope":      "AzCopy process user+kernel time; excludes Azure CLI authentication subprocesses",
		"MemoryScope":   "Windows peak working set and peak process commit, sampled every 10ms; not Go heap",
		"Identity":      "precreated installation ID for both modes; fresh isolated user directory per process",
		"Warmups":       "one discarded enabled and disabled transfer per workload", "BandwidthCap": "none",
		"TelemetryFailures": "retain and label configured-enabled failures; report healthy-pair subset separately without retrying samples",
	}
	writeJSON := func(name string, value any) {
		encoded, err := json.MarshalIndent(value, "", "  ")
		require.NoError(t, err)
		require.NoError(t, os.WriteFile(filepath.Join(output, name), encoded, 0600))
	}
	writeJSON("metadata.json", metadata)
	ctx, cancel := context.WithTimeout(context.Background(), 35*time.Minute)
	defer cancel()
	target := loadLiveTelemetryTarget(t, "shutdown")
	storage, container := newLiveCLIContainer(t, ctx, target, account)
	home, err := os.UserHomeDir()
	require.NoError(t, err)
	azureConfig := os.Getenv("AZURE_CONFIG_DIR")
	if azureConfig == "" {
		azureConfig = filepath.Join(home, ".azure")
	}
	azureConfig, err = filepath.Abs(azureConfig)
	require.NoError(t, err)
	trialsFile, err := os.Create(filepath.Join(output, "trials.jsonl"))
	require.NoError(t, err)
	defer trialsFile.Close()
	encoder := json.NewEncoder(trialsFile)
	comparisons := map[string][]telemetryCLIPerfComparison{}
	for _, workload := range []struct {
		name  string
		files int
		size  int
	}{
		{"large-blob", 1, 64 * 1024 * 1024},
		{"small-blobs", 128, 16 * 1024},
	} {
		hashes := map[string][32]byte{}
		contents := make([]byte, workload.size)
		_, err := rand.Read(contents)
		require.NoError(t, err)
		for index := 0; index < workload.files; index++ {
			name := fmt.Sprintf("file-%04d.bin", index)
			_, err := storage.UploadBuffer(ctx, container, workload.name+"/"+name, contents, nil)
			require.NoError(t, err)
			hashes[name] = sha256.Sum256(contents)
		}
		contents = nil
		runtime.GC()
		run := func(mode string, pair, order int, warmup bool) telemetryCLIPerfTrial {
			trialDir := filepath.Join(output, fmt.Sprintf("%s-%02d-%s", workload.name, pair, mode))
			destination, logs, plans, userDir := filepath.Join(trialDir, "download"), filepath.Join(trialDir, "logs"), filepath.Join(trialDir, "plans"), filepath.Join(trialDir, "home")
			for _, directory := range []string{destination, logs, plans, filepath.Join(userDir, ".azcopy")} {
				require.NoError(t, os.MkdirAll(directory, 0700))
			}
			require.NoError(t, os.WriteFile(filepath.Join(userDir, ".azcopy", "installation_id"), []byte("1234567890abcdef1234567890abcdef"), 0600))
			stdout, err := os.Create(filepath.Join(trialDir, "stdout.jsonl"))
			require.NoError(t, err)
			stderr, err := os.Create(filepath.Join(trialDir, "stderr.txt"))
			require.NoError(t, err)
			processCtx, processCancel := context.WithTimeout(ctx, 3*time.Minute)
			defer processCancel()
			command := exec.CommandContext(processCtx, executable, "copy",
				"https://"+account+".blob.core.windows.net/"+container+"/"+workload.name+"/*", destination,
				"--from-to=BlobLocal", "--recursive=true", "--output-type=json", "--log-level=INFO", "--check-version=false", "--block-size-mb=4")
			command.Env = liveCLIEnvironment(os.Environ(), map[string]string{
				"AZCOPY_DISABLE_TELEMETRY": strconv.FormatBool(mode == "disabled"), "AZCOPY_TELEMETRY_CONNECTION_STRING": target.connection,
				"AZCOPY_AUTO_LOGIN_TYPE": "AZCLI", "AZURE_CONFIG_DIR": azureConfig,
				"AZCOPY_CONCURRENCY_VALUE": "16", "GOMAXPROCS": "8",
				"AZCOPY_LOG_LOCATION": logs, "AZCOPY_JOB_PLAN_LOCATION": plans,
				"AZCOPY_E2E_TELEMETRY_RUN_ID":              "cli-performance/" + uuid.NewString(),
				common.EEnvironmentVariable.UserDir().Name: userDir,
			})
			command.Stdout, command.Stderr = stdout, stderr
			metrics, runErr := measureTelemetryCLIProcess(command)
			require.NoError(t, stdout.Close())
			require.NoError(t, stderr.Close())
			require.NoError(t, runErr, "invalid performance sample; inspect %s", trialDir)
			outputBytes, err := os.ReadFile(stdout.Name())
			require.NoError(t, err)
			summary, err := liveCLISummary(outputBytes)
			require.NoError(t, err)
			require.Equal(t, common.EJobStatus.Completed(), summary.JobStatus)
			require.Zero(t, summary.TransfersFailed)
			require.Zero(t, summary.TransfersSkipped)
			require.EqualValues(t, workload.files, summary.TransfersCompleted)
			require.EqualValues(t, workload.files*workload.size, summary.TotalBytesTransferred)
			jobLog, err := os.ReadFile(filepath.Join(logs, summary.JobID.String()+".log"))
			require.NoError(t, err)
			stderrBytes, err := os.ReadFile(stderr.Name())
			require.NoError(t, err)
			startedSent := strings.Count(string(stderrBytes), "telemetry: sent packed azcopy.job.started event")
			finishedSent := strings.Count(string(stderrBytes), "telemetry: sent packed azcopy.job.finished event")
			state, stopReason := mode, ""
			const stopPrefix = "telemetry: disabled for this process after delivery failure sending "
			for _, line := range strings.Split(string(jobLog), "\n") {
				if index := strings.Index(line, stopPrefix); index >= 0 {
					stopReason = strings.TrimSpace(line[index:])
				}
			}
			if mode == "disabled" {
				require.Zero(t, startedSent+finishedSent, "disabled sample emitted telemetry")
				require.Empty(t, stopReason, "disabled sample initialized telemetry")
			} else if stopReason != "" {
				state = "enabled-stopped"
				t.Logf("%s telemetry stopped: %s", trialDir, stopReason)
			} else {
				require.Equal(t, 1, startedSent, "enabled sample lacks verified telemetry start: %s", trialDir)
				require.Equal(t, 1, finishedSent, "enabled sample lacks verified telemetry finish: %s", trialDir)
				state = "enabled-healthy"
			}
			for name, expected := range hashes {
				file, err := os.Open(filepath.Join(destination, name))
				require.NoError(t, err)
				hash := sha256.New()
				_, err = io.Copy(hash, file)
				require.NoError(t, err)
				require.NoError(t, file.Close())
				require.Equal(t, expected[:], hash.Sum(nil), "download content mismatch")
			}
			require.NoError(t, os.RemoveAll(destination))
			trial := telemetryCLIPerfTrial{telemetryCLIProcessMetrics: metrics, Workload: workload.name, Mode: mode, Pair: pair, Order: order, Warmup: warmup, JobID: summary.JobID.String(), Bytes: summary.TotalBytesTransferred, Files: workload.files, TelemetryState: state, TelemetryStopReason: stopReason}
			require.Positive(t, metrics.CPUSeconds)
			require.NoError(t, encoder.Encode(trial))
			require.NoError(t, trialsFile.Sync())
			t.Logf("%s pair=%d mode=%s warmup=%t wall=%.3fs cpu=%.3fs peakWS=%.2fMiB peakCommit=%.2fMiB state=%s", workload.name, pair, mode, warmup, metrics.WallSeconds, metrics.CPUSeconds, float64(metrics.PeakWorkingSetBytes)/(1024*1024), float64(metrics.PeakCommitBytes)/(1024*1024), state)
			return trial
		}
		run("disabled", 0, 0, true)
		run("enabled", 0, 1, true)
		disabled, enabled := []telemetryCLIPerfTrial{}, []telemetryCLIPerfTrial{}
		for pair := 1; pair <= pairs; pair++ {
			modes := []string{"disabled", "enabled"}
			if pair%2 == 0 {
				modes[0], modes[1] = modes[1], modes[0]
			}
			for order, mode := range modes {
				trial := run(mode, pair, order, false)
				if mode == "disabled" {
					disabled = append(disabled, trial)
				} else {
					enabled = append(enabled, trial)
				}
			}
		}
		for _, metric := range []struct {
			name string
			unit string
			get  func(telemetryCLIPerfTrial) float64
		}{
			{"wall", "seconds", func(trial telemetryCLIPerfTrial) float64 { return trial.WallSeconds }},
			{"cpu", "seconds", func(trial telemetryCLIPerfTrial) float64 { return trial.CPUSeconds }},
			{"peak-working-set", "MiB", func(trial telemetryCLIPerfTrial) float64 { return float64(trial.PeakWorkingSetBytes) / (1024 * 1024) }},
			{"peak-commit", "MiB", func(trial telemetryCLIPerfTrial) float64 { return float64(trial.PeakCommitBytes) / (1024 * 1024) }},
		} {
			disabledValues, enabledValues := make([]float64, pairs), make([]float64, pairs)
			for index := range disabled {
				disabledValues[index], enabledValues[index] = metric.get(disabled[index]), metric.get(enabled[index])
			}
			healthyDisabled, healthyEnabled := []float64{}, []float64{}
			for index := range enabled {
				if enabled[index].TelemetryState == "enabled-healthy" {
					healthyDisabled, healthyEnabled = append(healthyDisabled, disabledValues[index]), append(healthyEnabled, enabledValues[index])
				}
			}
			if len(healthyEnabled) >= 3 {
				comparisons[workload.name+"/healthy-only"] = append(comparisons[workload.name+"/healthy-only"], telemetryCLIPerfCompare(metric.name, metric.unit, healthyDisabled, healthyEnabled))
			}
			if metric.name == "wall" {
				t.Logf("%s measured enabled health: %d/%d healthy; %d stopped on delivery failure", workload.name, len(healthyEnabled), pairs, pairs-len(healthyEnabled))
			}
			comparison := telemetryCLIPerfCompare(metric.name, metric.unit, disabledValues, enabledValues)
			comparisons[workload.name] = append(comparisons[workload.name], comparison)
			t.Logf("%s %s: disabled median=%.3f enabled median=%.3f paired delta=%.3f %s (95%% %.3f..%.3f), paired overhead=%.2f%% (95%% %.2f..%.2f)", workload.name, metric.name, comparison.DisabledMedian, comparison.EnabledMedian, comparison.PairedDeltaMedian, metric.unit, comparison.DeltaLower95, comparison.DeltaUpper95, comparison.PairedPercentMedian, comparison.PercentLower95, comparison.PercentUpper95)
		}
		writeJSON("comparison.json", comparisons)
	}
}
