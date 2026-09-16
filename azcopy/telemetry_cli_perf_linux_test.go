//go:build telemetrylive && telemetryperf

package azcopy

import (
	"fmt"
	"os"
	"os/exec"
	"runtime"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func telemetryProcMemory(contents string) map[string]uint64 {
	values := map[string]uint64{}
	for _, line := range strings.Split(contents, "\n") {
		fields := strings.Fields(line)
		if len(fields) == 3 && fields[2] == "kB" {
			value, err := strconv.ParseUint(fields[1], 10, 64)
			if err == nil {
				values[strings.TrimSuffix(fields[0], ":")] = value * 1024
			}
		}
	}
	return values
}

func measureTelemetryCLIProcess(command *exec.Cmd) (telemetryCLIProcessMetrics, error) {
	var metrics telemetryCLIProcessMetrics
	started := time.Now()
	if err := command.Start(); err != nil {
		return metrics, err
	}
	procRoot := fmt.Sprintf("/proc/%d/", command.Process.Pid)
	profiling := os.Getenv("AZCOPY_CLI_TELEMETRY_PROFILE") == "1"
	var average telemetryMemoryAverage
	lastBreakdown := time.Time{}
	sample := func() {
		data, err := os.ReadFile(procRoot + "status")
		if err != nil {
			return
		}
		values := telemetryProcMemory(string(data))
		if values["VmRSS"] == 0 {
			return
		}
		elapsed := time.Since(started)
		metrics.MemorySamples++
		metrics.PeakWorkingSetBytes = max(metrics.PeakWorkingSetBytes, values["VmHWM"])
		metrics.PeakAnonymousRSSBytes = max(metrics.PeakAnonymousRSSBytes, values["RssAnon"])
		average.add(elapsed, values["VmRSS"], values["RssAnon"])
		if profiling && time.Since(lastBreakdown) >= 250*time.Millisecond {
			lastBreakdown = time.Now()
			data, err := os.ReadFile(procRoot + "smaps_rollup")
			if err != nil {
				return
			}
			rollup := telemetryProcMemory(string(data))
			metrics.ResidentSamples = append(metrics.ResidentSamples, telemetryResidentSample{
				ElapsedSeconds: elapsed.Seconds(), TotalBytes: rollup["Rss"], WorkingSetBytes: values["VmRSS"],
				AnonymousRSSBytes: values["RssAnon"], FileRSSBytes: values["RssFile"], PSSBytes: rollup["Pss"],
				PrivateRSSBytes: rollup["Private_Clean"] + rollup["Private_Dirty"], SwapBytes: rollup["Swap"],
			})
		}
	}
	type completion struct {
		err     error
		elapsed time.Duration
	}
	finished := make(chan completion, 1)
	go func() { err := command.Wait(); finished <- completion{err, time.Since(started)} }()
	ticker := time.NewTicker(10 * time.Millisecond)
	defer ticker.Stop()
	for {
		sample()
		select {
		case <-ticker.C:
		case result := <-finished:
			metrics.WallSeconds = result.elapsed.Seconds()
			metrics.CPUSeconds = command.ProcessState.UserTime().Seconds() + command.ProcessState.SystemTime().Seconds()
			metrics.MemoryObservedSeconds = average.observedSeconds
			if average.observedSeconds > 0 {
				metrics.AverageWorkingSetBytes = average.workingSetIntegral / average.observedSeconds
				metrics.AverageAnonymousRSSBytes = average.commitIntegral / average.observedSeconds
			}
			if result.err != nil {
				return metrics, result.err
			}
			if metrics.MemorySamples < 2 || average.observedSeconds <= 0 || metrics.PeakWorkingSetBytes == 0 {
				return metrics, fmt.Errorf("Linux did not return valid child process RSS measurements")
			}
			if profiling && len(metrics.ResidentSamples) == 0 {
				return metrics, fmt.Errorf("Linux did not return a smaps_rollup diagnostic snapshot")
			}
			return metrics, nil
		}
	}
}

func TestTelemetryCLIProcMemory(t *testing.T) {
	values := telemetryProcMemory("Name:\tazcopy\nVmRSS:\t32768 kB\nRssAnon:\t16384 kB\nPss:\tbad kB\nThreads:\t2\n")
	require.Equal(t, map[string]uint64{"VmRSS": 32 * 1024 * 1024, "RssAnon": 16 * 1024 * 1024}, values)
	var average telemetryMemoryAverage
	average.add(time.Second, 100, 200)
	average.add(3*time.Second, 300, 600)
	average.add(4*time.Second, 500, 1000)
	require.InDelta(t, 800.0/3, average.workingSetIntegral/average.observedSeconds, 0.0001)
}

func TestTelemetryCLIProcessMeasurementWorker(t *testing.T) {
	if os.Getenv("AZCOPY_CLI_PERF_METRICS_WORKER") != "1" {
		t.Skip("process measurement helper")
	}
	buffer := make([]byte, 32*1024*1024)
	for index := range buffer {
		buffer[index] = byte(index + 1)
	}
	deadline := time.Now().Add(200 * time.Millisecond)
	for time.Now().Before(deadline) {
		buffer[0]++
	}
	runtime.KeepAlive(buffer)
}

func TestTelemetryCLIProcessMeasurement(t *testing.T) {
	t.Setenv("AZCOPY_CLI_TELEMETRY_PROFILE", "1")
	command := exec.Command(os.Args[0], "-test.run=^TestTelemetryCLIProcessMeasurementWorker$")
	command.Env = append(os.Environ(), "AZCOPY_CLI_PERF_METRICS_WORKER=1")
	metrics, err := measureTelemetryCLIProcess(command)
	require.NoError(t, err)
	require.Greater(t, metrics.WallSeconds, 0.2)
	require.Positive(t, metrics.CPUSeconds)
	require.Greater(t, metrics.PeakWorkingSetBytes, uint64(32*1024*1024))
	require.Positive(t, metrics.AverageAnonymousRSSBytes)
	require.Positive(t, metrics.MemoryObservedSeconds)
	require.NotEmpty(t, metrics.ResidentSamples)
	require.Zero(t, metrics.AverageCommitBytes)
}
