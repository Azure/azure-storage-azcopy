//go:build telemetrylive && telemetryperf

package azcopy

import (
	"fmt"
	"os"
	"os/exec"
	"runtime"
	"testing"
	"time"
	"unsafe"

	"github.com/stretchr/testify/require"
	"golang.org/x/sys/windows"
)

func TestTelemetryCLIMemoryAverage(t *testing.T) {
	var average telemetryMemoryAverage
	average.add(time.Second, 100, 200)
	average.add(3*time.Second, 300, 600)
	average.add(4*time.Second, 500, 1000)
	average.add(4*time.Second, 0, 0)
	require.Equal(t, 3.0, average.observedSeconds)
	require.InDelta(t, 800.0/3, average.workingSetIntegral/average.observedSeconds, 0.0001)
	require.InDelta(t, 1600.0/3, average.commitIntegral/average.observedSeconds, 0.0001)
}

type telemetryProcessMemoryCounters struct {
	Size                  uint32
	PageFaultCount        uint32
	PeakWorkingSetSize    uintptr
	WorkingSetSize        uintptr
	QuotaPeakPagedPool    uintptr
	QuotaPagedPool        uintptr
	QuotaPeakNonPagedPool uintptr
	QuotaNonPagedPool     uintptr
	PagefileUsage         uintptr
	PeakPagefileUsage     uintptr
	PrivateUsage          uintptr
}

func measureTelemetryCLIProcess(command *exec.Cmd) (telemetryCLIProcessMetrics, error) {
	var metrics telemetryCLIProcessMetrics
	getMemory := windows.NewLazySystemDLL("kernel32.dll").NewProc("K32GetProcessMemoryInfo")
	if err := getMemory.Find(); err != nil {
		return metrics, err
	}
	started := time.Now()
	if err := command.Start(); err != nil {
		return metrics, err
	}
	handle, err := windows.OpenProcess(windows.PROCESS_QUERY_INFORMATION|windows.PROCESS_VM_READ, false, uint32(command.Process.Pid))
	if err != nil {
		_ = command.Process.Kill()
		_ = command.Wait()
		return metrics, err
	}
	defer windows.CloseHandle(handle)
	var average telemetryMemoryAverage
	sample := func() {
		var counters telemetryProcessMemoryCounters
		counters.Size = uint32(unsafe.Sizeof(counters))
		ok, _, _ := getMemory.Call(uintptr(handle), uintptr(unsafe.Pointer(&counters)), uintptr(counters.Size))
		if ok == 0 {
			return
		}
		metrics.PeakWorkingSetBytes = max(metrics.PeakWorkingSetBytes, uint64(counters.PeakWorkingSetSize))
		metrics.PeakCommitBytes = max(metrics.PeakCommitBytes, uint64(counters.PeakPagefileUsage))
		if counters.WorkingSetSize == 0 || counters.PrivateUsage == 0 {
			return
		}
		metrics.MemorySamples++
		average.add(time.Since(started), uint64(counters.WorkingSetSize), uint64(counters.PrivateUsage))
	}
	type completion struct {
		err     error
		elapsed time.Duration
	}
	finished := make(chan completion, 1)
	go func() {
		err := command.Wait()
		finished <- completion{err: err, elapsed: time.Since(started)}
	}()
	ticker := time.NewTicker(10 * time.Millisecond)
	defer ticker.Stop()
	for {
		sample()
		select {
		case <-ticker.C:
		case result := <-finished:
			sample()
			metrics.WallSeconds = result.elapsed.Seconds()
			metrics.CPUSeconds = command.ProcessState.UserTime().Seconds() + command.ProcessState.SystemTime().Seconds()
			metrics.MemoryObservedSeconds = average.observedSeconds
			if average.observedSeconds > 0 {
				metrics.AverageWorkingSetBytes = average.workingSetIntegral / average.observedSeconds
				metrics.AverageCommitBytes = average.commitIntegral / average.observedSeconds
			}
			if result.err != nil {
				return metrics, result.err
			}
			if metrics.MemorySamples < 2 || metrics.MemoryObservedSeconds <= 0 || metrics.PeakWorkingSetBytes == 0 || metrics.PeakCommitBytes == 0 {
				return metrics, fmt.Errorf("Windows did not return valid child process memory measurements")
			}
			return metrics, nil
		}
	}
}

func TestTelemetryCLIProcessMeasurementWorker(t *testing.T) {
	if os.Getenv("AZCOPY_CLI_PERF_METRICS_WORKER") != "1" {
		t.Skip("process measurement helper")
	}
	buffer := make([]byte, 32*1024*1024)
	for index := range buffer {
		buffer[index] = byte(index + 1)
	}
	deadline := time.Now().Add(100 * time.Millisecond)
	for time.Now().Before(deadline) {
		buffer[0]++
	}
	runtime.KeepAlive(buffer)
}

func TestTelemetryCLIProcessMeasurement(t *testing.T) {
	command := exec.Command(os.Args[0], "-test.run=^TestTelemetryCLIProcessMeasurementWorker$")
	command.Env = append(os.Environ(), "AZCOPY_CLI_PERF_METRICS_WORKER=1")
	metrics, err := measureTelemetryCLIProcess(command)
	require.NoError(t, err)
	require.Greater(t, metrics.WallSeconds, 0.1)
	require.Positive(t, metrics.CPUSeconds)
	require.Greater(t, metrics.PeakWorkingSetBytes, uint64(32*1024*1024))
	require.Greater(t, metrics.PeakCommitBytes, uint64(32*1024*1024))
	require.Greater(t, metrics.MemorySamples, 1)
	require.Positive(t, metrics.AverageWorkingSetBytes)
	require.Positive(t, metrics.AverageCommitBytes)
	require.LessOrEqual(t, metrics.AverageWorkingSetBytes, float64(metrics.PeakWorkingSetBytes))
	require.LessOrEqual(t, metrics.AverageCommitBytes, float64(metrics.PeakCommitBytes))
	require.Positive(t, metrics.MemoryObservedSeconds)
}
