//go:build telemetryperf

package azcopy

import (
	"bufio"
	"encoding/json"
	"fmt"
	"io"
	"math"
	"math/rand"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Azure/azure-storage-azcopy/v10/common"
	"github.com/Azure/azure-storage-azcopy/v10/telemetry"
	"github.com/Azure/azure-storage-azcopy/v10/traverser"
	"github.com/stretchr/testify/require"
	"gopkg.in/yaml.v3"
)

type telemetryPerfRun struct {
	Workload              string
	Mode                  string
	Pair                  int
	Bytes                 int64
	TransferSeconds       float64
	ProcessCPUSeconds     float64
	RetainedHeapBytes     int64
	SampledPeakHeapBytes  int64
	AllocatedBytes        uint64
	InitializationSeconds float64
	WarmIdentity          bool
	FlushSeconds          float64
	Requests              int64
}

type telemetryPerfWorkerConfig struct {
	Workload     string
	Mode         string
	Source       string
	Destination  string
	Endpoint     string
	Seconds      float64
	WarmIdentity bool
}

type telemetryPerfGate struct {
	Name    string
	Median  float64
	Lower95 float64
	Upper95 float64
	Limit   float64
	Status  string
}

func telemetryPerfInterval(name string, values []float64, limit float64) telemetryPerfGate {
	median := func(sample []float64) float64 {
		sort.Float64s(sample)
		middle := len(sample) / 2
		if len(sample)%2 == 0 {
			return (sample[middle-1] + sample[middle]) / 2
		}
		return sample[middle]
	}
	result := telemetryPerfGate{Name: name, Limit: limit, Status: "inconclusive"}
	if len(values) < 3 {
		return result
	}
	for _, value := range values {
		if math.IsNaN(value) || math.IsInf(value, 0) {
			return result
		}
	}
	result.Median = median(append([]float64(nil), values...))
	random := rand.New(rand.NewSource(42))
	bootstrap := make([]float64, 2000)
	sample := make([]float64, len(values))
	for index := range bootstrap {
		for position := range sample {
			sample[position] = values[random.Intn(len(values))]
		}
		bootstrap[index] = median(sample)
	}
	sort.Float64s(bootstrap)
	result.Lower95, result.Upper95 = bootstrap[49], bootstrap[1949]
	if result.Upper95 <= limit {
		result.Status = "pass"
	} else if result.Lower95 > limit {
		result.Status = "fail"
	}
	return result
}

func TestTelemetryPerformanceAnalysis(t *testing.T) {
	require.Equal(t, "pass", telemetryPerfInterval("stable", []float64{0.2, 0.2, 0.2}, 1).Status)
	require.Equal(t, "fail", telemetryPerfInterval("regression", []float64{2, 2, 2}, 1).Status)
	require.Equal(t, "inconclusive", telemetryPerfInterval("noisy", []float64{-2, 0, 3}, 1).Status)
	require.Equal(t, "inconclusive", telemetryPerfInterval("short", []float64{0}, 1).Status)
	require.Equal(t, "inconclusive", telemetryPerfInterval("invalid", []float64{0, math.NaN(), 0}, 1).Status)
}

func TestTelemetryPerformancePipelineIsManual(t *testing.T) {
	content, err := os.ReadFile("../telemetry-performance.yml")
	require.NoError(t, err)
	var pipeline map[string]any
	require.NoError(t, yaml.Unmarshal(content, &pipeline))
	require.Equal(t, "none", pipeline["trigger"])
	require.Equal(t, "none", pipeline["pr"])
	require.Nil(t, pipeline["schedules"])
	require.Nil(t, pipeline["resources"])
	require.NotContains(t, string(content), "AzureCLI@")
	e2e, err := os.ReadFile("../azurePipelineTemplates/run-e2e.yml")
	require.NoError(t, err)
	require.NotContains(t, string(e2e), "telemetry-performance")
}

func TestTelemetryPerformanceWorker(t *testing.T) {
	encoded := os.Getenv("AZCOPY_TELEMETRY_PERF_WORKER")
	if encoded == "" {
		t.Skip("subprocess helper")
	}
	var config telemetryPerfWorkerConfig
	require.NoError(t, json.Unmarshal([]byte(encoded), &config))
	t.Setenv(envTelemetryConnectionString, "InstrumentationKey=perf-local;IngestionEndpoint="+config.Endpoint)
	t.Setenv(envDisableTelemetry, strconv.FormatBool(config.Mode == "disabled"))
	t.Setenv(common.EEnvironmentVariable.UserDir().Name, t.TempDir())
	if config.WarmIdentity {
		require.Len(t, installationIDInDir(common.GetAzCopyAppPath()), 32)
	}
	buffer := make([]byte, 1024*1024)
	copyFile := func() int64 {
		source, err := os.Open(config.Source)
		require.NoError(t, err)
		destination, err := os.Create(config.Destination)
		require.NoError(t, err)
		count, err := io.CopyBuffer(struct{ io.Writer }{destination}, struct{ io.Reader }{source}, buffer)
		require.NoError(t, err)
		require.NoError(t, source.Close())
		require.NoError(t, destination.Close())
		return count
	}
	copyFile()
	scopes := make([]string, 256)
	for index := range scopes {
		scopes[index] = fmt.Sprintf("%03d", index) + strings.Repeat("x", maxTrackedSourceScopeBytes-3)
	}
	runtime.GC()
	var before, after runtime.MemStats
	runtime.ReadMemStats(&before)
	var peak atomic.Uint64
	peak.Store(before.HeapAlloc)
	sampleHeap := func() {
		var current runtime.MemStats
		runtime.ReadMemStats(&current)
		for previous := peak.Load(); current.HeapAlloc > previous; previous = peak.Load() {
			if peak.CompareAndSwap(previous, current.HeapAlloc) {
				break
			}
		}
	}
	stopSamples := make(chan struct{})
	samplesDone := make(chan struct{})
	var stopOnce sync.Once
	stopSampling := func() { stopOnce.Do(func() { close(stopSamples) }); <-samplesDone }
	t.Cleanup(stopSampling)
	go func() {
		defer close(samplesDone)
		ticker := time.NewTicker(time.Millisecond)
		defer ticker.Stop()
		for {
			select {
			case <-ticker.C:
				sampleHeap()
			case <-stopSamples:
				return
			}
		}
	}()
	initializationStart := time.Now()
	agent := getTelemetryAgent()
	initializationSeconds := time.Since(initializationStart).Seconds()
	require.Equal(t, config.Mode != "disabled", agent.enabled)
	sampleHeap()
	var tracker *sourceShapeTracker
	if agent.enabled {
		require.Len(t, agent.resource.InstallationID, 32)
		tracker = newSourceShapeTracker(common.ELocation.Blob(), common.ESymlinkHandlingType.Skip(), common.EHardlinkHandlingType.Follow())
		tracker.isActive = agent.isActive
		tracker.accountScope = true
	}
	started := time.Now()
	finalizer := agent.newAttempt(func() telemetry.JobDimensions {
		return copyJobDimensions(&CookedTransferOptions{source: common.ResourceString{Value: config.Source}, destination: common.ResourceString{Value: "https://perfaccount.blob.core.windows.net/container"}, fromTo: common.EFromTo.LocalBlob()}, common.ECredentialType.Anonymous(), common.ECredentialType.Anonymous())
	}, "perf-job", newTelemetryInvocationID(), started)
	finalizer.shapeFn = tracker.snapshot
	finalizer.startEvent()
	var transferred int64
	objects := 0
	for time.Since(started).Seconds() < config.Seconds {
		count := copyFile()
		object := traverser.StoredObject{Size: count, EntityType: common.EEntityType.File(), RelativePath: "level/file", ContainerName: scopes[objects%len(scopes)]}
		require.NoError(t, tracker.recordScanned(object))
		tracker.recordScheduled(object)
		transferred += count
		objects++
	}
	elapsed := time.Since(started).Seconds()
	finalizer.setFinalSummary(common.ListJobSummaryResponse{JobStatus: common.EJobStatus.Completed(), TotalBytesTransferred: uint64(transferred)})
	finalizer.finish(nil)
	sampleHeap()
	flushStart := time.Now()
	agent.flush(telemetryFlushTimeout)
	flushSeconds := time.Since(flushStart).Seconds()
	sampleHeap()
	stopSampling()
	runtime.GC()
	runtime.ReadMemStats(&after)
	runtime.KeepAlive(agent)
	runtime.KeepAlive(tracker)
	runtime.KeepAlive(scopes)
	runtime.KeepAlive(buffer)
	result := telemetryPerfRun{Workload: config.Workload, Mode: config.Mode, Bytes: transferred, TransferSeconds: elapsed, RetainedHeapBytes: int64(after.HeapAlloc) - int64(before.HeapAlloc), FlushSeconds: flushSeconds,
		InitializationSeconds: initializationSeconds, WarmIdentity: config.WarmIdentity, SampledPeakHeapBytes: int64(peak.Load()) - int64(before.HeapAlloc), AllocatedBytes: after.TotalAlloc - before.TotalAlloc}
	encodedResult, err := json.Marshal(result)
	require.NoError(t, err)
	fmt.Println("AZCOPY_PERF_RESULT=" + string(encodedResult))
}

func TestTelemetryPerformanceGate(t *testing.T) {
	if os.Getenv("AZCOPY_RUN_TELEMETRY_PERF") != "1" {
		t.Skip("manual performance pipeline only")
	}
	samples, err := strconv.Atoi(os.Getenv("AZCOPY_TELEMETRY_PERF_SAMPLES"))
	require.NoError(t, err)
	seconds, err := strconv.ParseFloat(os.Getenv("AZCOPY_TELEMETRY_PERF_SECONDS"), 64)
	require.NoError(t, err)
	require.GreaterOrEqual(t, samples, 3)
	require.Greater(t, seconds, 0.0)
	outputDirectory := os.Getenv("AZCOPY_TELEMETRY_PERF_OUTPUT")
	require.NotEmpty(t, outputDirectory)
	require.NoError(t, os.MkdirAll(outputDirectory, 0700))
	executable, err := os.Executable()
	require.NoError(t, err)
	var requests atomic.Int64
	server := httptest.NewServer(http.HandlerFunc(func(response http.ResponseWriter, request *http.Request) {
		_, _ = io.Copy(io.Discard, io.LimitReader(request.Body, 16*1024))
		requests.Add(1)
		response.WriteHeader(http.StatusOK)
	}))
	defer server.Close()
	stalled := httptest.NewServer(http.HandlerFunc(func(response http.ResponseWriter, request *http.Request) {
		_, _ = io.Copy(io.Discard, io.LimitReader(request.Body, 16*1024))
		<-request.Context().Done()
	}))
	defer stalled.Close()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	unreachable := "http://" + listener.Addr().String()
	require.NoError(t, listener.Close())
	var runs []telemetryPerfRun
	var gates []telemetryPerfGate
	for _, workload := range []struct {
		name string
		size int
		warm bool
	}{{"small-files/cold", 8 * 1024, false}, {"small-files/warm", 8 * 1024, true}, {"large-files/cold", 32 * 1024 * 1024, false}, {"large-files/warm", 32 * 1024 * 1024, true}} {
		directory := t.TempDir()
		source := filepath.Join(directory, "source")
		require.NoError(t, os.WriteFile(source, make([]byte, workload.size), 0600))
		run := func(mode string, pair int) telemetryPerfRun {
			endpoint := server.URL
			if mode == "unreachable" {
				endpoint = unreachable
			}
			if mode == "stalled" {
				endpoint = stalled.URL
			}
			config := telemetryPerfWorkerConfig{Workload: workload.name, Mode: mode, Source: source, Destination: filepath.Join(directory, "destination"), Endpoint: endpoint, Seconds: seconds, WarmIdentity: workload.warm}
			encoded, err := json.Marshal(config)
			require.NoError(t, err)
			command := exec.Command(executable, "-test.run=^TestTelemetryPerformanceWorker$", "-test.timeout=2m")
			command.Env = append(os.Environ(), "AZCOPY_TELEMETRY_PERF_WORKER="+string(encoded))
			previousRequests := requests.Load()
			output, err := command.CombinedOutput()
			require.NoError(t, err, "worker failed: %s", output)
			var result telemetryPerfRun
			found := false
			scanner := bufio.NewScanner(strings.NewReader(string(output)))
			for scanner.Scan() {
				if strings.HasPrefix(scanner.Text(), "AZCOPY_PERF_RESULT=") {
					require.NoError(t, json.Unmarshal([]byte(strings.TrimPrefix(scanner.Text(), "AZCOPY_PERF_RESULT=")), &result))
					found = true
				}
			}
			require.True(t, found, "worker did not return measurements")
			result.Pair = pair
			result.ProcessCPUSeconds = command.ProcessState.UserTime().Seconds() + command.ProcessState.SystemTime().Seconds()
			result.Requests = requests.Load() - previousRequests
			if pair >= 0 {
				runs = append(runs, result)
			}
			return result
		}
		run("disabled", -1)
		run("healthy", -1)
		var losses, cpuOverheads, heaps, peaks, startup []float64
		for pair := 0; pair < samples; pair++ {
			var baseline, enabled telemetryPerfRun
			if pair%2 == 0 {
				baseline = run("disabled", pair)
				enabled = run("healthy", pair)
			} else {
				enabled = run("healthy", pair)
				baseline = run("disabled", pair)
			}
			require.Zero(t, baseline.Requests)
			require.EqualValues(t, 2, enabled.Requests, "enabled run must exercise both production sends")
			require.Positive(t, baseline.Bytes)
			require.Positive(t, enabled.Bytes)
			require.Positive(t, baseline.ProcessCPUSeconds)
			losses = append(losses, 100*(1-(float64(enabled.Bytes)/enabled.TransferSeconds)/(float64(baseline.Bytes)/baseline.TransferSeconds)))
			cpuOverheads = append(cpuOverheads, 100*((enabled.ProcessCPUSeconds/float64(enabled.Bytes))/(baseline.ProcessCPUSeconds/float64(baseline.Bytes))-1))
			heaps = append(heaps, float64(enabled.RetainedHeapBytes-baseline.RetainedHeapBytes))
			peaks = append(peaks, float64(enabled.SampledPeakHeapBytes-baseline.SampledPeakHeapBytes))
			startup = append(startup, enabled.InitializationSeconds-baseline.InitializationSeconds)
		}
		gates = append(gates, telemetryPerfInterval(workload.name+" throughput loss (%)", losses, 1), telemetryPerfInterval(workload.name+" CPU per byte overhead (%)", cpuOverheads, 1), telemetryPerfInterval(workload.name+" retained heap delta (bytes)", heaps, 100*1024))
		gates = append(gates, telemetryPerfInterval(workload.name+" sampled peak heap delta (bytes)", peaks, 100*1024), telemetryPerfInterval(workload.name+" production initialization delta (seconds)", startup, 2.25))
		for _, mode := range []string{"unreachable", "stalled"} {
			result := run(mode, samples)
			status := "pass"
			if result.FlushSeconds > telemetryFlushTimeout.Seconds()+0.25 {
				status = "fail"
			}
			gates = append(gates, telemetryPerfGate{Name: workload.name + " " + mode + " flush seconds", Median: result.FlushSeconds, Limit: telemetryFlushTimeout.Seconds() + 0.25, Status: status})
		}
	}
	report := struct {
		GoVersion, GOOS, GOARCH string
		CPUs, Samples           int
		Seconds                 float64
		Runs                    []telemetryPerfRun
		Gates                   []telemetryPerfGate
	}{runtime.Version(), runtime.GOOS, runtime.GOARCH, runtime.NumCPU(), samples, seconds, runs, gates}
	encoded, err := json.MarshalIndent(report, "", "  ")
	require.NoError(t, err)
	require.NoError(t, os.WriteFile(filepath.Join(outputDirectory, "telemetry-performance.json"), encoded, 0600))
	for _, gate := range gates {
		t.Logf("%s: %s median=%.3f 95%%=[%.3f, %.3f] limit=%.3f", gate.Status, gate.Name, gate.Median, gate.Lower95, gate.Upper95, gate.Limit)
		if os.Getenv("AZCOPY_TELEMETRY_PERF_ENFORCE") != "false" && gate.Status != "pass" {
			t.Errorf("performance gate %s: %s", gate.Name, gate.Status)
		}
	}
}
