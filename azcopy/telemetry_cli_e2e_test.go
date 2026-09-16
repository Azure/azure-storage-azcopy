//go:build telemetrylive

package azcopy

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Azure/azure-storage-azcopy/v10/common"
	"github.com/stretchr/testify/require"
)

type cliE2EEnvelope struct {
	Data struct {
		BaseType string `json:"baseType"`
		BaseData struct {
			Name         string             `json:"name"`
			Properties   map[string]string  `json:"properties"`
			Measurements map[string]float64 `json:"measurements"`
		} `json:"baseData"`
	} `json:"data"`
}

type cliE2EReceiver struct {
	mu     sync.Mutex
	bodies [][]byte
	server *httptest.Server
}

func newCLIE2EReceiver(t *testing.T, response ...http.HandlerFunc) *cliE2EReceiver {
	t.Helper()
	receiver := &cliE2EReceiver{}
	receiver.server = httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		if request.Method != http.MethodPost || request.URL.Path != "/v2.1/track" {
			http.Error(writer, "unexpected ingestion request", http.StatusBadRequest)
			return
		}
		body, err := io.ReadAll(io.LimitReader(request.Body, 64*1024))
		if err != nil {
			http.Error(writer, "cannot read request", http.StatusBadRequest)
			return
		}
		receiver.mu.Lock()
		receiver.bodies = append(receiver.bodies, body)
		receiver.mu.Unlock()
		if len(response) > 0 {
			response[0](writer, request)
			return
		}
		writer.WriteHeader(http.StatusOK)
	}))
	t.Cleanup(receiver.server.Close)
	return receiver
}

func (receiver *cliE2EReceiver) connection() string {
	return "InstrumentationKey=11111111-2222-3333-4444-555555555555;IngestionEndpoint=" + receiver.server.URL
}

func (receiver *cliE2EReceiver) events(t *testing.T) []cliE2EEnvelope {
	t.Helper()
	receiver.mu.Lock()
	defer receiver.mu.Unlock()
	var events []cliE2EEnvelope
	for _, body := range receiver.bodies {
		var batch []cliE2EEnvelope
		require.NoError(t, json.Unmarshal(body, &batch))
		require.Len(t, batch, 1)
		budget := 8 * 1024
		if batch[0].Data.BaseData.Name == "azcopy.job.started" {
			budget = 4 * 1024
		}
		require.LessOrEqual(t, len(body), budget)
		events = append(events, batch...)
	}
	return events
}

type cliE2EResult struct {
	stdout  []byte
	stderr  []byte
	log     string
	elapsed time.Duration
}

func runCLIE2E(t *testing.T, executable, home, connection string, environment map[string]string, arguments ...string) cliE2EResult {
	t.Helper()
	root := t.TempDir()
	settings := map[string]string{
		"AZCOPY_DISABLE_TELEMETRY": "false", "AZCOPY_TELEMETRY_CONNECTION_STRING": connection,
		"AZCOPY_LOG_LOCATION": root, "AZCOPY_JOB_PLAN_LOCATION": root,
		common.EEnvironmentVariable.UserDir().Name: home,
		"GOMAXPROCS": "2", "AZCOPY_CONCURRENCY_VALUE": "4",
	}
	for name, value := range environment {
		settings[name] = value
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	command := exec.CommandContext(ctx, executable, arguments...)
	command.Env = liveCLIEnvironment(os.Environ(), settings)
	command.Dir = root
	var stdout, stderr bytes.Buffer
	command.Stdout, command.Stderr = &stdout, &stderr
	stdin, err := command.StdinPipe()
	require.NoError(t, err)
	defer stdin.Close()
	start := time.Now()
	err = command.Run()
	elapsed := time.Since(start)
	require.NoError(t, err, "stdout=%s stderr=%s", stdout.String(), stderr.String())
	var logs strings.Builder
	files, err := filepath.Glob(filepath.Join(root, "*.log"))
	require.NoError(t, err)
	for _, file := range files {
		contents, err := os.ReadFile(file)
		require.NoError(t, err)
		logs.Write(contents)
	}
	return cliE2EResult{stdout.Bytes(), stderr.Bytes(), logs.String(), elapsed}
}

func newCLIE2EBlob(t *testing.T, content ...[]byte) (string, []byte) {
	t.Helper()
	payload := bytes.Repeat([]byte("telemetry transfer fixture\n"), 1024)
	if len(content) > 0 {
		payload = content[0]
	}
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		if request.URL.Path != "/account/container/payload.bin" || (request.Method != http.MethodHead && request.Method != http.MethodGet) {
			http.NotFound(writer, request)
			return
		}
		writer.Header().Set("x-ms-blob-type", "BlockBlob")
		writer.Header().Set("x-ms-request-id", "fixture-request-id")
		writer.Header().Set("ETag", `"fixture-etag"`)
		if byteRange := request.Header.Get("x-ms-range"); byteRange != "" {
			request.Header.Set("Range", byteRange)
		}
		http.ServeContent(writer, request, "payload.bin", time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC), bytes.NewReader(payload))
	}))
	t.Cleanup(server.Close)
	return server.URL + "/account/container/payload.bin", payload
}

func runCLIE2ETransfer(t *testing.T, executable, home, connection, source, verb string, payload []byte, environment map[string]string, flags ...string) (cliE2EResult, common.ListJobSummaryResponse) {
	t.Helper()
	destination := filepath.Join(t.TempDir(), "download.bin")
	arguments := append([]string{verb, source, destination, "--from-to=BlobLocal", "--output-type=json", "--check-version=false", "--log-level=INFO"}, flags...)
	result := runCLIE2E(t, executable, home, connection, environment, arguments...)
	summary, err := liveCLISummary(result.stdout)
	require.NoError(t, err, "%s", result.stdout)
	require.Equal(t, common.EJobStatus.Completed(), summary.JobStatus)
	require.EqualValues(t, 1, summary.TransfersCompleted)
	require.Zero(t, summary.TransfersFailed)
	require.Zero(t, summary.TransfersSkipped)
	require.EqualValues(t, len(payload), summary.TotalBytesTransferred)
	actual, err := os.ReadFile(destination)
	require.NoError(t, err)
	require.Equal(t, sha256.Sum256(payload), sha256.Sum256(actual))
	return result, summary
}

func assertCLIE2ELifecycle(t *testing.T, events []cliE2EEnvelope, summary common.ListJobSummaryResponse, command string, sourceAuth ...string) {
	t.Helper()
	require.Len(t, events, 2)
	started, finished := events[0].Data.BaseData, events[1].Data.BaseData
	require.Equal(t, "azcopy.job.started", started.Name)
	require.Equal(t, "azcopy.job.finished", finished.Name)
	auth := "PublicAnonymous"
	if len(sourceAuth) > 0 {
		auth = sourceAuth[0]
	}
	for _, properties := range []map[string]string{started.Properties, finished.Properties} {
		require.Equal(t, command, properties["Command"])
		require.Equal(t, summary.JobID.String(), properties["JobID"])
		require.Equal(t, "1", properties["SchemaVersion"])
		require.Equal(t, "BlobLocal", properties["FromTo"])
		require.Equal(t, "Blob", properties["SourceType"])
		require.Equal(t, "Local", properties["DestType"])
		require.Equal(t, auth, properties["SourceAuthMechanism"])
		require.Equal(t, "NotApplicable", properties["DestAuthMechanism"])
		require.Equal(t, "public", properties["SourceEndpointKind"])
		require.Empty(t, properties["DestEndpointKind"])
		require.Regexp(t, `^[a-f0-9]{32}$`, properties["InstallationID"])
		require.Regexp(t, `^[a-f0-9]{32}$`, properties["InvocationID"])
	}
	require.Equal(t, started.Properties["InstallationID"], finished.Properties["InstallationID"])
	require.Equal(t, started.Properties["InvocationID"], finished.Properties["InvocationID"])
	require.Equal(t, "Completed", finished.Properties["JobStatus"])
	for metric, expected := range map[string]float64{
		"azcopy.bytes_transferred":           float64(summary.TotalBytesTransferred),
		"azcopy.bytes_enumerated":            float64(summary.TotalBytesEnumerated),
		"azcopy.bytes_expected":              float64(summary.TotalBytesExpected),
		"azcopy.transfers_total":             float64(summary.TotalTransfers),
		"azcopy.transfers_failed":            float64(summary.TransfersFailed),
		"azcopy.transfers_skipped":           float64(summary.TransfersSkipped),
		"azcopy.regular_files_scheduled":     float64(summary.FileTransfers),
		"azcopy.folder_properties_scheduled": float64(summary.FolderPropertyTransfers),
		"azcopy.objects_completed":           float64(summary.TransfersCompleted - summary.FoldersCompleted),
		"azcopy.folder_properties_completed": float64(summary.FoldersCompleted),
		"azcopy.storage_http_attempt_count":  float64(summary.StorageHTTPAttemptCount),
		"azcopy.source_bytes_scanned":        float64(summary.TotalBytesEnumerated),
		"azcopy.source_objects_scanned":      1,
		"azcopy.source_max_directory_depth":  0,
	} {
		value, present := finished.Measurements[metric]
		require.True(t, present, metric)
		require.Equal(t, expected, value, metric)
	}
	require.EqualValues(t, 1, started.Measurements["azcopy.job.started"])
	require.EqualValues(t, 1, finished.Measurements["azcopy.job.finished"])
}

func TestTelemetryCLIIntegration(t *testing.T) {
	if os.Getenv("AZCOPY_RUN_TELEMETRY_CLI_INTEGRATION") != "1" {
		t.Skip("loopback CLI integration tests; set AZCOPY_RUN_TELEMETRY_CLI_INTEGRATION=1")
	}
	extension := ""
	if runtime.GOOS == "windows" {
		extension = ".exe"
	}
	executable := filepath.Join(t.TempDir(), "azcopy"+extension)
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()
	build := exec.CommandContext(ctx, "go", "build", "-o", executable, "..")
	output, err := build.CombinedOutput()
	require.NoError(t, err, "%s", output)
	for _, test := range []struct {
		name       string
		arguments  []string
		env        map[string]string
		connection string
		wantEvents int
	}{
		{name: "eligible command", arguments: []string{"jobs", "list"}, wantEvents: 1},
		{name: "CLI opt-out", arguments: []string{"jobs", "list", "--disable-telemetry"}},
		{name: "environment opt-out", arguments: []string{"jobs", "list"}, env: map[string]string{"AZCOPY_DISABLE_TELEMETRY": "true"}},
		{name: "help", arguments: []string{"help"}},
		{name: "help flag", arguments: []string{"copy", "--help"}},
		{name: "environment command", arguments: []string{"env"}},
		{name: "completion", arguments: []string{"completion", "bash"}},
		{name: "zsh completion", arguments: []string{"completion", "zsh"}},
		{name: "fish completion", arguments: []string{"completion", "fish"}},
		{name: "powershell completion", arguments: []string{"completion", "powershell"}},
		{name: "documentation", arguments: []string{"doc"}},
		{name: "missing key", arguments: []string{"jobs", "list"}, connection: "IngestionEndpoint={receiver}"},
		{name: "missing endpoint", arguments: []string{"jobs", "list"}, connection: "InstrumentationKey=11111111-2222-3333-4444-555555555555"},
	} {
		t.Run(test.name, func(t *testing.T) {
			receiver := newCLIE2EReceiver(t)
			connection := receiver.connection()
			if test.connection != "" {
				connection = string(bytes.ReplaceAll([]byte(test.connection), []byte("{receiver}"), []byte(receiver.server.URL)))
			}
			arguments := append(append([]string(nil), test.arguments...), "--check-version=false")
			runCLIE2E(t, executable, t.TempDir(), connection, test.env, arguments...)
			events := receiver.events(t)
			require.Len(t, events, test.wantEvents)
			if test.wantEvents > 0 {
				require.Equal(t, "EventData", events[0].Data.BaseType)
				require.Equal(t, "azcopy.command.invoked", events[0].Data.BaseData.Name)
				require.Equal(t, "jobs.list", events[0].Data.BaseData.Properties["Command"])
				require.Equal(t, "1", events[0].Data.BaseData.Properties["SchemaVersion"])
			}
		})
	}
	t.Run("copy reconciliation", func(t *testing.T) {
		receiver := newCLIE2EReceiver(t)
		source, payload := newCLIE2EBlob(t)
		_, summary := runCLIE2ETransfer(t, executable, t.TempDir(), receiver.connection(), source, "copy", payload, nil)
		assertCLIE2ELifecycle(t, receiver.events(t), summary, "copy")
	})
	t.Run("copy opt-out", func(t *testing.T) {
		receiver := newCLIE2EReceiver(t)
		source, payload := newCLIE2EBlob(t)
		runCLIE2ETransfer(t, executable, t.TempDir(), receiver.connection(), source, "copy", payload, nil, "--disable-telemetry")
		require.Empty(t, receiver.events(t))
	})
	t.Run("source SAS privacy", func(t *testing.T) {
		receiver := newCLIE2EReceiver(t)
		source, payload := newCLIE2EBlob(t)
		const canary = "private-source-sas-canary"
		_, summary := runCLIE2ETransfer(t, executable, t.TempDir(), receiver.connection(), source+"?sv=2024-11-04&sig="+canary, "copy", payload, nil)
		events := receiver.events(t)
		assertCLIE2ELifecycle(t, events, summary, "copy", "SAS")
		receiver.mu.Lock()
		serialized := bytes.Join(receiver.bodies, nil)
		receiver.mu.Unlock()
		for _, private := range []string{canary, source, "payload.bin", "download.bin", "fixture-request-id"} {
			require.NotContains(t, string(serialized), private)
		}
	})
	t.Run("copy dry-run", func(t *testing.T) {
		receiver := newCLIE2EReceiver(t)
		source, _ := newCLIE2EBlob(t)
		destination := filepath.Join(t.TempDir(), "not-created.bin")
		runCLIE2E(t, executable, t.TempDir(), receiver.connection(), nil, "copy", source, destination, "--from-to=BlobLocal", "--dry-run", "--check-version=false")
		require.NoFileExists(t, destination)
		require.Empty(t, receiver.events(t))
	})
	t.Run("sync reconciliation", func(t *testing.T) {
		receiver := newCLIE2EReceiver(t)
		source, payload := newCLIE2EBlob(t)
		_, summary := runCLIE2ETransfer(t, executable, t.TempDir(), receiver.connection(), source, "sync", payload, nil)
		assertCLIE2ELifecycle(t, receiver.events(t), summary, "sync")
	})
	t.Run("sync dry-run", func(t *testing.T) {
		receiver := newCLIE2EReceiver(t)
		source, _ := newCLIE2EBlob(t)
		destination := filepath.Join(t.TempDir(), "not-created.bin")
		runCLIE2E(t, executable, t.TempDir(), receiver.connection(), nil, "sync", source, destination, "--from-to=BlobLocal", "--dry-run", "--check-version=false")
		require.NoFileExists(t, destination)
		require.Empty(t, receiver.events(t))
	})
	t.Run("concurrent process identity", func(t *testing.T) {
		receiver := newCLIE2EReceiver(t)
		source, payload := newCLIE2EBlob(t)
		home := t.TempDir()
		expectedIdentity := installationIDInDir(filepath.Join(home, ".azcopy"))
		require.Len(t, expectedIdentity, 32)
		summaries := make([]common.ListJobSummaryResponse, 4)
		t.Run("workers", func(t *testing.T) {
			for index := range summaries {
				t.Run(fmt.Sprintf("process-%d", index), func(t *testing.T) {
					t.Parallel()
					_, summaries[index] = runCLIE2ETransfer(t, executable, home, receiver.connection(), source, "copy", payload, nil)
				})
			}
		})
		events := receiver.events(t)
		require.Len(t, events, 8)
		installations, invocations, jobs := map[string]bool{}, map[string]bool{}, map[string]bool{}
		for _, summary := range summaries {
			var pair []cliE2EEnvelope
			for _, event := range events {
				if event.Data.BaseData.Properties["JobID"] == summary.JobID.String() {
					pair = append(pair, event)
				}
			}
			assertCLIE2ELifecycle(t, pair, summary, "copy")
			properties := pair[0].Data.BaseData.Properties
			installations[properties["InstallationID"]] = true
			invocations[properties["InvocationID"]] = true
			jobs[properties["JobID"]] = true
		}
		require.Len(t, installations, 1)
		require.True(t, installations[expectedIdentity])
		require.Len(t, invocations, 4)
		require.Len(t, jobs, 4)
		identity, err := os.ReadFile(filepath.Join(home, ".azcopy", "installation_id"))
		require.NoError(t, err)
		require.True(t, installations[strings.TrimSpace(string(identity))])
	})
	for _, test := range []struct {
		name   string
		status int
		body   string
	}{
		{"rejected", http.StatusBadRequest, "private-ingestion-error-canary"},
		{"partial acceptance", http.StatusPartialContent, `{"itemsReceived":1,"itemsAccepted":0,"errors":[{"index":0,"statusCode":400,"message":"private-ingestion-error-canary"}]}`},
		{"malformed partial acceptance", http.StatusPartialContent, "private-ingestion-error-canary"},
		{"throttled", http.StatusTooManyRequests, "private-ingestion-error-canary"},
		{"unavailable", http.StatusServiceUnavailable, "private-ingestion-error-canary"},
	} {
		t.Run("non-interference/"+test.name, func(t *testing.T) {
			receiver := newCLIE2EReceiver(t, func(writer http.ResponseWriter, request *http.Request) {
				writer.Header().Set("Retry-After", "60")
				writer.WriteHeader(test.status)
				_, _ = io.WriteString(writer, test.body)
			})
			source, payload := newCLIE2EBlob(t)
			result, _ := runCLIE2ETransfer(t, executable, t.TempDir(), receiver.connection(), source, "copy", payload, nil)
			events := receiver.events(t)
			require.Len(t, events, 1, "delivery failure must not retry or send a finish")
			require.Equal(t, "azcopy.job.started", events[0].Data.BaseData.Name)
			require.Equal(t, 1, strings.Count(result.log, "telemetry: disabled for this process after delivery failure"))
			require.NotContains(t, result.log+string(result.stderr), "private-ingestion-error-canary")
			require.Less(t, result.elapsed, 10*time.Second, "must not honor Retry-After or block transfer completion")
		})
	}
	t.Run("non-interference/unreachable ingestion", func(t *testing.T) {
		source, payload := newCLIE2EBlob(t, bytes.Repeat([]byte("ingestion failure must not affect transfers\n"), 50000)[:2*1024*1024])
		connection := "InstrumentationKey=11111111-2222-3333-4444-555555555555;IngestionEndpoint=http://192.0.2.1"
		result, _ := runCLIE2ETransfer(t, executable, t.TempDir(), connection, source, "copy", payload,
			map[string]string{"NO_PROXY": "192.0.2.1,127.0.0.1,localhost"}, "--cap-mbps=1", "--block-size-mb=0.0625")
		require.Equal(t, 1, strings.Count(result.log, "telemetry: disabled for this process after delivery failure sending azcopy.job.started:"))
		require.NotContains(t, result.log+string(result.stderr), "telemetry: sent packed")
		require.NotContains(t, string(result.stderr), "192.0.2.1")
		require.Less(t, result.elapsed, 25*time.Second)
	})
	t.Run("non-interference/stalled ingestion", func(t *testing.T) {
		release := make(chan struct{})
		receiver := newCLIE2EReceiver(t, func(writer http.ResponseWriter, request *http.Request) {
			select {
			case <-request.Context().Done():
			case <-release:
			}
		})
		t.Cleanup(func() { close(release) })
		source, payload := newCLIE2EBlob(t)
		result, _ := runCLIE2ETransfer(t, executable, t.TempDir(), receiver.connection(), source, "copy", payload, nil)
		require.Len(t, receiver.events(t), 1)
		require.Less(t, result.elapsed, telemetryFlushTimeout+5*time.Second)
		require.GreaterOrEqual(t, result.elapsed, 3*time.Second)
	})
	t.Run("non-interference/slow success", func(t *testing.T) {
		var requests atomic.Int32
		receiver := newCLIE2EReceiver(t, func(writer http.ResponseWriter, request *http.Request) {
			if requests.Add(1) == 1 {
				timer := time.NewTimer(2500 * time.Millisecond)
				defer timer.Stop()
				select {
				case <-timer.C:
				case <-request.Context().Done():
					return
				}
			}
			writer.WriteHeader(http.StatusOK)
		})
		source, payload := newCLIE2EBlob(t)
		_, summary := runCLIE2ETransfer(t, executable, t.TempDir(), receiver.connection(), source, "copy", payload, nil)
		assertCLIE2ELifecycle(t, receiver.events(t), summary, "copy")
	})
	t.Run("non-interference/stalled finish", func(t *testing.T) {
		var requests atomic.Int32
		release := make(chan struct{})
		finishArrived := make(chan time.Time, 1)
		receiver := newCLIE2EReceiver(t, func(writer http.ResponseWriter, request *http.Request) {
			if requests.Add(1) == 1 {
				writer.WriteHeader(http.StatusOK)
				return
			}
			finishArrived <- time.Now()
			select {
			case <-request.Context().Done():
			case <-release:
			}
		})
		t.Cleanup(func() { close(release) })
		source, payload := newCLIE2EBlob(t)
		_, summary := runCLIE2ETransfer(t, executable, t.TempDir(), receiver.connection(), source, "copy", payload, nil)
		assertCLIE2ELifecycle(t, receiver.events(t), summary, "copy")
		select {
		case arrived := <-finishArrived:
			require.Less(t, time.Since(arrived), telemetryFlushTimeout+time.Second)
			require.Greater(t, time.Since(arrived), telemetryFlushTimeout-time.Second)
		default:
			t.Fatal("finish request was not observed")
		}
	})
	t.Run("non-interference/unreachable ingestion", func(t *testing.T) {
		receiver := newCLIE2EReceiver(t)
		receiver.server.Close()
		source, payload := newCLIE2EBlob(t)
		result, _ := runCLIE2ETransfer(t, executable, t.TempDir(), receiver.connection(), source, "copy", payload, nil)
		require.Empty(t, receiver.events(t))
		require.Equal(t, 1, strings.Count(result.log, "telemetry: disabled for this process after delivery failure"))
	})
}
