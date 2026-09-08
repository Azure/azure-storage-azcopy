package e2etest

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/Azure/azure-storage-azcopy/v10/cmd"
	"github.com/Azure/azure-storage-azcopy/v10/common"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func manifestEvent(process, invocation, name string) observedTelemetryEvent {
	return observedTelemetryEvent{
		Name: name,
		Properties: map[string]string{
			"E2ETestRunID": process, "JobID": "job", "InvocationID": invocation,
			"InstallationID": "installation", "Command": "copy", "SchemaVersion": "3",
			"JobStatus": "Completed", "TerminalStage": "completed",
		},
		Measurements: map[string]float64{name: 1, "azcopy.bytes_transferred": 100},
	}
}

func TestTelemetryDeliveryEvidence(t *testing.T) {
	const started = "telemetry: sent packed azcopy.job.started event to App Insights\n"
	const finished = "telemetry: sent packed azcopy.job.finished event to App Insights\n"
	healthy := collectTelemetryDeliveryEvidence(started+finished, strings.NewReader("transfer completed\n"))
	assert.Equal(t, telemetryDeliveryEvidence{StartedSends: 1, FinishedSends: 1, JobLogReadable: true}, healthy)
	stopped := collectTelemetryDeliveryEvidence(started, strings.NewReader(
		"WARN: telemetry: disabled for this process after delivery failure sending azcopy.job.finished: send metrics: context deadline exceeded\n"))
	assert.Equal(t, telemetryDeliveryEvidence{StartedSends: 1, StopMessages: 1, DeadlineErrors: 1, JobLogReadable: true}, stopped)
	duplicate := collectTelemetryDeliveryEvidence(started+started+finished, nil)
	assert.Equal(t, 2, duplicate.StartedSends)
	assert.False(t, duplicate.JobLogReadable)
	other := collectTelemetryDeliveryEvidence("", strings.NewReader("WARN: telemetry: dropped event after send panic\nprivate-path-token-canary\n"))
	assert.Equal(t, 1, other.OtherErrors)
	encoded, err := json.Marshal(other)
	require.NoError(t, err)
	assert.NotContains(t, string(encoded), "canary")
	unreadable := collectTelemetryDeliveryEvidence("", strings.NewReader(strings.Repeat("x", 1024*1024+1)))
	assert.False(t, unreadable.JobLogReadable)
}

func TestTelemetryManifest(t *testing.T) {
	expected := []telemetryExpectation{{ProcessRunID: "process", JobID: "job", Command: "copy", Properties: map[string]string{"SchemaVersion": "3"}, Measurements: map[string]float64{"azcopy.bytes_transferred": 100}}}
	started := manifestEvent("process", "invocation", "azcopy.job.started")
	finished := manifestEvent("process", "invocation", "azcopy.job.finished")
	t.Run("finish arrives before start", func(t *testing.T) {
		missing, err := checkTelemetryManifest(expected, []observedTelemetryEvent{finished, started})
		require.NoError(t, err)
		assert.Empty(t, missing)
	})
	t.Run("orphan waits for partner", func(t *testing.T) {
		missing, err := checkTelemetryManifest(expected, []observedTelemetryEvent{finished})
		require.NoError(t, err)
		assert.Equal(t, []string{"process/azcopy.job.started"}, missing)
	})
	t.Run("duplicate cannot mask missing attempt", func(t *testing.T) {
		_, err := checkTelemetryManifest(expected, []observedTelemetryEvent{finished, finished})
		assert.ErrorContains(t, err, "duplicate")
	})
	t.Run("different invocation", func(t *testing.T) {
		other := manifestEvent("process", "other", "azcopy.job.finished")
		_, err := checkTelemetryManifest(expected, []observedTelemetryEvent{started, other})
		assert.ErrorContains(t, err, "different invocation")
	})
	t.Run("resume process must arrive independently", func(t *testing.T) {
		attempts := append(append([]telemetryExpectation{}, expected...), telemetryExpectation{ProcessRunID: "resume", JobID: "job", Command: "jobs.resume"})
		missing, err := checkTelemetryManifest(attempts, []observedTelemetryEvent{started, finished})
		require.NoError(t, err)
		assert.Len(t, missing, 2)
	})
	t.Run("wrong dimensions", func(t *testing.T) {
		wrong := manifestEvent("process", "invocation", "azcopy.job.started")
		wrong.Properties["SchemaVersion"] = "future"
		_, err := checkTelemetryManifest(expected, []observedTelemetryEvent{wrong})
		assert.ErrorContains(t, err, "property SchemaVersion")
		assert.ErrorContains(t, err, `expected "3", got "future"`)
	})
	t.Run("wrong counters", func(t *testing.T) {
		wrong := manifestEvent("process", "invocation", "azcopy.job.finished")
		wrong.Measurements["azcopy.bytes_transferred"] = 101
		_, err := checkTelemetryManifest(expected, []observedTelemetryEvent{wrong})
		assert.ErrorContains(t, err, "measurement")
	})
}

type manifestQueryStub struct {
	responses [][]observedTelemetryEvent
	errors    []error
	calls     int
	query     string
}

func (s *manifestQueryStub) QueryTelemetryEvents(ctx context.Context, workspaceID, query string) ([]observedTelemetryEvent, error) {
	s.query = query
	index := s.calls
	s.calls++
	if index < len(s.errors) && s.errors[index] != nil {
		return nil, s.errors[index]
	}
	if index < len(s.responses) {
		return s.responses[index], nil
	}
	return nil, nil
}

func TestTelemetryManifestPolling(t *testing.T) {
	started := manifestEvent("run/process", "invocation", "azcopy.job.started")
	finished := manifestEvent("run/process", "invocation", "azcopy.job.finished")
	expected := []telemetryExpectation{{ProcessRunID: "run/process", JobID: "job", Command: "copy"}}
	client := &manifestQueryStub{
		responses: [][]observedTelemetryEvent{nil, {finished}, {finished, started}},
		errors:    []error{retryableQueryError{errors.New("transient")}},
	}
	verifier := telemetryManifestVerifier{queryClient: client, pollInterval: time.Millisecond, timeout: time.Second}
	require.NoError(t, verifier.Verify(context.Background(), "workspace", "run", time.Now(), expected))
	assert.Equal(t, 3, client.calls)
	assert.Contains(t, client.query, `startswith "run/"`)
	assert.Contains(t, client.query, "Measurements=todynamic(Measurements)")
	client = &manifestQueryStub{errors: []error{errors.New("forbidden")}}
	verifier.queryClient = client
	assert.ErrorContains(t, verifier.Verify(context.Background(), "workspace", "run", time.Now(), expected), "forbidden")
	assert.Equal(t, 1, client.calls)
	client = &manifestQueryStub{responses: [][]observedTelemetryEvent{{finished, finished}}}
	verifier.queryClient = client
	assert.ErrorContains(t, verifier.Verify(context.Background(), "workspace", "run", time.Now(), expected), "duplicate")
	client = &manifestQueryStub{}
	verifier.queryClient = client
	verifier.timeout = 5 * time.Millisecond
	assert.ErrorContains(t, verifier.Verify(context.Background(), "workspace", "run", time.Now(), expected), "manifest incomplete")
}

type manifestQueryFunc func(context.Context, string, string) ([]observedTelemetryEvent, error)

func (query manifestQueryFunc) QueryTelemetryEvents(ctx context.Context, workspaceID, text string) ([]observedTelemetryEvent, error) {
	return query(ctx, workspaceID, text)
}

func TestTelemetryManifestQueryAcrossObservationDeadline(t *testing.T) {
	started := manifestEvent("run/process", "invocation", "azcopy.job.started")
	finished := manifestEvent("run/process", "invocation", "azcopy.job.finished")
	complete := []observedTelemetryEvent{started, finished}
	expected := []telemetryExpectation{
		{ProcessRunID: "run/process", JobID: "job", Command: "copy"},
		{ProcessRunID: "run/disabled", NoEvents: true},
	}
	for _, test := range []struct {
		name         string
		events       []observedTelemetryEvent
		queryErr     error
		cancelParent bool
		wantError    string
	}{
		{name: "successful query completes after observation", events: complete},
		{name: "missing finish still fails", events: []observedTelemetryEvent{started}, wantError: "incomplete"},
		{name: "duplicate still fails", events: []observedTelemetryEvent{started, finished, finished}, wantError: "duplicate"},
		{name: "late opt-out event still fails", events: append(append([]observedTelemetryEvent(nil), complete...), manifestEvent("run/disabled", "other", "azcopy.job.started")), wantError: "unexpected telemetry"},
		{name: "query failure still fails", queryErr: retryableQueryError{errors.New("service unavailable")}, wantError: "service unavailable"},
		{name: "parent cancellation still fails", cancelParent: true, wantError: "context canceled"},
	} {
		t.Run(test.name, func(t *testing.T) {
			parent, cancel := context.WithCancel(context.Background())
			defer cancel()
			calls := 0
			client := manifestQueryFunc(func(ctx context.Context, _, _ string) ([]observedTelemetryEvent, error) {
				calls++
				if calls == 1 {
					return complete, nil
				}
				_, bounded := ctx.Deadline()
				require.True(t, bounded, "queries must retain their own timeout")
				if test.cancelParent {
					cancel()
				}
				timer := time.NewTimer(60 * time.Millisecond)
				defer timer.Stop()
				select {
				case <-ctx.Done():
					return nil, retryableQueryError{ctx.Err()}
				case <-timer.C:
					return test.events, test.queryErr
				}
			})
			verifier := telemetryManifestVerifier{queryClient: client, pollInterval: time.Millisecond, timeout: 30 * time.Millisecond}
			err := verifier.Verify(parent, "workspace", "run", time.Now(), expected)
			if test.wantError == "" {
				require.NoError(t, err)
			} else {
				require.ErrorContains(t, err, test.wantError)
			}
			require.Equal(t, 2, calls)
		})
	}
}

func TestTelemetryManifestParsing(t *testing.T) {
	var result logAnalyticsQueryResponse
	require.NoError(t, json.Unmarshal([]byte(`{"tables":[{"columns":[{"name":"Measurements"},{"name":"Name"},{"name":"Properties"}],"rows":[[{"azcopy.job.started":1},"azcopy.job.started","{\"JobID\":\"job\"}"]]}]}`), &result))
	events, err := parseTelemetryEvents(result)
	require.NoError(t, err)
	require.Len(t, events, 1)
	assert.Equal(t, "job", events[0].Properties["JobID"])
	assert.Equal(t, float64(1), events[0].Measurements["azcopy.job.started"])
	result.Tables[0].Rows[0] = result.Tables[0].Rows[0][:1]
	_, err = parseTelemetryEvents(result)
	assert.ErrorContains(t, err, "short lifecycle")
}

func TestTelemetryManifestEndpointTypes(t *testing.T) {
	for _, test := range []struct {
		name, source, destination, fromTo, wantSource, wantDestination string
	}{
		{"files URL inference", "https://account.file.core.windows.net/nfs/source", "https://account.file.core.windows.net/nfs/destination", "", "File", "File"},
		{"explicit NFS", "https://account.file.core.windows.net/nfs/source", "https://account.file.core.windows.net/nfs/destination", "FileNFSFileNFS", "FileNFS", "FileNFS"},
		{"SMB alias", "https://account.file.core.windows.net/share/source", "https://account.blob.core.windows.net/container/destination", "FileSMBBlob", "File", "Blob"},
		{"DFS endpoint", "https://account.dfs.core.windows.net/filesystem/source", "https://account.blob.core.windows.net/container/destination", "", "BlobFS", "Blob"},
		{"local upload", "local-source", "https://account.blob.core.windows.net/container/destination", "", "Local", "Blob"},
	} {
		t.Run(test.name, func(t *testing.T) {
			source, destination, err := telemetryEndpointTypes([]string{test.source, test.destination}, map[string]string{"from-to": test.fromTo})
			require.NoError(t, err)
			assert.Equal(t, test.wantSource, source)
			assert.Equal(t, test.wantDestination, destination)
		})
	}
	source, destination, err := telemetryEndpointTypes(nil, nil)
	require.NoError(t, err)
	assert.Empty(t, source)
	assert.Empty(t, destination)
	_, _, err = telemetryEndpointTypes([]string{"source", "destination"}, map[string]string{"from-to": "invalid"})
	require.Error(t, err)
}

func TestTelemetryManifestRegistration(t *testing.T) {
	resetAppInsightsValidation()
	t.Cleanup(resetAppInsightsValidation)
	jobID := common.NewJobID()
	summary := common.ListJobSummaryResponse{
		JobID: jobID, JobStatus: common.EJobStatus.Completed(), TotalBytesTransferred: 123, TransfersCompleted: 2,
	}
	capture := newAzCopyJobIDCapture(&AzCopyRawStdout{})
	_, err := capture.Write([]byte("Job " + jobID.String() + " has started\n"))
	require.NoError(t, err)
	encodedSummary, err := json.Marshal(summary)
	require.NoError(t, err)
	encodedOutput, err := json.Marshal(cmd.JsonOutputTemplate{MessageType: cmd.EOutputMessageType.EndOfJob().String(), MessageContent: string(encodedSummary)})
	require.NoError(t, err)
	_, err = capture.Write(encodedOutput[:len(encodedOutput)/2])
	require.NoError(t, err)
	_, err = capture.Write(encodedOutput[len(encodedOutput)/2:])
	require.NoError(t, err)
	registerTelemetryExpectation("run/process", capture.JobID(), AzCopyVerbJobsResume, capture.FinalSummary(), "Blob", "File")
	attempt := snapshotAppInsightsValidation().expectedAttempts[0]
	assert.Equal(t, "jobs.resume", attempt.Command)
	assert.Equal(t, "job-cumulative", attempt.Properties["SummaryCounterScope"])
	assert.Equal(t, "Blob", attempt.Properties["SourceType"])
	assert.Equal(t, "Completed", attempt.FinishedProperties["JobStatus"])
	assert.Equal(t, float64(123), attempt.Measurements["azcopy.bytes_transferred"])
	environment, first := telemetryProcessEnvironment([]string{"KEEP=yes", "AZCOPY_E2E_TELEMETRY_RUN_ID=old", "azcopy_e2e_telemetry_run_id=other"}, "run")
	_, second := telemetryProcessEnvironment(nil, "run")
	assert.NotEqual(t, first, second)
	assert.Equal(t, []string{"KEEP=yes", "AZCOPY_E2E_TELEMETRY_RUN_ID=" + first}, environment)
	assert.True(t, strings.HasPrefix(first, "run/"))
	_, err = validateAppInsightsValidationConfig(AppInsightsValidationConfig{RunID: strings.Repeat("x", 81), WorkspaceID: "workspace", ConnectionString: "InstrumentationKey=test"})
	assert.ErrorContains(t, err, "at most 80 bytes")
}

func TestTelemetryManifestOptOut(t *testing.T) {
	for _, flags := range []map[string]string{{"disable-telemetry": "true"}, {"disable-telemetry": "1"}, {"dry-run": "true"}} {
		assert.False(t, azCopyCommandProducesJobFinishedTelemetry(AzCopyVerbCopy, flags))
		assert.Empty(t, decideAppInsightsJobValidation(AzCopyVerbCopy, flags, false, "job").jobID)
	}
	assert.True(t, telemetryExpectsNoEvents(AzCopyVerbCopy, nil, []string{"AZCOPY_DISABLE_TELEMETRY=TRUE"}))
	assert.True(t, azCopyCommandProducesJobFinishedTelemetry(AzCopyVerbCopy, map[string]string{"disable-telemetry": "false"}))
	expected := []telemetryExpectation{{ProcessRunID: "run/disabled", NoEvents: true}}
	missing, err := checkTelemetryManifest(expected, nil)
	require.NoError(t, err)
	assert.Empty(t, missing)
	_, err = checkTelemetryManifest(expected, []observedTelemetryEvent{manifestEvent("run/disabled", "invocation", "azcopy.job.started")})
	assert.ErrorContains(t, err, "unexpected telemetry")
	client := &manifestQueryStub{}
	verifier := telemetryManifestVerifier{queryClient: client, pollInterval: time.Millisecond, timeout: 15 * time.Millisecond}
	require.NoError(t, verifier.Verify(context.Background(), "workspace", "run", time.Now(), expected))
	assert.Greater(t, client.calls, 1)
	client = &manifestQueryStub{responses: [][]observedTelemetryEvent{nil, {manifestEvent("run/disabled", "invocation", "azcopy.job.finished")}}}
	verifier.queryClient = client
	assert.ErrorContains(t, verifier.Verify(context.Background(), "workspace", "run", time.Now(), expected), "unexpected telemetry")
	client = &manifestQueryStub{errors: []error{errors.New("forbidden")}}
	verifier.queryClient = client
	assert.Error(t, verifier.Verify(context.Background(), "workspace", "run", time.Now(), expected))
}
