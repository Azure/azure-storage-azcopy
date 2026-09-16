package azcopy

import (
	"fmt"
	"io"
	"net/http"
	"runtime"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Azure/azure-storage-azcopy/v10/common"
	"github.com/Azure/azure-storage-azcopy/v10/telemetry"
	"github.com/Azure/azure-storage-azcopy/v10/traverser"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type budgetBlockingClient struct {
	entered            chan struct{}
	release            chan struct{}
	ignoreCancellation bool
}

func (client *budgetBlockingClient) Do(request *http.Request) (*http.Response, error) {
	client.entered <- struct{}{}
	if client.ignoreCancellation {
		<-client.release
	} else {
		select {
		case <-request.Context().Done():
		case <-client.release:
		}
	}
	return nil, request.Context().Err()
}

func TestTelemetryBudgetBoundsBlockedSendsAndFlushes(t *testing.T) {
	client := &budgetBlockingClient{entered: make(chan struct{}, telemetryMaxPendingSends), release: make(chan struct{}), ignoreCancellation: true}
	t.Cleanup(func() { close(client.release) })
	agent := &telemetryAgent{enabled: true, sendTimeout: time.Minute, reporter: telemetry.NewReporter(telemetry.Config{
		Backend: telemetry.BackendAppInsights, ConnectionString: "InstrumentationKey=test;IngestionEndpoint=https://example.test", HTTPClient: client,
	})}
	before := runtime.NumGoroutine()
	for index := 0; index < telemetryMaxPendingSends; index++ {
		agent.reportStarted(telemetry.JobDimensions{}, "job", fmt.Sprint(index), time.Now())
		select {
		case <-client.entered:
		case <-time.After(time.Second):
			t.Fatal("sender did not enter transport")
		}
	}
	for index := 0; index < 1000; index++ {
		agent.reportStarted(telemetry.JobDimensions{}, "overflow", fmt.Sprint(index), time.Now())
		agent.flush(time.Nanosecond)
	}
	agent.dispatchMu.Lock()
	assert.Len(t, agent.pending, telemetryMaxPendingSends)
	agent.dispatchMu.Unlock()
	tracked := 0
	agent.startedSends.Range(func(key, value any) bool { tracked++; return true })
	assert.LessOrEqual(t, tracked, telemetryMaxPendingSends)
	assert.LessOrEqual(t, runtime.NumGoroutine()-before, 2*telemetryMaxPendingSends+2)
}

func TestTelemetryBudgetFlushCancelsTransport(t *testing.T) {
	client := &budgetBlockingClient{entered: make(chan struct{}, 1), release: make(chan struct{})}
	agent := &telemetryAgent{enabled: true, reporter: telemetry.NewReporter(telemetry.Config{
		Backend: telemetry.BackendAppInsights, ConnectionString: "InstrumentationKey=test;IngestionEndpoint=https://example.test", HTTPClient: client,
	})}
	complete := agent.dispatch(telemetry.CommandInvokedEvent{}, nil)
	select {
	case <-client.entered:
	case <-time.After(time.Second):
		t.Fatal("sender did not enter transport")
	}
	agent.flush(time.Nanosecond)
	select {
	case <-complete:
	case <-time.After(time.Second):
		t.Fatal("flush did not cancel active request")
	}
	agent.dispatchMu.Lock()
	assert.Empty(t, agent.pending)
	agent.dispatchMu.Unlock()
}

func TestTelemetryBudgetDefaultDeadlineAllowsSlowSuccess(t *testing.T) {
	require.Equal(t, 5*time.Second, telemetrySendTimeout)
	require.Equal(t, 4*time.Second, telemetryFlushTimeout)
	deadlines := make(chan time.Duration, 2)
	var requests atomic.Int64
	client := &http.Client{Transport: telemetryFailureTransport(func(request *http.Request) (*http.Response, error) {
		deadline, present := request.Context().Deadline()
		if !present {
			return nil, fmt.Errorf("telemetry request has no deadline")
		}
		deadlines <- time.Until(deadline)
		if requests.Add(1) == 1 {
			timer := time.NewTimer(2500 * time.Millisecond)
			defer timer.Stop()
			select {
			case <-timer.C:
			case <-request.Context().Done():
				return nil, request.Context().Err()
			}
		}
		return &http.Response{StatusCode: http.StatusOK, Body: io.NopCloser(strings.NewReader("")), Header: make(http.Header)}, nil
	})}
	agent := &telemetryAgent{enabled: true, reporter: telemetry.NewReporter(telemetry.Config{
		Backend: telemetry.BackendAppInsights, ConnectionString: "InstrumentationKey=test;IngestionEndpoint=https://example.test", HTTPClient: client,
	})}
	started := agent.dispatch(telemetry.JobStartedEvent{}, nil)
	finished := agent.dispatch(telemetry.JobFinishedEvent{}, started)
	t.Cleanup(func() { agent.flush(time.Nanosecond) })
	agent.flush(telemetryFlushTimeout)
	select {
	case <-finished:
	case <-time.After(time.Second):
		t.Fatal("healthy ordered sends did not complete")
	}
	require.True(t, agent.isActive(), "a successful request exceeding two seconds must survive exit flush")
	require.EqualValues(t, 2, requests.Load())
	for index := 0; index < 2; index++ {
		remaining := <-deadlines
		assert.Greater(t, remaining, 4*time.Second)
		assert.LessOrEqual(t, remaining, 5*time.Second)
	}
}

func TestTelemetryBudgetTimedOutStartStopsFinish(t *testing.T) {
	client := &budgetBlockingClient{entered: make(chan struct{}, 2), release: make(chan struct{})}
	agent := &telemetryAgent{enabled: true, sendTimeout: 50 * time.Millisecond, reporter: telemetry.NewReporter(telemetry.Config{
		Backend: telemetry.BackendAppInsights, ConnectionString: "InstrumentationKey=test;IngestionEndpoint=https://example.test", HTTPClient: client,
	})}
	agent.reportStarted(telemetry.JobDimensions{}, "job", "attempt", time.Now())
	agent.reportFinished(telemetry.JobFinishedEvent{JobID: "job", InvocationID: "attempt"})
	select {
	case <-client.entered:
	case <-time.After(time.Second):
		t.Fatal("start did not reach the endpoint")
	}
	agent.flush(time.Second)
	assert.False(t, agent.isActive())
	assert.Empty(t, client.entered, "finish must not be sent after the start times out")
}

func TestTelemetryBudgetBoundsSourceScopes(t *testing.T) {
	tracker := newSourceShapeTracker(common.ELocation.Blob(), common.ESymlinkHandlingType.Skip(), common.EHardlinkHandlingType.Follow())
	tracker.accountScope = true
	for index := 0; index < 10000; index++ {
		object := traverser.StoredObject{ContainerName: fmt.Sprint(index), EntityType: common.EEntityType.File(), Size: 10}
		require.NoError(t, tracker.recordScanned(object))
		tracker.recordScheduled(object)
	}
	assert.Len(t, tracker.scannedScope, maxTrackedSourceScopes)
	assert.Len(t, tracker.touchedScope, maxTrackedSourceScopes)
	summary := tracker.snapshot()
	assert.EqualValues(t, -1, summary.ContainersScanned)
	assert.EqualValues(t, -1, summary.ContainersTouched)
	assert.EqualValues(t, 10000, summary.ObjectsScanned)
	assert.EqualValues(t, 100000, summary.BytesScanned)
	tracker = newSourceShapeTracker(common.ELocation.S3(), common.ESymlinkHandlingType.Skip(), common.EHardlinkHandlingType.Follow())
	tracker.addScannedScope(strings.Repeat("x", maxTrackedSourceScopeBytes+1))
	assert.Empty(t, tracker.scannedScope)
	assert.EqualValues(t, -1, tracker.snapshot().BucketsScanned)
}
