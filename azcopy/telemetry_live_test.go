//go:build telemetrylive

package azcopy

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"sync"
	"testing"
	"time"

	"github.com/Azure/azure-storage-azcopy/v10/telemetry"
	"github.com/stretchr/testify/require"
)

type liveTelemetryResponse struct {
	Status int
	Body   []byte
}

func (response liveTelemetryResponse) rejectsWith(status int) bool {
	if response.Status == status {
		return true
	}
	if response.Status != http.StatusPartialContent {
		return false
	}
	var result struct {
		ItemsReceived int `json:"itemsReceived"`
		ItemsAccepted int `json:"itemsAccepted"`
		Errors        []struct {
			StatusCode int `json:"statusCode"`
		} `json:"errors"`
	}
	if json.Unmarshal(response.Body, &result) != nil || result.ItemsReceived < 1 || result.ItemsAccepted != 0 || len(result.Errors) != result.ItemsReceived {
		return false
	}
	for _, failure := range result.Errors {
		if failure.StatusCode != status {
			return false
		}
	}
	return true
}

type liveTelemetryClient struct {
	client    *http.Client
	mu        sync.Mutex
	responses []liveTelemetryResponse
	bytesSent int64
}

func (client *liveTelemetryClient) Do(request *http.Request) (*http.Response, error) {
	client.mu.Lock()
	if request.ContentLength < 0 || client.bytesSent+request.ContentLength > 64*1024*1024 {
		client.mu.Unlock()
		return nil, fmt.Errorf("manual telemetry test reached its 64 MiB upload limit")
	}
	client.bytesSent += request.ContentLength
	client.mu.Unlock()
	response, err := client.client.Do(request)
	if err != nil {
		return nil, err
	}
	body, err := io.ReadAll(io.LimitReader(response.Body, 64*1024+1))
	response.Body.Close()
	if err != nil || len(body) > 64*1024 {
		return nil, fmt.Errorf("cannot capture bounded ingestion response")
	}
	client.mu.Lock()
	client.responses = append(client.responses, liveTelemetryResponse{Status: response.StatusCode, Body: body})
	client.mu.Unlock()
	response.Body = io.NopCloser(bytes.NewReader(body))
	return response, nil
}

func (client *liveTelemetryClient) snapshot() []liveTelemetryResponse {
	client.mu.Lock()
	defer client.mu.Unlock()
	return append([]liveTelemetryResponse(nil), client.responses...)
}

func liveTelemetryAgent(connection string, client *liveTelemetryClient) *telemetryAgent {
	return &telemetryAgent{
		enabled: true,
		reporter: telemetry.NewReporter(telemetry.Config{
			Backend: telemetry.BackendAppInsights, ConnectionString: connection, HTTPClient: client,
		}),
		resource: telemetry.ResourceAttributes{AzCopyVersion: "manual-live-test", SchemaVersion: "3", InstallationID: "manual-live-test"},
	}
}

func requireLiveTelemetryStopped(t *testing.T, agent *telemetryAgent, client *liveTelemetryClient) {
	t.Helper()
	require.False(t, agent.isActive(), "real ingestion rejection must stop the agent")
	before := len(client.snapshot())
	agent.reportStarted(telemetry.JobDimensions{Command: "copy"}, "suppressed", "suppressed", time.Now())
	agent.reportFinished(telemetry.JobFinishedEvent{JobID: "suppressed", InvocationID: "suppressed"})
	agent.reportCommand("list", "suppressed", "suppressed", telemetry.OptionAttributes{})
	collected := false
	agent.newAttempt(func() telemetry.JobDimensions {
		collected = true
		return telemetry.JobDimensions{}
	}, "suppressed", "suppressed", time.Now()).finish(nil)
	agent.flush(time.Second)
	require.False(t, collected, "disabled agent must not collect new dimensions")
	require.Len(t, client.snapshot(), before, "stopped agent must not issue another HTTP request")
}

func TestLiveTelemetryResponseClassification(t *testing.T) {
	for _, test := range []struct {
		name     string
		response liveTelemetryResponse
		status   int
		want     bool
	}{
		{"quota", liveTelemetryResponse{Status: 439}, 439, true},
		{"authentication", liveTelemetryResponse{Status: 401}, 401, true},
		{"throttling is not quota", liveTelemetryResponse{Status: 429}, 439, false},
		{"success is not rejection", liveTelemetryResponse{Status: 200}, 439, false},
		{"partial quota", liveTelemetryResponse{Status: 206, Body: []byte(`{"itemsReceived":1,"itemsAccepted":0,"errors":[{"statusCode":439}]}`)}, 439, true},
		{"malformed partial", liveTelemetryResponse{Status: 206, Body: []byte(`{}`)}, 439, false},
		{"accepted item", liveTelemetryResponse{Status: 206, Body: []byte(`{"itemsReceived":2,"itemsAccepted":1,"errors":[{"statusCode":439}]}`)}, 439, false},
	} {
		t.Run(test.name, func(t *testing.T) { require.Equal(t, test.want, test.response.rejectsWith(test.status)) })
	}
}

func liveTelemetrySend(t *testing.T, ctx context.Context, agent *telemetryAgent, client *liveTelemetryClient) liveTelemetryResponse {
	t.Helper()
	before := len(client.snapshot())
	agent.reportCommand("list", "manual-live-test", fmt.Sprint(time.Now().UnixNano()), telemetry.OptionAttributes{})
	agent.flush(2 * time.Second)
	responses := client.snapshot()
	require.Len(t, responses, before+1, "no HTTP response: timeout/transport failure is not a server rejection; context=%v", ctx.Err())
	response := responses[len(responses)-1]
	t.Logf("real Application Insights response: HTTP %d", response.Status)
	return response
}
