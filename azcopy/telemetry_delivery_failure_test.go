package azcopy

import (
	"crypto/tls"
	"encoding/json"
	"errors"
	"github.com/Azure/azure-storage-azcopy/v10/telemetry"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

type telemetryFailureTransport func(*http.Request) (*http.Response, error)

func (transport telemetryFailureTransport) RoundTrip(request *http.Request) (*http.Response, error) {
	return transport(request)
}

func TestTelemetryTransportFailuresStopPipeline(t *testing.T) {
	for _, test := range []struct {
		name    string
		failure error
	}{
		{"dns", &net.DNSError{Err: "private-canary", Name: "private-host"}},
		{"tls", &tls.CertificateVerificationError{Err: errors.New("private-canary")}},
		{"offline", &net.OpError{Op: "dial", Net: "tcp", Err: errors.New("private-canary")}},
	} {
		t.Run(test.name, func(t *testing.T) {
			var calls atomic.Int64
			client := &http.Client{Transport: telemetryFailureTransport(func(*http.Request) (*http.Response, error) { calls.Add(1); return nil, test.failure })}
			agent := &telemetryAgent{enabled: true, reporter: telemetry.NewReporter(telemetry.Config{Backend: telemetry.BackendAppInsights, ConnectionString: "InstrumentationKey=test;IngestionEndpoint=https://example.test", HTTPClient: client})}
			agent.reportStarted(telemetry.JobDimensions{}, "job", "attempt", time.Now())
			agent.reportFinished(telemetry.JobFinishedEvent{JobID: "job", InvocationID: "attempt"})
			agent.flush(time.Second)
			agent.reportCommand("list", "next-job", "next-attempt", telemetry.OptionAttributes{})
			agent.flush(time.Second)
			assert.False(t, agent.isActive())
			assert.EqualValues(t, 1, calls.Load())
		})
	}
}

func TestTelemetryMockEndpointRejectionAndThrottling(t *testing.T) {
	for _, test := range []struct {
		name   string
		status int
		body   string
	}{
		{"case3/rejection", 400, "private-response-canary"},
		{"case3/partial-acceptance", 206, `{"itemsReceived":1,"itemsAccepted":0,"errors":[{"index":0,"statusCode":400,"message":"private-response-canary"}]}`},
		{"case4/throttle429", 429, "private-response-canary"},
		{"case4/throttle503", 503, "private-response-canary"},
	} {
		t.Run(test.name, func(t *testing.T) {
			var requests atomic.Int64
			entered := make(chan struct{})
			release := make(chan struct{})
			var releaseOnce sync.Once
			unblock := func() { releaseOnce.Do(func() { close(release) }) }
			server := httptest.NewServer(http.HandlerFunc(func(response http.ResponseWriter, request *http.Request) {
				_, _ = io.Copy(io.Discard, request.Body)
				if requests.Add(1) == 1 {
					close(entered)
					select {
					case <-release:
					case <-request.Context().Done():
						return
					}
					response.Header().Set("Retry-After", "1")
					response.WriteHeader(test.status)
					_, _ = io.WriteString(response, test.body)
					return
				}
				response.WriteHeader(http.StatusOK)
			}))
			t.Cleanup(server.Close)
			t.Cleanup(unblock)
			agent := &telemetryAgent{enabled: true, reporter: telemetry.NewReporter(telemetry.Config{Backend: telemetry.BackendAppInsights, ConnectionString: "InstrumentationKey=test;IngestionEndpoint=" + server.URL, HTTPClient: server.Client()})}
			agent.reportStarted(telemetry.JobDimensions{}, "job", "attempt", time.Now())
			select {
			case <-entered:
			case <-time.After(time.Second):
				t.Fatal("start did not reach local endpoint")
			}
			agent.reportFinished(telemetry.JobFinishedEvent{JobID: "job", InvocationID: "attempt"})
			unblock()
			agent.flush(time.Second)
			require.False(t, agent.isActive())
			agent.reportStarted(telemetry.JobDimensions{}, "next-job", "next-attempt", time.Now())
			agent.reportCommand("list", "command-job", "command-attempt", telemetry.OptionAttributes{})
			agent.flush(time.Second)
			assert.EqualValues(t, 1, requests.Load(), "queued finish and future events must stay suppressed even if endpoint recovers")
		})
	}
}

func TestTelemetryDeliveryFailureCancelsActiveRequests(t *testing.T) {
	entered := make(chan struct{})
	cancelled := make(chan struct{})
	release := make(chan struct{})
	var requests atomic.Int64
	server := httptest.NewServer(http.HandlerFunc(func(response http.ResponseWriter, request *http.Request) {
		requests.Add(1)
		var envelopes []struct {
			Data struct {
				BaseData struct{ Properties map[string]string }
			}
		}
		if err := json.NewDecoder(request.Body).Decode(&envelopes); err != nil || len(envelopes) != 1 {
			response.WriteHeader(400)
			return
		}
		if envelopes[0].Data.BaseData.Properties["JobID"] == "blocked-job" {
			close(entered)
			select {
			case <-request.Context().Done():
				close(cancelled)
			case <-release:
			}
			return
		}
		response.WriteHeader(429)
	}))
	t.Cleanup(server.Close)
	t.Cleanup(func() { close(release) })
	agent := &telemetryAgent{enabled: true, sendTimeout: time.Minute, reporter: telemetry.NewReporter(telemetry.Config{Backend: telemetry.BackendAppInsights, ConnectionString: "InstrumentationKey=test;IngestionEndpoint=" + server.URL, HTTPClient: server.Client()})}
	agent.reportCommand("list", "blocked-job", "blocked-attempt", telemetry.OptionAttributes{})
	select {
	case <-entered:
	case <-time.After(time.Second):
		t.Fatal("first request was not started")
	}
	agent.reportCommand("list", "rejected-job", "rejected-attempt", telemetry.OptionAttributes{})
	select {
	case <-cancelled:
	case <-time.After(2 * time.Second):
		t.Fatal("active request was not cancelled by delivery failure")
	}
	agent.flush(time.Second)
	assert.False(t, agent.isActive())
	agent.dispatchMu.Lock()
	assert.Empty(t, agent.pending)
	agent.dispatchMu.Unlock()
	agent.reportCommand("list", "later-job", "later-attempt", telemetry.OptionAttributes{})
	agent.flush(time.Second)
	assert.EqualValues(t, 2, requests.Load())
}
