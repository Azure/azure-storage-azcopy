package telemetry

import (
	"context"
	"errors"
	"fmt"
	"io"
	"math"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestReporterRequestDeadline(t *testing.T) {
	for _, timeout := range []time.Duration{0, time.Second, time.Minute} {
		t.Run(timeout.String(), func(t *testing.T) {
			ctx := context.Background()
			if timeout > 0 {
				var cancel context.CancelFunc
				ctx, cancel = context.WithTimeout(ctx, timeout)
				defer cancel()
			}
			client := &stubClient{status: http.StatusOK}
			reporter := NewReporter(Config{ConnectionString: testConnString, HTTPClient: client})
			before := time.Now()
			require.NoError(t, reporter.ReportEvent(ctx, sampleStarted()))
			after := time.Now()
			deadline, ok := client.lastReq.Context().Deadline()
			require.True(t, ok, "every HTTP request must have a deadline")
			if timeout == 0 {
				assert.False(t, deadline.Before(before.Add(5*time.Second)))
				assert.False(t, deadline.After(after.Add(5*time.Second)))
				assert.ErrorIs(t, client.lastReq.Context().Err(), context.Canceled)
				assert.NoError(t, ctx.Err())
			} else {
				parentDeadline, _ := ctx.Deadline()
				assert.Equal(t, parentDeadline, deadline)
				assert.NoError(t, ctx.Err())
			}
		})
	}
}

func TestReporterStalledRequest(t *testing.T) {
	for _, name := range []string{"fallback", "caller deadline", "caller cancellation"} {
		t.Run(name, func(t *testing.T) {
			ctx := context.Background()
			var cancel context.CancelFunc
			if name == "caller deadline" {
				ctx, cancel = context.WithTimeout(ctx, time.Second)
				defer cancel()
			} else if name == "caller cancellation" {
				ctx, cancel = context.WithCancel(ctx)
				defer cancel()
			}
			var calls atomic.Int32
			release := make(chan struct{})
			server := httptest.NewServer(http.HandlerFunc(func(response http.ResponseWriter, request *http.Request) {
				calls.Add(1)
				_, _ = io.Copy(io.Discard, request.Body)
				if name == "caller cancellation" {
					cancel()
				}
				select {
				case <-request.Context().Done():
				case <-release:
				}
			}))
			t.Cleanup(server.Close)
			t.Cleanup(func() { close(release) })
			reporter := NewReporter(Config{ConnectionString: "InstrumentationKey=test;IngestionEndpoint=" + server.URL})
			err := reporter.ReportEvent(ctx, sampleStarted())
			require.Error(t, err)
			assert.True(t, IsDeliveryFailure(err))
			assert.Equal(t, int32(1), calls.Load())
			assert.NotContains(t, err.Error(), server.URL)
			if name == "caller cancellation" {
				assert.ErrorIs(t, err, context.Canceled)
			} else {
				assert.ErrorIs(t, err, context.DeadlineExceeded)
			}
			if name == "fallback" {
				assert.NoError(t, ctx.Err())
			}
		})
	}
}

func TestReporterFallbackContextReleasedAfterFailure(t *testing.T) {
	client := &stubClient{status: http.StatusServiceUnavailable}
	reporter := NewReporter(Config{ConnectionString: testConnString, HTTPClient: client})
	require.Error(t, reporter.ReportEvent(context.Background(), sampleStarted()))
	assert.ErrorIs(t, client.lastReq.Context().Err(), context.Canceled)
}

func TestDeliveryFailureClassification(t *testing.T) {
	for _, test := range []struct {
		name         string
		client       *stubClient
		connection   string
		wantDelivery bool
	}{
		{"transport", &stubClient{err: errors.New("private-transport-canary")}, testConnString, true},
		{"rejection", &stubClient{status: 400, respBody: "private-response-canary"}, testConnString, true},
		{"server-error", &stubClient{status: 500}, testConnString, true},
		{"partial", &stubClient{status: 206, respBody: `{"itemsReceived":1,"itemsAccepted":0,"errors":[{"index":0,"statusCode":400,"message":"private-response-canary"}]}`}, testConnString, true},
		{"invalid-partial", &stubClient{status: 206, respBody: "private-response-canary"}, testConnString, true},
		{"throttle429", &stubClient{status: 429}, testConnString, true},
		{"throttle503", &stubClient{status: 503}, testConnString, true},
		{"local-configuration", &stubClient{}, "InstrumentationKey=test", false},
	} {
		t.Run(test.name, func(t *testing.T) {
			reporter := NewReporter(Config{ConnectionString: test.connection, HTTPClient: test.client})
			err := reporter.ReportEvent(context.Background(), sampleStarted())
			require.Error(t, err)
			assert.Equal(t, test.wantDelivery, IsDeliveryFailure(err))
			assert.Equal(t, test.wantDelivery, IsDeliveryFailure(fmt.Errorf("wrapped: %w", err)))
			assert.NotContains(t, err.Error(), "canary")
		})
	}
	assert.False(t, IsDeliveryFailure(nil))
	assert.False(t, IsDeliveryFailure(errors.New("send metrics: transport failure")), "classification must not depend on message text")
	event := sampleFinished()
	event.Measurements.JobThroughputMbps = math.NaN()
	client := &stubClient{status: http.StatusOK}
	reporter := NewReporter(Config{ConnectionString: testConnString, HTTPClient: client})
	err := reporter.ReportEvent(context.Background(), event)
	require.Error(t, err)
	assert.False(t, IsDeliveryFailure(err))
	assert.Zero(t, client.calls)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	err = postEnvelopes(ctx, &stubClient{err: context.Canceled}, "https://example.test", nil)
	assert.True(t, IsDeliveryFailure(err))
	assert.ErrorIs(t, err, context.Canceled)
}
