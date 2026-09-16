package telemetry

import (
	"context"
	"errors"
	"fmt"
	"math"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestDeliveryFailureClassification(t *testing.T) {
	for _, backend := range []Backend{BackendAppInsights, BackendOTel} {
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
			t.Run(string(backend)+"/"+test.name, func(t *testing.T) {
				reporter := NewReporter(Config{Backend: backend, ConnectionString: test.connection, HTTPClient: test.client})
				err := reporter.ReportEvent(context.Background(), sampleStarted())
				require.Error(t, err)
				assert.Equal(t, test.wantDelivery, IsDeliveryFailure(err))
				assert.Equal(t, test.wantDelivery, IsDeliveryFailure(fmt.Errorf("wrapped: %w", err)))
				assert.NotContains(t, err.Error(), "canary")
			})
		}
	}
	assert.False(t, IsDeliveryFailure(nil))
	assert.False(t, IsDeliveryFailure(errors.New("send metrics: transport failure")), "classification must not depend on message text")
	event := sampleFinished()
	event.JobThroughputMbps = math.NaN()
	client := &stubClient{status: http.StatusOK}
	reporter := NewReporter(Config{Backend: BackendAppInsights, ConnectionString: testConnString, HTTPClient: client})
	err := reporter.ReportEvent(context.Background(), event)
	require.Error(t, err)
	assert.False(t, IsDeliveryFailure(err))
	assert.Zero(t, client.calls)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	_, err = postEnvelopes(ctx, &stubClient{err: context.Canceled}, "https://example.test", nil)
	assert.True(t, IsDeliveryFailure(err))
	assert.ErrorIs(t, err, context.Canceled)
}
