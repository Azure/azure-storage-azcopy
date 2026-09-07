package telemetry

import (
	"context"
	"errors"
	"io"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestTelemetryFailureDiagnosticsExcludeCanaries(t *testing.T) {
	const canary = "private-response-path-token-canary"
	for _, test := range []struct {
		name       string
		client     *stubClient
		connection string
	}{
		{"rejection", &stubClient{status: 400, respBody: canary}, testConnString},
		{"partial", &stubClient{status: 206, respBody: `{"itemsReceived":1,"itemsAccepted":0,"errors":[{"index":0,"statusCode":400,"message":"` + canary + `"}]}`}, testConnString},
		{"malformed partial", &stubClient{status: 206, respBody: canary}, testConnString},
		{"transport", &stubClient{err: errors.New(canary)}, testConnString},
		{"config endpoint", &stubClient{}, "InstrumentationKey=test;IngestionEndpoint=https://example.test/?sig=" + canary},
		{"config auth", &stubClient{}, testConnString + ";Authorization=" + canary},
	} {
		t.Run(test.name, func(t *testing.T) {
			reporter := NewReporter(Config{Backend: BackendAppInsights, ConnectionString: test.connection, HTTPClient: test.client})
			err := reporter.ReportEvent(context.Background(), sampleFinished())
			require.Error(t, err)
			assert.NotContains(t, err.Error(), canary)
			assert.Less(t, len(err.Error()), 256)
			assert.NotContains(t, string(test.client.lastBody), canary)
		})
	}
}

type unreadableResponseBody struct {
	reads  int
	closed bool
}

func (b *unreadableResponseBody) Read(buffer []byte) (int, error) {
	b.reads++
	return 0, errors.New("private-reader-canary")
}
func (b *unreadableResponseBody) Close() error { b.closed = true; return nil }

type responseBodyClient struct{ body io.ReadCloser }

func (c responseBodyClient) Do(*http.Request) (*http.Response, error) {
	return &http.Response{StatusCode: 503, Body: c.body}, nil
}

func TestTelemetryRejectionDoesNotReadResponseBody(t *testing.T) {
	body := &unreadableResponseBody{}
	reporter := NewReporter(Config{Backend: BackendAppInsights, ConnectionString: testConnString, HTTPClient: responseBodyClient{body}})
	err := reporter.ReportEvent(context.Background(), sampleStarted())
	assert.ErrorContains(t, err, "503")
	assert.Zero(t, body.reads)
	assert.True(t, body.closed)
}
