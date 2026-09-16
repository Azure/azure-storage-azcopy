package telemetry

import (
	"context"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestTelemetrySerializedEnvelopeBudgets(t *testing.T) {
	for _, backend := range []Backend{BackendAppInsights, BackendOTel} {
		for _, test := range []struct {
			name   string
			event  MetricEvent
			budget int
		}{
			{"started", sampleStarted(), 4 * 1024},
			{"finished", sampleFinished(), 8 * 1024},
		} {
			t.Run(string(backend)+"/"+test.name, func(t *testing.T) {
				client := &stubClient{status: http.StatusOK}
				reporter := NewReporter(Config{Backend: backend, ConnectionString: testConnString, HTTPClient: client})
				require.NoError(t, reporter.ReportEvent(context.Background(), test.event))
				assert.LessOrEqual(t, len(client.lastBody), test.budget)
			})
		}
	}
}

func TestTelemetryUnavailableMeasurementsAreOmitted(t *testing.T) {
	for _, backend := range []Backend{BackendAppInsights, BackendOTel} {
		t.Run(string(backend), func(t *testing.T) {
			event := sampleFinished()
			event.Dimensions.SummaryCounterScope = "job-cumulative"
			event.ContainersScanned = -1
			event.ContainersTouched = -1
			metrics := event.measurements()
			for _, metric := range metrics {
				assert.NotEqual(t, "azcopy.job_throughput_mbps", metric.Name)
				assert.NotEqual(t, "azcopy.transfer_phase_throughput_mbps", metric.Name)
				assert.NotEqual(t, "azcopy.containers_scanned", metric.Name)
				assert.NotEqual(t, "azcopy.containers_touched", metric.Name)
			}
			assert.Equal(t, "unavailable-cumulative-summary", event.attributes()["ThroughputStatus"])
			assert.Equal(t, "incomplete", event.attributes()["SourceScopeCountsStatus"])
			client := &stubClient{status: http.StatusOK}
			reporter := NewReporter(Config{Backend: backend, ConnectionString: testConnString, HTTPClient: client})
			require.NoError(t, reporter.ReportEvent(context.Background(), event))
			assert.NotContains(t, string(client.lastBody), `"azcopy.job_throughput_mbps"`)
			assert.NotContains(t, string(client.lastBody), `"azcopy.containers_scanned"`)
			assert.Contains(t, string(client.lastBody), `"azcopy.bytes_transferred"`)
		})
	}
}

func TestSourceEndpointKindWireContract(t *testing.T) {
	for _, backend := range []Backend{BackendAppInsights, BackendOTel} {
		for _, kind := range []string{"", "public", "private-endpoint"} {
			t.Run(string(backend)+"/"+kind, func(t *testing.T) {
				started := sampleStarted()
				started.Dimensions.SourceEndpointKind = kind
				started.Dimensions.DestEndpointKind = "public"
				finished := sampleFinished()
				finished.Dimensions = started.Dimensions
				for _, event := range []MetricEvent{started, finished} {
					assert.Equal(t, kind, event.attributes()["SourceEndpointKind"])
					assert.Equal(t, "public", event.attributes()["DestEndpointKind"])
					client := &stubClient{status: http.StatusOK}
					reporter := NewReporter(Config{Backend: backend, ConnectionString: testConnString, HTTPClient: client})
					require.NoError(t, reporter.ReportEvent(context.Background(), event))
					assert.Contains(t, string(client.lastBody), `"SourceEndpointKind"`)
					assert.Contains(t, string(client.lastBody), `"DestEndpointKind"`)
					assert.Contains(t, string(client.lastBody), `"SchemaVersion"`)
				}
			})
		}
	}
}
