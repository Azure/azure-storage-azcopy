package telemetry

import (
	"context"
	"encoding/json"
	"net/http"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestTelemetrySerializedEnvelopeBudgets(t *testing.T) {
	for _, test := range []struct {
		name   string
		event  MetricEvent
		budget int
	}{
		{"started", sampleStarted(), 4 * 1024},
		{"finished", sampleFinished(), 8 * 1024},
	} {
		t.Run(test.name, func(t *testing.T) {
			client := &stubClient{status: http.StatusOK}
			reporter := NewReporter(Config{ConnectionString: testConnString, HTTPClient: client})
			require.NoError(t, reporter.ReportEvent(context.Background(), test.event))
			assert.LessOrEqual(t, len(client.lastBody), test.budget)
		})
	}
}

func TestFinishedMeasurementsStayFlatOnWire(t *testing.T) {
	event := sampleFinished()
	expected := map[string]float64{}
	for _, metric := range event.measurements() {
		expected[metric.Name] = metric.Value
	}
	require.Len(t, expected, 49)
	client := &stubClient{status: http.StatusOK}
	reporter := NewReporter(Config{ConnectionString: testConnString, HTTPClient: client})
	require.NoError(t, reporter.ReportEvent(context.Background(), event))
	var envelopes []appInsightsEnvelope
	require.NoError(t, json.Unmarshal(client.lastBody, &envelopes))
	require.Len(t, envelopes, 1)
	data := envelopes[0].Data.BaseData
	assert.Equal(t, "azcopy.job.finished", data.Name)
	assert.Equal(t, expected, data.Measurements)
	assert.Equal(t, event.properties(), data.Properties)
}

func TestEventsDoNotNeedOccurrenceMeasurements(t *testing.T) {
	started := sampleStarted()
	command := CommandInvokedEvent{Resource: started.Resource, Command: "jobs.list", JobID: "command-job", InvocationID: "command-attempt", Timestamp: started.Timestamp}
	for _, event := range []MetricEvent{started, command, sampleFinished()} {
		t.Run(event.EventName(), func(t *testing.T) {
			client := &stubClient{status: http.StatusOK}
			reporter := NewReporter(Config{ConnectionString: testConnString, HTTPClient: client})
			require.NoError(t, reporter.ReportEvent(context.Background(), event))
			require.Equal(t, 1, client.calls)
			var envelopes []appInsightsEnvelope
			require.NoError(t, json.Unmarshal(client.lastBody, &envelopes))
			require.Len(t, envelopes, 1)
			data := envelopes[0].Data.BaseData
			assert.Equal(t, event.EventName(), data.Name)
			assert.Equal(t, event.timestamp().UTC().Format(time.RFC3339), envelopes[0].Time)
			assert.Equal(t, event.properties(), data.Properties)
			for _, name := range []string{"azcopy.job.started", "azcopy.job.finished", "azcopy.command.invoked"} {
				assert.NotContains(t, data.Measurements, name)
			}
			if event.EventName() == "azcopy.job.finished" {
				assert.Len(t, data.Measurements, 49)
			} else {
				assert.Empty(t, data.Measurements)
				assert.NotContains(t, string(client.lastBody), `"measurements"`)
			}
		})
	}
}

func TestTelemetryUnavailableMeasurementsAreOmitted(t *testing.T) {
	event := sampleFinished()
	event.Dimensions.SummaryCounterScope = "job-cumulative"
	event.Measurements.ContainersScanned = -1
	event.Measurements.ContainersTouched = -1
	metrics := event.measurements()
	for _, metric := range metrics {
		assert.NotEqual(t, "azcopy.job_throughput_mbps", metric.Name)
		assert.NotEqual(t, "azcopy.transfer_phase_throughput_mbps", metric.Name)
		assert.NotEqual(t, "azcopy.containers_scanned", metric.Name)
		assert.NotEqual(t, "azcopy.containers_touched", metric.Name)
	}
	assert.Equal(t, "unavailable-cumulative-summary", event.properties()["ThroughputStatus"])
	assert.Equal(t, "incomplete", event.properties()["SourceScopeCountsStatus"])
	client := &stubClient{status: http.StatusOK}
	reporter := NewReporter(Config{ConnectionString: testConnString, HTTPClient: client})
	require.NoError(t, reporter.ReportEvent(context.Background(), event))
	assert.NotContains(t, string(client.lastBody), `"azcopy.job_throughput_mbps"`)
	assert.NotContains(t, string(client.lastBody), `"azcopy.containers_scanned"`)
	assert.Contains(t, string(client.lastBody), `"azcopy.bytes_transferred"`)
}

func TestSourceEndpointKindWireContract(t *testing.T) {
	for _, kind := range []string{"", "public", "private-endpoint"} {
		t.Run(kind, func(t *testing.T) {
			started := sampleStarted()
			started.Dimensions.SourceEndpointKind = kind
			started.Dimensions.DestEndpointKind = "public"
			finished := sampleFinished()
			finished.Dimensions = started.Dimensions
			for _, event := range []MetricEvent{started, finished} {
				assert.Equal(t, kind, event.properties()["SourceEndpointKind"])
				assert.Equal(t, "public", event.properties()["DestEndpointKind"])
				client := &stubClient{status: http.StatusOK}
				reporter := NewReporter(Config{ConnectionString: testConnString, HTTPClient: client})
				require.NoError(t, reporter.ReportEvent(context.Background(), event))
				assert.Contains(t, string(client.lastBody), `"SourceEndpointKind"`)
				assert.Contains(t, string(client.lastBody), `"DestEndpointKind"`)
				assert.Contains(t, string(client.lastBody), `"SchemaVersion"`)
			}
		})
	}
}
