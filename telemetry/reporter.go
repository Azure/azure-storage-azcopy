// Copyright © Microsoft <wastore@microsoft.com>
//
// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files (the "Software"), to deal
// in the Software without restriction, including without limitation the rights
// to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
// copies of the Software, and to permit persons to whom the Software is
// furnished to do so, subject to the following conditions:
//
// The above copyright notice and this permission notice shall be included in
// all copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
// THE SOFTWARE.

package telemetry

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"strings"
	"time"

	"go.opentelemetry.io/otel/attribute"
	otelmetric "go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
	"go.opentelemetry.io/otel/sdk/resource"
	semconv "go.opentelemetry.io/otel/semconv/v1.26.0"
)

// httpDoer is the subset of *http.Client used to send telemetry. It is an
// interface so tests can inject a stub transport instead of making real calls.
type httpDoer interface {
	Do(req *http.Request) (*http.Response, error)
}

// Backend selects which telemetry backend to use.
type Backend string

const (
	BackendOTel        Backend = "otel"        // OpenTelemetry SDK with App Insights exporter
	BackendAppInsights Backend = "appinsights" // Direct App Insights Track API
)

// Config holds the configuration for telemetry reporting.
type Config struct {
	// Backend selects otel or appinsights.
	Backend Backend

	// ConnectionString is the Application Insights connection string.
	// Required when Backend == BackendOTel or BackendAppInsights.
	ConnectionString string

	// HTTPClient is the client used to POST telemetry. Optional; when nil the
	// reporter falls back to http.DefaultClient. Injecting a client makes the
	// reporter fully unit-testable.
	HTTPClient httpDoer
}

// Reporter sends telemetry events to Azure Monitor.
type Reporter struct {
	cfg Config
}

// NewReporter creates a Reporter from the given config.
func NewReporter(cfg Config) *Reporter {
	return &Reporter{cfg: cfg}
}

// httpClient returns the configured HTTP client, defaulting to
// http.DefaultClient when none was injected.
func (r *Reporter) httpClient() httpDoer {
	if r.cfg.HTTPClient != nil {
		return r.cfg.HTTPClient
	}
	return http.DefaultClient
}

// ---------------------------------------------------------------------------
// Shared helpers
// ---------------------------------------------------------------------------

// parseConnectionString extracts key-value pairs from an Application Insights
// connection string (e.g. "InstrumentationKey=abc;IngestionEndpoint=https://...").
func parseConnectionString(cs string) map[string]string {
	m := make(map[string]string)
	for _, part := range strings.Split(cs, ";") {
		kv := strings.SplitN(part, "=", 2)
		if len(kv) == 2 {
			m[strings.TrimSpace(kv[0])] = strings.TrimSpace(kv[1])
		}
	}
	return m
}

// endpointAndKey parses the configured connection string and returns the
// ingestion endpoint (no trailing slash) and instrumentation key.
func (r *Reporter) endpointAndKey() (endpoint, ikey string, err error) {
	parts := parseConnectionString(r.cfg.ConnectionString)
	endpoint = strings.TrimRight(parts["IngestionEndpoint"], "/")
	ikey = parts["InstrumentationKey"]
	if endpoint == "" || ikey == "" {
		return "", "", fmt.Errorf("connection string must contain IngestionEndpoint and InstrumentationKey")
	}
	return endpoint, ikey, nil
}

// ---------------------------------------------------------------------------
// OTel backend – real OpenTelemetry SDK with a custom App Insights exporter
// ---------------------------------------------------------------------------

// sendEventOTel records a MetricEvent's measurements through the OpenTelemetry
// SDK pipeline (meter -> instruments -> manual reader -> App Insights exporter)
// instead of hand-building envelopes. This is the "real" OTel path: the same
// instrumentation could target any OTel-compatible backend (OTLP collector,
// Prometheus, another vendor) simply by swapping the exporter, without touching
// the calling code.
func (r *Reporter) sendEventOTel(ctx context.Context, evt MetricEvent) error {
	endpoint, ikey, err := r.endpointAndKey()
	if err != nil {
		return err
	}

	exporter := &appInsightsExporter{
		endpoint:  endpoint,
		ikey:      ikey,
		client:    r.httpClient(),
		eventName: evt.EventName(),
		timestamp: evt.timestamp(),
	}

	res, err := resource.New(ctx,
		resource.WithAttributes(
			semconv.ServiceNameKey.String("azcopy"),
		),
	)
	if err != nil {
		return fmt.Errorf("create resource: %w", err)
	}

	reader := metric.NewManualReader()
	provider := metric.NewMeterProvider(
		metric.WithResource(res),
		metric.WithReader(reader),
	)
	defer func() {
		if err := provider.Shutdown(ctx); err != nil {
			log.Printf("telemetry: otel shutdown error: %v", err)
		}
	}()

	meter := provider.Meter("azcopy")

	// The event's flattened resource + dimension properties become OTel
	// attributes carried on every measurement.
	attrs := propsToAttributes(evt.attributes())

	// Each numeric measurement becomes its own counter instrument, recorded
	// with the shared attribute set. The manual reader then collects them for
	// a single export pass.
	for _, m := range evt.measurements() {
		counter, err := meter.Float64Counter(m.Name)
		if err != nil {
			return fmt.Errorf("create instrument %s: %w", m.Name, err)
		}
		counter.Add(ctx, m.Value, otelmetric.WithAttributes(attrs...))
	}

	var rm metricdata.ResourceMetrics
	if err := reader.Collect(ctx, &rm); err != nil {
		return fmt.Errorf("collect metrics: %w", err)
	}
	if err := exporter.Export(ctx, &rm); err != nil {
		return fmt.Errorf("export metrics: %w", err)
	}

	log.Printf("telemetry: sent packed %s event to App Insights via OTel SDK", evt.EventName())
	return nil
}

// propsToAttributes converts a flat string property map into OTel attributes.
func propsToAttributes(props map[string]string) []attribute.KeyValue {
	attrs := make([]attribute.KeyValue, 0, len(props))
	for k, v := range props {
		attrs = append(attrs, attribute.String(k, v))
	}
	return attrs
}

// appInsightsExporter implements a lightweight OTel metric exporter
// that sends data to the Application Insights /v2.1/track endpoint.
type appInsightsExporter struct {
	endpoint  string
	ikey      string
	client    httpDoer
	eventName string
	timestamp time.Time
}

func (e *appInsightsExporter) Export(ctx context.Context, rm *metricdata.ResourceMetrics) error {
	// Collect every data point before sending them in one HTTP batch.
	var points []metricPoint
	for _, sm := range rm.ScopeMetrics {
		for _, m := range sm.Metrics {
			points = append(points, metricToPoints(m)...)
		}
	}

	envelopes := e.pointsToEnvelopes(points)
	if len(envelopes) == 0 {
		return nil
	}

	client := e.client
	if client == nil {
		client = http.DefaultClient
	}

	resp, err := postEnvelopes(ctx, client, e.endpoint, envelopes)
	if err != nil {
		return err
	}

	log.Printf("telemetry: otel export to App Insights (HTTP %d, %d envelopes)", resp, len(envelopes))
	return nil
}

// metricPoint is a single data point extracted from OTel metric data, carrying
// the attribute set it was recorded with.
type metricPoint struct {
	name  string
	value float64
	props map[string]string
}

func metricToPoints(m metricdata.Metrics) []metricPoint {
	var points []metricPoint
	add := func(value float64, count int, attrs attribute.Set) {
		points = append(points, metricPoint{
			name:  m.Name,
			value: value,
			props: attrsToProps(attrs),
		})
	}

	switch data := m.Data.(type) {
	case metricdata.Sum[int64]:
		for _, dp := range data.DataPoints {
			add(float64(dp.Value), 1, dp.Attributes)
		}
	case metricdata.Sum[float64]:
		for _, dp := range data.DataPoints {
			add(dp.Value, 1, dp.Attributes)
		}
	case metricdata.Gauge[int64]:
		for _, dp := range data.DataPoints {
			add(float64(dp.Value), 1, dp.Attributes)
		}
	case metricdata.Gauge[float64]:
		for _, dp := range data.DataPoints {
			add(dp.Value, 1, dp.Attributes)
		}
	case metricdata.Histogram[int64]:
		for _, dp := range data.DataPoints {
			add(float64(dp.Sum), int(dp.Count), dp.Attributes)
		}
	case metricdata.Histogram[float64]:
		for _, dp := range data.DataPoints {
			add(dp.Sum, int(dp.Count), dp.Attributes)
		}
	}

	return points
}

// pointsToEnvelopes packs all data points from one MetricEvent into one custom
// event row. All points recorded by sendEventOTel carry the same properties.
func (e *appInsightsExporter) pointsToEnvelopes(points []metricPoint) []appInsightsEnvelope {
	if len(points) == 0 {
		return nil
	}

	measurements := make(map[string]float64, len(points))
	for _, point := range points {
		measurements[point.name] = point.value
	}

	return []appInsightsEnvelope{{
		Name: "Microsoft.ApplicationInsights.Event",
		Time: e.timestamp.UTC().Format(time.RFC3339),
		IKey: e.ikey,
		Data: appInsightsData{
			BaseType: "EventData",
			BaseData: appInsightsEventData{
				Version:      2,
				Name:         e.eventName,
				Properties:   points[0].props,
				Measurements: measurements,
			},
		},
	}}
}

func attrsToProps(attrs attribute.Set) map[string]string {
	props := make(map[string]string)
	iter := attrs.Iter()
	for iter.Next() {
		kv := iter.Attribute()
		props[string(kv.Key)] = kv.Value.Emit()
	}
	return props
}

// ---------------------------------------------------------------------------
// Shared App Insights /v2.1/track envelope types (used by both backends)
// ---------------------------------------------------------------------------

// appInsightsEnvelope is the telemetry envelope for the /v2.1/track API.
type appInsightsEnvelope struct {
	Name string          `json:"name"`
	Time string          `json:"time"`
	IKey string          `json:"iKey"`
	Data appInsightsData `json:"data"`
}

type appInsightsData struct {
	BaseType string               `json:"baseType"`
	BaseData appInsightsEventData `json:"baseData"`
}

type appInsightsEventData struct {
	Version      int                `json:"ver"`
	Name         string             `json:"name"`
	Properties   map[string]string  `json:"properties,omitempty"`
	Measurements map[string]float64 `json:"measurements,omitempty"`
}

// postEnvelopes sends a batch of telemetry envelopes to the App Insights
// /v2.1/track ingestion endpoint using the given client. It returns the HTTP
// status code on success.
func postEnvelopes(ctx context.Context, client httpDoer, endpoint string, envelopes []appInsightsEnvelope) (int, error) {
	body, err := json.Marshal(envelopes)
	if err != nil {
		return 0, fmt.Errorf("marshal envelopes: %w", err)
	}

	url := endpoint + "/v2.1/track"
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, url, bytes.NewReader(body))
	if err != nil {
		return 0, fmt.Errorf("create request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err := client.Do(req)
	if err != nil {
		return 0, fmt.Errorf("send metrics: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode >= 300 {
		respBody, _ := io.ReadAll(resp.Body)
		return resp.StatusCode, fmt.Errorf("app insights returned %d: %s", resp.StatusCode, string(respBody))
	}
	return resp.StatusCode, nil
}
