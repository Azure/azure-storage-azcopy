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

// This file configures Application Insights reporting and handles HTTP delivery,
// including connection validation, response checks, and sanitized delivery errors.

package telemetry

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"time"
)

const defaultRequestTimeout = 5 * time.Second

// httpDoer is the subset of *http.Client used to send telemetry. It is an
// interface so tests can inject a stub transport instead of making real calls.
type httpDoer interface {
	Do(req *http.Request) (*http.Response, error)
}

// Config holds the configuration for telemetry reporting.
type Config struct {
	// ConnectionString is the Application Insights connection string.
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
		key, value, found := strings.Cut(part, "=")
		if found {
			m[strings.ToLower(strings.TrimSpace(key))] = strings.TrimSpace(value)
		}
	}
	return m
}

// endpointAndKey parses the configured connection string and returns the
// ingestion endpoint (no trailing slash) and instrumentation key.
func (r *Reporter) endpointAndKey() (endpoint, ikey string, err error) {
	parts := parseConnectionString(r.cfg.ConnectionString)
	ikey = parts["instrumentationkey"]
	if ikey == "" {
		return "", "", fmt.Errorf("connection string must contain InstrumentationKey")
	}
	if authorization := parts["authorization"]; authorization != "" && !strings.EqualFold(authorization, "ikey") {
		return "", "", errors.New("unsupported Application Insights authorization")
	}

	endpoint = parts["ingestionendpoint"]
	if endpoint == "" {
		suffix := parts["endpointsuffix"]
		if suffix == "" {
			return "", "", fmt.Errorf("connection string must contain IngestionEndpoint or EndpointSuffix")
		}
		if strings.ContainsAny(suffix, "/:@?#") {
			return "", "", errors.New("invalid Application Insights EndpointSuffix")
		}
		locationPrefix := ""
		if location := parts["location"]; location != "" {
			if strings.ContainsAny(location, "/.:@?#") {
				return "", "", errors.New("invalid Application Insights Location")
			}
			locationPrefix = location + "."
		}
		endpoint = "https://" + locationPrefix + "dc." + suffix
	}

	endpoint = strings.TrimRight(endpoint, "/")
	parsedEndpoint, parseErr := url.Parse(endpoint)
	if parseErr != nil || parsedEndpoint.Host == "" ||
		(parsedEndpoint.Scheme != "http" && parsedEndpoint.Scheme != "https") ||
		parsedEndpoint.User != nil || parsedEndpoint.RawQuery != "" || parsedEndpoint.Fragment != "" {
		return "", "", errors.New("invalid Application Insights ingestion endpoint")
	}
	return endpoint, ikey, nil
}

// ---------------------------------------------------------------------------
// App Insights /v2.1/track envelope types
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

type deliveryFailure struct {
	error
}

func (failure *deliveryFailure) Unwrap() error {
	return failure.error
}

func IsDeliveryFailure(err error) bool {
	var failure *deliveryFailure
	return errors.As(err, &failure)
}

// postEnvelopes sends a batch of telemetry envelopes to the App Insights
// /v2.1/track ingestion endpoint using the given client. Requests without a
// caller deadline default to five seconds.
func postEnvelopes(ctx context.Context, client httpDoer, endpoint string, envelopes []appInsightsEnvelope) error {
	if _, hasDeadline := ctx.Deadline(); !hasDeadline {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, defaultRequestTimeout)
		defer cancel()
	}

	body, err := json.Marshal(envelopes)
	if err != nil {
		return errors.New("marshal envelopes: invalid telemetry payload")
	}

	url := endpoint + "/v2.1/track"
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, url, bytes.NewReader(body))
	if err != nil {
		return errors.New("create request: invalid telemetry endpoint")
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err := client.Do(req)
	if err != nil {
		if ctx.Err() != nil {
			return &deliveryFailure{fmt.Errorf("send metrics: %w", ctx.Err())}
		}
		return &deliveryFailure{errors.New("send metrics: transport failure")}
	}
	defer resp.Body.Close()

	if resp.StatusCode == http.StatusPartialContent {
		if err := validatePartialIngestionResponse(resp.Body); err != nil {
			return &deliveryFailure{err}
		}
	}
	if resp.StatusCode >= 300 {
		return &deliveryFailure{fmt.Errorf("app insights returned HTTP %d", resp.StatusCode)}
	}
	return nil
}

type partialIngestionResponse struct {
	ItemsReceived int                     `json:"itemsReceived"`
	ItemsAccepted int                     `json:"itemsAccepted"`
	Errors        []partialIngestionIssue `json:"errors"`
}

type partialIngestionIssue struct {
	Index      int `json:"index"`
	StatusCode int `json:"statusCode"`
}

func validatePartialIngestionResponse(body io.Reader) error {
	var result partialIngestionResponse
	decoder := json.NewDecoder(io.LimitReader(body, 64*1024))
	if err := decoder.Decode(&result); err != nil {
		return errors.New("app insights returned 206 with an invalid response")
	}
	if err := decoder.Decode(&struct{}{}); err != io.EOF {
		return errors.New("app insights returned 206 with an invalid response: trailing content")
	}
	if result.ItemsReceived <= 0 || result.ItemsAccepted < 0 || result.ItemsAccepted > result.ItemsReceived {
		return fmt.Errorf(
			"app insights returned 206 with invalid item counts (received=%d accepted=%d)",
			result.ItemsReceived,
			result.ItemsAccepted)
	}

	rejected := result.ItemsReceived - result.ItemsAccepted
	if rejected <= 0 || len(result.Errors) != rejected {
		return fmt.Errorf(
			"app insights returned 206 with inconsistent rejection details (received=%d accepted=%d errors=%d)",
			result.ItemsReceived,
			result.ItemsAccepted,
			len(result.Errors))
	}
	rejectedIndexes := make(map[int]struct{}, len(result.Errors))
	for _, issue := range result.Errors {
		if issue.Index < 0 || issue.Index >= result.ItemsReceived {
			return fmt.Errorf(
				"app insights returned 206 with an invalid rejected item index %d (received=%d)",
				issue.Index,
				result.ItemsReceived)
		}
		if _, exists := rejectedIndexes[issue.Index]; exists {
			return fmt.Errorf("app insights returned 206 with duplicate rejected item index %d", issue.Index)
		}
		rejectedIndexes[issue.Index] = struct{}{}
		if issue.StatusCode < http.StatusBadRequest || issue.StatusCode > 599 {
			return fmt.Errorf(
				"app insights returned 206 with invalid rejection status %d at item index %d",
				issue.StatusCode,
				issue.Index)
		}
	}

	first := result.Errors[0]
	return fmt.Errorf(
		"app insights partially accepted telemetry (received=%d accepted=%d rejected=%d; first rejection index=%d status=%d)",
		result.ItemsReceived,
		result.ItemsAccepted,
		rejected,
		first.Index,
		first.StatusCode)
}
