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
	"errors"
	"io"
	"net/http"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const testConnString = "InstrumentationKey=11111111-2222-3333-4444-555555555555;IngestionEndpoint=https://eastus.example.com/"

// stubClient is an httpDoer that records the last request it received and
// returns a canned response (or error), so we can assert on telemetry traffic
// without making real network calls.
type stubClient struct {
	lastReq  *http.Request
	lastBody []byte
	status   int
	respBody string
	err      error
	calls    int
}

func (c *stubClient) Do(req *http.Request) (*http.Response, error) {
	c.calls++
	c.lastReq = req
	if req.Body != nil {
		c.lastBody, _ = io.ReadAll(req.Body)
	}
	if c.err != nil {
		return nil, c.err
	}
	status := c.status
	if status == 0 {
		status = http.StatusOK
	}
	return &http.Response{
		StatusCode: status,
		Body:       io.NopCloser(bytes.NewReader([]byte(c.respBody))),
		Header:     make(http.Header),
	}, nil
}

func TestParseConnectionString(t *testing.T) {
	m := parseConnectionString(testConnString)
	assert.Equal(t, "11111111-2222-3333-4444-555555555555", m["instrumentationkey"])
	assert.Equal(t, "https://eastus.example.com/", m["ingestionendpoint"])

	// Keys are case-insensitive; whitespace and malformed segments are tolerated.
	m = parseConnectionString(" A = 1 ; bogus ; b=2")
	assert.Equal(t, "1", m["a"])
	assert.Equal(t, "2", m["b"])
	_, ok := m["bogus"]
	assert.False(t, ok)

	m = parseConnectionString("value=one=two;empty=")
	assert.Equal(t, map[string]string{"value": "one=two", "empty": ""}, m)
}

func TestEndpointAndKey(t *testing.T) {
	const ikey = "11111111-2222-3333-4444-555555555555"
	tests := []struct {
		name           string
		connection     string
		wantEndpoint   string
		wantErrContain string
	}{
		{
			name:         "explicit endpoint",
			connection:   testConnString,
			wantEndpoint: "https://eastus.example.com",
		},
		{
			name:         "explicit endpoint takes precedence",
			connection:   "InstrumentationKey=" + ikey + ";IngestionEndpoint=https://proxy.example.test/custom/;EndpointSuffix=ai.contoso.com;Location=westus2;Authorization=IKEY",
			wantEndpoint: "https://proxy.example.test/custom",
		},
		{
			name:         "case insensitive keys",
			connection:   " instrumentationkey=" + ikey + "; ingestionendpoint = https://example.test/ ",
			wantEndpoint: "https://example.test",
		},
		{
			name:         "endpoint suffix",
			connection:   "InstrumentationKey=" + ikey + ";EndpointSuffix=applicationinsights.azure.cn",
			wantEndpoint: "https://dc.applicationinsights.azure.cn",
		},
		{
			name:         "endpoint suffix with location",
			connection:   "InstrumentationKey=" + ikey + ";EndpointSuffix=ai.contoso.com;Location=westus2",
			wantEndpoint: "https://westus2.dc.ai.contoso.com",
		},
		{
			name:           "missing instrumentation key",
			connection:     "IngestionEndpoint=https://example.test",
			wantErrContain: "InstrumentationKey",
		},
		{
			name:           "missing endpoint configuration",
			connection:     "InstrumentationKey=" + ikey,
			wantErrContain: "IngestionEndpoint or EndpointSuffix",
		},
		{
			name:           "unsupported authorization",
			connection:     "InstrumentationKey=" + ikey + ";IngestionEndpoint=https://example.test;Authorization=AAD",
			wantErrContain: "unsupported",
		},
		{
			name:           "malformed endpoint",
			connection:     "InstrumentationKey=" + ikey + ";IngestionEndpoint=example.test",
			wantErrContain: "invalid",
		},
		{
			name:           "endpoint with query",
			connection:     "InstrumentationKey=" + ikey + ";IngestionEndpoint=https://example.test?route=ingestion",
			wantErrContain: "invalid",
		},
		{
			name:           "malformed endpoint suffix",
			connection:     "InstrumentationKey=" + ikey + ";EndpointSuffix=https://example.test",
			wantErrContain: "EndpointSuffix",
		},
		{
			name:           "malformed location",
			connection:     "InstrumentationKey=" + ikey + ";EndpointSuffix=ai.contoso.com;Location=west/us",
			wantErrContain: "Location",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			endpoint, gotKey, err := NewReporter(Config{ConnectionString: tt.connection}).endpointAndKey()
			if tt.wantErrContain != "" {
				require.Error(t, err)
				assert.Contains(t, err.Error(), tt.wantErrContain)
				return
			}
			require.NoError(t, err)
			assert.Equal(t, tt.wantEndpoint, endpoint)
			assert.Equal(t, ikey, gotKey)
		})
	}
}

func TestReportEventAppInsights(t *testing.T) {
	client := &stubClient{status: http.StatusOK}
	r := NewReporter(Config{
		ConnectionString: testConnString,
		HTTPClient:       client,
	})

	err := r.ReportEvent(context.Background(), sampleFinished())
	require.NoError(t, err)
	require.Equal(t, 1, client.calls)

	// Validate the request targets the track endpoint with JSON content.
	assert.Equal(t, http.MethodPost, client.lastReq.Method)
	assert.Equal(t, "https://eastus.example.com/v2.1/track", client.lastReq.URL.String())
	assert.Equal(t, "application/json", client.lastReq.Header.Get("Content-Type"))

	var envs []appInsightsEnvelope
	require.NoError(t, json.Unmarshal(client.lastBody, &envs))
	require.Len(t, envs, 1)
	assert.Equal(t, "Microsoft.ApplicationInsights.Event", envs[0].Name)
	assert.Equal(t, "EventData", envs[0].Data.BaseType)
	assert.Equal(t, "azcopy.job.finished", envs[0].Data.BaseData.Name)
	assert.Len(t, envs[0].Data.BaseData.Measurements, 49)
	assert.Equal(t, float64(100), envs[0].Data.BaseData.Measurements["azcopy.percent_complete"])
}

func TestReportEventAppInsightsServerError(t *testing.T) {
	client := &stubClient{status: http.StatusInternalServerError, respBody: "boom"}
	r := NewReporter(Config{
		ConnectionString: testConnString,
		HTTPClient:       client,
	})
	err := r.ReportEvent(context.Background(), sampleStarted())
	require.Error(t, err)
	assert.Contains(t, err.Error(), "500")
}

func TestReportEventAppInsightsRejectsPartialAcceptance(t *testing.T) {
	client := &stubClient{
		status:   http.StatusPartialContent,
		respBody: `{"itemsReceived":2,"itemsAccepted":1,"errors":[{"index":1,"statusCode":400,"message":"invalid field"}]}`,
	}
	r := NewReporter(Config{
		ConnectionString: testConnString,
		HTTPClient:       client,
	})

	err := r.ReportEvent(context.Background(), sampleStarted())
	require.Error(t, err)
	assert.ErrorContains(t, err, "partially accepted telemetry")
	assert.ErrorContains(t, err, "index=1 status=400")
}

func TestReportEventAppInsightsRejectsInvalidPartialResponses(t *testing.T) {
	tests := []struct {
		name string
		body string
		want string
	}{
		{name: "empty", body: "", want: "invalid response"},
		{name: "malformed", body: "{", want: "invalid response"},
		{name: "invalid counts", body: `{"itemsReceived":1,"itemsAccepted":2}`, want: "invalid item counts"},
		{name: "missing errors", body: `{"itemsReceived":2,"itemsAccepted":1}`, want: "inconsistent rejection details"},
		{name: "invalid index", body: `{"itemsReceived":2,"itemsAccepted":1,"errors":[{"index":2}]}`, want: "invalid rejected item index"},
		{name: "no rejection", body: `{"itemsReceived":1,"itemsAccepted":1}`, want: "inconsistent rejection details"},
		{name: "trailing content", body: `{"itemsReceived":2,"itemsAccepted":1,"errors":[{"index":1,"statusCode":400}]} trailing`, want: "trailing content"},
		{name: "duplicate index", body: `{"itemsReceived":3,"itemsAccepted":1,"errors":[{"index":1,"statusCode":400},{"index":1,"statusCode":400}]}`, want: "duplicate rejected item index"},
		{name: "invalid status", body: `{"itemsReceived":2,"itemsAccepted":1,"errors":[{"index":1,"statusCode":0}]}`, want: "invalid rejection status"},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			client := &stubClient{status: http.StatusPartialContent, respBody: test.body}
			r := NewReporter(Config{
				ConnectionString: testConnString,
				HTTPClient:       client,
			})

			err := r.ReportEvent(context.Background(), sampleStarted())
			require.Error(t, err)
			assert.ErrorContains(t, err, test.want)
		})
	}
}

func TestReportEventTransportError(t *testing.T) {
	client := &stubClient{err: errors.New("network down")}
	r := NewReporter(Config{
		ConnectionString: testConnString,
		HTTPClient:       client,
	})
	err := r.ReportEvent(context.Background(), sampleStarted())
	require.Error(t, err)
	assert.Contains(t, err.Error(), "transport failure")
	assert.NotContains(t, err.Error(), "network down")
}

func TestReportEventBoundsProperties(t *testing.T) {
	oversized := strings.Repeat("x", maxPropValueLen+100)
	event := sampleFinished()
	event.Resource.OSVersion = oversized
	event.Dimensions.Options.Values = map[string]string{"OptFutureValue": oversized}
	event.JobID = oversized

	client := &stubClient{status: http.StatusOK}
	reporter := NewReporter(Config{
		ConnectionString: testConnString,
		HTTPClient:       client,
	})
	require.NoError(t, reporter.ReportEvent(context.Background(), event))

	var envelopes []appInsightsEnvelope
	require.NoError(t, json.Unmarshal(client.lastBody, &envelopes))
	require.Len(t, envelopes, 1)
	for name, value := range envelopes[0].Data.BaseData.Properties {
		assert.LessOrEqual(t, len(value), propertyValueLimit(name), name)
	}
	assert.Len(t, envelopes[0].Data.BaseData.Properties["OSVersion"], maxHostValueLen)
	assert.Len(t, envelopes[0].Data.BaseData.Properties["JobID"], maxIdentifierValueLen)
	assert.Len(t, envelopes[0].Data.BaseData.Properties["OptFutureValue"], maxOptionValueLen)
}

func TestReportEventMissingConnectionString(t *testing.T) {
	r := NewReporter(Config{ConnectionString: ""})
	err := r.ReportEvent(context.Background(), sampleStarted())
	require.Error(t, err)
}
