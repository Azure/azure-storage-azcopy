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

package azcopy

import (
	"errors"
	"io"
	"net/http"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/Azure/azure-storage-azcopy/v10/telemetry"
	"github.com/stretchr/testify/assert"
)

type failurePolicyClient struct {
	mu           sync.Mutex
	bodies       []string
	status       int
	panicFirst   bool
	failFirst    bool
	responseBody string
}

func (c *failurePolicyClient) Do(request *http.Request) (*http.Response, error) {
	body, err := io.ReadAll(request.Body)
	if err != nil {
		return nil, err
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	c.bodies = append(c.bodies, string(body))
	if len(c.bodies) == 1 {
		if c.panicFirst {
			panic("private-panic-canary")
		}
		if c.failFirst {
			return nil, errors.New("offline")
		}
		if c.status != 0 {
			return &http.Response{StatusCode: c.status, Body: io.NopCloser(strings.NewReader(c.responseBody)), Header: make(http.Header)}, nil
		}
	}
	return &http.Response{StatusCode: 200, Body: io.NopCloser(strings.NewReader("")), Header: make(http.Header)}, nil
}

func failureTestAgent(client *failurePolicyClient) *telemetryAgent {
	return &telemetryAgent{enabled: true, reporter: telemetry.NewReporter(telemetry.Config{
		ConnectionString: "InstrumentationKey=test;IngestionEndpoint=https://example.test", HTTPClient: client,
	})}
}

func TestTelemetrySendPanicDropsOnlyAffectedEvent(t *testing.T) {
	client := &failurePolicyClient{panicFirst: true}
	agent := failureTestAgent(client)
	agent.reportStarted(telemetry.JobDimensions{}, "job", "attempt", time.Now())
	agent.reportFinished(telemetry.JobFinishedEvent{JobID: "job", InvocationID: "attempt"})
	agent.flush(time.Second)
	client.mu.Lock()
	defer client.mu.Unlock()
	assert.Len(t, client.bodies, 2)
	assert.True(t, agent.isActive())
}

func TestTelemetryInitializationPanicDisablesAgent(t *testing.T) {
	agent := initializeTelemetryAgent(func() *telemetryAgent { panic("private-canary") })
	assert.NotNil(t, agent)
	assert.False(t, agent.isActive())
}
