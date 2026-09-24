package azcopy

import (
	"errors"
	"github.com/Azure/azure-storage-azcopy/v10/telemetry"
	"github.com/stretchr/testify/assert"
	"io"
	"net/http"
	"strings"
	"sync"
	"testing"
	"time"
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
