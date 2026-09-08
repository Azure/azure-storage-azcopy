package azcopy

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/Azure/azure-storage-azcopy/v10/common"
	"github.com/Azure/azure-storage-azcopy/v10/telemetry"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
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
		Backend: telemetry.BackendAppInsights, ConnectionString: "InstrumentationKey=test;IngestionEndpoint=https://example.test", HTTPClient: client,
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

func TestTelemetryInitializationFailureFinalization(t *testing.T) {
	for _, command := range []string{"copy", "sync", "jobs.resume"} {
		t.Run(command, func(t *testing.T) {
			client := &failurePolicyClient{}
			agent := failureTestAgent(client)
			finalizer := newAttemptTelemetryFinalizer(agent, telemetry.JobDimensions{Command: command}, "job", "attempt", time.Now())
			finalizer.startEvent()
			finalizer.finish(errors.New("initialization-private-canary"))
			finalizer.finish(nil)
			agent.flush(time.Second)
			client.mu.Lock()
			defer client.mu.Unlock()
			require.Len(t, client.bodies, 2)
			var envelopes []struct {
				Data struct {
					BaseData struct{ Properties map[string]string }
				}
			}
			require.NoError(t, json.Unmarshal([]byte(client.bodies[1]), &envelopes))
			properties := envelopes[0].Data.BaseData.Properties
			assert.Equal(t, "Failed", properties["JobStatus"])
			assert.Equal(t, "initialization", properties["TerminalStage"])
			assert.NotContains(t, client.bodies[1], "private-canary")
		})
	}
}

func TestTelemetryCollectionPanicsDoNotEscape(t *testing.T) {
	agent := initializeTelemetryAgent(func() *telemetryAgent { panic("metadata-private-canary") })
	assert.False(t, agent.enabled)
	finalizer := newAttemptTelemetryFinalizer(failureTestAgent(&failurePolicyClient{}), telemetry.JobDimensions{}, "job", "attempt", time.Now())
	finalizer.summaryFn = func() (common.ListJobSummaryResponse, bool) { panic("summary-private-canary") }
	assert.NotPanics(t, func() { finalizer.finish(nil) })
	assert.True(t, finalizer.finished)
	assert.NotPanics(t, func() {
		failureTestAgent(&failurePolicyClient{}).reportInitializationFailure(func() telemetry.JobDimensions { panic("private-canary") }, "job", "attempt", time.Now(), errors.New("failure"))
	})
}

func TestTelemetryDimensionCollectionFailureIsNoop(t *testing.T) {
	client := &failurePolicyClient{}
	agent := failureTestAgent(client)
	collectorCalled := false
	collect := func() telemetry.JobDimensions { collectorCalled = true; panic("dimension-private-canary") }
	finalizer := agent.newAttempt(collect, "job", "attempt", time.Now())
	assert.True(t, collectorCalled)
	finalizer.startEvent()
	finalizer.finish(errors.New("transfer failure"))
	agent.flush(time.Second)
	assert.Empty(t, client.bodies)
	collectorCalled = false
	agent.enabled = false
	finalizer = agent.newAttempt(collect, "job", "attempt", time.Now())
	finalizer.summaryFn = func() (common.ListJobSummaryResponse, bool) { collectorCalled = true; panic("summary") }
	finalizer.finish(nil)
	assert.False(t, collectorCalled)
}

type privacyTestLogger struct {
	common.ILoggerResetable
	mu       sync.Mutex
	messages []string
}

func (logger *privacyTestLogger) Log(level common.LogLevel, message string) {
	logger.mu.Lock()
	defer logger.mu.Unlock()
	logger.messages = append(logger.messages, message)
}

func TestTelemetryPrivacyCanariesInEnvelopesAndLogs(t *testing.T) {
	const canary = "private-path-sas-canary"
	logger := &privacyTestLogger{}
	previous := common.AzcopyCurrentJobLogger
	common.AzcopyCurrentJobLogger = logger
	t.Cleanup(func() { common.AzcopyCurrentJobLogger = previous })
	for _, status := range []int{400, 503, 206} {
		client := &failurePolicyClient{}
		agent := failureTestAgent(client)
		source := common.ResourceString{Value: "https://sourceaccount.blob.core.windows.net/" + canary, SAS: "sig=" + canary}
		destination := common.ResourceString{Value: "https://destaccount.blob.core.windows.net/" + canary, SAS: "sig=" + canary}
		dimensions := resumeJobDimensions(common.GetJobDetailsResponse{FromTo: common.EFromTo.BlobBlob()}, source, destination, common.ECredentialType.Anonymous(), common.ECredentialType.Anonymous(), telemetry.OptionAttributes{})
		agent.reportInitializationFailure(func() telemetry.JobDimensions { return dimensions }, "job", "attempt", time.Now(), &os.PathError{Op: "open", Path: canary, Err: os.ErrNotExist})
		agent.flush(time.Second)
		client.mu.Lock()
		require.Len(t, client.bodies, 2)
		bodies := strings.Join(client.bodies, "\n")
		assert.NotContains(t, bodies, canary)
		assert.Contains(t, bodies, "sourceaccount")
		assert.Contains(t, bodies, "destaccount")
		client.mu.Unlock()
		rejectedClient := &failurePolicyClient{status: status, responseBody: `{"itemsReceived":1,"itemsAccepted":0,"errors":[{"index":0,"statusCode":400,"message":"` + canary + `"}]}`}
		rejectedAgent := failureTestAgent(rejectedClient)
		rejectedAgent.reportCommand("list", "command-job", "command-attempt", telemetry.OptionAttributes{})
		rejectedAgent.flush(time.Second)
		rejectedClient.mu.Lock()
		require.Len(t, rejectedClient.bodies, 1)
		assert.Contains(t, rejectedClient.bodies[0], "azcopy.command.invoked")
		assert.NotContains(t, rejectedClient.bodies[0], canary)
		rejectedClient.mu.Unlock()
		assert.False(t, rejectedAgent.isActive())
	}
	panicClient := &failurePolicyClient{panicFirst: true}
	agent := failureTestAgent(panicClient)
	agent.reportStarted(telemetry.JobDimensions{}, "job", "panic-attempt", time.Now())
	agent.flush(time.Second)
	logger.mu.Lock()
	defer logger.mu.Unlock()
	require.NotEmpty(t, logger.messages)
	assert.NotContains(t, strings.Join(logger.messages, "\n"), canary)
	assert.NotContains(t, strings.Join(logger.messages, "\n"), "private-panic-canary")
	assert.Contains(t, strings.Join(logger.messages, "\n"), "disabled for this process after delivery failure")
}

func TestTelemetryInvalidInputBoundary(t *testing.T) {
	client := &Client{}
	_, err := client.Copy(context.Background(), "", "", CopyOptions{})
	require.Error(t, err)
	_, err = client.ResumeJob(context.Background(), common.JobID{}, ResumeJobOptions{})
	require.Error(t, err)
}
