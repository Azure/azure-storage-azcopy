package e2etest

import (
	"context"
	"encoding/json"
	"errors"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestValidateAppInsightsValidationConfig(t *testing.T) {
	enabled, err := validateAppInsightsValidationConfig(AppInsightsValidationConfig{})
	require.NoError(t, err)
	assert.False(t, enabled)

	enabled, err = validateAppInsightsValidationConfig(AppInsightsValidationConfig{
		ConnectionString: "InstrumentationKey=11111111-2222-3333-4444-555555555555",
		WorkspaceID:      "workspace-id",
		RunID:            "run-id",
	})
	require.NoError(t, err)
	assert.True(t, enabled)

	_, err = validateAppInsightsValidationConfig(AppInsightsValidationConfig{
		WorkspaceID: "workspace-id",
	})
	assert.ErrorContains(t, err, "must all be set")

	_, err = validateAppInsightsValidationConfig(AppInsightsValidationConfig{
		ConnectionString: "invalid",
		WorkspaceID:      "workspace-id",
		RunID:            "run-id",
	})
	assert.ErrorContains(t, err, "not a valid")
}

func TestExpectedAppInsightsJobsAreCountedConcurrently(t *testing.T) {
	resetAppInsightsValidation()
	t.Cleanup(resetAppInsightsValidation)

	globalAppInsightsValidation.mu.Lock()
	globalAppInsightsValidation.enabled = true
	globalAppInsightsValidation.expectedJobs = make(map[string]int)
	globalAppInsightsValidation.mu.Unlock()

	const registrations = 20
	var wg sync.WaitGroup
	for index := 0; index < registrations; index++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			RegisterExpectedAppInsightsJob("job-1")
		}()
	}
	wg.Wait()

	snapshot := snapshotAppInsightsValidation()
	assert.Equal(t, registrations, snapshot.expectedJobs["job-1"])
}

func TestBuildFinishedEventQuery(t *testing.T) {
	startedAt := time.Date(2026, 7, 1, 12, 30, 0, 0, time.UTC)
	query := buildFinishedEventQuery(`run"id`, startedAt)

	assert.Contains(t, query, "AppEvents")
	assert.Contains(t, query, `datetime(2026-07-01T12:30:00Z)`)
	assert.Contains(t, query, `E2ETestRunID) == "run\"id"`)
	assert.Contains(t, query, `Name == "azcopy.job.finished"`)
}

func TestParseFinishedEventCountsUsesColumnNames(t *testing.T) {
	result := logAnalyticsQueryResponse{}
	result.Tables = append(result.Tables, struct {
		Columns []struct {
			Name string `json:"name"`
		} `json:"columns"`
		Rows [][]json.RawMessage `json:"rows"`
	}{})
	result.Tables[0].Columns = append(
		result.Tables[0].Columns,
		struct {
			Name string `json:"name"`
		}{Name: "ReceivedCount"},
		struct {
			Name string `json:"name"`
		}{Name: "JobID"},
	)
	result.Tables[0].Rows = [][]json.RawMessage{
		{json.RawMessage(`2`), json.RawMessage(`"job-1"`)},
		{json.RawMessage(`1`), json.RawMessage(`"job-2"`)},
	}

	counts, err := parseFinishedEventCounts(result)
	require.NoError(t, err)
	assert.Equal(t, map[string]int{"job-1": 2, "job-2": 1}, counts)
}

func TestParseJSONIntRejectsOverflow(t *testing.T) {
	overflow := "2147483648"
	if strconv.IntSize == 64 {
		overflow = "9223372036854775808"
	}

	_, err := parseJSONInt(json.RawMessage(overflow))
	require.Error(t, err)
}

type stubFinishedEventQueryClient struct {
	mu        sync.Mutex
	responses []map[string]int
	errors    []error
	calls     int
}

func (c *stubFinishedEventQueryClient) QueryFinishedEventCounts(
	_ context.Context,
	_ string,
	_ string,
) (map[string]int, error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	index := c.calls
	c.calls++
	if index < len(c.errors) && c.errors[index] != nil {
		return nil, c.errors[index]
	}
	if index < len(c.responses) {
		return c.responses[index], nil
	}
	return map[string]int{}, nil
}

func TestAppInsightsVerifierPollsUntilExpectedEventsArrive(t *testing.T) {
	client := &stubFinishedEventQueryClient{
		responses: []map[string]int{
			{"job-1": 1},
			{"job-1": 2, "job-2": 1},
		},
	}
	verifier := appInsightsVerifier{
		queryClient:  client,
		pollInterval: time.Millisecond,
		timeout:      100 * time.Millisecond,
	}

	err := verifier.Verify(
		context.Background(),
		"workspace-id",
		"run-id",
		time.Now().Add(-time.Minute),
		map[string]int{"job-1": 2, "job-2": 1},
	)
	require.NoError(t, err)
	assert.Equal(t, 2, client.calls)
}

func TestAppInsightsVerifierRetriesTransientQueryErrors(t *testing.T) {
	client := &stubFinishedEventQueryClient{
		errors: []error{
			retryableQueryError{err: errors.New("throttled")},
			nil,
		},
		responses: []map[string]int{
			nil,
			{"job-1": 1},
		},
	}
	verifier := appInsightsVerifier{
		queryClient:  client,
		pollInterval: time.Millisecond,
		timeout:      100 * time.Millisecond,
	}

	err := verifier.Verify(
		context.Background(),
		"workspace-id",
		"run-id",
		time.Now().Add(-time.Minute),
		map[string]int{"job-1": 1},
	)
	require.NoError(t, err)
	assert.Equal(t, 2, client.calls)
}

func TestAppInsightsVerifierReportsMissingEventsOnTimeout(t *testing.T) {
	client := &stubFinishedEventQueryClient{}
	verifier := appInsightsVerifier{
		queryClient:  client,
		pollInterval: time.Millisecond,
		timeout:      3 * time.Millisecond,
	}

	err := verifier.Verify(
		context.Background(),
		"workspace-id",
		"run-id",
		time.Now().Add(-time.Minute),
		map[string]int{"job-2": 1, "job-1": 2},
	)
	require.Error(t, err)
	assert.True(t, strings.Contains(err.Error(), "job-1 (2), job-2 (1)"), err)
	assert.Contains(t, err.Error(), `run "run-id"`)
	assert.Contains(t, err.Error(), "AppEvents")
}

func TestAppInsightsVerifierStopsOnPermanentQueryError(t *testing.T) {
	client := &stubFinishedEventQueryClient{
		errors: []error{errors.New("forbidden")},
	}
	verifier := appInsightsVerifier{
		queryClient:  client,
		pollInterval: time.Millisecond,
		timeout:      100 * time.Millisecond,
	}

	err := verifier.Verify(
		context.Background(),
		"workspace-id",
		"run-id",
		time.Now().Add(-time.Minute),
		map[string]int{"job-1": 1},
	)
	require.Error(t, err)
	assert.ErrorContains(t, err, "forbidden")
	assert.Equal(t, 1, client.calls)
}
