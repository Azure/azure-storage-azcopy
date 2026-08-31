package e2etest

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"
)

const (
	appInsightsQueryEndpoint       = "https://api.loganalytics.azure.com/v1/workspaces"
	appInsightsQueryRequestTimeout = 30 * time.Second
	appInsightsPollInterval        = 15 * time.Second
	appInsightsPollTimeout         = 5 * time.Minute
	maxQueryErrorBodyBytes         = 8 * 1024
)

type appInsightsValidationState struct {
	mu           sync.RWMutex
	enabled      bool
	workspaceID  string
	runID        string
	startedAt    time.Time
	expectedJobs map[string]int
}

var globalAppInsightsValidation appInsightsValidationState

func SetupAppInsightsTelemetryValidation(a Asserter) {
	resetAppInsightsValidation()

	config := GlobalConfig.AppInsightsValidationConfig
	config.ConnectionString = strings.TrimSpace(config.ConnectionString)
	config.WorkspaceID = strings.TrimSpace(config.WorkspaceID)
	config.RunID = strings.TrimSpace(config.RunID)

	enabled, err := validateAppInsightsValidationConfig(config)
	if err != nil {
		a.NoError("configure Application Insights validation", err)
		return
	}
	if !enabled {
		a.Log("Application Insights validation is disabled.")
		return
	}

	globalAppInsightsValidation.mu.Lock()
	globalAppInsightsValidation.enabled = true
	globalAppInsightsValidation.workspaceID = config.WorkspaceID
	globalAppInsightsValidation.runID = config.RunID
	globalAppInsightsValidation.startedAt = time.Now().UTC()
	globalAppInsightsValidation.expectedJobs = make(map[string]int)
	globalAppInsightsValidation.mu.Unlock()

	a.Log("Application Insights validation enabled for run %q.", config.RunID)
}

func validateAppInsightsValidationConfig(config AppInsightsValidationConfig) (bool, error) {
	configuredValues := 0
	for _, value := range []string{config.ConnectionString, config.WorkspaceID, config.RunID} {
		if strings.TrimSpace(value) != "" {
			configuredValues++
		}
	}
	if configuredValues == 0 {
		return false, nil
	}
	if configuredValues != 3 {
		return false, errors.New(
			"AZCOPY_TELEMETRY_CONNECTION_STRING, NEW_E2E_APP_INSIGHTS_WORKSPACE_ID, and AZCOPY_E2E_TELEMETRY_RUN_ID must all be set")
	}
	if !strings.Contains(strings.ToLower(config.ConnectionString), "instrumentationkey=") {
		return false, errors.New(
			"AZCOPY_TELEMETRY_CONNECTION_STRING is not a valid Application Insights connection string")
	}
	return true, nil
}

func resetAppInsightsValidation() {
	globalAppInsightsValidation.mu.Lock()
	globalAppInsightsValidation.enabled = false
	globalAppInsightsValidation.workspaceID = ""
	globalAppInsightsValidation.runID = ""
	globalAppInsightsValidation.startedAt = time.Time{}
	globalAppInsightsValidation.expectedJobs = nil
	globalAppInsightsValidation.mu.Unlock()
}

func AppInsightsTelemetryValidationEnabled() bool {
	globalAppInsightsValidation.mu.RLock()
	defer globalAppInsightsValidation.mu.RUnlock()
	return globalAppInsightsValidation.enabled
}

func RegisterExpectedAppInsightsJob(jobID string) {
	jobID = strings.TrimSpace(jobID)
	if jobID == "" {
		return
	}

	globalAppInsightsValidation.mu.Lock()
	defer globalAppInsightsValidation.mu.Unlock()
	if globalAppInsightsValidation.enabled {
		globalAppInsightsValidation.expectedJobs[jobID]++
	}
}

func VerifyAppInsightsTelemetry(a Asserter) {
	snapshot := snapshotAppInsightsValidation()
	if !snapshot.enabled {
		return
	}
	if len(snapshot.expectedJobs) == 0 {
		a.NoError("verify Application Insights telemetry", errors.New(
			"no AzCopy job IDs were collected during the E2E run"))
		return
	}

	verifier := appInsightsVerifier{
		queryClient: &logAnalyticsQueryClient{
			tokens: PrimaryOAuthCache,
			client: &http.Client{Timeout: appInsightsQueryRequestTimeout},
		},
		pollInterval: appInsightsPollInterval,
		timeout:      appInsightsPollTimeout,
	}
	err := verifier.Verify(
		context.Background(),
		snapshot.workspaceID,
		snapshot.runID,
		snapshot.startedAt,
		snapshot.expectedJobs,
	)
	a.NoError("verify Application Insights telemetry", err)
}

func snapshotAppInsightsValidation() appInsightsValidationState {
	globalAppInsightsValidation.mu.RLock()
	defer globalAppInsightsValidation.mu.RUnlock()

	expectedJobs := make(map[string]int, len(globalAppInsightsValidation.expectedJobs))
	for jobID, count := range globalAppInsightsValidation.expectedJobs {
		expectedJobs[jobID] = count
	}
	return appInsightsValidationState{
		enabled:      globalAppInsightsValidation.enabled,
		workspaceID:  globalAppInsightsValidation.workspaceID,
		runID:        globalAppInsightsValidation.runID,
		startedAt:    globalAppInsightsValidation.startedAt,
		expectedJobs: expectedJobs,
	}
}

type finishedEventQueryClient interface {
	QueryFinishedEventCounts(ctx context.Context, workspaceID, query string) (map[string]int, error)
}

type appInsightsVerifier struct {
	queryClient  finishedEventQueryClient
	pollInterval time.Duration
	timeout      time.Duration
}

func (v appInsightsVerifier) Verify(
	ctx context.Context,
	workspaceID string,
	runID string,
	startedAt time.Time,
	expected map[string]int,
) error {
	if v.queryClient == nil {
		return errors.New("Application Insights query client is nil")
	}
	if v.pollInterval <= 0 || v.timeout <= 0 {
		return errors.New("Application Insights polling interval and timeout must be positive")
	}

	query := buildFinishedEventQuery(runID, startedAt)
	deadline := time.Now().Add(v.timeout)
	var (
		lastReceived map[string]int
		lastErr      error
	)

	for {
		received, err := v.queryClient.QueryFinishedEventCounts(ctx, workspaceID, query)
		if err == nil {
			lastReceived = received
			missing := missingExpectedEvents(expected, received)
			if len(missing) == 0 {
				return nil
			}
		} else {
			lastErr = err
			if !isRetryableQueryError(err) {
				return fmt.Errorf("query Application Insights telemetry: %w", err)
			}
		}

		remaining := time.Until(deadline)
		if remaining <= 0 {
			break
		}
		wait := v.pollInterval
		if remaining < wait {
			wait = remaining
		}
		timer := time.NewTimer(wait)
		select {
		case <-ctx.Done():
			timer.Stop()
			return fmt.Errorf("wait for Application Insights telemetry: %w", ctx.Err())
		case <-timer.C:
		}
	}

	missing := missingExpectedEvents(expected, lastReceived)
	return fmt.Errorf(
		"timed out after %s waiting for Application Insights telemetry for run %q; missing events: %s; last query error: %v; query:\n%s",
		v.timeout,
		runID,
		formatMissingEvents(missing),
		lastErr,
		query,
	)
}

func buildFinishedEventQuery(runID string, startedAt time.Time) string {
	return fmt.Sprintf(`AppEvents
| where TimeGenerated >= datetime(%s)
| where Name == "azcopy.job.finished"
| extend EventProperties = todynamic(Properties)
| where tostring(EventProperties.E2ETestRunID) == "%s"
| summarize ReceivedCount = count() by JobID = tostring(EventProperties.JobID)`,
		startedAt.UTC().Format(time.RFC3339Nano),
		escapeKQLString(runID),
	)
}

func escapeKQLString(value string) string {
	return strings.ReplaceAll(value, `"`, `\"`)
}

func missingExpectedEvents(expected, received map[string]int) map[string]int {
	missing := make(map[string]int)
	for jobID, expectedCount := range expected {
		if receivedCount := received[jobID]; receivedCount < expectedCount {
			missing[jobID] = expectedCount - receivedCount
		}
	}
	return missing
}

func formatMissingEvents(missing map[string]int) string {
	if len(missing) == 0 {
		return "none"
	}
	jobIDs := make([]string, 0, len(missing))
	for jobID := range missing {
		jobIDs = append(jobIDs, jobID)
	}
	sort.Strings(jobIDs)

	parts := make([]string, 0, len(jobIDs))
	for _, jobID := range jobIDs {
		parts = append(parts, fmt.Sprintf("%s (%d)", jobID, missing[jobID]))
	}
	return strings.Join(parts, ", ")
}

type accessTokenProvider interface {
	GetAccessToken(scope string) (*AzCoreAccessToken, error)
}

type httpDoer interface {
	Do(req *http.Request) (*http.Response, error)
}

type logAnalyticsQueryClient struct {
	tokens accessTokenProvider
	client httpDoer
}

type logAnalyticsQueryRequest struct {
	Query string `json:"query"`
}

type logAnalyticsQueryResponse struct {
	Tables []struct {
		Columns []struct {
			Name string `json:"name"`
		} `json:"columns"`
		Rows [][]json.RawMessage `json:"rows"`
	} `json:"tables"`
}

func (c *logAnalyticsQueryClient) QueryFinishedEventCounts(
	ctx context.Context,
	workspaceID string,
	query string,
) (map[string]int, error) {
	if c.tokens == nil {
		return nil, errors.New("OAuth token provider is nil")
	}
	if c.client == nil {
		return nil, errors.New("HTTP client is nil")
	}

	token, err := c.tokens.GetAccessToken(LogAnalyticsResource)
	if err != nil {
		return nil, fmt.Errorf("get Log Analytics access token: %w", err)
	}
	tokenValue, err := token.FreshToken()
	if err != nil {
		return nil, fmt.Errorf("refresh Log Analytics access token: %w", err)
	}

	payload, err := json.Marshal(logAnalyticsQueryRequest{Query: query})
	if err != nil {
		return nil, fmt.Errorf("serialize Log Analytics query: %w", err)
	}
	endpoint := fmt.Sprintf("%s/%s/query", appInsightsQueryEndpoint, url.PathEscape(workspaceID))
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, endpoint, bytes.NewReader(payload))
	if err != nil {
		return nil, fmt.Errorf("create Log Analytics query request: %w", err)
	}
	req.Header.Set("Authorization", "Bearer "+tokenValue)
	req.Header.Set("Content-Type", "application/json")

	resp, err := c.client.Do(req)
	if err != nil {
		return nil, retryableQueryError{err: fmt.Errorf("send Log Analytics query: %w", err)}
	}
	defer resp.Body.Close()

	if resp.StatusCode < http.StatusOK || resp.StatusCode >= http.StatusMultipleChoices {
		body, readErr := io.ReadAll(io.LimitReader(resp.Body, maxQueryErrorBodyBytes))
		if readErr != nil {
			return nil, fmt.Errorf("read Log Analytics error response: %w", readErr)
		}
		statusErr := fmt.Errorf("Log Analytics query returned HTTP %d: %s", resp.StatusCode, strings.TrimSpace(string(body)))
		if isRetryableHTTPStatus(resp.StatusCode) {
			return nil, retryableQueryError{err: statusErr}
		}
		return nil, statusErr
	}

	var result logAnalyticsQueryResponse
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		return nil, fmt.Errorf("decode Log Analytics query response: %w", err)
	}
	return parseFinishedEventCounts(result)
}

func parseFinishedEventCounts(result logAnalyticsQueryResponse) (map[string]int, error) {
	if len(result.Tables) != 1 {
		return nil, fmt.Errorf("expected one Log Analytics result table, got %d", len(result.Tables))
	}
	table := result.Tables[0]
	columnIndexes := make(map[string]int, len(table.Columns))
	for index, column := range table.Columns {
		columnIndexes[column.Name] = index
	}
	jobIDIndex, hasJobID := columnIndexes["JobID"]
	countIndex, hasCount := columnIndexes["ReceivedCount"]
	if !hasJobID || !hasCount {
		return nil, fmt.Errorf("Log Analytics response is missing JobID or ReceivedCount columns")
	}

	counts := make(map[string]int, len(table.Rows))
	for rowIndex, row := range table.Rows {
		if jobIDIndex >= len(row) || countIndex >= len(row) {
			return nil, fmt.Errorf("Log Analytics response row %d has fewer columns than expected", rowIndex)
		}
		var jobID string
		if err := json.Unmarshal(row[jobIDIndex], &jobID); err != nil {
			return nil, fmt.Errorf("decode JobID in row %d: %w", rowIndex, err)
		}
		count, err := parseJSONInt(row[countIndex])
		if err != nil {
			return nil, fmt.Errorf("decode ReceivedCount in row %d: %w", rowIndex, err)
		}
		if jobID != "" {
			counts[jobID] = count
		}
	}
	return counts, nil
}

func parseJSONInt(value json.RawMessage) (int, error) {
	var number json.Number
	if err := json.Unmarshal(value, &number); err != nil {
		return 0, err
	}
	parsed, err := strconv.Atoi(number.String())
	if err != nil {
		return 0, err
	}
	return parsed, nil
}

type retryableQueryError struct {
	err error
}

func (e retryableQueryError) Error() string {
	return e.err.Error()
}

func (e retryableQueryError) Unwrap() error {
	return e.err
}

func isRetryableQueryError(err error) bool {
	var retryable retryableQueryError
	return errors.As(err, &retryable)
}

func isRetryableHTTPStatus(statusCode int) bool {
	switch statusCode {
	case http.StatusRequestTimeout,
		http.StatusTooManyRequests,
		http.StatusInternalServerError,
		http.StatusBadGateway,
		http.StatusServiceUnavailable,
		http.StatusGatewayTimeout:
		return true
	default:
		return false
	}
}
