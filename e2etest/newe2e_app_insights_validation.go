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
	"regexp"
	"strings"
	"sync"
	"time"
)

const (
	appInsightsQueryEndpoint       = "https://api.loganalytics.azure.com/v1/workspaces"
	appInsightsQueryRequestTimeout = 30 * time.Second
	appInsightsPollInterval        = 15 * time.Second
	appInsightsPollTimeout         = 5 * time.Minute
	appInsightsAbsenceWait         = 60 * time.Second
	maxQueryErrorBodyBytes         = 8 * 1024
)

type appInsightsValidationState struct {
	mu               sync.RWMutex
	enabled          bool
	workspaceID      string
	runID            string
	startedAt        time.Time
	expectedAttempts []telemetryExpectation
}

var globalAppInsightsValidation appInsightsValidationState

// Run IDs are embedded in KQL queries and per-process correlation IDs.
var e2eTelemetryRunIDPattern = regexp.MustCompile(`^[A-Za-z0-9._-]+$`)

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
	globalAppInsightsValidation.expectedAttempts = nil
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
	if len(config.RunID) > 80 {
		return false, errors.New("E2E telemetry run ID must be at most 80 bytes to leave room for process correlation")
	}
	if !e2eTelemetryRunIDPattern.MatchString(config.RunID) {
		return false, errors.New("E2E telemetry run ID may contain only letters, digits, '.', '_', and '-'")
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
	globalAppInsightsValidation.expectedAttempts = nil
	globalAppInsightsValidation.mu.Unlock()
}

func AppInsightsTelemetryValidationEnabled() bool {
	globalAppInsightsValidation.mu.RLock()
	defer globalAppInsightsValidation.mu.RUnlock()
	return globalAppInsightsValidation.enabled
}

func VerifyAppInsightsTelemetry(a Asserter) {
	snapshot := snapshotAppInsightsValidation()
	if !snapshot.enabled {
		return
	}
	if len(snapshot.expectedAttempts) == 0 {
		a.NoError("verify Application Insights telemetry", errors.New(
			"no telemetry expectations were collected during the E2E run"))
		return
	}

	verifier := telemetryManifestVerifier{
		queryClient: &logAnalyticsQueryClient{
			tokens: PrimaryOAuthCache,
			client: &http.Client{Timeout: appInsightsQueryRequestTimeout},
		},
		pollInterval: appInsightsPollInterval,
		timeout:      appInsightsPollTimeout,
		absenceWait:  appInsightsAbsenceWait,
	}
	err := verifier.Verify(
		context.Background(),
		snapshot.workspaceID,
		snapshot.runID,
		snapshot.startedAt,
		snapshot.expectedAttempts,
	)
	a.NoError("verify Application Insights telemetry", err)
}

func snapshotAppInsightsValidation() appInsightsValidationState {
	globalAppInsightsValidation.mu.RLock()
	defer globalAppInsightsValidation.mu.RUnlock()
	return appInsightsValidationState{
		enabled:          globalAppInsightsValidation.enabled,
		workspaceID:      globalAppInsightsValidation.workspaceID,
		runID:            globalAppInsightsValidation.runID,
		startedAt:        globalAppInsightsValidation.startedAt,
		expectedAttempts: append([]telemetryExpectation(nil), globalAppInsightsValidation.expectedAttempts...),
	}
}

func escapeKQLString(value string) string {
	encoded, _ := json.Marshal(value)
	return string(encoded[1 : len(encoded)-1])
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
	Error  json.RawMessage `json:"error"`
	Tables []struct {
		Columns []struct {
			Name string `json:"name"`
		} `json:"columns"`
		Rows [][]json.RawMessage `json:"rows"`
	} `json:"tables"`
}

func (c *logAnalyticsQueryClient) queryResults(ctx context.Context, workspaceID, query string) (logAnalyticsQueryResponse, error) {
	failure := logAnalyticsQueryResponse{}
	if c.tokens == nil {
		return failure, errors.New("OAuth token provider is nil")
	}
	if c.client == nil {
		return failure, errors.New("HTTP client is nil")
	}

	token, err := c.tokens.GetAccessToken(LogAnalyticsResource)
	if err != nil {
		return failure, retryableQueryError{err: fmt.Errorf("get Log Analytics access token: %w", err)}
	}
	tokenValue, err := token.FreshToken()
	if err != nil {
		return failure, retryableQueryError{err: fmt.Errorf("refresh Log Analytics access token: %w", err)}
	}

	payload, err := json.Marshal(logAnalyticsQueryRequest{Query: query})
	if err != nil {
		return failure, fmt.Errorf("serialize Log Analytics query: %w", err)
	}
	endpoint := fmt.Sprintf("%s/%s/query", appInsightsQueryEndpoint, url.PathEscape(workspaceID))
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, endpoint, bytes.NewReader(payload))
	if err != nil {
		return failure, fmt.Errorf("create Log Analytics query request: %w", err)
	}
	req.Header.Set("Authorization", "Bearer "+tokenValue)
	req.Header.Set("Content-Type", "application/json")

	resp, err := c.client.Do(req)
	if err != nil {
		return failure, retryableQueryError{err: fmt.Errorf("send Log Analytics query: %w", err)}
	}
	defer resp.Body.Close()

	if resp.StatusCode < http.StatusOK || resp.StatusCode >= http.StatusMultipleChoices {
		body, readErr := io.ReadAll(io.LimitReader(resp.Body, maxQueryErrorBodyBytes))
		if readErr != nil {
			return failure, fmt.Errorf("read Log Analytics error response: %w", readErr)
		}
		statusErr := fmt.Errorf("Log Analytics query returned HTTP %d: %s", resp.StatusCode, strings.TrimSpace(string(body)))
		if isRetryableHTTPStatus(resp.StatusCode) {
			return failure, retryableQueryError{err: statusErr}
		}
		return failure, statusErr
	}

	var result logAnalyticsQueryResponse
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		return failure, fmt.Errorf("decode Log Analytics query response: %w", err)
	}
	if len(result.Error) > 0 && string(result.Error) != "null" {
		detail := result.Error
		if len(detail) > maxQueryErrorBodyBytes {
			detail = detail[:maxQueryErrorBodyBytes]
		}
		return failure, fmt.Errorf("Log Analytics returned a partial query error: %s", detail)
	}
	return result, nil
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
