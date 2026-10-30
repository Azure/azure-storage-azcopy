package e2etest

import (
	"errors"
	"os"
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
		disableUnconfiguredTelemetry()
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

// disabledTelemetryConnectionString uses the all-zero key, which AzCopy treats as unconfigured.
const disabledTelemetryConnectionString = "InstrumentationKey=00000000-0000-0000-0000-000000000000"

// disableUnconfiguredTelemetry keeps E2E AzCopy processes from falling back to the
// embedded production connection string when no telemetry instance is configured.
func disableUnconfiguredTelemetry() {
	if strings.TrimSpace(os.Getenv("AZCOPY_TELEMETRY_CONNECTION_STRING")) == "" {
		_ = os.Setenv("AZCOPY_TELEMETRY_CONNECTION_STRING", disabledTelemetryConnectionString)
	}
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
