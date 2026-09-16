package e2etest

import (
	"errors"
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
	expectedJobs     map[string]int
	expectedAttempts []telemetryExpectation
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
	globalAppInsightsValidation.expectedAttempts = nil
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

func snapshotAppInsightsValidation() appInsightsValidationState {
	globalAppInsightsValidation.mu.RLock()
	defer globalAppInsightsValidation.mu.RUnlock()

	expectedJobs := make(map[string]int, len(globalAppInsightsValidation.expectedJobs))
	for jobID, count := range globalAppInsightsValidation.expectedJobs {
		expectedJobs[jobID] = count
	}
	return appInsightsValidationState{
		enabled:          globalAppInsightsValidation.enabled,
		workspaceID:      globalAppInsightsValidation.workspaceID,
		runID:            globalAppInsightsValidation.runID,
		startedAt:        globalAppInsightsValidation.startedAt,
		expectedJobs:     expectedJobs,
		expectedAttempts: append([]telemetryExpectation(nil), globalAppInsightsValidation.expectedAttempts...),
	}
}
