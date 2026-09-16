package e2etest

import (
	"github.com/Azure/azure-storage-azcopy/v10/common"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"sync"
	"testing"
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

func TestAzCopyVerbProducesJobFinishedTelemetry(t *testing.T) {
	for _, verb := range []AzCopyVerb{AzCopyVerbCopy, AzCopyVerbSync, AzCopyVerbJobsResume} {
		assert.True(t, azCopyVerbProducesJobFinishedTelemetry(verb), verb)
	}

	for _, verb := range []AzCopyVerb{
		AzCopyVerbRemove,
		AzCopyVerbList,
		AzCopyVerbLogin,
		AzCopyVerbLoginStatus,
		AzCopyVerbLogout,
		AzCopyVerbJobsList,
		AzCopyVerbJobsClean,
		AzCopyVerbJobsRemove,
		AzCopyVerbJobsShow,
	} {
		assert.False(t, azCopyVerbProducesJobFinishedTelemetry(verb), verb)
	}
}

func TestAzCopyCommandProducesJobFinishedTelemetryExcludesDryRuns(t *testing.T) {
	assert.True(t, azCopyCommandProducesJobFinishedTelemetry(AzCopyVerbCopy, nil))
	assert.True(t, azCopyCommandProducesJobFinishedTelemetry(
		AzCopyVerbSync,
		map[string]string{"dry-run": "false"}))
	assert.False(t, azCopyCommandProducesJobFinishedTelemetry(
		AzCopyVerbCopy,
		map[string]string{"dry-run": "true"}))
	assert.False(t, azCopyCommandProducesJobFinishedTelemetry(
		AzCopyVerbSync,
		map[string]string{"dry-run": "TRUE"}))
	assert.False(t, azCopyCommandProducesJobFinishedTelemetry(
		AzCopyVerbRemove,
		map[string]string{"dry-run": "false"}))
}

func TestDecideAppInsightsJobValidation(t *testing.T) {
	jobID := common.NewJobID().String()
	tests := []struct {
		name       string
		verb       AzCopyVerb
		flags      map[string]string
		shouldFail bool
		jobID      string
		expected   appInsightsJobValidationDecision
	}{
		{
			name:     "successful command requires a job ID",
			verb:     AzCopyVerbCopy,
			expected: appInsightsJobValidationDecision{missingJobID: true},
		},
		{
			name:       "expected failure may omit a job ID",
			verb:       AzCopyVerbSync,
			shouldFail: true,
			expected:   appInsightsJobValidationDecision{},
		},
		{
			name:       "expected failure still registers an emitted job ID",
			verb:       AzCopyVerbCopy,
			shouldFail: true,
			jobID:      jobID,
			expected:   appInsightsJobValidationDecision{jobID: jobID},
		},
		{
			name:  "successful command registers an emitted job ID",
			verb:  AzCopyVerbJobsResume,
			jobID: jobID,
			expected: appInsightsJobValidationDecision{
				jobID: jobID,
			},
		},
		{
			name:     "dry run does not require or register a job ID",
			verb:     AzCopyVerbCopy,
			flags:    map[string]string{"dry-run": "true"},
			jobID:    jobID,
			expected: appInsightsJobValidationDecision{},
		},
		{
			name:     "non-terminal command does not require or register a job ID",
			verb:     AzCopyVerbRemove,
			jobID:    jobID,
			expected: appInsightsJobValidationDecision{},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			assert.Equal(t, test.expected, decideAppInsightsJobValidation(
				test.verb,
				test.flags,
				test.shouldFail,
				test.jobID))
		})
	}
}
