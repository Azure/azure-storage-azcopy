package e2etest

import (
	"bytes"
	"encoding/json"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/Azure/azure-storage-azcopy/v10/cmd"
	"github.com/Azure/azure-storage-azcopy/v10/common"
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

	_, err = validateAppInsightsValidationConfig(AppInsightsValidationConfig{
		ConnectionString: "InstrumentationKey=11111111-2222-3333-4444-555555555555",
		WorkspaceID:      "workspace-id",
		RunID:            `run"id`,
	})
	assert.ErrorContains(t, err, "may contain only")

	_, err = validateAppInsightsValidationConfig(AppInsightsValidationConfig{
		ConnectionString: "InstrumentationKey=11111111-2222-3333-4444-555555555555",
		WorkspaceID:      "workspace-id",
		RunID:            strings.Repeat("x", 81),
	})
	assert.ErrorContains(t, err, "at most 80 bytes")
}

func TestAzCopyVerbProducesJobFinishedTelemetry(t *testing.T) {
	for _, verb := range []AzCopyVerb{AzCopyVerbCopy, AzCopyVerbSync, AzCopyVerbJobsResume} {
		assert.True(t, azCopyVerbProducesJobFinishedTelemetry(verb), verb)
	}

	for _, verb := range []AzCopyVerb{
		AzCopyVerbBenchmark,
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

func TestAzCopyJobIDCaptureForwardsAndCapturesJSONOutput(t *testing.T) {
	var target bytes.Buffer
	capture := newAzCopyJobIDCapture(&testAzCopyStdout{Buffer: &target})
	ready := false
	capture.onJobID = func() { ready = true }
	jobID := common.NewJobID().String()
	initMessage, err := json.Marshal(cmd.InitMsgJsonTemplate{JobID: jobID})
	require.NoError(t, err)
	output, err := json.Marshal(cmd.JsonOutputTemplate{
		MessageType:    cmd.EOutputMessageType.Init().String(),
		MessageContent: string(initMessage),
	})
	require.NoError(t, err)
	output = append(output, '\n')

	n, err := capture.Write(output)
	require.NoError(t, err)
	assert.Equal(t, len(output), n)
	assert.Equal(t, output, target.Bytes())
	assert.True(t, ready)
	assert.Equal(t, jobID, capture.JobID())
}

func TestAzCopyJobIDCaptureHandlesSplitTextOutput(t *testing.T) {
	var target bytes.Buffer
	capture := newAzCopyJobIDCapture(&testAzCopyStdout{Buffer: &target})
	jobID := common.NewJobID().String()
	output := []byte("\nJob " + jobID + " has started\nLog file is located at: log.txt\n")

	for _, chunk := range [][]byte{output[:9], output[9:31], output[31:]} {
		n, err := capture.Write(chunk)
		require.NoError(t, err)
		assert.Equal(t, len(chunk), n)
	}

	assert.Equal(t, output, target.Bytes())
	assert.Equal(t, jobID, capture.JobID())
}

func TestAzCopyJobIDCaptureFlushesFinalLineAndRejectsInvalidIDs(t *testing.T) {
	var target bytes.Buffer
	capture := newAzCopyJobIDCapture(&testAzCopyStdout{Buffer: &target})
	validJobID := common.NewJobID().String()
	output := []byte("Job not-a-job-id has started\nJob " + validJobID + " has started")

	n, err := capture.Write(output)
	require.NoError(t, err)
	assert.Equal(t, len(output), n)
	assert.Equal(t, validJobID, capture.JobID())
	assert.Equal(t, output, target.Bytes())
}

func TestAzCopyJobIDCaptureSupportsRawAndDiscardStdout(t *testing.T) {
	for name, target := range map[string]AzCopyStdout{
		"raw":     &AzCopyRawStdout{},
		"discard": &AzCopyDiscardStdout{},
	} {
		t.Run(name, func(t *testing.T) {
			capture := newAzCopyJobIDCapture(target)
			jobID := common.NewJobID().String()
			output := []byte("Job " + jobID + " has started\n")

			n, err := capture.Write(output)
			require.NoError(t, err)
			assert.Equal(t, len(output), n)
			assert.Equal(t, jobID, capture.JobID())
		})
	}
}

type testAzCopyStdout struct {
	*bytes.Buffer
}

func (s *testAzCopyStdout) RawStdout() []string {
	return strings.Split(s.String(), "\n")
}
