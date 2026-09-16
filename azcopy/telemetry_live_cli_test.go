//go:build telemetrylive

package azcopy

import (
	"bytes"
	"encoding/json"
	"fmt"
	"github.com/Azure/azure-storage-azcopy/v10/common"
	"github.com/stretchr/testify/require"
	"io"
	"regexp"
	"strings"
	"testing"
)

func liveCLIEnvironment(inherited []string, overrides map[string]string) []string {
	result := make([]string, 0, len(inherited)+len(overrides))
	for _, entry := range inherited {
		name, _, _ := strings.Cut(entry, "=")
		upper := strings.ToUpper(name)
		if strings.HasPrefix(upper, "AZCOPY_") || strings.HasPrefix(upper, "NEW_E2E_") {
			continue
		}
		replaced := false
		for key := range overrides {
			if strings.EqualFold(name, key) {
				replaced = true
				break
			}
		}
		if !replaced {
			result = append(result, entry)
		}
	}
	for key, value := range overrides {
		result = append(result, key+"="+value)
	}
	return result
}

func liveCLISummary(stdout []byte) (common.ListJobSummaryResponse, error) {
	var summary common.ListJobSummaryResponse
	decoder := json.NewDecoder(bytes.NewReader(stdout))
	found := false
	for {
		var message struct {
			MessageType    string
			MessageContent string
		}
		if err := decoder.Decode(&message); err == io.EOF {
			break
		} else if err != nil {
			return summary, fmt.Errorf("decode CLI JSON: %w", err)
		}
		if message.MessageType == "EndOfJob" {
			if found {
				return summary, fmt.Errorf("CLI produced duplicate EndOfJob summaries")
			}
			if err := json.Unmarshal([]byte(message.MessageContent), &summary); err != nil {
				return summary, err
			}
			found = true
		}
	}
	if !found || summary.JobID.IsEmpty() {
		return summary, fmt.Errorf("CLI did not produce an EndOfJob summary with a job ID")
	}
	return summary, nil
}

func liveCLIShutdownDiagnostic(jobLog string) bool {
	const prefix = "telemetry: disabled for this process after delivery failure sending "
	return strings.Count(jobLog, prefix) == 1 && regexp.MustCompile(
		`telemetry: disabled for this process after delivery failure sending azcopy\.job\.started: app insights returned HTTP (401|403)(\r?\n|$)`).MatchString(jobLog)
}

func TestLiveTelemetryCLIContract(t *testing.T) {
	environment := liveCLIEnvironment([]string{"PATH=tools", "AZCOPY_DISABLE_TELEMETRY=true", "azcopy_telemetry_connection_string=shared", "USERPROFILE=original"},
		map[string]string{"AZCOPY_DISABLE_TELEMETRY": "false", "AZCOPY_TELEMETRY_CONNECTION_STRING": "isolated", "USERPROFILE": "temporary"})
	require.Contains(t, environment, "PATH=tools")
	require.Contains(t, environment, "AZCOPY_DISABLE_TELEMETRY=false")
	require.Contains(t, environment, "AZCOPY_TELEMETRY_CONNECTION_STRING=isolated")
	require.NotContains(t, environment, "USERPROFILE=original")
	require.NotContains(t, environment, "azcopy_telemetry_connection_string=shared")
	job := common.ListJobSummaryResponse{JobID: common.NewJobID(), JobStatus: common.EJobStatus.Completed(), FileTransfers: 3, TransfersCompleted: 3}
	content, err := json.Marshal(job)
	require.NoError(t, err)
	wire, err := json.Marshal(map[string]string{"MessageType": "EndOfJob", "MessageContent": string(content)})
	require.NoError(t, err)
	summary, err := liveCLISummary(wire)
	require.NoError(t, err)
	require.Equal(t, job.JobID, summary.JobID)
	require.EqualValues(t, 3, summary.TransfersCompleted)
	_, err = liveCLISummary(append(append([]byte(nil), wire...), wire...))
	require.Error(t, err)
	_, err = liveCLISummary([]byte(`{"MessageType":"Progress","MessageContent":"{}"}`))
	require.Error(t, err)
	const prefix = "WARN: telemetry: disabled for this process after delivery failure sending azcopy.job.started: app insights returned HTTP "
	require.True(t, liveCLIShutdownDiagnostic(prefix+"401\n"))
	require.True(t, liveCLIShutdownDiagnostic(prefix+"403\r\n"))
	for _, invalid := range []string{prefix + "429\n", prefix + "4010\n", prefix + "401\n" + prefix + "401\n", strings.ReplaceAll(prefix+"401\n", "job.started", "job.finished"), "context deadline exceeded"} {
		require.False(t, liveCLIShutdownDiagnostic(invalid))
	}
}
