package e2etest

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/Azure/azure-storage-azcopy/v10/azcopy"
	"github.com/Azure/azure-storage-azcopy/v10/common"
)

type telemetryExpectation struct {
	NoEvents           bool
	ProcessRunID       string
	JobID              string
	Command            string
	Properties         map[string]string
	Measurements       map[string]float64
	FinishedProperties map[string]string
}

type observedTelemetryEvent struct {
	Name         string
	Properties   map[string]string
	Measurements map[string]float64
}

type telemetryDeliveryEvidence struct {
	StartedSends   int
	FinishedSends  int
	StopMessages   int
	DeadlineErrors int
	OtherErrors    int
	JobLogReadable bool
}

func collectTelemetryDeliveryEvidence(stderr string, jobLog io.Reader) telemetryDeliveryEvidence {
	evidence := telemetryDeliveryEvidence{
		StartedSends:  strings.Count(stderr, "telemetry: sent packed azcopy.job.started event to App Insights"),
		FinishedSends: strings.Count(stderr, "telemetry: sent packed azcopy.job.finished event to App Insights"),
	}
	if jobLog == nil {
		return evidence
	}
	scanner := bufio.NewScanner(jobLog)
	scanner.Buffer(make([]byte, 64*1024), 1024*1024)
	for scanner.Scan() {
		_, diagnostic, present := strings.Cut(scanner.Text(), "telemetry: ")
		if !present {
			continue
		}
		if strings.HasPrefix(diagnostic, "disabled for this process after delivery failure sending ") {
			evidence.StopMessages++
		}
		if strings.Contains(diagnostic, "context deadline exceeded") {
			evidence.DeadlineErrors++
		} else if strings.HasPrefix(diagnostic, "disabled for this process ") ||
			strings.HasPrefix(diagnostic, "failed to send ") || strings.HasPrefix(diagnostic, "dropped event ") {
			evidence.OtherErrors++
		}
	}
	evidence.JobLogReadable = scanner.Err() == nil
	return evidence
}

func logTelemetryDeliveryEvidence(a Asserter, processRunID, jobID, jobLogPath, stderr string) {
	var reader io.Reader
	if file, err := os.Open(jobLogPath); err == nil {
		defer file.Close()
		reader = file
	}
	evidence := collectTelemetryDeliveryEvidence(stderr, reader)
	a.Log("Telemetry delivery process=%s job=%s startedSends=%d finishedSends=%d stops=%d deadlines=%d otherErrors=%d jobLogReadable=%t",
		processRunID, jobID, evidence.StartedSends, evidence.FinishedSends, evidence.StopMessages,
		evidence.DeadlineErrors, evidence.OtherErrors, evidence.JobLogReadable)
}

func checkTelemetryManifest(expected []telemetryExpectation, events []observedTelemetryEvent) ([]string, error) {
	byProcess := make(map[string][]observedTelemetryEvent)
	for _, event := range events {
		process := event.Properties["E2ETestRunID"]
		byProcess[process] = append(byProcess[process], event)
	}
	var missing []string
	invocationProcesses := make(map[string]string)
	for _, attempt := range expected {
		if attempt.NoEvents {
			if len(byProcess[attempt.ProcessRunID]) != 0 {
				return nil, fmt.Errorf("unexpected telemetry for opted-out or dry-run process %s", attempt.ProcessRunID)
			}
			continue
		}
		invocation := ""
		installation := ""
		counts := make(map[string]int)
		for _, event := range byProcess[attempt.ProcessRunID] {
			if event.Name != "azcopy.job.started" && event.Name != "azcopy.job.finished" {
				return nil, fmt.Errorf("unexpected event type for process %s", attempt.ProcessRunID)
			}
			counts[event.Name]++
			if counts[event.Name] > 1 {
				return nil, fmt.Errorf("duplicate %s for process %s", event.Name, attempt.ProcessRunID)
			}
			properties := event.Properties
			if properties["JobID"] != attempt.JobID || properties["Command"] != attempt.Command {
				return nil, fmt.Errorf("job or command mismatch for process %s", attempt.ProcessRunID)
			}
			if properties["InvocationID"] == "" || properties["InstallationID"] == "" {
				return nil, fmt.Errorf("missing identity for process %s", attempt.ProcessRunID)
			}
			if invocation != "" && invocation != properties["InvocationID"] {
				return nil, fmt.Errorf("different invocation IDs for process %s", attempt.ProcessRunID)
			}
			if installation != "" && installation != properties["InstallationID"] {
				return nil, fmt.Errorf("different installation IDs for process %s", attempt.ProcessRunID)
			}
			invocation, installation = properties["InvocationID"], properties["InstallationID"]
			if process, exists := invocationProcesses[invocation]; exists && process != attempt.ProcessRunID {
				return nil, fmt.Errorf("invocation ID reused across processes %s and %s", process, attempt.ProcessRunID)
			}
			invocationProcesses[invocation] = attempt.ProcessRunID
			for key, value := range attempt.Properties {
				if properties[key] != value {
					return nil, fmt.Errorf("property %s mismatch for process %s: expected %q, got %q", key, attempt.ProcessRunID, value, properties[key])
				}
			}
			if value, exists := event.Measurements[event.Name]; !exists || value != 1 {
				return nil, fmt.Errorf("invalid lifecycle counter for process %s", attempt.ProcessRunID)
			}
			if event.Name == "azcopy.job.finished" {
				for key, want := range attempt.FinishedProperties {
					if properties[key] != want {
						return nil, fmt.Errorf("terminal property %s mismatch for process %s", key, attempt.ProcessRunID)
					}
				}
				for key, want := range attempt.Measurements {
					got, exists := event.Measurements[key]
					if !exists || math.IsNaN(got) || math.IsInf(got, 0) || math.Abs(got-want) > math.Max(0.001, math.Abs(want)*1e-10) {
						return nil, fmt.Errorf("measurement %s mismatch for process %s", key, attempt.ProcessRunID)
					}
				}
				if properties["JobStatus"] == "" || properties["TerminalStage"] == "" {
					return nil, fmt.Errorf("missing terminal outcome for process %s", attempt.ProcessRunID)
				}
			}
		}
		for _, name := range []string{"azcopy.job.started", "azcopy.job.finished"} {
			if counts[name] == 0 {
				missing = append(missing, attempt.ProcessRunID+"/"+name)
			}
		}
	}
	return missing, nil
}

func telemetryEndpointTypes(targets []string, flags map[string]string) (string, string, error) {
	if len(targets) != 2 {
		return "", "", nil
	}
	fromTo, err := azcopy.InferAndValidateFromTo(targets[0], targets[1], flags["from-to"])
	if err != nil {
		return "", "", err
	}
	return fromTo.From().String(), fromTo.To().String(), nil
}

func registerTelemetryExpectation(processRunID, jobID string, verb AzCopyVerb, summary *common.ListJobSummaryResponse, sourceType, destType string) {
	if jobID == "" || processRunID == "" {
		return
	}
	expected := telemetryExpectation{
		ProcessRunID: processRunID, JobID: jobID, Command: strings.ReplaceAll(string(verb), " ", "."),
		Properties: map[string]string{"SchemaVersion": "3"},
	}
	if verb == AzCopyVerbJobsResume {
		expected.Properties["SummaryCounterScope"] = "job-cumulative"
	}
	if sourceType != "" && destType != "" {
		expected.Properties["SourceType"] = sourceType
		expected.Properties["DestType"] = destType
	}
	if summary != nil && !summary.JobID.IsEmpty() {
		expected.FinishedProperties = map[string]string{"JobStatus": summary.JobStatus.String()}
		expected.Measurements = map[string]float64{
			"azcopy.bytes_transferred":   float64(summary.TotalBytesTransferred),
			"azcopy.bytes_over_wire":     float64(summary.BytesOverWire),
			"azcopy.transfers_total":     float64(summary.TotalTransfers),
			"azcopy.transfers_completed": float64(summary.TransfersCompleted),
			"azcopy.transfers_failed":    float64(summary.TransfersFailed),
			"azcopy.transfers_skipped":   float64(summary.TransfersSkipped),
		}
	}
	globalAppInsightsValidation.mu.Lock()
	defer globalAppInsightsValidation.mu.Unlock()
	globalAppInsightsValidation.expectedAttempts = append(globalAppInsightsValidation.expectedAttempts, expected)
}

func telemetryProcessEnvironment(environment []string, runID string) ([]string, string) {
	processID := runID + "/" + common.NewJobID().String()
	filtered := make([]string, 0, len(environment)+1)
	for _, value := range environment {
		key, _, _ := strings.Cut(value, "=")
		if !strings.EqualFold(key, "AZCOPY_E2E_TELEMETRY_RUN_ID") {
			filtered = append(filtered, value)
		}
	}
	return append(filtered, "AZCOPY_E2E_TELEMETRY_RUN_ID="+processID), processID
}

func telemetryExpectsNoEvents(verb AzCopyVerb, flags map[string]string, environment []string) bool {
	disabled, _ := strconv.ParseBool(flags["disable-telemetry"])
	for _, entry := range environment {
		key, value, _ := strings.Cut(entry, "=")
		if strings.EqualFold(key, "AZCOPY_DISABLE_TELEMETRY") && strings.EqualFold(value, "true") {
			disabled = true
		}
	}
	dryrun, _ := strconv.ParseBool(flags["dry-run"])
	return disabled || (azCopyVerbProducesJobFinishedTelemetry(verb) && dryrun)
}

func registerNoTelemetryExpectation(processRunID string) {
	globalAppInsightsValidation.mu.Lock()
	defer globalAppInsightsValidation.mu.Unlock()
	globalAppInsightsValidation.expectedAttempts = append(globalAppInsightsValidation.expectedAttempts, telemetryExpectation{ProcessRunID: processRunID, NoEvents: true})
}

type telemetryEventQueryClient interface {
	QueryTelemetryEvents(context.Context, string, string) ([]observedTelemetryEvent, error)
}

type telemetryManifestVerifier struct {
	queryClient  telemetryEventQueryClient
	pollInterval time.Duration
	timeout      time.Duration
}

func (v telemetryManifestVerifier) Verify(ctx context.Context, workspaceID, runID string, startedAt time.Time, expected []telemetryExpectation) error {
	if v.queryClient == nil || v.pollInterval <= 0 || v.timeout <= 0 || len(expected) == 0 {
		return errors.New("invalid telemetry manifest verifier configuration or empty manifest")
	}
	parentContext := ctx
	observeAbsence := false
	for _, attempt := range expected {
		observeAbsence = observeAbsence || attempt.NoEvents
	}
	queriedSuccessfully := false
	ctx, cancel := context.WithTimeout(ctx, v.timeout)
	defer cancel()
	query := fmt.Sprintf(`AppEvents
| where TimeGenerated >= datetime(%s)
| extend EventProperties = todynamic(Properties)
| where tostring(EventProperties.E2ETestRunID) startswith "%s/"
| project Name, Properties=EventProperties, Measurements=todynamic(Measurements)`, startedAt.Add(-5*time.Minute).UTC().Format(time.RFC3339Nano), escapeKQLString(runID))
	var missing []string
	var lastErr error
	for {
		queryContext, queryCancel := context.WithTimeout(parentContext, appInsightsQueryRequestTimeout)
		events, err := v.queryClient.QueryTelemetryEvents(queryContext, workspaceID, query)
		queryCancel()
		if err == nil {
			queriedSuccessfully = true
			missing, err = checkTelemetryManifest(expected, events)
			if err != nil {
				return err
			}
			if len(missing) == 0 && !observeAbsence {
				return nil
			}
		} else if !isRetryableQueryError(err) {
			return err
		}
		lastErr = err
		timer := time.NewTimer(v.pollInterval)
		select {
		case <-ctx.Done():
			timer.Stop()
			if parentContext.Err() == nil && observeAbsence && queriedSuccessfully && lastErr == nil && len(missing) == 0 {
				return nil
			}
			return fmt.Errorf("telemetry manifest incomplete: missing %v; last query error: %v: %w", missing, lastErr, ctx.Err())
		case <-timer.C:
		}
	}
}

func (c *logAnalyticsQueryClient) QueryTelemetryEvents(ctx context.Context, workspaceID, query string) ([]observedTelemetryEvent, error) {
	result, err := c.queryResults(ctx, workspaceID, query)
	if err != nil {
		return nil, err
	}
	return parseTelemetryEvents(result)
}

func parseTelemetryEvents(result logAnalyticsQueryResponse) ([]observedTelemetryEvent, error) {
	if len(result.Tables) != 1 {
		return nil, errors.New("expected one lifecycle result table")
	}
	table := result.Tables[0]
	indexes := make(map[string]int)
	for index, column := range table.Columns {
		indexes[column.Name] = index
	}
	for _, name := range []string{"Name", "Properties", "Measurements"} {
		if _, exists := indexes[name]; !exists {
			return nil, fmt.Errorf("missing lifecycle column %s", name)
		}
	}
	var events []observedTelemetryEvent
	for _, row := range table.Rows {
		event := observedTelemetryEvent{}
		for name, target := range map[string]any{"Name": &event.Name, "Properties": &event.Properties, "Measurements": &event.Measurements} {
			index := indexes[name]
			if index >= len(row) {
				return nil, errors.New("short lifecycle result row")
			}
			value := row[index]
			if name != "Name" && len(value) > 0 && value[0] == '"' {
				var encoded string
				if err := json.Unmarshal(value, &encoded); err != nil {
					return nil, fmt.Errorf("invalid encoded lifecycle %s", name)
				}
				value = json.RawMessage(encoded)
			}
			if err := json.Unmarshal(value, target); err != nil {
				return nil, fmt.Errorf("invalid lifecycle %s", name)
			}
		}
		events = append(events, event)
	}
	return events, nil
}
