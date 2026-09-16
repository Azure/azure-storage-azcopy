package e2etest

import (
	"bufio"
	"errors"
	"fmt"
	"github.com/Azure/azure-storage-azcopy/v10/azcopy"
	"github.com/Azure/azure-storage-azcopy/v10/common"
	"io"
	"math"
	"os"
	"strconv"
	"strings"
)

type telemetryExpectation struct {
	NoEvents                bool
	CommandOnly             bool
	InstallationGroup       string
	ForbiddenValues         []string
	AbsentMeasurements      []string
	ProcessRunID            string
	JobID                   string
	Command                 string
	Properties              map[string]string
	Measurements            map[string]float64
	FinishedProperties      map[string]string
	TerminalStages          []string
	IncompleteProgress      bool
	DeliveryFailureExpected bool
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
		} else if (strings.HasPrefix(diagnostic, "disabled for this process ") && !strings.HasPrefix(diagnostic, "disabled for this process after delivery failure sending ")) ||
			strings.HasPrefix(diagnostic, "failed to send ") || strings.HasPrefix(diagnostic, "dropped event ") {
			evidence.OtherErrors++
		}
	}
	evidence.JobLogReadable = scanner.Err() == nil
	return evidence
}

func validateTelemetryDeliveryFailure(evidence telemetryDeliveryEvidence) error {
	if !evidence.JobLogReadable || evidence.StartedSends != 0 || evidence.FinishedSends != 0 || evidence.StopMessages != 1 || evidence.OtherErrors != 0 {
		return errors.New("expected readable job log, one telemetry delivery-failure stop, no successful sends and no other telemetry errors")
	}
	return nil
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
	installations := make(map[string]string)
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
		names := []string{"azcopy.job.started", "azcopy.job.finished"}
		if attempt.CommandOnly {
			names = []string{"azcopy.command.invoked"}
		}
		for _, event := range byProcess[attempt.ProcessRunID] {
			allowed := false
			for _, name := range names {
				allowed = allowed || event.Name == name
			}
			if !allowed {
				return nil, fmt.Errorf("unexpected event type for process %s", attempt.ProcessRunID)
			}
			counts[event.Name]++
			if counts[event.Name] > 1 {
				return nil, fmt.Errorf("duplicate %s for process %s", event.Name, attempt.ProcessRunID)
			}
			properties := event.Properties
			if (!attempt.CommandOnly && properties["JobID"] != attempt.JobID) || properties["Command"] != attempt.Command {
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
			if attempt.InstallationGroup != "" {
				if previous, exists := installations[attempt.InstallationGroup]; exists && previous != installation {
					return nil, fmt.Errorf("installation ID changed within group %s", attempt.InstallationGroup)
				}
				installations[attempt.InstallationGroup] = installation
			}
			for _, private := range attempt.ForbiddenValues {
				for _, value := range properties {
					if private != "" && strings.Contains(value, private) {
						return nil, fmt.Errorf("private value emitted for process %s", attempt.ProcessRunID)
					}
				}
			}
			for _, name := range attempt.AbsentMeasurements {
				if _, exists := event.Measurements[name]; exists {
					return nil, fmt.Errorf("unexpected measurement %s for process %s", name, attempt.ProcessRunID)
				}
			}
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
				if len(attempt.TerminalStages) > 0 {
					allowedStage := false
					for _, stage := range attempt.TerminalStages {
						allowedStage = allowedStage || properties["TerminalStage"] == stage
					}
					if !allowedStage {
						return nil, fmt.Errorf("unexpected terminal stage for process %s", attempt.ProcessRunID)
					}
				}
				if attempt.IncompleteProgress {
					percent, exists := event.Measurements["azcopy.percent_complete"]
					if !exists || math.IsNaN(percent) || math.IsInf(percent, 0) || percent < 0 || percent >= 100 {
						return nil, fmt.Errorf("expected incomplete progress for process %s", attempt.ProcessRunID)
					}
				}
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
		for _, name := range names {
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

func validateTelemetryTerminalSummary(expected *telemetryExpectation, summary *common.ListJobSummaryResponse) error {
	if expected == nil || expected.NoEvents || expected.CommandOnly || expected.FinishedProperties["JobStatus"] == "" {
		return nil
	}
	if summary == nil || summary.JobID.IsEmpty() {
		return errors.New("expected terminal scenario did not produce a final summary with a job ID")
	}
	if summary.JobStatus.String() != expected.FinishedProperties["JobStatus"] {
		return fmt.Errorf("expected terminal status %s, got %s", expected.FinishedProperties["JobStatus"], summary.JobStatus)
	}
	if expected.IncompleteProgress {
		percent := float64(summary.PercentComplete)
		if math.IsNaN(percent) || math.IsInf(percent, 0) || percent < 0 || percent >= 100 {
			return errors.New("expected terminal scenario did not have incomplete progress")
		}
	}
	return nil
}

func registerTelemetryExpectation(processRunID, jobID string, verb AzCopyVerb, summary *common.ListJobSummaryResponse, sourceType, destType string, extra ...*telemetryExpectation) {
	if jobID == "" || processRunID == "" {
		return
	}
	expected := telemetryExpectation{
		ProcessRunID: processRunID, JobID: jobID, Command: strings.ReplaceAll(string(verb), " ", "."),
		InstallationGroup: jobID,
		Properties:        map[string]string{"SchemaVersion": "1"},
	}
	if verb == AzCopyVerbJobsResume {
		expected.Properties["SummaryCounterScope"] = "job-cumulative"
		expected.AbsentMeasurements = []string{"azcopy.job_throughput_mbps", "azcopy.transfer_phase_throughput_mbps"}
	}
	if sourceType != "" && destType != "" {
		expected.Properties["SourceType"] = sourceType
		expected.Properties["DestType"] = destType
	}
	if summary != nil && !summary.JobID.IsEmpty() {
		expected.FinishedProperties = map[string]string{"JobStatus": summary.JobStatus.String()}
		expected.Measurements = map[string]float64{
			"azcopy.bytes_transferred":                   float64(summary.TotalBytesTransferred),
			"azcopy.bytes_over_wire":                     float64(summary.BytesOverWire),
			"azcopy.transfers_total":                     float64(summary.TotalTransfers),
			"azcopy.transfers_completed":                 float64(summary.TransfersCompleted),
			"azcopy.transfers_failed":                    float64(summary.TransfersFailed),
			"azcopy.transfers_skipped":                   float64(summary.TransfersSkipped),
			"azcopy.bytes_enumerated":                    float64(summary.TotalBytesEnumerated),
			"azcopy.bytes_expected":                      float64(summary.TotalBytesExpected),
			"azcopy.regular_files_scheduled":             float64(summary.FileTransfers),
			"azcopy.folder_properties_scheduled":         float64(summary.FolderPropertyTransfers),
			"azcopy.objects_scheduled":                   float64(max(int64(summary.TotalTransfers)-int64(summary.FolderPropertyTransfers), 0)),
			"azcopy.objects_completed":                   float64(max(int64(summary.TransfersCompleted)-int64(summary.FoldersCompleted), 0)),
			"azcopy.objects_failed":                      float64(max(int64(summary.TransfersFailed)-int64(summary.FoldersFailed), 0)),
			"azcopy.objects_skipped":                     float64(max(int64(summary.TransfersSkipped)-int64(summary.FoldersSkipped), 0)),
			"azcopy.symlinks_scheduled":                  float64(summary.SymlinkTransfers),
			"azcopy.hardlinks_converted_scheduled":       float64(summary.HardlinksConvertedCount),
			"azcopy.folder_properties_completed":         float64(summary.FoldersCompleted),
			"azcopy.folder_properties_failed":            float64(summary.FoldersFailed),
			"azcopy.folder_properties_skipped":           float64(summary.FoldersSkipped),
			"azcopy.storage_http_attempt_count":          float64(summary.StorageHTTPAttemptCount),
			"azcopy.network_error_attempt_count":         float64(summary.NetworkErrorAttemptCount),
			"azcopy.server_busy_503_count":               float64(summary.ServerBusy503Count),
			"azcopy.server_busy_throughput_count":        float64(summary.ServerBusyThroughputCount),
			"azcopy.server_busy_iops_count":              float64(summary.ServerBusyIOPSCount),
			"azcopy.server_busy_other_count":             float64(summary.ServerBusyOtherCount),
			"azcopy.avg_iops":                            float64(summary.AverageIOPS),
			"azcopy.average_storage_http_attempt_e2e_ms": float64(summary.AverageE2EMilliseconds),
			"azcopy.percent_complete":                    float64(summary.PercentComplete),
		}
	}
	if len(extra) > 0 && extra[0] != nil {
		addition := extra[0]
		expected.TerminalStages = addition.TerminalStages
		expected.IncompleteProgress = addition.IncompleteProgress
		if addition.InstallationGroup != "" {
			expected.InstallationGroup = addition.InstallationGroup
		}
		expected.ForbiddenValues = addition.ForbiddenValues
		expected.AbsentMeasurements = append(expected.AbsentMeasurements, addition.AbsentMeasurements...)
		for key, value := range addition.Properties {
			expected.Properties[key] = value
		}
		if expected.FinishedProperties == nil {
			expected.FinishedProperties = map[string]string{}
		}
		for key, value := range addition.FinishedProperties {
			expected.FinishedProperties[key] = value
		}
		if expected.Measurements == nil {
			expected.Measurements = map[string]float64{}
		}
		for key, value := range addition.Measurements {
			expected.Measurements[key] = value
		}
	}
	globalAppInsightsValidation.mu.Lock()
	defer globalAppInsightsValidation.mu.Unlock()
	globalAppInsightsValidation.expectedAttempts = append(globalAppInsightsValidation.expectedAttempts, expected)
}

func registerCommandTelemetryExpectation(processRunID string, verb AzCopyVerb, extra *telemetryExpectation) {
	expected := *extra
	expected.ProcessRunID = processRunID
	expected.Command = strings.ReplaceAll(string(verb), " ", ".")
	if expected.Properties == nil {
		expected.Properties = map[string]string{}
	}
	expected.Properties["SchemaVersion"] = "1"
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
