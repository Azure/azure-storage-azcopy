package e2etest

import (
	"encoding/json"
	"github.com/Azure/azure-storage-azcopy/v10/common"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"strings"
	"testing"
)

func manifestEvent(process, invocation, name string) observedTelemetryEvent {
	return observedTelemetryEvent{
		Name: name,
		Properties: map[string]string{
			"E2ETestRunID": process, "JobID": "job", "InvocationID": invocation,
			"InstallationID": "installation", "Command": "copy", "SchemaVersion": "1",
			"JobStatus": "Completed", "TerminalStage": "completed",
		},
		Measurements: map[string]float64{name: 1, "azcopy.bytes_transferred": 100},
	}
}

func TestTelemetryDeliveryEvidence(t *testing.T) {
	const started = "telemetry: sent packed azcopy.job.started event to App Insights\n"
	const finished = "telemetry: sent packed azcopy.job.finished event to App Insights\n"
	healthy := collectTelemetryDeliveryEvidence(started+finished, strings.NewReader("transfer completed\n"))
	assert.Equal(t, telemetryDeliveryEvidence{StartedSends: 1, FinishedSends: 1, JobLogReadable: true}, healthy)
	stopped := collectTelemetryDeliveryEvidence(started, strings.NewReader(
		"WARN: telemetry: disabled for this process after delivery failure sending azcopy.job.finished: send metrics: context deadline exceeded\n"))
	assert.Equal(t, telemetryDeliveryEvidence{StartedSends: 1, StopMessages: 1, DeadlineErrors: 1, JobLogReadable: true}, stopped)
	duplicate := collectTelemetryDeliveryEvidence(started+started+finished, nil)
	assert.Equal(t, 2, duplicate.StartedSends)
	assert.False(t, duplicate.JobLogReadable)
	other := collectTelemetryDeliveryEvidence("", strings.NewReader("WARN: telemetry: dropped event after send panic\nprivate-path-token-canary\n"))
	assert.Equal(t, 1, other.OtherErrors)
	encoded, err := json.Marshal(other)
	require.NoError(t, err)
	assert.NotContains(t, string(encoded), "canary")
	unreadable := collectTelemetryDeliveryEvidence("", strings.NewReader(strings.Repeat("x", 1024*1024+1)))
	assert.False(t, unreadable.JobLogReadable)
}

func TestTelemetryManifest(t *testing.T) {
	expected := []telemetryExpectation{{ProcessRunID: "process", JobID: "job", Command: "copy", Properties: map[string]string{"SchemaVersion": "1"}, Measurements: map[string]float64{"azcopy.bytes_transferred": 100}}}
	started := manifestEvent("process", "invocation", "azcopy.job.started")
	finished := manifestEvent("process", "invocation", "azcopy.job.finished")
	t.Run("finish arrives before start", func(t *testing.T) {
		missing, err := checkTelemetryManifest(expected, []observedTelemetryEvent{finished, started})
		require.NoError(t, err)
		assert.Empty(t, missing)
	})
	t.Run("orphan waits for partner", func(t *testing.T) {
		missing, err := checkTelemetryManifest(expected, []observedTelemetryEvent{finished})
		require.NoError(t, err)
		assert.Equal(t, []string{"process/azcopy.job.started"}, missing)
	})
	t.Run("duplicate cannot mask missing attempt", func(t *testing.T) {
		_, err := checkTelemetryManifest(expected, []observedTelemetryEvent{finished, finished})
		assert.ErrorContains(t, err, "duplicate")
	})
	t.Run("different invocation", func(t *testing.T) {
		other := manifestEvent("process", "other", "azcopy.job.finished")
		_, err := checkTelemetryManifest(expected, []observedTelemetryEvent{started, other})
		assert.ErrorContains(t, err, "different invocation")
	})
	t.Run("resume process must arrive independently", func(t *testing.T) {
		attempts := append(append([]telemetryExpectation{}, expected...), telemetryExpectation{ProcessRunID: "resume", JobID: "job", Command: "jobs.resume"})
		missing, err := checkTelemetryManifest(attempts, []observedTelemetryEvent{started, finished})
		require.NoError(t, err)
		assert.Len(t, missing, 2)
	})
	t.Run("wrong dimensions", func(t *testing.T) {
		wrong := manifestEvent("process", "invocation", "azcopy.job.started")
		wrong.Properties["SchemaVersion"] = "future"
		_, err := checkTelemetryManifest(expected, []observedTelemetryEvent{wrong})
		assert.ErrorContains(t, err, "property SchemaVersion")
		assert.ErrorContains(t, err, `expected "1", got "future"`)
	})
	t.Run("wrong counters", func(t *testing.T) {
		wrong := manifestEvent("process", "invocation", "azcopy.job.finished")
		wrong.Measurements["azcopy.bytes_transferred"] = 101
		_, err := checkTelemetryManifest(expected, []observedTelemetryEvent{wrong})
		assert.ErrorContains(t, err, "measurement")
	})
}

func TestTelemetryManifestFunctionalCases(t *testing.T) {
	command := manifestEvent("command", "invocation", "azcopy.command.invoked")
	command.Properties["Command"] = "jobs.list"
	command.Properties["JobID"] = ""
	expected := []telemetryExpectation{{ProcessRunID: "command", Command: "jobs.list", CommandOnly: true}}
	missing, err := checkTelemetryManifest(expected, []observedTelemetryEvent{command})
	require.NoError(t, err)
	require.Empty(t, missing)
	_, err = checkTelemetryManifest(expected, []observedTelemetryEvent{command, command})
	require.ErrorContains(t, err, "duplicate")
	expected[0].NoEvents = true
	_, err = checkTelemetryManifest(expected, []observedTelemetryEvent{command})
	require.ErrorContains(t, err, "unexpected telemetry")
	expected[0].NoEvents = false
	expected[0].ForbiddenValues = []string{"private-canary"}
	command.Properties["Unexpected"] = "private-canary"
	_, err = checkTelemetryManifest(expected, []observedTelemetryEvent{command})
	require.ErrorContains(t, err, "private value")
	delete(command.Properties, "Unexpected")
	expected[0].ForbiddenValues = []string{`C:\private\download.bin`}
	command.Properties["Unexpected"] = `C:\private\download.bin`
	_, err = checkTelemetryManifest(expected, []observedTelemetryEvent{command})
	require.ErrorContains(t, err, "private value")
	delete(command.Properties, "Unexpected")
	expected[0].AbsentMeasurements = []string{"azcopy.job_throughput_mbps"}
	command.Measurements["azcopy.job_throughput_mbps"] = 1
	_, err = checkTelemetryManifest(expected, []observedTelemetryEvent{command})
	require.ErrorContains(t, err, "unexpected measurement")
	delete(command.Measurements, "azcopy.job_throughput_mbps")
	expected[0].InstallationGroup = "shared"
	expected = append(expected, telemetryExpectation{ProcessRunID: "other", JobID: "job", Command: "copy", InstallationGroup: "shared"})
	other := manifestEvent("other", "different-invocation", "azcopy.job.started")
	other.Properties["InstallationID"] = "different-installation"
	_, err = checkTelemetryManifest(expected, []observedTelemetryEvent{command, other})
	require.ErrorContains(t, err, "installation ID changed")
}

func TestTelemetryManifestAuthCloud(t *testing.T) {
	for _, test := range []struct {
		name, fromTo, sourceAuth, destinationAuth, sourceCloud, destinationCloud string
	}{
		{"SAS download", "BlobLocal", "SAS", "NotApplicable", "public", ""},
		{"OAuth upload", "LocalBlobFS", "NotApplicable", "OAuth", "", "public"},
		{"OAuth service copy", "FileBlob", "OAuth", "OAuth", "public", "public"},
		{"mixed service auth", "BlobBlob", "SAS", "OAuth", "public", "public"},
	} {
		t.Run(test.name, func(t *testing.T) {
			properties := map[string]string{
				"FromTo":              test.fromTo,
				"SourceAuthMechanism": test.sourceAuth, "DestAuthMechanism": test.destinationAuth,
				"SourceCloudType": test.sourceCloud, "DestCloudType": test.destinationCloud,
			}
			expected := []telemetryExpectation{{ProcessRunID: "process", JobID: "job", Command: "copy", Properties: properties}}
			makeEvents := func() []observedTelemetryEvent {
				events := []observedTelemetryEvent{
					manifestEvent("process", "invocation", "azcopy.job.started"),
					manifestEvent("process", "invocation", "azcopy.job.finished"),
				}
				for _, event := range events {
					for key, value := range properties {
						event.Properties[key] = value
					}
				}
				return events
			}
			missing, err := checkTelemetryManifest(expected, makeEvents())
			require.NoError(t, err)
			require.Empty(t, missing)
			for key := range properties {
				for index := range 2 {
					events := makeEvents()
					events[index].Properties[key] = "wrong"
					_, err := checkTelemetryManifest(expected, events)
					require.ErrorContains(t, err, "property "+key+" mismatch")
				}
			}
		})
	}
}

func TestTelemetryManifestEndpointTypes(t *testing.T) {
	for _, test := range []struct {
		name, source, destination, fromTo, wantSource, wantDestination string
	}{
		{"files URL inference", "https://account.file.core.windows.net/nfs/source", "https://account.file.core.windows.net/nfs/destination", "", "File", "File"},
		{"explicit NFS", "https://account.file.core.windows.net/nfs/source", "https://account.file.core.windows.net/nfs/destination", "FileNFSFileNFS", "FileNFS", "FileNFS"},
		{"SMB alias", "https://account.file.core.windows.net/share/source", "https://account.blob.core.windows.net/container/destination", "FileSMBBlob", "File", "Blob"},
		{"DFS endpoint", "https://account.dfs.core.windows.net/filesystem/source", "https://account.blob.core.windows.net/container/destination", "", "BlobFS", "Blob"},
		{"local upload", "local-source", "https://account.blob.core.windows.net/container/destination", "", "Local", "Blob"},
	} {
		t.Run(test.name, func(t *testing.T) {
			source, destination, err := telemetryEndpointTypes([]string{test.source, test.destination}, map[string]string{"from-to": test.fromTo})
			require.NoError(t, err)
			assert.Equal(t, test.wantSource, source)
			assert.Equal(t, test.wantDestination, destination)
		})
	}
	source, destination, err := telemetryEndpointTypes(nil, nil)
	require.NoError(t, err)
	assert.Empty(t, source)
	assert.Empty(t, destination)
	_, _, err = telemetryEndpointTypes([]string{"source", "destination"}, map[string]string{"from-to": "invalid"})
	require.Error(t, err)
}

func TestTelemetryExistingWorkflowAssertions(t *testing.T) {
	resetAppInsightsValidation()
	t.Cleanup(resetAppInsightsValidation)
	jobID := common.NewJobID()
	summary := common.ListJobSummaryResponse{
		JobID: jobID, JobStatus: common.EJobStatus.Cancelled(),
		TotalTransfers: 8, FileTransfers: 3, FolderPropertyTransfers: 2, SymlinkTransfers: 1, HardlinksConvertedCount: 2,
		TransfersCompleted: 3, FoldersCompleted: 1, TransfersFailed: 2, FoldersFailed: 1, TransfersSkipped: 1,
		TotalBytesTransferred: 100, TotalBytesExpected: 200, TotalBytesEnumerated: 400, PercentComplete: 50,
		StorageHTTPAttemptCount: 11, NetworkErrorAttemptCount: 2,
	}
	registerTelemetryExpectation("copy", jobID.String(), AzCopyVerbCopy, &summary, "File", "Local")
	summary.JobStatus = common.EJobStatus.Completed()
	summary.TotalBytesTransferred, summary.PercentComplete = 200, 100
	registerTelemetryExpectation("resume", jobID.String(), AzCopyVerbJobsResume, &summary, "File", "Local")
	expected := snapshotAppInsightsValidation().expectedAttempts
	makeEvents := func() []observedTelemetryEvent {
		var events []observedTelemetryEvent
		for _, attempt := range expected {
			for _, name := range []string{"azcopy.job.started", "azcopy.job.finished"} {
				event := manifestEvent(attempt.ProcessRunID, attempt.ProcessRunID+"-invocation", name)
				event.Properties["JobID"], event.Properties["Command"] = attempt.JobID, attempt.Command
				for key, value := range attempt.Properties {
					event.Properties[key] = value
				}
				if name == "azcopy.job.finished" {
					for key, value := range attempt.FinishedProperties {
						event.Properties[key] = value
					}
					for key, value := range attempt.Measurements {
						event.Measurements[key] = value
					}
				}
				events = append(events, event)
			}
		}
		return events
	}
	require.Equal(t, float64(6), expected[0].Measurements["azcopy.objects_scheduled"])
	require.Equal(t, float64(2), expected[0].Measurements["azcopy.objects_completed"])
	require.Equal(t, float64(1), expected[0].Measurements["azcopy.objects_failed"])
	missing, err := checkTelemetryManifest(expected, makeEvents())
	require.NoError(t, err)
	require.Empty(t, missing)
	for _, key := range []string{"azcopy.folder_properties_failed", "azcopy.hardlinks_converted_scheduled", "azcopy.network_error_attempt_count", "azcopy.percent_complete"} {
		events := makeEvents()
		events[1].Measurements[key]++
		_, err := checkTelemetryManifest(expected, events)
		require.ErrorContains(t, err, "measurement "+key)
	}
	events := makeEvents()
	events[2].Properties["InstallationID"] = "different-home"
	_, err = checkTelemetryManifest(expected, events)
	require.ErrorContains(t, err, "installation ID changed")
	events = makeEvents()
	events[3].Measurements["azcopy.job_throughput_mbps"] = 99
	_, err = checkTelemetryManifest(expected, events)
	require.ErrorContains(t, err, "unexpected measurement")
}

func TestTelemetrySkipFailureTerminalConstraints(t *testing.T) {
	for _, test := range []struct {
		name, status, stage, category, code string
		failed, skipped                     float64
	}{
		{"skipped", "CompletedWithSkipped", "completed", "", "", 0, 1},
		{"failed summary", "Failed", "completion", "completion", "completion-error", 1, 0},
	} {
		t.Run(test.name, func(t *testing.T) {
			expected := []telemetryExpectation{{
				ProcessRunID: "process", JobID: "job", Command: "copy",
				FinishedProperties: map[string]string{
					"JobStatus": test.status, "TerminalStage": test.stage,
					"JobErrorCategory": test.category, "JobErrorCode": test.code,
				},
				Measurements: map[string]float64{
					"azcopy.transfers_total": 1, "azcopy.transfers_completed": 0,
					"azcopy.transfers_failed": test.failed, "azcopy.transfers_skipped": test.skipped,
					"azcopy.bytes_transferred": 0,
				},
			}}
			makeEvents := func() []observedTelemetryEvent {
				events := []observedTelemetryEvent{manifestEvent("process", "invocation", "azcopy.job.started"), manifestEvent("process", "invocation", "azcopy.job.finished")}
				for key, value := range expected[0].FinishedProperties {
					events[1].Properties[key] = value
				}
				for key, value := range expected[0].Measurements {
					events[1].Measurements[key] = value
				}
				return events
			}
			missing, err := checkTelemetryManifest(expected, makeEvents())
			require.NoError(t, err)
			require.Empty(t, missing)
			for key := range expected[0].FinishedProperties {
				events := makeEvents()
				events[1].Properties[key] = "wrong"
				_, err := checkTelemetryManifest(expected, events)
				require.ErrorContains(t, err, "terminal property "+key)
			}
			for key := range expected[0].Measurements {
				events := makeEvents()
				events[1].Measurements[key]++
				_, err := checkTelemetryManifest(expected, events)
				require.ErrorContains(t, err, "measurement "+key)
			}
		})
	}
}
