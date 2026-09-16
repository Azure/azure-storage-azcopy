//go:build telemetrylive

package azcopy

import (
	"bytes"
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"strings"
	"testing"
	"time"

	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/bloberror"
	"github.com/Azure/azure-storage-azcopy/v10/common"
	"github.com/Azure/azure-storage-azcopy/v10/telemetry"
	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
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

func newLiveCLIContainer(t *testing.T, ctx context.Context, target *liveTelemetryTarget, account string) (*azblob.Client, string) {
	t.Helper()
	storage, err := azblob.NewClient("https://"+account+".blob.core.windows.net/", target.credential, nil)
	require.NoError(t, err)
	container := "telemetry-cli-" + uuid.NewString()
	_, err = storage.CreateContainer(ctx, container, nil)
	require.NoError(t, err, "Azure CLI identity needs Blob data permissions on the test account")
	grantID := ""
	t.Cleanup(func() {
		cleanup, cleanupCancel := context.WithTimeout(context.Background(), 2*time.Minute)
		defer cleanupCancel()
		if _, err := storage.DeleteContainer(cleanup, container, nil); err != nil {
			t.Errorf("delete temporary container %s: %v", container, err)
		}
		if grantID != "" {
			if err := target.arm(context.Background(), "DELETE", grantID, "2022-04-01", nil, "", nil); err != nil {
				t.Errorf("REMOVE TEMPORARY ROLE FAILED: %s: %v", grantID, err)
			} else {
				t.Log("temporary container-scoped role assignment removed")
			}
		}
	})
	if principal := os.Getenv("AZCOPY_LIVE_TELEMETRY_STORAGE_PRINCIPAL"); principal != "" {
		_, err := uuid.Parse(principal)
		require.NoError(t, err)
		accountID := os.Getenv("AZCOPY_LIVE_TELEMETRY_STORAGE_RESOURCE_ID")
		subscription := os.Getenv("AZCOPY_LIVE_TELEMETRY_SUBSCRIPTION")
		require.Regexp(t, `(?i)^/subscriptions/`+regexp.QuoteMeta(subscription)+`/resourceGroups/[a-z0-9_.()-]+/providers/Microsoft.Storage/storageAccounts/`+regexp.QuoteMeta(account)+`$`, accountID)
		grantID = accountID + "/blobServices/default/containers/" + container + "/providers/Microsoft.Authorization/roleAssignments/" + uuid.NewString()
		t.Logf("temporary container=%s; role assignment=%s", container, grantID)
		role := map[string]any{"properties": map[string]string{
			"principalId": principal, "principalType": "User",
			"roleDefinitionId": "/subscriptions/" + subscription + "/providers/Microsoft.Authorization/roleDefinitions/ba92f5b4-2d11-453d-a403-e96b0029c9fe",
		}}
		require.NoError(t, target.arm(ctx, "PUT", grantID, "2022-04-01", role, "", nil))
		permissionCtx, permissionCancel := context.WithTimeout(ctx, 5*time.Minute)
		defer permissionCancel()
		for {
			_, err = storage.UploadBuffer(permissionCtx, container, "empty.bin", []byte{}, nil)
			if err == nil {
				break
			}
			require.True(t, bloberror.HasCode(err, bloberror.AuthorizationPermissionMismatch), "unexpected Blob permission probe failure: %v", err)
			t.Log("waiting for the container-scoped Blob permission to propagate")
			waitLiveTelemetry(t, permissionCtx, 15*time.Second)
		}
	}
	return storage, container
}

func TestLiveTelemetryCLIEmergencyShutdown(t *testing.T) {
	if os.Getenv("AZCOPY_RUN_LIVE_TELEMETRY") != "1" {
		t.Skip("manual full CLI test; use testSuite/telemetry-live.ps1 -Scenario cli-shutdown")
	}
	executable, err := filepath.Abs(os.Getenv("AZCOPY_LIVE_TELEMETRY_EXECUTABLE"))
	require.NoError(t, err)
	info, err := os.Stat(executable)
	require.NoError(t, err, "supply a built AzCopy executable through the manual runner")
	require.False(t, info.IsDir())
	account := os.Getenv("AZCOPY_LIVE_TELEMETRY_STORAGE_ACCOUNT")
	require.Regexp(t, `^[a-z0-9]{3,24}$`, account, "specify a dedicated test storage account")
	evidence := os.Getenv("AZCOPY_LIVE_TELEMETRY_OUTPUT")
	require.NotEmpty(t, evidence)
	evidence = filepath.Join(evidence, "cli-"+uuid.NewString())
	require.NoError(t, os.MkdirAll(evidence, 0700))
	t.Logf("full CLI evidence: %s", evidence)
	target := loadLiveTelemetryTarget(t, "shutdown")
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Minute)
	defer cancel()
	storage, container := newLiveCLIContainer(t, ctx, target, account)
	large := make([]byte, 16*1024*1024)
	_, err = rand.Read(large)
	require.NoError(t, err)
	files := map[string][]byte{"payload.bin": large, "nested/note.txt": []byte("real AzCopy CLI telemetry shutdown test\n"), "empty.bin": {}}
	var totalBytes uint64
	for name, contents := range files {
		_, err = storage.UploadBuffer(ctx, container, name, contents, nil)
		require.NoError(t, err)
		totalBytes += uint64(len(contents))
	}
	home, err := os.UserHomeDir()
	require.NoError(t, err)
	azureConfig := os.Getenv("AZURE_CONFIG_DIR")
	if azureConfig == "" {
		azureConfig = filepath.Join(home, ".azure")
	}
	azureConfig, err = filepath.Abs(azureConfig)
	require.NoError(t, err)
	runCopy := func(phase string, expectShutdown bool) bool {
		phaseDir := filepath.Join(evidence, phase)
		destination := filepath.Join(phaseDir, "download")
		logs := filepath.Join(phaseDir, "logs")
		plans := filepath.Join(phaseDir, "plans")
		userDir := filepath.Join(phaseDir, "home")
		for _, directory := range []string{destination, logs, plans, userDir} {
			require.NoError(t, os.MkdirAll(directory, 0700))
		}
		processCtx, processCancel := context.WithTimeout(ctx, 3*time.Minute)
		defer processCancel()
		command := exec.CommandContext(processCtx, executable, "copy",
			"https://"+account+".blob.core.windows.net/"+container+"/*", destination,
			"--recursive=true", "--from-to=BlobLocal", "--output-type=json", "--log-level=INFO", "--cap-mbps=16", "--block-size-mb=1", "--check-version=false")
		command.Env = liveCLIEnvironment(os.Environ(), map[string]string{
			"AZCOPY_DISABLE_TELEMETRY": "false", "AZCOPY_TELEMETRY_CONNECTION_STRING": target.connection,
			"AZCOPY_AUTO_LOGIN_TYPE": "AZCLI", "AZURE_CONFIG_DIR": azureConfig,
			"AZCOPY_LOG_LOCATION": logs, "AZCOPY_JOB_PLAN_LOCATION": plans,
			"AZCOPY_E2E_TELEMETRY_RUN_ID":              "cli-shutdown/" + uuid.NewString(),
			common.EEnvironmentVariable.UserDir().Name: userDir,
		})
		stdout, err := os.Create(filepath.Join(phaseDir, "stdout.jsonl"))
		require.NoError(t, err)
		stderr, err := os.Create(filepath.Join(phaseDir, "stderr.txt"))
		require.NoError(t, err)
		command.Stdout, command.Stderr = stdout, stderr
		runErr := command.Run()
		require.NoError(t, stdout.Close())
		require.NoError(t, stderr.Close())
		require.NoError(t, runErr, "CLI %s must exit 0; inspect %s", phase, phaseDir)
		output, err := os.ReadFile(stdout.Name())
		require.NoError(t, err)
		summary, err := liveCLISummary(output)
		require.NoError(t, err)
		require.Equal(t, common.EJobStatus.Completed(), summary.JobStatus)
		require.Empty(t, summary.ErrorMsg)
		require.True(t, summary.CompleteJobOrdered)
		require.EqualValues(t, len(files), summary.FileTransfers)
		require.EqualValues(t, len(files), summary.TransfersCompleted-summary.FoldersCompleted)
		require.Zero(t, summary.TransfersFailed)
		require.Zero(t, summary.TransfersSkipped)
		require.Equal(t, totalBytes, summary.TotalBytesTransferred)
		for name, original := range files {
			downloaded, err := os.ReadFile(filepath.Join(destination, filepath.FromSlash(name)))
			require.NoError(t, err)
			require.Equal(t, sha256.Sum256(original), sha256.Sum256(downloaded), "CLI downloaded content mismatch: %s", name)
		}
		jobLog, err := os.ReadFile(filepath.Join(logs, summary.JobID.String()+".log"))
		require.NoError(t, err)
		stderrBytes, err := os.ReadFile(stderr.Name())
		require.NoError(t, err)
		if strings.HasPrefix(phase, "restored-") && liveCLIShutdownDiagnostic(string(jobLog)) {
			t.Logf("CLI %s: transfer and hashes passed, but fresh connection still received auth rejection; waiting for restoration propagation", phase)
			return false
		}
		if expectShutdown {
			require.True(t, liveCLIShutdownDiagnostic(string(jobLog)), "expected one auth-rejected job.started shutdown diagnostic; inspect %s", logs)
			require.False(t, strings.Contains(string(stderrBytes), "telemetry: sent packed"), "disabled CLI must not report a successful lifecycle send; inspect %s", phaseDir)
		} else {
			require.False(t, strings.Contains(string(jobLog), "telemetry: disabled for this process"), "healthy CLI unexpectedly stopped telemetry; inspect %s", phaseDir)
			for _, event := range []string{"azcopy.job.started", "azcopy.job.finished"} {
				require.Contains(t, string(stderrBytes), "telemetry: sent packed "+event+" event", "healthy CLI must enable and send telemetry")
			}
		}
		t.Logf("CLI %s: exit=0 JobStatus=Completed files=%d bytes=%d SHA256 verified; auth-rejection shutdown=%t", phase, len(files), totalBytes, expectShutdown)
		return true
	}
	runCopy("baseline", false)
	restored := false
	t.Cleanup(func() {
		if !restored {
			cleanup, cleanupCancel := context.WithTimeout(context.Background(), 2*time.Minute)
			defer cleanupCancel()
			if err := target.setLocalAuth(cleanup, false); err != nil {
				t.Errorf("RESTORE FAILED: set DisableLocalAuth=false on %s: %v", target.componentID, err)
			}
		}
	})
	require.NoError(t, target.setLocalAuth(ctx, true))
	for {
		response, err := target.send(t, ctx, telemetry.OptionAttributes{})
		if response.rejectsWith(401) || response.rejectsWith(403) {
			t.Logf("server authentication disabled before CLI launch; real probe HTTP %d", response.Status)
			break
		}
		requireLiveAccepted(t, response, err)
		waitLiveTelemetry(t, ctx, 15*time.Second)
	}
	runCopy("auth-disabled", true)
	require.NoError(t, target.setLocalAuth(ctx, false))
	restored = true
	for {
		response, err := target.send(t, ctx, telemetry.OptionAttributes{})
		if response.Status == 200 {
			requireLiveAccepted(t, response, err)
			break
		}
		require.True(t, response.rejectsWith(401) || response.rejectsWith(403), "unexpected restore HTTP %d", response.Status)
		waitLiveTelemetry(t, ctx, 15*time.Second)
	}
	for attempt := 1; attempt <= 5; attempt++ {
		if runCopy(fmt.Sprintf("restored-%d", attempt), false) {
			return
		}
		waitLiveTelemetry(t, ctx, 15*time.Second)
	}
	t.Fatal("local auth was restored in ARM, but five fresh CLI processes still received authentication rejections")
}
