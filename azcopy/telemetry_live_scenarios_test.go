//go:build telemetrylive

package azcopy

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"regexp"
	"strings"
	"testing"
	"time"

	"github.com/Azure/azure-sdk-for-go/sdk/azcore"
	"github.com/Azure/azure-sdk-for-go/sdk/azcore/policy"
	"github.com/Azure/azure-sdk-for-go/sdk/azidentity"
	"github.com/Azure/azure-storage-azcopy/v10/telemetry"
	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
)

type liveARMResource struct {
	ID         string                     `json:"id"`
	Location   string                     `json:"location"`
	Kind       string                     `json:"kind"`
	ETag       string                     `json:"etag"`
	Tags       map[string]string          `json:"tags"`
	Properties map[string]json.RawMessage `json:"properties"`
}

type liveTelemetryTarget struct {
	credential  azcore.TokenCredential
	management  *http.Client
	client      *liveTelemetryClient
	componentID string
	workspaceID string
	connection  string
	scenario    string
}

func (target *liveTelemetryTarget) arm(ctx context.Context, method, resourceID, version string, body any, etag string, result any) error {
	ctx, cancel := context.WithTimeout(ctx, time.Minute)
	defer cancel()
	token, err := target.credential.GetToken(ctx, policy.TokenRequestOptions{Scopes: []string{"https://management.azure.com/.default"}})
	if err != nil {
		return fmt.Errorf("ARM credential: %w", err)
	}
	var payload []byte
	if body != nil {
		payload, err = json.Marshal(body)
		if err != nil {
			return err
		}
	}
	request, err := http.NewRequestWithContext(ctx, method, "https://management.azure.com"+resourceID+"?api-version="+version, bytes.NewReader(payload))
	if err != nil {
		return err
	}
	request.Header.Set("Authorization", "Bearer "+token.Token)
	request.Header.Set("Content-Type", "application/json")
	if etag != "" {
		request.Header.Set("If-Match", etag)
	}
	response, err := target.management.Do(request)
	if err != nil {
		return err
	}
	defer response.Body.Close()
	if response.StatusCode < 200 || response.StatusCode >= 300 {
		return fmt.Errorf("ARM %s %s returned HTTP %d", method, resourceID, response.StatusCode)
	}
	if result != nil {
		return json.NewDecoder(io.LimitReader(response.Body, 2*1024*1024)).Decode(result)
	}
	return nil
}

func liveProperty[Value any](t *testing.T, resource liveARMResource, name string) Value {
	t.Helper()
	var value Value
	require.NoError(t, json.Unmarshal(resource.Properties[name], &value), "missing/invalid ARM property %s", name)
	return value
}

func requireLiveFaultResource(t *testing.T, resource liveARMResource, scenario string) {
	t.Helper()
	require.Equal(t, "test", resource.Tags["environment"])
	require.Equal(t, "manual-telemetry-faults", resource.Tags["purpose"], "refusing to use a shared telemetry resource")
	require.Equal(t, scenario, resource.Tags["scenario"])
}

func liveTelemetryCredential(mode string) (azcore.TokenCredential, error) {
	switch mode {
	case "", "AZCLI":
		return azidentity.NewAzureCLICredential(nil)
	case "MSI":
		return azidentity.NewManagedIdentityCredential(nil)
	default:
		return nil, fmt.Errorf("unsupported live telemetry authentication mode")
	}
}

func TestLiveTelemetryCredentialMode(t *testing.T) {
	for _, mode := range []string{"", "AZCLI", "MSI"} {
		credential, err := liveTelemetryCredential(mode)
		require.NoError(t, err)
		require.NotNil(t, credential)
	}
	_, err := liveTelemetryCredential("invalid")
	require.Error(t, err)
}

// The suffix becomes part of the fault-test resource names.
var liveTelemetrySuffixPattern = regexp.MustCompile(`^[a-z0-9-]{1,12}$`)

func loadLiveTelemetryTarget(t *testing.T, scenario string) *liveTelemetryTarget {
	t.Helper()
	if os.Getenv("AZCOPY_RUN_LIVE_TELEMETRY") != "1" {
		t.Skip("manual Azure test; use testSuite/telemetry-live.ps1")
	}
	subscription := os.Getenv("AZCOPY_LIVE_TELEMETRY_SUBSCRIPTION")
	group := os.Getenv("AZCOPY_LIVE_TELEMETRY_RESOURCE_GROUP")
	suffix := os.Getenv("AZCOPY_LIVE_TELEMETRY_SUFFIX")
	require.Regexp(t, `^[0-9a-fA-F-]{36}$`, subscription)
	require.Regexp(t, `^[a-zA-Z0-9_.()-]{1,90}$`, group)
	require.Regexp(t, liveTelemetrySuffixPattern, suffix)
	credential, err := liveTelemetryCredential(os.Getenv("AZCOPY_LIVE_TELEMETRY_AUTH_MODE"))
	require.NoError(t, err)
	transport := http.DefaultTransport.(*http.Transport).Clone()
	t.Cleanup(transport.CloseIdleConnections)
	target := &liveTelemetryTarget{
		credential: credential, scenario: scenario,
		management: &http.Client{Timeout: time.Minute, CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }},
		client: &liveTelemetryClient{client: &http.Client{Transport: transport, Timeout: 30 * time.Second,
			CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}},
	}
	base := fmt.Sprintf("/subscriptions/%s/resourceGroups/%s/providers/", subscription, group)
	name := "azcopy-telfault-" + suffix + "-" + scenario
	target.componentID = base + "Microsoft.Insights/components/" + name + "-ai"
	target.workspaceID = base + "Microsoft.OperationalInsights/workspaces/" + name + "-law"
	var component, workspace liveARMResource
	require.NoError(t, target.arm(context.Background(), "GET", target.componentID, "2020-02-02", nil, "", &component))
	require.NoError(t, target.arm(context.Background(), "GET", target.workspaceID, "2023-09-01", nil, "", &workspace))
	requireLiveFaultResource(t, component, scenario)
	requireLiveFaultResource(t, workspace, scenario)
	require.True(t, strings.EqualFold(target.workspaceID, liveProperty[string](t, component, "WorkspaceResourceId")))
	require.False(t, liveProperty[bool](t, component, "DisableLocalAuth"), "restore the isolated shutdown target before rerunning")
	require.EqualValues(t, 100, liveProperty[float64](t, component, "SamplingPercentage"))
	require.Equal(t, "Enabled", liveProperty[string](t, component, "publicNetworkAccessForIngestion"))
	workspaceCap := liveProperty[struct {
		DailyQuotaGB        float64 `json:"dailyQuotaGb"`
		DataIngestionStatus string  `json:"dataIngestionStatus"`
	}](t, workspace, "workspaceCapping")
	wantWorkspaceCap, wantAppCap := 0.024, 0.0323
	require.InDelta(t, wantWorkspaceCap, workspaceCap.DailyQuotaGB, 0.000001)
	require.NotEqual(t, "OverQuota", workspaceCap.DataIngestionStatus, "use a fresh suffix or wait until the daily reset")
	var billing struct{ DataVolumeCap struct{ Cap float64 } }
	require.NoError(t, target.arm(context.Background(), "GET", target.componentID+"/currentbillingfeatures", "2015-05-01", nil, "", &billing))
	require.InDelta(t, wantAppCap, billing.DataVolumeCap.Cap, 0.000001, "provision Application Insights cap with the infrastructure runner")
	require.False(t, target.appOverQuota(t), "use a fresh suffix or wait until the daily reset")
	target.connection = liveProperty[string](t, component, "ConnectionString")
	endpoint := ""
	for _, part := range strings.Split(target.connection, ";") {
		key, value, _ := strings.Cut(part, "=")
		if strings.EqualFold(key, "IngestionEndpoint") {
			endpoint = value
		}
	}
	parsed, err := url.Parse(endpoint)
	require.NoError(t, err)
	require.True(t, parsed.Scheme == "https" && strings.HasSuffix(parsed.Hostname(), ".in.applicationinsights.azure.com") && parsed.User == nil && parsed.RawQuery == "" && parsed.Fragment == "" && parsed.Port() == "", "only real regional public Azure ingestion endpoints are allowed")
	t.Logf("target=%s workspace cap=%g GB/day Application Insights cap=%g GB/day", name, workspaceCap.DailyQuotaGB, billing.DataVolumeCap.Cap)
	return target
}

func (target *liveTelemetryTarget) appOverQuota(t *testing.T) bool {
	t.Helper()
	var status struct{ ShouldBeThrottled bool }
	require.NoError(t, target.arm(context.Background(), "GET", target.componentID+"/quotastatus", "2015-05-01", nil, "", &status))
	return status.ShouldBeThrottled
}

func waitLiveTelemetry(t *testing.T, ctx context.Context, interval time.Duration) {
	t.Helper()
	timer := time.NewTimer(interval)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		t.Fatal("no expected ingestion response within the bounded observation window")
	case <-timer.C:
	}
}

func liveTelemetryEvent(options telemetry.OptionAttributes) telemetry.JobStartedEvent {
	return telemetry.JobStartedEvent{
		Resource:   telemetry.ResourceAttributes{AzCopyVersion: "manual-live-test", SchemaVersion: telemetrySchemaVersion, InstallationID: "manual-live-test"},
		Dimensions: telemetry.JobDimensions{Command: "copy", Options: options},
		JobID:      "manual-live-test", InvocationID: fmt.Sprint(time.Now().UnixNano()), Timestamp: time.Now().UTC(),
	}
}

func (target *liveTelemetryTarget) send(t *testing.T, ctx context.Context, options telemetry.OptionAttributes) (liveTelemetryResponse, error) {
	t.Helper()
	reporter := telemetry.NewReporter(telemetry.Config{ConnectionString: target.connection, HTTPClient: target.client})
	before := len(target.client.snapshot())
	err := reporter.ReportEvent(ctx, liveTelemetryEvent(options))
	responses := target.client.snapshot()
	require.True(t, len(responses) == before+1, "no HTTP response: transport failure is not evidence of a server rejection: %v", err)
	return responses[len(responses)-1], err
}

func requireLiveAccepted(t *testing.T, response liveTelemetryResponse, err error) {
	t.Helper()
	require.NoError(t, err)
	require.Equal(t, http.StatusOK, response.Status)
	var result struct {
		ItemsReceived int
		ItemsAccepted int
		Errors        []json.RawMessage
	}
	require.NoError(t, json.Unmarshal(response.Body, &result))
	require.Equal(t, 1, result.ItemsReceived)
	require.Equal(t, 1, result.ItemsAccepted)
	require.Empty(t, result.Errors)
}

func TestLiveTelemetryInvalidInstrumentationKey(t *testing.T) {
	target := loadLiveTelemetryTarget(t, "shutdown")
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	parts := strings.Split(target.connection, ";")
	replaced := 0
	for index, part := range parts {
		if key, _, _ := strings.Cut(part, "="); strings.EqualFold(key, "InstrumentationKey") {
			parts[index] = key + "=" + uuid.NewString()
			replaced++
		}
	}
	require.Equal(t, 1, replaced, "connection string must have one instrumentation key")
	agent := liveTelemetryAgent(strings.Join(parts, ";"), target.client)
	response := liveTelemetrySend(t, ctx, agent, target.client)
	require.True(t, response.Status/100 == 4 || response.rejectsWith(http.StatusBadRequest), "unknown instrumentation key must be rejected; got HTTP %d", response.Status)
	requireLiveTelemetryStopped(t, agent, target.client)
}

func (target *liveTelemetryTarget) setLocalAuth(ctx context.Context, disabled bool) error {
	var component liveARMResource
	if err := target.arm(ctx, "GET", target.componentID, "2020-02-02", nil, "", &component); err != nil {
		return err
	}
	if component.Tags["purpose"] != "manual-telemetry-faults" || component.Tags["scenario"] != "shutdown" || component.Tags["environment"] != "test" {
		return fmt.Errorf("refusing to change authentication on a non-shutdown test resource")
	}
	component.Properties["DisableLocalAuth"], _ = json.Marshal(disabled)
	body := map[string]any{"location": component.Location, "kind": component.Kind, "tags": component.Tags, "properties": component.Properties}
	if err := target.arm(ctx, "PUT", target.componentID, "2020-02-02", body, component.ETag, nil); err != nil {
		return err
	}
	if err := target.arm(ctx, "GET", target.componentID, "2020-02-02", nil, "", &component); err != nil {
		return err
	}
	var actual bool
	if err := json.Unmarshal(component.Properties["DisableLocalAuth"], &actual); err != nil || actual != disabled {
		return fmt.Errorf("DisableLocalAuth readback did not match %t", disabled)
	}
	return nil
}

func TestLiveTelemetryEmergencyShutdown(t *testing.T) {
	target := loadLiveTelemetryTarget(t, "shutdown")
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Minute)
	defer cancel()
	response, err := target.send(t, ctx, telemetry.OptionAttributes{})
	requireLiveAccepted(t, response, err)
	agent := liveTelemetryAgent(target.connection, target.client)
	require.Equal(t, 200, liveTelemetrySend(t, ctx, agent, target.client).Status)
	require.True(t, agent.isActive())
	restored := false
	t.Cleanup(func() {
		if !restored {
			cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 2*time.Minute)
			defer cleanupCancel()
			if err := target.setLocalAuth(cleanupCtx, false); err != nil {
				t.Errorf("RESTORE FAILED: set DisableLocalAuth=false on %s: %v", target.componentID, err)
			}
		}
	})
	require.NoError(t, target.setLocalAuth(ctx, true))
	for {
		response = liveTelemetrySend(t, ctx, agent, target.client)
		if response.rejectsWith(401) || response.rejectsWith(403) {
			break
		}
		requireLiveAccepted(t, response, nil)
		waitLiveTelemetry(t, ctx, 15*time.Second)
	}
	requireLiveTelemetryStopped(t, agent, target.client)
	require.NoError(t, target.setLocalAuth(ctx, false))
	restored = true
	for {
		response, err = target.send(t, ctx, telemetry.OptionAttributes{})
		if response.Status == 200 {
			requireLiveAccepted(t, response, err)
			break
		}
		require.True(t, response.rejectsWith(401) || response.rejectsWith(403), "unexpected HTTP %d while waiting for restoration", response.Status)
		waitLiveTelemetry(t, ctx, 15*time.Second)
	}
	requireLiveTelemetryStopped(t, agent, target.client)
	fresh := liveTelemetryAgent(target.connection, target.client)
	require.Equal(t, 200, liveTelemetrySend(t, ctx, fresh, target.client).Status)
	require.True(t, fresh.isActive())
	t.Log("local authentication restored; stopped agent remains disabled; fresh agent sends successfully")
}

func TestLiveTelemetryResourceNameValidation(t *testing.T) {
	require.True(t, liveTelemetrySuffixPattern.MatchString("manual"))
	for _, suffix := range []string{"", "../shared", "shared?x=1", strings.Repeat("x", 13)} {
		require.False(t, liveTelemetrySuffixPattern.MatchString(suffix))
	}
}
