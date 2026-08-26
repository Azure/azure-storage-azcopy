package e2etest

import (
	"io"
	"net/http"
	"net/url"
	"strings"

	"github.com/Azure/azure-storage-azcopy/v10/common"
)

func init() {
	suiteManager.RegisterSuite(&FileShareStatsTestSuite{})
}

// FileShareStatsTestSuite exercises common.ShareStatsProvider / azfileShareStatsSource
// against a live Azure Files share.
//
// The unit tests in common/azfileShareStatsSource_test.go drive the same code over an
// httptest loopback server with a hand-written XML fixture. That covers the HTTP call,
// the XML unmarshal, and the prevSample/counter-reset state machine, but it cannot
// cover the one thing that actually breaks in production: whether the service's real
// response matches the xml struct tags in common/azfileShareStatsTypes.go.
//
// That failure mode is silent. If the schema drifts, xml.Unmarshal leaves
// ShareStatsResponse.ThrottlingStats nil, and azfileShareStatsSource.PollStats maps nil
// to a zero-valued ResourceStats -- which the rate limit controller reads as "unlimited,
// no throttles". Throttling turns itself off with no error and no log line, and every
// loopback unit test stays green.
//
// So the assertions below are deliberately about the *content* of the parsed response,
// not just about the call succeeding.
//
// # Running these
//
// Requires a real premium (Kind=FileStorage) storage account. The share itself is created
// and deleted by the framework; you only supply the account.
//
//	export NEW_E2E_PREMIUM_FILESHARE_ACCOUNT_NAME='<premium acct>'
//	export NEW_E2E_PREMIUM_FILESHARE_ACCOUNT_KEY='<key>'
//	export NEW_E2E_AZCOPY_PATH='<path to azcopy binary>'   # required by config; unused here
//	export NEW_E2E_STATIC_TENANT_ID='<tenant guid>'
//	export NEW_E2E_STATIC_CLI_INHERIT=true                 # else SetupOAuthCache fails
//	unset NEW_E2E_SUBSCRIPTION_ID                          # selects static-account mode
//
//	go test ./e2etest/ -v -timeout 30m \
//	  -run 'TestNewE2E/FileShareStatsTestSuite/Scenario_GetShareStatsPremiumShare'
//
// Add NEW_E2E_STANDARD_ACCOUNT_NAME/_KEY and drop the scenario name from -run to also run
// Scenario_GetShareStatsStandardShare. Keep -v: the raw XML is logged, which is the point.
//
// Known framework issue: DeleteCreatedResources (newe2e_scenario_variation_manager.go)
// requires both Delete() and EntityType(); FileShareResourceManager lacks EntityType, so
// created shares are silently skipped at cleanup and leak. Harmless in dynamic mode (the
// whole account is torn down), but in static mode shares accumulate -- delete them by hand.
type FileShareStatsTestSuite struct{}

// shareStatsScenarioLogger adapts the scenario asserter to common.ILogger so provider
// diagnostics land in the test output.
type shareStatsScenarioLogger struct{ a Asserter }

func (l shareStatsScenarioLogger) ShouldLog(_ common.LogLevel) bool { return true }
func (l shareStatsScenarioLogger) Log(_ common.LogLevel, msg string) {
	l.a.Log("[GetShareStats] %s", msg)
}
func (l shareStatsScenarioLogger) Panic(err error) { l.a.Error(err.Error()) }

// getShareStatsRaw issues the same request ShareStatsProvider.FetchStats issues, and
// returns the untouched response body. Used to assert on the wire format and to capture
// a real fixture for the loopback unit tests.
func getShareStatsRaw(a Asserter, shareURLWithSAS string) (status int, body string) {
	a.HelperMarker().Helper()

	parsed, err := url.Parse(shareURLWithSAS)
	a.NoError("parse share URL", err, true)

	parsed.Path = strings.TrimRight(parsed.Path, "/")
	q := parsed.Query()
	q.Set("restype", "share")
	q.Set("comp", "stats")
	parsed.RawQuery = q.Encode()

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, parsed.String(), nil)
	a.NoError("build GetShareStats request", err, true)

	req.Header.Set("x-ms-file-return-throttling-stats", "true")
	req.Header.Set("x-ms-version", enum.EEnvironmentVariable.DefaultServiceApiVersion().Get())

	resp, err := http.DefaultClient.Do(req)
	a.NoError("execute GetShareStats request", err, true)
	defer resp.Body.Close()

	raw, err := io.ReadAll(resp.Body)
	a.NoError("read GetShareStats body", err, true)

	return resp.StatusCode, string(raw)
}

// Scenario_GetShareStatsPremiumShare is the load-bearing test. A premium file share is
// the only SKU that returns ShareThrottlingStats, so it is the only place the parsing
// path can be validated for real.
func (s *FileShareStatsTestSuite) Scenario_GetShareStatsPremiumShare(svm *ScenarioVariationManager) {
	shareRM := CreateResource[ContainerResourceManager](svm,
		GetRootResource(svm, common.ELocation.File(), GetResourceOptions{
			PreferredAccount: pointerTo(PremiumFileShareAcct),
		}),
		ResourceDefinitionContainer{})

	if svm.Dryrun() {
		return
	}

	shareURL := shareRM.URI(GetURIOptions{AzureOpts: AzureURIOpts{WithSAS: true}})

	// --- Wire format -------------------------------------------------------
	// Assert on the raw XML before involving the parser, so a schema break reports
	// as "the block is missing" rather than as a confusing zero-valued ResourceStats.
	status, body := getShareStatsRaw(svm, shareURL)
	svm.AssertNow("GetShareStats should return 200", Equal{}, status, http.StatusOK)

	// Logged so the response can be lifted verbatim into newFakeShareStatsServer in
	// common/azfileShareStatsSource_test.go. Today that fixture is hand-written by the
	// same author as the struct tags, so it agrees with them by construction rather
	// than because it matches what the service emits.
	svm.Log("raw GetShareStats response body:\n%s", body)

	svm.AssertNow("premium share response must contain a ShareThrottlingStats block "+
		"(absent => x-ms-file-return-throttling-stats was not honored)",
		Equal{}, strings.Contains(body, "<ShareThrottlingStats>"), true)

	// --- Parsed contract ---------------------------------------------------
	src := common.NewAzfileShareStatsSource(shareURL, http.DefaultClient, shareStatsScenarioLogger{svm})

	stats, err := src.PollStats()
	svm.NoError("first PollStats", err, true)

	// These two are the schema-fidelity assertions. A tag mismatch, an added XML
	// namespace, or a casing change all land here as zero.
	svm.Assert("IopsLimit must be non-zero -- zero means the throttling block failed "+
		"to parse and the controller would treat the share as unlimited",
		Equal{}, stats.IopsLimit > 0, true)
	svm.Assert("BandwidthLimitBytesPerSec must be non-zero for the same reason",
		Equal{}, stats.BandwidthLimitBytesPerSec > 0, true)

	svm.Log("live premium share limits: IopsLimit=%d BandwidthLimitBytesPerSec=%d "+
		"IopsThrottleCount=%d BandwidthThrottleCount=%d",
		stats.IopsLimit, stats.BandwidthLimitBytesPerSec,
		stats.IopsThrottleCount, stats.BandwidthThrottleCount)

	// --- Second poll -------------------------------------------------------
	// Exercises the prevSample branch against real StartTime/EndTime values. This is
	// not redundant with the loopback test: ShareThrottlingStats.StartTime is a
	// time.Time, and encoding/xml only accepts RFC3339 for that. If the service emits
	// any other layout, FetchStats fails at unmarshal -- which no fixture can reveal.
	stats2, err := src.PollStats()
	svm.NoError("second PollStats", err, true)

	svm.Assert("IopsLimit should be stable across polls on an idle share",
		Equal{}, stats2.IopsLimit, stats.IopsLimit)
	svm.Assert("BandwidthLimitBytesPerSec should be stable across polls on an idle share",
		Equal{}, stats2.BandwidthLimitBytesPerSec, stats.BandwidthLimitBytesPerSec)
}

// Scenario_GetShareStatsStandardShare pins the documented non-premium behavior against
// the real service. A standard share returns no throttling block, and PollStats maps
// that to an all-zero ResourceStats.
//
// This is the same observable result as a schema break on a premium share, which is
// exactly why Scenario_GetShareStatsPremiumShare asserts on non-zero limits: without
// that, the two cases are indistinguishable.
func (s *FileShareStatsTestSuite) Scenario_GetShareStatsStandardShare(svm *ScenarioVariationManager) {
	shareRM := CreateResource[ContainerResourceManager](svm,
		GetRootResource(svm, common.ELocation.File()),
		ResourceDefinitionContainer{})

	if svm.Dryrun() {
		return
	}

	shareURL := shareRM.URI(GetURIOptions{AzureOpts: AzureURIOpts{WithSAS: true}})

	status, body := getShareStatsRaw(svm, shareURL)
	svm.AssertNow("GetShareStats should return 200 on a standard share", Equal{}, status, http.StatusOK)
	svm.Log("raw GetShareStats response body (standard share):\n%s", body)

	src := common.NewAzfileShareStatsSource(shareURL, http.DefaultClient, shareStatsScenarioLogger{svm})

	stats, err := src.PollStats()
	svm.NoError("PollStats on a standard share should not error", err, true)

	svm.Assert("standard share reports no IOPS limit", Equal{}, stats.IopsLimit, int64(0))
	svm.Assert("standard share reports no bandwidth limit", Equal{}, stats.BandwidthLimitBytesPerSec, int64(0))
}
