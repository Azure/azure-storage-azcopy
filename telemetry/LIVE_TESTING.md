# Manual Real-Endpoint Telemetry Tests

These tests send real, billable EventData to regional Application Insights
`/v2.1/track` endpoints. They do not use an HTTP mock, loopback server, or fabricated
ingestion responses. The quota and `shutdown` scenarios exercise the production
reporter and agent directly. The separate `cli-shutdown` scenario builds this
checkout's AzCopy executable and performs real Blob-to-local downloads through it.

The `telemetrylive` build tag and `AZCOPY_RUN_LIVE_TELEMETRY=1` are both required.
Normal unit/E2E pipelines do not run these tests. The standalone
[infrastructure pipeline](../telemetry-infrastructure.yml) has no push, PR, or
scheduled triggers and only provisions resources; it never invokes the live tests.

## Infrastructure

The [Bicep template](infra/manual-faults.bicep) creates three independent component
and workspace pairs, named `azcopy-telfault-<suffix>-<scenario>-ai` and `-law`:

| Scenario | Workspace daily cap | Application Insights daily cap |
| --- | --- | --- |
| workspace | 0.024 GB | 1 GB |
| application | 1 GB | 0.0323 GB |
| shutdown | 0.024 GB | 0.0323 GB |

Azure rejected a 0.01 GB workspace cap during live validation: the service requires
more than 0.023 GB. The template uses 0.024 GB instead.

The other quota is deliberately higher in each quota scenario to distinguish which
service enforced the limit. Workspace caps are defined in Bicep. Application
Insights caps use the separate `currentbillingfeatures` management API after the
deployment; they are not a property of the component resource. The runner verifies
both settings. Rejected or rounded settings cause provisioning to fail, not silently
fall back to larger limits. Sampling remains 100%.

Resources have `environment=test`, `purpose=manual-telemetry-faults`, and a scenario
tag. The runner rejects existing names without fault-test tags. Tests read both
resources through ARM and verify tags, caps, workspace linkage, local authentication,
sampling, and the real HTTPS ingestion host. They never use the embedded/default
connection string or normal E2E telemetry environment variables.

## Run Manually

Prerequisites: PowerShell 7, the repository's Go toolchain, Azure CLI with Bicep,
an existing Azure CLI login, and an existing resource group in public Azure. The
identity needs deployment and component/workspace read/write permissions including
`Microsoft.Insights/components/currentbillingfeatures/write`. Tests use
`AzureCLICredential` for ARM; telemetry deliberately uses the same tokenless
ingestion as AzCopy. No credentials are embedded or written to the evidence files.

Provision through the infrastructure pipeline, or locally:

```powershell
./testSuite/telemetry-live.ps1 -Provision `
  -SubscriptionId '<subscription-guid>' -ResourceGroup '<test-resource-group>' `
  -Suffix 'manual' -Location 'eastus'
```

Provisioning changes only the dedicated targets, leaves the low caps configured,
and enables their local authentication. It does not run a test. To execute:

```powershell
./testSuite/telemetry-live.ps1 -Run `
  -SubscriptionId '<subscription-guid>' -ResourceGroup '<test-resource-group>' `
  -Suffix 'manual' -Scenario all
```

`-Scenario` can also be `shutdown`, `workspace`, or `application`. `-Provision -Run`
combines both steps. Use a fresh lowercase alphanumeric/hyphen suffix (1-12
characters) for independent runs. Never provision or run concurrently against the
same suffix. The runner uses explicit subscription resource IDs without changing
the active CLI subscription, and restores its process environment variables.

## Full CLI Shutdown E2E

Run this scenario explicitly; `all` retains the three reporter/agent scenarios and
does not implicitly require a storage account or grant storage permissions:

```powershell
./testSuite/telemetry-live.ps1 -Run -Scenario cli-shutdown `
  -SubscriptionId '31347be8-d066-464e-9866-7e58d85027b7' `
  -ResourceGroup 'azcopy-telemetry-test-rg' -Suffix 'sep08' `
  -StorageAccountName 'ankursstorage' -GrantStoragePermission
```

Use a test storage account with Entra-authenticated container create/delete and
blob upload/read access. Omit `-GrantStoragePermission` if the identity already has
the necessary data permissions. That optional switch is an explicit authorization
to grant the signed-in user Storage Blob Data Contributor on only the newly created
test container; the runner resolves the user's object ID and the storage account
in the specified subscription. It requires role-assignment write/delete permissions
and waits up to five minutes for propagation. No account keys are used, and no
account-wide role is assigned.

The test creates a unique container and seeds a 16 MiB random file, a nested text
file, and an empty file. It launches independent CLI processes for these phases:

1. Baseline: download all three files and require successful started/finished
  telemetry diagnostics.
2. Auth disabled: set `DisableLocalAuth=true` on the isolated shutdown component,
  verify readback, and wait for a real authentication rejection BEFORE starting
  the CLI. Explicitly enable telemetry in the child environment so it cannot
  inherit the parent test runner's telemetry opt-out. Require exactly one
  `azcopy.job.started` process-shutdown diagnostic with HTTP 401/403 in its job log,
  and no successful lifecycle-send diagnostic in stderr. Timeouts, quota errors,
  or a rejected finish event do not satisfy this assertion.
3. Restored: restore local authentication and verify a fresh CLI downloads and
  sends both lifecycle events again. Up to five fresh processes are allowed if
  the transfer succeeds but a fresh connection still receives the exact auth
  rejection while configuration propagates; transfer failures and other telemetry
  errors fail immediately. Already stopped processes are never re-enabled.

Every CLI transfer must exit 0, report a JSON `EndOfJob` summary with `Completed`,
three completed file transfers, zero failures/skips, and exactly 16,777,256 bytes.
All downloaded files are checked with SHA-256 against the seeded data. The download
uses `--cap-mbps=16 --block-size-mb=1` so the chunk size fits the pacing budget.

The evidence directory retains the built binary, JSON test log, and a unique
subdirectory with stdout, stderr, job logs, plan files, isolated user directories,
and downloaded files for each phase. Azure CLI authentication still uses the
original `AZURE_CONFIG_DIR`. The test restores server authentication and deletes
only its container and optional temporary role, including on assertion failure.
Forced termination can prevent cleanup: use the logged container/role IDs to remove
those test resources, and restore local authentication on the isolated component.

This is full CLI coverage of one Blob-to-local copy workflow, not every AzCopy
command. Pipeline shutdown is checked through the real CLI's auth-specific stop
diagnostic and absence of successful sends; internal collector gating and queued
send cancellation remain covered by the lower-level tests. It does not claim
that no collection occurs before the first rejected telemetry request.

## Assertions And Evidence

- Quota: require a healthy HTTP 200 baseline with one accepted item; fill using
  production serialized started events with synthetic padding; observe HTTP 439
  (or a 206 containing only 439 item rejections); verify the corresponding ARM
  quota state and that the other resource's cap is not exhausted. Then two fresh
  agents must each receive a real quota rejection and suppress subsequent sends
  and dimension collection. A 429, 5xx, timeout, or missing log row is not a pass.
- Emergency shutdown: verify healthy ingestion; set `DisableLocalAuth=true` and
  verify ARM readback; wait for a real 401/403 and verify agent shutdown; restore
  `DisableLocalAuth=false`; wait for successful ingestion again. The stopped agent
  must remain stopped while a fresh agent can send. Cleanup attempts restoration
  even after assertion failures, using an independent context.
- Each quota case has a 20-minute observation window. Padding stops after about
  50 MiB of request bodies; small probes continue every 15 seconds. The observing
  HTTP client refuses to exceed 64 MiB of total request bodies per scenario.
  Emergency shutdown has a 15-minute window. These bounds are not billing caps.
- New components or policy changes may take time to propagate. Baseline failures,
  prior quota exhaustion, missing ARM permissions, and transport errors fail
  explicitly. Use a fresh suffix or wait for the daily reset before rerunning a
  quota case. Redeploying does not reset already-consumed daily usage.
- If a workspace becomes OverQuota but Application Insights continues returning
  200, the test fails at its deadline. That is important evidence that this quota
  is not a usable client-side shutdown signal, not a reason to fabricate a rejection.

The runner prints its evidence directory and writes per-scenario Go JSON test logs
including actual HTTP status, request bytes, quota state, assertions, and cleanup
failures. It does not record tokens, connection strings, or raw response messages.
The tests retain their resource pairs and synthetic records for investigation;
delete the dedicated resources after use if they are no longer needed.

Forced process termination can prevent Go cleanup. If interrupted, inspect the
shutdown component and restore its local authentication to Enabled before rerunning.
Do not reset unrelated component settings or remove the shared E2E resources.

## Local Validation

```powershell
go test -tags telemetrylive ./azcopy -run '^TestLiveTelemetry(ResponseClassification|ResourceNameValidation|CLIContract)$' -count=1
```

The pure checks require no server or Azure login. Without the explicit opt-in,
the quota/shutdown/CLI tests skip before obtaining credentials or making any request.

## Live Validation Record

On 2026-09-08, the `sep08` targets were deployed in East US under
`azcopy-telemetry-test-rg`. ARM readback confirmed the caps in the table above.
The normal `azcopy-telemetry-test-ai` and `azcopy-telemetry-test-law` resources
were outside the deployment changes.

The emergency-shutdown test passed in about 53 seconds: a healthy HTTP 200,
HTTP 401 after `DisableLocalAuth=true` propagated, and HTTP 200 after restoring
local authentication. The original agent remained stopped; a fresh agent sent
successfully. This is an observed response, not a simulated one.

The full CLI `TestLiveTelemetryCLIEmergencyShutdown` subsequently passed in about
246 seconds, including temporary RBAC propagation and cleanup. Baseline,
auth-disabled, and restored CLI processes each downloaded three files (16,777,256
bytes), exited 0 with `Completed`, and passed SHA-256 checks. The auth-disabled
process logged a job-start telemetry shutdown caused by HTTP 401. Baseline and
restored processes sent both lifecycle events. Independent readback confirmed
`DisableLocalAuth=false`, the temporary container absent, and its role assignment
removed. The existing shared E2E component was not changed.

Earlier attempts exposed a too-large default download chunk for the chosen pacer
limit, and a stale auth rejection on a fresh connection after a warmed connection
had already recovered. The explicit block size and bounded fresh-process recovery
check above address those fixture/propagation issues without changing production code.

An initial quota fixture underfilled the cap because the production reporter
truncated property values. The corrected fixture uses more synthetic properties
and checks its actual serialized size (42,911 bytes observed) against 32-60 KiB
bounds. A timeout or a run that underfills the cap is not a passing quota test.

Both corrected quota experiments failed the requested HTTP-rejection assertion:

| Scenario | Observation | Result |
| --- | --- | --- |
| Application Insights | HTTP 200 throughout the 20-minute window; approximately 50 MiB of request bodies in the corrected run; ARM `ShouldBeThrottled=false` on subsequent readback | No quota rejection demonstrated |
| Workspace | ARM changed to `OverQuota` after approximately 30 MB of request bodies; HTTP 200 continued through the 20-minute deadline; final request-body total 52,469,477 bytes; Application Insights `ShouldBeThrottled=false` | Workspace quota reached, but no client-visible rejection |

A query of the isolated Application Insights target's workspace after its two
runs found 4,049 events with `_IsBillable="True"` and a total `_BilledSize` of
66,701,887 bytes. Request-body volume alone is not Azure's billable-volume measure.
Use a case-insensitive comparison or `tobool(_IsBillable)` when querying this flag.

These results do not prove that quotas never reject, nor that a cap cannot take
effect later. They do show that these daily caps cannot currently be relied upon
as a prompt HTTP shutdown signal for AzCopy. The tests intentionally remain failing
when that requested signal is absent; a workspace `OverQuota` flag or missing rows
must not be substituted for an HTTP rejection. Use the independently verified
local-authentication switch for an operator-triggered shutdown.

## References

- [Daily caps, billing overshoot, and reset behavior](https://learn.microsoft.com/en-us/azure/azure-monitor/logs/daily-cap)
- [Disable local authentication](https://learn.microsoft.com/en-us/azure/azure-monitor/app/azure-ad-authentication#disable-local-authentication)
- [Application Insights authentication response codes](https://learn.microsoft.com/en-us/troubleshoot/azure/azure-monitor/app-insights/telemetry/investigate-missing-telemetry#troubleshoot-microsoft-entra-authentication-issues)
- [Billing settings and quota status REST schema](https://github.com/Azure/azure-rest-api-specs/blob/main/specification/applicationinsights/resource-manager/Microsoft.Insights/ApplicationInsights/stable/2015-05-01/componentFeaturesAndPricing_API.json)
- [Official SDK response codes, including 439](https://github.com/microsoft/ApplicationInsights-dotnet/blob/2.22.0/BASE/src/ServerTelemetryChannel/Implementation/ResponseStatusCodes.cs)