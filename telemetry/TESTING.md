# Telemetry Regression Tests

## Destination Configuration

This feature branch embeds the connection string for `azcopy-telemetry-test-ai`
in `azcopy-telemetry-test-rg` as a temporary default. Local and platform builds
use that same source configuration; no telemetry linker injection is required.
Set `AZCOPY_TELEMETRY_CONNECTION_STRING` before launching AzCopy to override it.
An unset or blank value falls back to the test default. Configuration is cached
when the process first initializes telemetry. Both `--disable-telemetry` and
`AZCOPY_DISABLE_TELEMETRY=true` still suppress initialization and sends.

Do not ship this temporary destination as a production default. Offline tests
that invoke production CLI paths must opt out or supply a loopback override.
The E2E pipeline still resolves its test component and sets the runtime override.

## Runtime Contract

- Copy and sync start lifecycle telemetry after executor creation and before enumerator initialization. Enumerator initialization errors finish the attempt with `TerminalStage=initialization`.
- Resume service-client creation failures emit an initialization-failure pair once a valid plan and parsed endpoints exist. Invalid arguments, unsupported benchmark resumes, unavailable plans, endpoint parsing failures, and copy/sync executor-construction failures remain outside this boundary.
- Dry runs, pipe redirection, and benchmark cleanup do not emit copy lifecycle events.
- Initialization/host-metadata panics disable the process telemetry agent. Dimension-collection panics drop the attempt's telemetry. Summary, duration, shape, and event-construction panics are contained; an affected finish event can be lost.
- Send failures, including HTTP rejections, throttling, transport errors, and panics, drop only the failed event. There is no retry or send-failure circuit breaker. A failed start does not prevent a later finish. This intentionally differs from a policy that disables the rest of the pipeline on any network failure.
- Diagnostics exclude response messages/bodies, raw transport errors, configuration values, and panic contents. Path/SAS canaries are checked in serialized lifecycle events and command events and in telemetry-generated job log messages. Recognized Azure Storage account names remain allowed.

## Focused Go Checks

Run from the repository root:

```powershell
go test ./telemetry -count=1
go test ./azcopy -run 'Test(Telemetry|AttemptTelemetry|DisabledAgent|TerminalAttempt|JobError|SanitizeJobError|StorageAccount|.*JobDimensions|ConfiguredTelemetryConnectionString|EndpointCloudType)' -count=1
go test ./e2etest -run 'Test(TelemetryManifest|AzCopyJobIDCapture|AppInsightsVerifier|BuildFinishedEventQuery|ParseFinishedEventCounts|ExpectedAppInsights|ValidateAppInsights)' -count=1
```

These are offline unit checks. They do not launch the credential-dependent cloud transfer E2E suite.

## E2E Manifest

When Application Insights verification is enabled, each child process receives a unique `AZCOPY_E2E_TELEMETRY_RUN_ID` under the configured run ID. The root run ID is limited to 80 bytes to leave room for that suffix.

The manifest derives JobID, command, and endpoint types from the harness, and available terminal counters/status from synchronous JSON stdout capture. It requires exactly one start and one finish per expected process, nonempty and matching installation/invocation IDs within a pair, and invocation IDs unique across processes. It checks schema, supplied dimensions, lifecycle counters, resume cumulative scope, and supplied terminal counters/status. A repeated JobID cannot satisfy a missing resume process. Arrival order is not constrained.

InvocationID and InstallationID are validated from observed events, not independently predicted by the harness. Text-only output without a JSON final summary cannot provide exact terminal counter expectations. Missing pairs are polled within a bounded query context; duplicates and inconsistent data in a returned snapshot fail immediately. Duplicates arriving after successful verification are outside that observation window. This verifies delivery in the E2E environment, not a production delivery guarantee.

## Aggregate Replay

```powershell
./dashboards/AzCopyQueries/test-aggregate-replay.ps1
./dashboards/AzCopyQueries/test-deployment.ps1
```

The replay script requires an existing Azure CLI login and Kusto query access. It inlines the checked-in producer into a read-only query, replaces all three external sources with synthetic tables, and checks 18 assertions: duplicate/repeated refresh idempotence, cumulative resumes across day/month boundaries, late events, inclusive lookback boundaries, outside-lookback exclusions, schema-3 account fields present/absent/empty/restored, and ownership/customer fallbacks. `-QueryOnly` emits the generated query without authenticating or executing it. No saved function, live source, or persisted aggregate table is read or modified.

ExecutionTime is excluded from idempotence comparisons. Resumes attribute cumulative usage to the latest attempt's day, not incremental bytes to each attempt day. A late resume can move a job out of an earlier reporting window. History older than the 30-day attempt lookback cannot restore its original command or attempt count.

The deployment test is entirely offline. Ten mocked cases check successful publication, invalid/empty/missing validation data, lost reporting days, stage/query failures, partial query failures, and a failed publication request. Earlier failures must not attempt replacement; simulated publication failure preserves the prior rows. Staging cleanup is attempted after every staging request. This is not a live destructive fault-injection test or proof of the outcome of a lost response after a server-side commit. The existing day-preservation guard also does not detect every possible partial loss within a retained day.

## Installation Identity Lock

`installation_id.lock` serializes first-time creation or repair of `installation_id` across processes sharing an AzCopy application-data directory. Without it, simultaneous processes can generate different random IDs and emit telemetry with an ID that loses the persistence race. The winning writer rechecks the file and publishes a complete ID through a temporary file and rename; readers reuse a valid existing ID without taking the lock.

The lock is an exclusively-created file, not a transfer or credential lock. Contenders retry up to 100 times at 10 ms intervals. A killed writer can leave a stale lock; if no valid identity exists, callers eventually return an empty ID. Crash recovery for that case is not changed by these tests.