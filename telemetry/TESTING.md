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
- Failure-mode cases 2, 3, and 4 now latch process telemetry off: transport errors (including DNS, TLS, offline, and request timeout/cancellation), ingestion rejection or partial acceptance (HTTP 206, 4xx/5xx), and throttling (429/503). The agent cancels active request contexts, suppresses queued and future lifecycle/command events, and stops existing source-shape trackers and subsequent telemetry collection. A failed start suppresses its finish. Already transmitted events cannot be recalled, and cancellation-ignoring custom clients cannot be forcibly terminated. There is no retry, cooldown, or automatic recovery; a new AzCopy process starts with fresh telemetry state. Transfer errors themselves do not disable telemetry.
- Local configuration/serialization errors and recoverable send panics remain event-local; they are not typed delivery failures. The first delivery failure logs a safe `telemetry: disabled for this process after delivery failure sending ...` diagnostic. Cascading request cancellations do not repeatedly log the stop transition.
- Diagnostics exclude response messages/bodies, raw transport errors, configuration values, and panic contents. Path/SAS canaries are checked in serialized lifecycle events and command events and in telemetry-generated job log messages. Recognized Azure Storage account names remain allowed.

## Resource Budgets

Each agent admits at most four events (including sends waiting for a matching start). Admission is nonblocking; excess events are dropped. Sends have a five-second deadline; a finish waiting for its start gets up to five additional seconds for ordering, but is suppressed if that start has a delivery failure. The independent process-exit flush budget is four seconds and can cancel a send before its five-second deadline. Flush still drains admitted work within that budget after the stop state is set and cancels admitted requests on timeout. A custom HTTP client that ignores cancellation can occupy at most those four slots; Go cannot forcibly stop such a client. The runtime tests exercise that case with 1,000 additional sends and flushes. A slow-success regression verifies an ordered start/finish pair survives exit flush when the start takes longer than two seconds.

Source-shape tracking retains at most 128 names per scanned/touched set and 256 bytes per name, cloning admitted names so a substring cannot retain a larger path buffer. Once a set exceeds either limit, its internal count is `-1`; the wire measurement is omitted and status metadata marks it unavailable. Object, byte, depth, and histogram measurements continue. Consumers must also exclude historical negative sentinels from totals. Representative serialized envelopes have 4 KiB started and 8 KiB finished regression budgets; these are fixture gates, not a universal cap for arbitrary custom reporter callers.

Metadata and identity probing remain synchronous and use their existing per-probe deadlines/retry bounds. These limits do not establish an end-to-end 100 KiB heap or 1% CPU/throughput guarantee; the separate manual performance suite measures that budget for documented workloads.

## Focused Go Checks

Run from the repository root:

```powershell
go test ./telemetry -count=1
go test ./azcopy -run 'Test(Telemetry|AttemptTelemetry|DisabledAgent|TerminalAttempt|JobError|SanitizeJobError|StorageAccount|.*JobDimensions|ConfiguredTelemetryConnectionString|EndpointCloudType)' -count=1
go test ./e2etest -run 'Test(TelemetryManifest|AzCopyJobIDCapture|AppInsightsVerifier|BuildFinishedEventQuery|ParseFinishedEventCounts|ExpectedAppInsights|ValidateAppInsights)' -count=1
```

These are offline unit checks. They do not launch the credential-dependent cloud transfer E2E suite.

## Delivery Failure Tests

Unit-level integration tests use the real reporter/dispatcher with local `httptest` endpoints for rejection, partial acceptance, 429/503 throttling, and concurrent-request cancellation. DNS, TLS, and offline failures are injected through the HTTP transport; a blocked client exercises deadline expiry. Tests verify a queued finish never reaches the endpoint after a failed start, later events remain disabled even if the endpoint would recover, existing collectors stop, and healthy ingestion still reports failed transfers. Reporter tests verify typed classification through both backends and preserve safe diagnostics. No Application Insights resource, credentials, or ingestion polling is required for these policy tests.

## Manual Real-Endpoint Fault Tests

See [manual live testing](LIVE_TESTING.md) for real Application Insights quota and
emergency-shutdown tests. These use separate low-cap components/workspaces, never
mock HTTP responses, and require both the `telemetrylive` build tag and explicit
manual opt-in. The separate infrastructure pipeline provisions targets only; the
normal E2E matrix and shared telemetry destination are unchanged.

## Manual Performance Gate

The separate [performance pipeline](../telemetry-performance.yml) has no push, PR, or scheduled triggers and is not referenced by the E2E pipeline. See [measurement methodology and gates](PERFORMANCE.md) for isolated paired runs, statistical classification, artifact contents, and known limitations. Measurement tests require both the `telemetryperf` build tag and an explicit opt-in environment setting.

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

The deployment test is entirely offline. Fourteen mocked cases also include partial same-day loss, resume date migration, and explicit reconciliation. Historical aggregate keys are compared at their full persisted dimensional grain; missing or reduced counts block publication even if their reporting day survives. Legitimate date/status/enrichment changes can also reduce keys, so operators must reconcile those changes before explicitly using `-AllowHistoricalReductions`. Approval cannot bypass empty, non-finite, negative, or malformed data. Because the persisted table is aggregated, equal or greater totals can still conceal compensating job-level changes; this guard is not durable job-level retention. This is not a live destructive fault-injection test or proof of the outcome of a lost response after a server-side commit.

## Installation Identity Lock

`installation_id.lock` serializes first-time creation or repair of `installation_id` across processes sharing an AzCopy application-data directory. Without it, simultaneous processes can generate different random IDs and emit telemetry with an ID that loses the persistence race. The winning writer rechecks the file and publishes a complete ID through a temporary file and rename; readers reuse a valid existing ID without taking the lock.

The file remains on disk; ownership uses a nonblocking OS file lock (`LockFileEx` on Windows, `flock` on Unix), released when the handle closes or the process dies. Contenders retry up to 100 times at 10 ms intervals. A process-kill test leaves a malformed identity, then verifies eight concurrent processes recover one stable ID. The lock is not a transfer or credential lock. Older binaries using existence-based locking do not participate in this protocol; mixed-version concurrent first-time identity creation is not supported.

## Review Corrections

CLI categorical values use per-option allowlists before event construction. Invalid free-form strings, URLs, and secrets cannot become category properties; numeric capture rejects NaN and infinity. Resumed job-cumulative summaries retain byte/object counters but omit attempt throughput, with `ThroughputStatus=unavailable-cumulative-summary`. Scope overflow omits unavailable numeric counts and sets `SourceScopeCountsStatus=incomplete`; inventory dashboards exclude historical negative sentinels and report unavailable-attempt counts alongside known totals.

The E2E manifest explicitly records zero-event expectations for opted-out commands and dry-run transfers, including the child environment override. Negative expectations observe the full configured polling window and require successful queries; an initially empty response is not enough to pass. This remains a bounded observation, not proof that a much later event cannot arrive.