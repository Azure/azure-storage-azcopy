# AzCopy Telemetry Metrics Feasibility Assessment

**Date:** 2026-07-21
**Source:** [StorageMoverAzCopyComparison.md](StorageMoverAzCopyComparison.md)
**Implementation reviewed:** `feature/telemetry` at `e16e7f82`, including current staged and unstaged workspace changes

## Executive Summary

**No, not everything in the comparison document is implemented.** The document mixes existing AzCopy capabilities, proposed Storage Mover investments, a new AzCopy-to-Storage-Mover nudge, client telemetry signals, and downstream business analytics. The current worktree implements much of the client-side telemetry, but the nudge, several requested signals, backend enrichment, dashboards, and support-system integrations are absent.

Most aggregate transfer signals requested in the comparison document are technically feasible without adding more telemetry requests. AzCopy can collect counters throughout enumeration and transfer, then send one start event and one aggregate finish event. Feasibility is not the same as current end-to-end implementation: this repository contains the AzCopy client emitter, not the proposed identity-enrichment, ingestion, dashboard, Storage Mover, or support-system work.

The current branch already emits basic host, endpoint, option, volume, outcome, and performance data for normal `copy` and `sync` completions. It is not yet sufficient for the complete business plan because:

1. Copy/sync/benchmark/resume attempts emit paired events only after options, endpoint dimensions, and enumerators are initialized. Observable failures after that point and explicit cancellation receive terminal events; earlier validation/initialization failures and hard process termination remain outside the paired lifecycle.
2. Main benchmark jobs now use paired attempt events with `Command=bench`, effective benchmark inputs, generic terminal results, and performance diagnostics. Automatic cleanup jobs are excluded.
3. Source transfer shape, enumeration/transfer duration, and resume outcomes are emitted. Finalization duration and comprehensive retry metrics remain absent.
4. Decision #3 limits the client payload to normalized Azure storage account names. AzCopy does not send S3/GCS bucket names, endpoint hostnames, tenant ID, or subscription ID. The proposed trusted-backend mapping from Azure storage account name to subscription and tenant is not implemented in this repository, so subscription/tenant reporting is not currently end-to-end available.
5. Geography is derived by Application Insights from the telemetry sender IP during ingestion; it is not emitted by AzCopy and is not the storage account's region. Account identity, derived geography, and host attributes still require privacy review and access controls.
6. Deterministic 1% `JobID` sampling is implemented with schema/rate/unit/sampler metadata. It is suitable for population estimates but cannot support exact per-customer histories or adoption funnels.
7. Supportability outcomes depend on support systems and user journeys outside the AzCopy process. Client telemetry alone cannot determine them.
8. Local builds fail closed with no embedded connection string. E2E and official artifact templates inject separate Test/Prod secure variables; pipeline owners must populate those variables and verify ingestion before release.

## Document-Wide Implementation Verdict

| Comparison-document section | Current implementation status | Finding |
| --- | --- | --- |
| Source-target comparison | Existing AzCopy behavior; Storage Mover parity not verifiable here | The table primarily documents product capabilities rather than requesting new AzCopy code. AzCopy's `FromTo` handling exists, but Storage Mover implementation and roadmap claims are outside this repository. |
| Migration features in AzCopy | Largely existing in AzCopy; Storage Mover work is external | The reviewed AzCopy flags and environment controls are registered in the current CLI, including filters, dates, blob type/tier, headers, CPK, overwrite, PUT sizing, MD5, concurrency, buffer size, and sync hash comparison. Whether Storage Mover exposes the corresponding settings cannot be established from this codebase. |
| In-product Storage Mover nudge | **Not implemented** | No production Go code contains the proposed message, a Storage Mover destination link, transfer-size policy, or source/destination eligibility logic. The only matches are in design/comparison Markdown. |
| Per-run telemetry data points | **Partially implemented** | Host, options, endpoints, source shape, outcomes, timing, throughput, benchmark diagnostics, and bounded errors are substantially covered. The confirmed gaps are listed below and in the detailed tables. |
| Derived business metrics | **Partially derivable, not end-to-end implemented** | AzCopy emits inputs for many sampled population metrics. This repository does not implement ingestion weighting, account-to-subscription/tenant enrichment, dashboards, longitudinal customer cohorts, or support-system joins. |
| Review comments | Not an implementation checklist | The preserved comments contain open questions and conflicting proposals; they should not be treated as accepted requirements without separate decisions. |

### Confirmed Current Gaps

| Gap | Current-code evidence and impact |
| --- | --- |
| Storage Mover nudge | No production implementation or final `aka.ms` destination exists. |
| Early attempt coverage | `copy` and `sync` create the paired-event finalizer only after transfer executor and enumerator initialization. Failures before that point emit no job attempt. Dry runs also emit no paired events. |
| Hard termination | A killed process cannot emit `job.finished`; abandonment needs ingestion-side timeout inference, which is not implemented here. |
| Container/bucket sizes | Counts are emitted, but aggregate or distributional container/bucket sizes are not. |
| Subscription, tenant, and storage region | Subscription and tenant are intentionally absent from the client and the proposed server enrichment is outside this repository. Storage-account region is not collected. |
| Public/private network bytes | `DestEndpointKind` only checks for `.privatelink.` in the destination hostname and bytes are not split by actual route. Private DNS using a public hostname is indistinguishable. |
| Retry and phase detail | 503 and network-attempt counters exist, but comprehensive SDK/body-read retry count and retry bytes do not. Enumeration and transfer timers exist; a separate finalization timer does not. |
| Supportability workflow | Diagnostic bundle/upload, case/ICM linkage, remediation attempt, and resolution/escalation outcomes are not implemented. |
| Exact customer history and funnels | The 1% deterministic `JobID` sample does not retain all jobs for an account/subscription, so exact recency, first-job, second-job, and time-to-first-success metrics cannot be reconstructed. |
| Benchmark description text | Result status, stable advice codes, and diagnostics are emitted, but the requested human-readable description is intentionally excluded. |

The recommended release scope is population-level, account-level, and server-enriched subscription/tenant-level usage, performance, and reliability reporting. For the business assessment, “per customer” is operationalized as **per subscription**, with optional tenant-level rollups. Longitudinal funnels remain limited by `JobID` sampling, and supportability reporting still requires external joins.

## Assessment Legend

| Rating | Meaning |
| --- | --- |
| Available | Emitted by the current branch for the relevant successful path. |
| Low | Source data already exists; primarily schema and wiring work. |
| Moderate | Requires new bounded counters or lifecycle instrumentation, but no new service dependency. |
| High | Requires cross-layer changes, durable state, network inspection, or a control-plane dependency. |
| Partial | Some requested meaning is available, but the current value has narrower semantics. |
| External | Requires another product or service as the system of record. |
| Not reliable | AzCopy cannot determine the value consistently for all supported authentication and endpoint types. |
| Clarify | The requested term needs a precise definition before implementation. |

## Required Design Decisions

These decisions affect several metrics and should be closed before the schema is treated as final.

### Define the Unit of Observation

- **Decision: the canonical unit of observation is a job attempt.** The original invocation is one attempt, and each resume invocation is a new attempt. Failed validation/enumeration, explicit cancellation, and other observable terminal failures are attempts even when no transfer is completed.
- **`InvocationID`:** a new unique identifier generated for each attempt. The attempt's start and finish events carry the same `InvocationID`.
- **`JobID`:** the existing AzCopy job identifier. The original attempt and every resumed attempt carry the same `JobID`, allowing attempts for one resumable job to be correlated without merging their measurements.
- **Logical job:** an analytical grouping of attempts with the same `JobID`, not the emitted unit of observation.
- **Transfer:** a scheduled file/object transfer. Folder-property transfers should not be counted as files.
- **Dataset shape:** all source objects scanned, or only objects selected and scheduled. For sync these are materially different.
- **Account identity:** the normalized Azure storage account name allowed in the client payload by Decision #3. It is not itself a customer, subscription, tenant, or organization identifier, but the backend can resolve it to subscription and tenant IDs.

Every attempt should emit exactly one start event and, when the process remains able to report, exactly one terminal finish event. Durations reset when a resume starts, while job-plan summary counters remain cumulative and are marked with `SummaryCounterScope=job-cumulative`. Dashboards may group attempts by `JobID` for resume analysis but must not add cumulative summary counters across attempts.

### Define the Sampling Contract

- **Decision #2: use JobID as the fixed sampling key for job-attempt telemetry.**
- Compute one deterministic inclusion decision from the canonical `JobID` and `SamplerVersion`. Do not include `InvocationID`, attempt type, timestamps, or process-specific values in the sampling key.
- Apply the same decision to the original attempt, every resumed attempt, and every start/finish event sharing that `JobID`. This preserves the complete observable resumption history for each included job.
- Changing the configured sampling rate may change the inclusion threshold, but it must not change the stable hash assigned to a `JobID`. This supports controlled ramp-up from 1% to higher rates without reshuffling the existing cohort.
- Job-ID sampling is appropriate for AzCopy-wide totals, distributions, and rates.
- Deterministic cohort sampling by account identity would be required for exact account-level history, first/second-job funnels, and recency. Decision #2 instead samples by `JobID`, so these longitudinal metrics remain estimates or out of scope.
- Start and finish events for one attempt must share that decision.
- Every emitted event must include the effective `SamplingRate`, `SamplerVersion`, and `SchemaVersion`, allowing ingestion queries to apply the correct weight and detect mixed sampler populations. `SamplerVersion=job-id-sha256-v1` defines the invariant JobID sampling key, so no separate `SamplingUnit` property is needed.

This decision preserves resumptions but does not make a 1% job-ID sample suitable for exact account metrics. A separate deterministic account cohort would be required if longitudinal account-level analysis is added later.

### Define Identity and Privacy Boundaries

**Decision #3: the AzCopy client sends only normalized Azure storage account names and the approved host attributes. It does not send geography, S3/GCS bucket names, endpoint hostnames, tenant ID, or subscription ID. Geography is derived by Application Insights during ingestion; subscription and tenant IDs may be derived server-side from an Azure storage account name.**

The identity contract is:

- Emit a normalized Azure storage account name when it can be parsed from a recognized endpoint.
- Never include an S3/GCS bucket name, endpoint hostname, URL user information, path, container/share/filesystem/object name, query string, SAS token, or fragment.
- Keep source and destination Azure storage account identities in separate fields. Non-Azure, local, custom-domain, emulator, and unparseable endpoints emit no client identity.
- AzCopy emits no geography. Application Insights may derive country or region from the public sender IP observed during ingestion. This is not the source or destination storage region.
- Host attributes are limited to the reviewed schema fields such as OS family/version, architecture, CPU count/model or normalized family, total memory bucket, NIC-speed bucket, Azure VM detection, and invocation context.
- `TenantID` and `SubscriptionID` are prohibited **client fields**. AzCopy must not derive them from OAuth tokens, environment variables, or ARM calls.
- In the trusted telemetry backend, resolve a normalized Azure storage account name to `DerivedSubscriptionID` and `DerivedTenantID`. Keep both derived values out of the raw client envelope and subject them to separate access, retention, and auditing controls.
- Record enrichment status and version, for example `IdentityResolutionStatus` and `IdentityResolverVersion`, so queries can distinguish resolved, unresolved, stale, ambiguous, and non-Azure identities.
- Do not assume every event is resolvable. Local endpoints, S3/GCS endpoints, custom domains, emulator endpoints, deleted accounts, and transient lookup failures may have no usable Azure subscription/tenant mapping.
- For S2S jobs, enrich source and destination identities separately. Queries must choose an endpoint role or deduplicate by `JobID`; otherwise one job can be attributed to two subscriptions or tenants.

Account identity, ingestion-derived geography, installation ID, and host attributes can still identify or fingerprint an organization or machine when combined. Privacy review must define retention, access controls, dashboard aggregation, and whether raw account names are permitted. A plain hash does not make this data anonymous, and a secret HMAC key cannot be protected in an open-source binary.

## Data Collection Feasibility

### All Commands

| Requested data | Current state | Feasibility | Recommended collection contract |
| --- | --- | --- | --- |
| Command type | Implemented using the canonical full Cobra command path. Aliases resolve to the canonical name, and nested commands remain distinct. Commands designated for paired attempt events and tooling-only commands are excluded from `command.invoked`; benchmark uses `Command=bench` and resume uses `Command=jobs.resume`. | Available | Keep the inventory synchronized with command registration. |
| Flags and environment overrides used | Implemented. Cobra's explicitly changed flags and explicitly set reviewed environment variables are sorted, deduplicated, and emitted as `OptFlagsSet` and `OptEnvVarsSet`. Selected safe values are normalized into individual `Opt*` properties. Built-in defaults are omitted. | Available | Keep every registered flag explicitly classified as value, presence-only, or denied. Values for credentials, identity, paths, filters, metadata, tags, headers, and arbitrary content are never read. |
| AzCopy version | Available as `AzCopyVersion`. Historical rows used `ServiceVersion`; dashboard queries coalesce both names during migration. | Available | Keep the semantic version and add build channel (`test`, `preview`, `GA`) separately. |
| Secrets redacted | Partial by construction because flag values are omitted. There is no general redaction validation. Other fields intentionally contain Azure storage account names. | Moderate | Add schema-level tests proving that SAS, keys, tokens, paths, command strings, and environment values cannot enter telemetry. Avoid describing the entire payload as anonymous until privacy review closes. |

Canonical `command.invoked` values are:

- Visible: `jobs.clean`, `jobs.list`, `jobs.remove`, `jobs.show`, `list`, `login`, `login.status`, `logout`, `make`, `remove`, and `set-properties`.
- Hidden: `cancel` and `pause`.

The following commands do **not** emit `command.invoked` because they use, or are designated to use, paired job-attempt start/finish events: `bench`, `copy`, `jobs.resume`, and `sync`. Tooling-only commands `help`, `completion`, `__complete`, `__completeNoDesc`, `doc`, `env`, and the hidden/deprecated `load.clfs` are also excluded. Parent-only groups such as `jobs` and `load`, the root invocation, `--help`, and `--version` do not emit invocation events. Aliases such as `cp`, `benchmark`, `ls`, `rm`, and `set-props` are recorded under their canonical values rather than as separate commands.

### Option Collection Contract

- A CLI flag appears only when Cobra reports it as explicitly changed. Supplying `--recursive=false` or explicitly supplying a flag's default value still counts as explicit and is captured.
- An environment variable appears only when it exists in the process environment with a non-empty value. An `EnvironmentVariable.DefaultValue` is never treated as explicit.
- `OptFlagsSet` and `OptEnvVarsSet` contain reviewed names only.
- Selected values use fixed `Opt*` property names and type-aware normalization. Numeric values remain exact so dashboards can define and revise buckets server-side.
- Invalid values may still appear in the presence list, but their value property is omitted. Normal AzCopy validation remains authoritative.
- Every emitted property value has a 1,024-byte default cap before export. Identifiers, categories, host/account text, endpoint identities, compact lists, and normalized option values use lower property-specific caps where possible. Truncation is UTF-8 safe and shared by the direct Application Insights and OTel paths.
- Positional arguments and unreviewed values are never inspected.
- The same option object is cloned into command events and paired copy/sync job events. Unset option properties are absent, not `false`, `0`, `None`, or another default.

### CLI Flag Review

Values are captured for these low-cardinality or numeric business inputs:

- **Transfer shape/performance:** `block-size-mb`, `put-blob-size-mb`, `cap-mbps`, `recursive`, `from-to`, `blob-type`, `block-blob-tier`, `page-blob-tier`, `overwrite`, `check-length`, `check-md5`, `put-md5`, `decompress`, `exclude-blob-type`, and `request-priority`.
- **Sync/reliability:** `compare-hash`, `delete-destination`, `delete-destination-file`, `delete-snapshots`, `mirror-mode`, `permanent-delete`, `rehydrate-priority`, `local-hash-storage-mode`, and `trailing-dot`.
- **Preservation and behavior:** `as-subdir`, `backup`, `follow-symlinks`, `hardlinks`, `include-directory-stub`, `include-root`, `no-guess-mime-type`, `force-if-read-only`, `preserve-info`, `preserve-last-modified-time`, `preserve-owner`, `preserve-permissions`, `preserve-posix-properties`, `preserve-smb-info`, `preserve-smb-permissions`, `preserve-symlinks`, `posix-properties-style`, and the reviewed S2S booleans/modes.
- **Benchmark shape:** effective `BenchmarkFileCount`, `BenchmarkFileSizeBytes`, `BenchmarkFolderCount`, `BenchmarkMode`, `BenchmarkCleanupRequested`, and `BenchmarkIsCleanup` are emitted on paired benchmark events. Explicitly changed reviewed flags remain available through the normal `Opt*` contract.
- **Other safe categorical/numeric controls:** `dry-run`, `disable-auto-decoding`, `login-type`, `quota-gb`, `service-principal`, and `skip-version-check`.

Presence only is captured for flags whose values may contain customer content, paths, patterns, headers, or otherwise provide little business value. This includes metadata/tags/content headers, include/exclude paths/patterns/regexes/attributes/containers, CPK selectors, output formatting, status filters, and diagnostic display controls.

The following flags are denied even as presence signals: `aad-endpoint`, `application-id`, `await-continue`, `await-open`, `certificate-path`, `debug-skip-files`, `destination-sas`, `hash-meta-dir`, `help`, managed-identity IDs, `list-of-files`, `list-of-versions`, `memory-profile`, `output-location`, `show-sensitive`, `source-sas`, `tenant-id`, and `trusted-microsoft-suffixes`.

### Environment Variable Review

| Environment variable | Capture | Value property | Business use |
| --- | --- | --- | --- |
| `AZCOPY_CONCURRENCY_VALUE` | Explicit name + exact integer or `auto` | `OptConcurrency` | Main transfer concurrency and performance diagnosis. |
| `AZCOPY_CONCURRENT_FILES` | Explicit name + exact integer | `OptConcurrentFiles` | Files in flight, throughput, handle use, and memory pressure. |
| `AZCOPY_CONCURRENT_SCAN` | Explicit name + exact integer | `OptConcurrentScan` | Enumeration parallelism and scan-time analysis. |
| `AZCOPY_BUFFER_GB` | Explicit name + normalized decimal | `OptBufferGB` | Memory tuning and transfer-shape/performance analysis. |
| `AZCOPY_PARALLEL_STAT_FILES` | Explicit name + boolean | `OptParallelStatFiles` | Local enumeration performance. |
| `AZCOPY_TUNE_TO_CPU` | Explicit name + boolean | `OptTuneToCPU` | Auto-tuning behavior and CPU bottleneck analysis. |
| `AZCOPY_DISABLE_HIERARCHICAL_SCAN` | Explicit name + boolean | `OptDisableHierarchicalScan` | Enumeration performance versus transaction-cost tradeoff. |
| `AZCOPY_PACE_PAGE_BLOBS` | Explicit name + boolean | `OptPacePageBlobs` | Page-blob throttling/performance behavior. |
| `AZCOPY_REQUEST_TRY_TIMEOUT` | Explicit name + normalized minutes | `OptRequestTryTimeoutMinutes` | Timeout and reliability analysis. |
| `AZCOPY_DOWNLOAD_TO_TEMP_PATH` | Explicit name + boolean | `OptDownloadToTempPath` | Download behavior and local-I/O outcomes. |
| `AZCOPY_OPTIMIZE_SPARSE_PAGE_BLOB` | Explicit name + boolean | `OptOptimizeSparsePageBlob` | Sparse page-blob performance behavior. |
| `AZCOPY_CACHE_PROXY_LOOKUP`, `AZCOPY_DISABLE_SYSLOG`, `AZCOPY_SHOW_PERF_STATES` | Explicit name only | None | Presence may explain runtime/debug behavior; values are not needed. |

Paths, credentials, tokens, IDs, encryption material, custom user-agent/API endpoints, profile locations, token-cache overrides, and test/internal environment variables are never collected. This includes Azure authentication variables, AWS/GCP credentials, CPK keys, log/job-plan locations, MIME-map paths, and injected OAuth state.

Evidence: [cmd/root.go](cmd/root.go), [azcopy/telemetry.go](azcopy/telemetry.go), and [telemetry/events.go](telemetry/events.go).

### Copy, Sync, and Benchmark: Host and Environment

| Requested data | Current state | Feasibility | Recommended collection contract |
| --- | --- | --- | --- |
| CPU count | Available as `HostNumCPU`. | Available | Bucket very large values if dashboard cardinality becomes an issue. |
| CPU model | Available as `HostCPUModel`. | Available | Consider a normalized CPU-family bucket. The raw model contributes to machine fingerprinting. |
| Memory | Available as total physical GB in `HostMemoryTotalGB`, not memory used by AzCopy. | Available for capacity; Moderate for process usage | Keep `HostMemoryTotalGB`. If the business need is AzCopy memory pressure, add peak working set separately. |
| Network | Implemented as Azure VM detection, job/transfer-phase throughput, Storage-operation latency/rate, raw network operation/error counts, and categorized server-busy 503 counts/rates. Adapter type and actual available bandwidth remain unknown. | Available/Partial | Keep the individual metric names and raw numerators. Do not present NIC link speed as measured bandwidth or 503 count as comprehensive retries. |
| NIC speed | Implemented as exact best-effort `HostNICSpeedMbps` and coarse `HostNICSpeedBucket`; `-1` and `unknown` indicate an unavailable probe. It may not be the interface used by AzCopy. | Available/Partial | Do not call this measured network capacity. Use the bucket for aggregate dashboards and exact value for diagnostics. |
| File protocol | Available as source/destination protocol. | Available | Keep separate source and destination fields. For local paths, optionally distinguish local disk, SMB, and NFS using mount detection. |
| Platform (Windows/Linux) | Available as `OSType`; OS version and architecture are also emitted. | Available | Use a low-cardinality OS family for dashboards; tightly restrict access to full OS-version plus host combinations. |

Evidence: [azcopy/hostinfo.go](azcopy/hostinfo.go), platform-specific `hostinfo` files, and `buildResourceAttributes` in [azcopy/telemetry.go](azcopy/telemetry.go).

### Copy, Sync, and Benchmark: Endpoints and Data Shape

| Requested data | Current state | Feasibility | Recommended collection contract |
| --- | --- | --- | --- |
| Endpoint type | Available as `SourceType`, `DestType`, and `FromTo`. | Available | Normalize endpoint families and retain `FromTo` for detailed analysis. |
| Transfer direction | Available as upload, download, S2S, or delete. | Available | Add an explicit benchmark mode where applicable. |
| Full storage account | Implemented with `SourceScope`/`DestScope`: service, container/share/bucket, object-or-prefix, local directory/object, stream, benchmark, or none. | Available | Service scope identifies account/service-level traversal without emitting resource paths. |
| Total bytes | Implemented as `BytesEnumerated`, `BytesExpected`, `BytesTransferred`, and `BytesOverWire` on the finish event. | Available | `BytesEnumerated` is scheduled source payload after filters; `BytesExpected` is successful plus still-expected payload; `BytesTransferred` is logical successful progress without retry duplication; `BytesOverWire` is physical payload traffic including retries and failed-transfer traffic. |
| Object count | Implemented for scheduled/outcome counts. Payload objects exclude folder-property transfers and include regular files/objects, preserved symlinks, and converted hardlinks. Folder-property counts and scheduled type breakdowns are emitted separately. | Available/Partial | These metrics describe scheduled work and transfer outcomes, not every object scanned. Source/destination scan counts and full dataset shape still need separate instrumentation, especially for sync. |
| Average object size | Implemented as `SourceBytesScanned` and `SourceAverageObjectSizeBytes` over filtered source payload objects. | Available | Uses the same source population as percentile and small-object metrics; folders and destination objects are excluded. |
| Percentile object size | Implemented for filtered source payload objects as approximate P50/P90/P95 upper bounds from a fixed bounded histogram. Individual sizes are never retained. | Available/Approximate | Emit `SourceObjectSizeP50BytesApprox`, `P90`, and `P95`. Dashboard labels must retain “approximate”; bucket boundaries require a client schema change. |
| Small-file ratio | Implemented for filtered source payload objects. “Small” is strictly less than 1 MiB. | Available | Emit both `SourceObjectsUnder1MiB` and `SourceObjectsUnder1MiBRatioPct` so ratios can be recomputed and weighted correctly. |
| Directory depth | Implemented as `SourceMaxDirectoryDepth` only. Root/single-file depth is 0; each containing relative-path segment adds one. Both `/` and `\` separators are supported and paths are not retained. | Available | Max depth is sufficient for the initial business need; add a bounded depth histogram only if max proves too sensitive to outliers. |
| Blob/ADLS blob count | Partial. File-transfer counts exist but are not emitted separately and represent selected/scheduled work, not necessarily every blob scanned. | Low/Moderate | Emit scanned and scheduled file/object counts separately. Do not infer blob count from total transfers. |
| Number of Blob/ADLS containers | Implemented as `ContainersScanned` and `ContainersTouched`; Azure Files shares and ADLS filesystems use the same generic Azure scope counters. Names are not emitted by these metrics. | Available | Scanned means selected for source traversal; touched means at least one source payload object from that scope was appended to a job part. |
| Size of Blob/ADLS containers | Not emitted. Aggregate object sizes can be accumulated by container. | Moderate | Clarify whether this means aggregate bytes across touched containers or a distribution of container sizes. Prefer aggregate totals and bounded histograms, not per-container records. |
| Number of S3/GCS buckets | Implemented as `BucketsScanned` and `BucketsTouched`. Names are retained only transiently in bounded distinct sets and are not emitted by these metrics. | Available | Same scanned/touched semantics as Azure scopes. A direct single-bucket source reports one scanned bucket even if no object is scheduled. |
| Size of S3/GCS buckets | Not emitted. Can be accumulated from enumerated object sizes. | Moderate | Emit aggregate bytes and bounded bucket-size distributions. Full bucket size may be unknown when the command targets a prefix. |

The best bounded collection point for **scheduled copy work** is `CopyTransferProcessor.scheduleTransfer`, which receives `StoredObject.Size`, `EntityType`, `RelativePath`, and `ContainerName`. For the shape of the complete source dataset, counters must be placed earlier in the traverser pipeline. This distinction is especially important for sync: it enumerates both sides and schedules only differences, so source-shape, destination-shape, and scheduled-delta statistics require separate counters. Benchmark upload shape can be taken directly from its generated file-count, file-size, and folder-count inputs. See [azcopy/zc_processor.go](azcopy/zc_processor.go), [traverser/zc_enumerator.go](traverser/zc_enumerator.go), and [cmd/benchmark.go](cmd/benchmark.go).

### Copy, Sync, and Benchmark: Account Identity and Geography

| Requested data | Current state | Feasibility | Recommended collection contract |
| --- | --- | --- | --- |
| Azure storage account name | Available as an unhashed DNS label. Decision #3 approves this identity type, subject to privacy controls. | Available | Normalize to lowercase and emit separately for source and destination. Do not append container, share, filesystem, or object paths. |
| Subscription ID | Not emitted by AzCopy. The backend can resolve a standard Azure storage account name to its subscription. | Server-side derived | Add no client field or ARM call. Enrich as `DerivedSubscriptionID` after ingestion, retain resolution status/version, and apply restricted access controls. |
| Tenant ID | Not emitted by AzCopy. The backend can resolve the account's owning tenant together with subscription identity. | Server-side derived | Add no client field or token-claim parsing. Enrich as `DerivedTenantID` after ingestion with the same status, versioning, and access controls as subscription enrichment. |
| Execution geography | No geography is emitted by the AzCopy client. Application Insights derives `client_CountryOrRegion` from the sender IP during ingestion and masks the stored IP by default. | Server-side derived | Use only the ingestion-derived country/region field for aggregate dashboards; do not add client-side IP lookup or expose city/state without separate approval. |
| Storage region | Not emitted and not encoded in normal data-plane endpoints. | Out of current scope | Subscription enrichment does not imply storage-region collection. Add region only through a separately approved server-side enrichment if a business need is established. |
| S3/GCS identity | Bucket names are not emitted. S3/GCS remain distinguishable through endpoint type, protocol, topology, and aggregate bucket counters. | Not collected | Keep bucket names and object prefixes out of client telemetry. |
| Cloud type | Implemented independently as `SourceCloudType` and `DestCloudType` for Azure endpoints, with values `public`, `gov`, `china`, `germany`, or `unknown`. Non-Azure endpoints are empty and remain identified by endpoint type. | Available | Use the role-specific fields so cross-environment transfers remain unambiguous. |
| Authentication mechanism | Implemented with distinct `SAS`, `PublicAnonymous`, provider/OAuth/shared-key values, and `NotApplicable` for local/pipe/benchmark endpoints. | Available | Classification uses parsed SAS presence only; credential material is never emitted. |

Evidence: `storageAccountName`, `hostOf`, `endpointCloudType`, and credential dimensions in [azcopy/telemetry.go](azcopy/telemetry.go), plus Application Insights ingestion context fields.

#### IP Geolocation Feasibility

Do not add an AzCopy-side IP geolocation call. Application Insights already performs IP geolocation during ingestion and exposes the result on custom events as `client_CountryOrRegion`, `client_StateOrProvince`, and `client_City`. By default, the ingestion service temporarily uses the sender IP for lookup, discards it, and stores `client_IP=0.0.0.0`. This behavior was verified on the telemetry test component: recent AzCopy custom events had populated country/region/city fields while the stored IP remained masked.

Country-level analysis can use the built-in field without increasing the AzCopy payload:

```kusto
customEvents
| where name startswith 'azcopy.'
| extend ExecutionCountry = tostring(client_CountryOrRegion)
```

This value represents the public network egress observed by Application Insights, not a guaranteed physical user location. Corporate NAT, VPNs, proxies, cloud runners, and forwarding infrastructure can move or obscure it. It may be empty when IP collection is unavailable or disabled. Country-level aggregation should remain subject to privacy review, and city/state fields should not be exposed unless separately justified.

A client-side implementation is technically possible through a third-party/Azure geolocation API or a bundled IP database, but is not recommended. An external API adds startup latency, an additional outbound dependency, proxy/firewall failure modes, service cost, credential distribution problems for an open-source binary, and disclosure to another processor. A bundled database adds binary/update/licensing cost and still cannot geolocate private interface addresses without separately discovering the public egress IP. Application Insights ingestion enrichment is therefore the preferred implementation.

### Copy and Sync: Execution and Outcome

| Requested data | Current state | Feasibility | Recommended collection contract |
| --- | --- | --- | --- |
| Job status | Implemented for copy/sync attempts that emit `job.started`: a deferred finalizer emits exactly one `job.finished` for success, partial success, enumeration/manager failure, and explicit cancellation. | Available | `JobStatus` carries the detailed outcome and `TerminalStage` identifies the bounded lifecycle stage. Hard process termination remains ingestion-inferred abandonment. |
| Error code if failed | Implemented as bounded `JobErrorCategory` and sanitized `JobErrorCode` on failed/partial-success finishes. Typed service, timeout, network, local-I/O, AzCopy, and lifecycle-stage failures are classified without raw messages. | Available/Partial | Service codes are limited to 64 safe ASCII characters; untyped errors use fixed stage codes. Transfer HTTP histograms remain separate. Some provider-specific errors can only use the stage fallback until typed adapters are added. |
| Files transferred | Implemented as `ObjectsCompleted`, excluding folder-property transfers, with scheduled regular-file/symlink/converted-hardlink breakdown. | Available | Existing aggregate transfer counts remain for compatibility. |
| Passed files | Defined as completed payload objects and represented by `ObjectsCompleted`; no duplicate “passed” metric is emitted. | Available | Use the completed object count consistently. |
| Failed files | Implemented as `ObjectsFailed`, with `FolderPropertiesFailed` separate. | Available | Uses saturating subtraction for inconsistent summaries. |
| Error-code distribution | Top ten numeric transfer code/count buckets plus `FailureErrorOtherCount` are emitted; job-level category/code covers observable terminal failures. | Available/Partial | Tail volume is measurable. Keep job-level classification separate from per-transfer HTTP outcomes. |
| Data over public versus private network | Current `DestEndpointKind` checks the hostname for `.privatelink.`. Azure Private DNS commonly uses the public hostname while resolving to a private address, so it does not identify the actual route. Bytes are not split by route. | High/Not reliable | Rename the current field to configured endpoint kind. If actual routing is required, inspect resolved/connected addresses at the transport layer and define behavior for proxies, VPNs, and mixed source/destination paths. |
| Start time | Available on the start event and retained in the finish model. | Available | Use UTC and keep attempt start distinct from transfer start. |
| End time | Emitted on success, partial success, observable failure, and explicit cancellation for started copy/sync attempts. | Available | Hard termination cannot emit and is inferred after an ingestion timeout. |
| Duration | Available as wall-clock attempt duration on every observable terminal finish. | Available | Includes initialization after prestart, enumeration, transfer, and completion work. Phase durations overlap. |
| Enumeration time | Implemented as `EnumerationPhaseDurationSeconds`, from progress-tracker start through final job-part dispatch. It overlaps transfer. | Available | Do not sum enumeration and transfer durations. Finalization remains undefined separately. |
| Latency | Emitted as `AverageStorageHTTPAttemptE2EMs`, the mean duration of an HTTP attempt observed by the SDK per-retry policy. Retries are separate attempts. | Available/Partial | It is not whole-job latency, logical-operation latency, network round-trip time, or a percentile. |
| Average network speed | `JobThroughputMbps` covers the full attempt; `TransferPhaseThroughputMbps` uses first-part-ordered through terminal wait and may overlap enumeration. | Available | Use job throughput for customer experience and transfer-phase throughput for transfer performance. Neither is physical link capacity. |
| Retry count | `ServerBusy503Count` and throughput/IOPS/other categories are emitted with operation counts. They do not include every SDK, network, or body-read retry. | Partial | Add explicit SDK retry-policy and body-read retry instrumentation before calling any metric comprehensive retry count. |
| Resume count | Implemented through `Command=jobs.resume`, paired events with the reused `JobID`, and a new `InvocationID`. | Available | Count resume starts and terminal outcomes. Resume job-plan summary counters have `SummaryCounterScope=job-cumulative`; invocation durations remain attempt-scoped. |
| Restart count | Ambiguous. AzCopy has restarted transfer status during resume, but no logical job restart concept. | Clarify/Moderate | Define restart as whole-command rerun, resumed transfer, or internally restarted file. Use separate counters if more than one is needed. |
| Percentage complete at cancellation | Emitted from the live summary with transferred/expected bytes and object counts when available. | Available/Partial | Cancellation before any job part exists can legitimately report zero progress. |

Existing source data is in `ListJobSummaryResponse`, `PipelineNetworkStats`, and performance advice. See [common/rpc-models.go](common/rpc-models.go), [jobsAdmin/init.go](jobsAdmin/init.go), and [ste/xferStatsPolicy.go](ste/xferStatsPolicy.go).

### Benchmark Jobs

| Requested data | Current state | Feasibility | Recommended collection contract |
| --- | --- | --- | --- |
| Result code | Implemented. Main benchmark events use `Command=bench` and `BenchmarkMode`; generic `JobStatus`, `TerminalStage`, and job-error fields are the result contract. Automatic cleanup jobs do not emit benchmark events. | Available | Do not add a duplicate benchmark-only result code. Use the generic terminal contract for consistent reliability analysis. |
| Description | Excluded by design. Human-readable benchmark/performance-advice descriptions are not emitted. | Excluded | Generate descriptions from stable codes in dashboards or UI. |
| Reason | Implemented as bounded, priority-ordered `PerformanceAdviceCodes` and `PerformanceConstraint`. Human-readable title/reason text is excluded. | Available | The first advice code is primary. Keep advice code sanitization and cardinality bounds. |
| Diagnostic statistics | Generic finish measurements provide IOPS, Storage HTTP attempt latency/count, server-busy and network-error counts/rates, bytes, durations, and throughput. Effective file count, file size, folder count, mode, cleanup request, and cleanup marker are benchmark dimensions. | Available | Reuse generic measurements; do not emit a second diagnostic payload. |

Benchmark inputs are converted into a copy job in [cmd/benchmark.go](cmd/benchmark.go). Performance advice is defined in [common/fe-ste-models.go](common/fe-ste-models.go).

### Supportability Signals

| Requested data | Current state | Feasibility | Recommended collection contract |
| --- | --- | --- | --- |
| Diagnostic bundle generated | No diagnostic-bundle workflow was found. | External until feature exists | Instrument the bundle feature when implemented; do not add a placeholder client metric. |
| Log bundle upload status | No upload workflow was found. | External until feature exists | Emit attempted/succeeded/failed only from the owning upload workflow. |
| Redaction status | No explicit redaction result exists. | Moderate after bundle feature exists | The redaction component must produce a versioned outcome. Do not infer success from bundle creation. |
| Support case linkage | No support case integration exists. | External | Store the AzCopy invocation/job ID in the support system rather than placing case IDs in general product telemetry. |
| ICM linkage | No ICM integration exists. | External | Join in the incident/support system with restricted access. |
| Command invocation ID | Implemented as random 128-bit `InvocationID` per command/job attempt. Copy/sync/benchmark/resume start and finish share it; `JobID` remains the resumable job key. | Available | Use `Command=jobs.resume` to distinguish resume attempts. |
| Job ID | Available as `JobID` for job events and command invocations that have an AzCopy job identifier. | Available | Reuse it across the original and resume attempts as the correlation key. |
| Top error category | A coarse job-level category set exists (`authentication`, `authorization`, `throttling`, `timeout`, `network`, `local-io`, `conflict`, `not-found`, `service`, `azcopy`, lifecycle stages, and `unknown`). It is not independently versioned, provider-complete, or unified with numeric transfer outcomes and performance-advice codes. | Moderate | Add `ErrorTaxonomyVersion` and a documented mapping contract. Preserve provider code separately from normalized category, stage, endpoint role, retryability, and remediation code. |
| Remediation shown | Not captured. Some performance advice is shown. | Low for owned advice | Emit an advice/remediation code when AzCopy actually displays it. Do not emit full text. |
| Remediation attempted | Usually not observable unless the user invokes a specific AzCopy action. | Partial/External | Record only explicit in-product actions and correlate by invocation ID. Do not infer user behavior outside AzCopy. |
| Resolved without escalation | AzCopy cannot know whether the user later opens a case or considers the issue resolved. | External | Compute in the support system from case creation and follow-up signals. This is not a client-only metric. |

#### Stable Support Error Taxonomy

The current telemetry has three useful but separate error-related surfaces:

1. `JobErrorCategory` and `JobErrorCode` classify the terminal attempt error. Typed Azure service errors map selected response codes/statuses into coarse categories; network, timeout, local-path, AzCopy, and lifecycle-stage fallbacks cover other observable failures.
2. `FailureErrorCodes` is a bounded top-ten histogram of numeric per-transfer outcomes with `FailureErrorOtherCount` for the omitted tail. It does not preserve endpoint role, provider semantics, or a normalized support category per bucket.
3. `PerformanceAdviceCodes` describes likely performance constraints. These codes are actionable signals, but they are not error categories and are maintained separately by the performance advisor.

A stable support taxonomy would define a versioned contract across those surfaces. At minimum it should emit a normalized category, stable normalized code, lifecycle stage, source/destination role, provider, retryability, customer-actionability, and remediation code while retaining the raw bounded provider code separately. Mapping changes must increment `ErrorTaxonomyVersion` so dashboards can compare like with like. Unknown provider codes should remain queryable as `unknown` plus sanitized provider code rather than silently collapsing into `service` or a lifecycle-stage fallback.

## Business Use-Case Coverage

The following assessment distinguishes the current branch from the complete proposed data list. “Covered after additions” means the calculation is technically possible only after the recommended schema/lifecycle work and under the stated sampling model.

### Per Customer (Implemented as Server-Enriched Subscription Reporting)

For this assessment, “per customer” means **per `DerivedSubscriptionID`**, matching the review comment that assumed subscription-level reporting. `DerivedTenantID` supports an optional higher-level rollup. AzCopy sends neither field; the trusted backend derives both from source/destination Azure storage account names. Queries must choose source or destination ownership, or deduplicate by `JobID`, because an S2S job can involve two subscriptions or tenants.

| Business metric | Required signals | Coverage assessment | Sampling/interpretation |
| --- | --- | --- | --- |
| Jobs in past N days | Derived subscription/tenant ID, endpoint role, unique job/attempt IDs, timestamp | Covered after server enrichment and lifecycle fixes as a sampled subscription/tenant estimate. | Weight sampled jobs by the effective sampling rate. Report sample size and confidence; low-volume subscriptions may have no observations. |
| Data transferred in past N days | Derived subscription/tenant ID, endpoint role, bytes transferred, timestamp | Covered after enrichment and lifecycle fixes as a sampled estimate. | Scale totals by inclusion probability. Keep source and destination views separate or deduplicate to avoid double-counting S2S bytes. |
| Average time to migrate per TB (P90/P95) | Derived subscription/tenant ID, endpoint role, bytes, duration, outcome, topology | Covered after enrichment and complete terminal events when a subscription has enough sampled jobs. | Report sample size. Percentiles for low-volume subscriptions are unavailable or unstable under 1% `JobID` sampling. |
| Average network speed | Derived subscription/tenant ID, endpoint role, bytes, transfer-only duration or job duration | Partial. Current job throughput is available but includes enumeration/finalization. | Define job throughput versus transfer-only throughput and require a minimum sampled count per subscription. |
| Failures and error codes | Derived subscription/tenant ID, endpoint role, complete outcome, failed counts, error histogram | Partial. Server identity is available, but early failures and error-tail coverage still need fixes. | Estimate rates for sufficiently represented subscriptions; do not describe them as complete histories under job sampling. |
| Distribution of subcommands | Derived subscription/tenant ID, endpoint role, canonical command on all events | Covered for command events and paired copy/sync/benchmark/resume events. | A sampled subscription-level command mix is feasible, but low-volume subscriptions may be absent. |
| Days since last active job / last job | Derived subscription/tenant ID and complete timestamps | Not reliably covered under `JobID` sampling. | The true latest job may be unsampled, so the result is “days since last observed sampled job,” not exact activity recency. |
| Week-over-week job frequency | Derived subscription/tenant ID, endpoint role, job ID, timestamp | Partially covered after enrichment. | Trends are estimable for high-volume subscriptions if sampling is stable; sparse subscriptions need a subscription/account cohort sampler. |
| AzCopy version in use | Derived subscription/tenant ID, endpoint role, service version, timestamp | Covered as observed sampled version usage. | Report version distribution or most recently observed sampled version, not a definitive current version. |
| Retry count per job | Comprehensive retry counters | Not covered. Only 503 count is cheaply available. | Sampled distributions are valid after instrumentation; totals require weighting. |
| Resume/restart count per job | Attempt start/outcome events, `InvocationID`, shared `JobID`, `Command`, and restart definition | Resume is covered; restart remains undefined. | Count `Command=jobs.resume` by `JobID`. Deterministic `JobID` sampling keeps all attempts for an included job together. Do not sum cumulative resume summary counters. |
| Abandonment rate and percent complete | Complete canceled/terminated finish events, expected/transferred work | Not reliable in the current branch because cancellations and killed processes may not send finish. | A hard-killed process can never send a final event. Derive probable abandonment server-side from sampled `started` events with no matching finish after a timeout, and report it separately from explicit cancellation. |

Conclusion: server enrichment restores the earlier subscription-level business assessment, with optional tenant rollups. Totals, distributions, and rates are estimable; exact recency and complete longitudinal history are not supported by 1% `JobID` sampling and would require a subscription/account cohort.

### AzCopy-Wide: Volume and Categories

| Business metric | Required signals | Coverage assessment | Sampling/interpretation |
| --- | --- | --- | --- |
| Data transferred by on-prem-to-Azure, Azure-to-Azure, AWS-to-Azure, and GCS-to-Azure | Source/destination type, `FromTo`, bytes | Implemented for emitted jobs. Dashboards derive `aws-to-azure`, `gcs-to-azure`, `intra-azure`, `local-to-azure`, and `azure-to-local` from canonical endpoint types. Early failures before paired-event initialization remain absent. | Estimate totals with inverse-probability weighting and include confidence bounds for small categories. Track unmatched or pre-initialization failures separately. |
| Average time to migrate per TB by category | Topology, bytes, duration, status | Covered after terminal-event fix. | Exclude zero-byte jobs; stratify by job-size buckets because tiny jobs produce unstable per-TB normalization. |
| Throughput P50/P90/P95 | Job throughput, topology, job-size bucket | Covered for current job-level throughput. | A 1% sample can support percentiles when the resulting sample count is adequate. Always show `n`; rare categories may be unusable. |
| Time to complete per TB | Bytes, duration, topology | Covered after terminal-event fix. | Same caveat as average migration time. Specify mean versus median/percentile. |
| Enumeration time | Enumeration-phase duration | Covered as tracker start through final job-part dispatch; it may overlap transfer. | Use the current phase definition consistently and do not sum overlapping durations. |
| Transfer time | Transfer-phase duration | Covered as first part ordered through terminal wait; it may overlap enumeration. | Use the current phase definition consistently and do not sum overlapping phase durations. |
| Finalization time | Phase durations | Not covered. | Add finalization timer. |
| Retry overhead | Bytes over wire, transferred bytes, retry counts, failed bytes | Partial. `BytesOverWire - BytesTransferred` is only a proxy and also includes failed-transfer traffic. | Emit explicit retry bytes/counts before labeling this retry overhead. |
| Top movers across categories | Derived subscription/tenant ID, endpoint role, category, bytes | Covered after server enrichment and lifecycle fixes as a subscription/tenant ranking. | Keep source and destination rankings separate, weight by sampling rate, restrict access, and label uncertainty. Job sampling can miss lower-frequency large movers. |

Conclusion: category volume and job-performance distributions are the strongest initial business use cases. Provider-specific category and endpoint cloud dimensions are now emitted. Enumeration/transfer-phase and server-busy analysis are available; finalization and comprehensive retry analysis still need instrumentation. Subscription/tenant mover rankings additionally require server enrichment and privacy controls that are not implemented in this repository.

### AzCopy-Wide: Adoption and Funnel

| Business metric | Required signals | Coverage assessment | Sampling/interpretation |
| --- | --- | --- | --- |
| First-job success rate | Derived subscription/tenant ID, observation start, ordered complete outcomes | Not reliably covered by `JobID` sampling. | “First ever” cannot be reconstructed when telemetry begins mid-lifecycle, and the first job may be unsampled. A subscription/account cohort is required. |
| Time to first successful job | Derived subscription/tenant ID, first observed attempt, later success, timestamps | Not reliably covered. | `JobID` sampling can omit attempts between observed events and bias elapsed time. |
| Percentage running a second job | Derived subscription/tenant ID and complete ordered job history | Not reliably covered. | Requires deterministic subscription/account cohort sampling with all jobs for included identities. |
| Week-over-week job frequency trend | Job timestamps | Covered AzCopy-wide with implemented deterministic sampling. | Stable uniform sampling can estimate trend direction and magnitude. Changes to sampling rate must be included in weighting. |

Conclusion: only the population-wide weekly trend is reliable under the selected job telemetry. Subscription/tenant funnels require separate longitudinal cohort sampling even though identity can be enriched server-side.

### AzCopy-Wide: Reliability

| Business metric | Required signals | Coverage assessment | Sampling/interpretation |
| --- | --- | --- | --- |
| Job completion rate | One terminal outcome per started copy/sync attempt | Covered for observable exits. | Estimate from sampled attempts. Track starts with no finish as probable hard termination/abandonment after a timeout. |
| Success, partial-success, and failure rates | Complete job status and terminal reason | Covered for observable copy/sync/benchmark/resume exits. | Population proportions are unbiased under uniform sampling; precision depends on sampled count, not sampling percentage alone. |
| Error-code distribution | Top ten transfer-error buckets plus exact omitted-tail count and bounded job-level category/code are emitted. | Available/Partial | Provider errors without a recognized typed error surface use a lifecycle-stage fallback; sampling can delay rare-code detection. |
| Failed-object rate | Payload object scheduled/failed counts and folder-property counts are emitted separately. | Available | Calculate both globally object-weighted rates and per-job rate distributions. |
| Throttling rate | Raw Storage HTTP attempt and categorized 503 counts plus percentages are emitted. | Available | Aggregate from numerators/denominators rather than averaging percentages. |
| Cancellation rate | Complete explicit cancellation outcomes | Covered for started copy/sync attempts. | Separate explicit cancellation from inferred hard-termination abandonment and failure. |
| Resume success rate | Attempt type, unique attempt ID, shared job ID, and terminal outcome | Covered for resume attempts that reach dimension resolution. | Calculate successful terminal resume attempts divided by terminal resume attempts. Treat unmatched starts as probable abandonment after timeout. |
| Abandonment rate and percent complete | Started event, explicit cancellation/finish, missing-finish timeout, expected/transferred work | Partial design only. | Hard termination is inferred, not directly observed. Use a timeout and label the result probable abandonment. |
| Top repeated error codes | Error code/count and timestamp | Partially covered. | Can be calculated at ingestion from timestamped histograms; sampling may miss rare errors. |
| New error codes in last 30 days | Error code/count and event timestamp with retained history | Partially covered. | No extra client `first-seen` field is required. Compare against historical ingestion data, but account for schema changes and sampling delay. |

Conclusion: copy/sync/benchmark/resume reliability is covered for observable terminal exits. Hard-termination abandonment still requires ingestion-time handling.

### AzCopy-Wide: Platform Mix

| Business metric | Required signals | Coverage assessment | Sampling/interpretation |
| --- | --- | --- | --- |
| Source platform distribution | Source type/protocol/mount plus host OS for local sources | Covered at a coarse level. | Define whether “platform” means provider, protocol, or source OS. Population proportions work under uniform job sampling. |
| Target platform distribution | Destination type/protocol/cloud | Covered at a coarse level. | Report both job-count mix and byte-weighted mix; they answer different questions. |

Conclusion: platform mix is feasible now after terminology is clarified.

### AzCopy-Wide: Supportability

| Business metric | Required signals | Coverage assessment | Sampling/interpretation |
| --- | --- | --- | --- |
| Support cases per active customer | Derived subscription/tenant ID, support case identity, activity window | Partially enabled by server enrichment, but still requires a restricted support-system join. | Product telemetry and support-case populations have different inclusion mechanisms. Keep derived identity and case linkage out of the raw client payload. |
| Support cases per 1,000 jobs | Case-to-job linkage and total jobs | Not covered. | The denominator can be estimated; linked cases must be complete or separately weighted. |
| Diagnostic bundle success rate | Bundle attempt and result | Not covered; owning feature absent. | Instrument at the bundle workflow when implemented. |
| Support-ready case rate | Definition of “support-ready,” bundle/redaction status, case ID | Not covered. | Requires support-process data and an agreed quality definition. |
| Self-service resolution rate | Remediation shown/attempted, later success, no escalation window | Not covered. | A later successful job is only a proxy. True resolution requires external/no-case observation and a defined time window. |
| Time to diagnosis | Case created and diagnosis timestamps | Not covered by the proposed client fields. | External support-system metric. |
| Time to mitigation | Mitigation timestamp | Not covered by the proposed client fields. | External support-system metric. |
| Time to resolution | Resolution timestamp | Not covered by the proposed client fields. | External support-system metric. |
| Repeat-contact rate | Derived subscription/tenant ID and complete support-contact history | Identity can be enriched, but complete contact history remains external. | External support-system metric joined in a restricted analytics environment. |
| Escalation rate | Case identity and escalation event | Not covered. | External support-system metric. |

Conclusion: the proposed supportability signal list is itself insufficient for most supportability business metrics because it lacks support workflow timestamps and complete case history. These metrics belong primarily in the support system, joined to AzCopy through a job/invocation ID when the customer provides one.

## Implementation Backlog

Completed in this branch: deterministic JobID sampling and sampler metadata; per-attempt `InvocationID`; paired copy/sync/benchmark/resume terminal events for observable exits; original/resume attempt typing and measurement scope; Test/Prod build-time connection-string injection; benchmark input dimensions with cleanup exclusion; bounded job-level error category/code; explicit option values; byte/object/source-shape metrics; scope/endpoint/auth dimensions; accurate AWS/GCS/Azure transfer categories; role-specific source/destination Azure cloud dimensions; NIC availability/bucket; enumeration/transfer timing; raw network/503 numerators; performance advice codes/constraint; and bounded transfer-error tail accounting.

### Release-Blocking

1. Infer hard-kill abandonment at ingestion for starts with no terminal event after a defined timeout.
2. Complete privacy review for account identity, geography, installation ID, and host attributes.
3. Populate the Test/Prod secure pipeline variables and verify ingestion from E2E, preview, and GA artifacts.
4. Decide explicitly whether dry-run paired events are in scope; currently no transfer start/finish is emitted for dry run.

### Moderate Follow-Up

1. Add initialization and finalization durations if stable independent boundaries are introduced. Enumeration and transfer phases are implemented and overlap by design.
2. Instrument comprehensive SDK, body-read, and network retry counts/bytes.
3. Define restart semantics separately from resume attempts.
4. Add provider-specific typed error adapters only where lifecycle-stage fallback proves too coarse.
5. Add per-container/bucket size distributions only after defining whether partial-prefix size or full-scope size is required.

### Defer or Move Server-Side

1. Client-side tenant ID and subscription ID collection. Server-side derivation from the emitted Azure storage account name is approved by Decision #3.
2. Actual public/private network-path attribution.
3. Any organization/customer mapping beyond derived subscription and tenant identity. Restricted subscription/tenant analysis may proceed after enrichment and privacy controls.
4. Support-case, ICM, diagnosis, mitigation, resolution, repeat-contact, and escalation metrics.

## Proposed Delivery Scope

### Suitable for Initial Release

- Command and flag-name adoption.
- Version, OS, host-capacity, endpoint, protocol, cloud, and authentication mix.
- Job and byte volume by provider-specific transfer categories derived from `FromTo`, `SourceType`, and `DestType`.
- Job throughput, operation latency, IOPS, server-busy rate, and network-error rate.
- Success, partial-success, failure, cancellation, failed-object, and top-error distributions after lifecycle fixes.
- Benchmark outcome, input-shape, performance-advice, and diagnostic analysis. Description text is intentionally excluded.

### Not Suitable for Initial Claims

- Exact per-customer metrics. Subscription/tenant metrics are server-enriched sampled estimates, not complete histories.
- First-job, time-to-first-success, or second-job funnel metrics.
- Client-emitted subscription or tenant IDs, and storage-region attribution. Subscription/tenant IDs are derived only in the trusted backend.
- Exact public/private network byte attribution.
- Complete retry overhead before new instrumentation; resume success is available, but resume byte/object fields are cumulative job state rather than attempt deltas.
- Self-service resolution and other support workflow outcomes.

## Final Recommendation

Approve the telemetry pipeline initially for AzCopy-wide, account-level, and server-enriched subscription/tenant-level product usage, performance, and reliability estimates, contingent on sampling, lifecycle completeness, enrichment quality, and privacy review. Treat longitudinal funnels and supportability reporting as separate projects with their own sampling, governance, and external-integration designs.

This scope provides useful decision data without making statistically or technically unsupported claims from the current two-event, 1%-sampled client telemetry model.

## Verification Performed

- Inspected the current staged and unstaged implementation in `telemetry/events.go`, `azcopy/telemetry.go`, copy/sync/resume lifecycle wiring, benchmark cooking, source-shape tracking, command option classification, summary counters, and build templates.
- Searched production Go code for the proposed Storage Mover nudge and supportability workflows; neither is present.
- Ran the focused telemetry, source-shape, reporter, command-option, and benchmark-cooking tests: **97 passed, 0 failed**.
- Did not validate live Application Insights ingestion, secure pipeline-variable population, server-side account enrichment, dashboards, support-system joins, or Storage Mover service behavior. Those are external to the available repository implementation.