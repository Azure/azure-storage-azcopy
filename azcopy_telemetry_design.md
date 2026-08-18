# Design for Telemetry for AzCopy 

## Project Context 
 
AzCopy (Azure/azure-storage-azcopy) is an open-source command-line utility for copying data to, from, and between supported storage locations. Depending on the scenario, supported endpoints include the local file system, Azure Blob Storage, Azure Files, Azure Data Lake Storage Gen2, Amazon S3, and Google Cloud Storage. 
 
AzCopy works by calling dataplane Azure Storage APIs, typically via the Azure Storage SDKs for Go. We add a header with every request of the form `User-Agent: AzCopy/10.24.0 azsdk-go-azblob/v1.3.1 (go1.19.12; Windows_NT)`, which helps collect some data about AzCopy usage on the server side. Example of using this can be accessed here: https://aka.ms/BlobClientTools (Ask Vishnu Charan TJ for access)

Storage Mover is an Azure cloud service which helps manage the transfer of data from and to different locations. It internally uses AzCopy. We collect metrics for Storage Mover, which can be accessed here: https://storagemoverprod-aaa9f9c6eyh6e7ex.eus.grafana.azure.com/dashboards/f/tpjJBAG4z/storage-mover-monitoring

## Problem statement 

Add client side telemetry/metrics collection to the AzCopy command line tool which will help us identify how customers are using AzCopy and its trends in usage over time.

## Goals and Non-Goals
 
- We want to collect metrics about: 
 - Amount of data transferred 
 - Number of objects transferred 
 - Latency of transfer 
 - Platform, such as Linux or Windows 
 - Country or region derived by Application Insights from the telemetry sender IP during ingestion
 - System configuration, such as CPU, memory, network, NIC speed, and whether data was transferred over public or private network 
 - Subcommand and CLI options usage 
   - Secret variables as part of CLI options should not be logged 
   - Need to consider very long argument lists 
 - Source, destination, and other source-target info, such as protocol, on-prem or intra-Azure 
   - Source mount information such as NAS, Windows Server, or clouds 
   - Destination information, including type and storage account 
 - Cloud type, such as public or government 
 - Auth mechanism, such as SAS key or Key Vault, without actual secret details 
 - Failure rates 
 - Any other customer info that can be captured, if possible, to give insights into where usage is coming from 
- Scalability such that ingestion of metrics from thousands of parallel AzCopy runs can be handled on the server side without data loss; eventual consistency is fine 
- Metrics should be easily accessible, similar to Azure Monitor or Geneva, and charts/dashboards can be made 
- Endpoint should be easily customizable, especially for testing 
- Retain data for at least 2 years 

## Scope

We are focused on CLI usage of AzCopy primarily. We already collect metrics from when AzCopy is used within the context of Storage Mover. 

The focus is on setting up a pipeline which is reliable and scalable and can help us make business decisions regarding focus on azcopy work.
  
## Stakeholders and Ownership 
 
- Product manager: Daniel Falkner 
- Other PMs: Raj Singh, Scott Hoag, Anusha Subramanian 
- Engineering managers: Sridhar Lanka and Raj Pathak 
- Engineer: Ankur Sharma

## Technical Design 

![AzCopy telemetry architecture: the AzCopy CLI binary on a customer-managed VM sends HTTP POST telemetry to an Application Insights endpoint in a Microsoft subscription (feeding dashboards), while the data path uses Entra ID auth to the Azure Storage endpoint in the customer subscription.](images/telemetry-design-20260630-135449.png)

### End-to-end telemetry analytics flow

![End-to-end AzCopy telemetry analytics flow: AzCopy emits sampled packed lifecycle events to workspace-based Application Insights and its Log Analytics customEvents table; ADX cross-service queries validate and separate suspicious data before persisting curated tables, which feed ADX and Azure Managed Grafana dashboards.](images/telemetry-analytics-flow.png)

Application Insights does not automatically copy data into the curated Kusto tables. The `customEvents` data remains in the Application Insights backing Log Analytics workspace until a scheduled query or orchestration process reads it through the ADX cross-service connection, applies validation rules, and writes accepted records or aggregates into owned ADX tables. The curated tables are the durable query surface for Grafana; rejected records and validation counters should be retained separately so filtering behavior can be audited.

The validation stage can use the following signals without treating any one signal as definitive proof:

- Supported schema and sampler versions, expected metric names, and required dimensions.
- Matching `job.started` and `job.finished` events for the same `JobID` and `InvocationID`.
- Valid relationships between bytes, transfer outcomes, percentages, durations, and sampling metadata.
- Plausible event volume and timing per installation, account, version, and topology.
- Optional correlation with Azure Storage server-side request/account telemetry at an approved aggregation level.

We plan to use a global Application Insights backend for collecting the metrics, provisioned as **two instances within a single dedicated subscription** — one **Test** instance fed only by the E2E pipeline, and one **Prod** instance fed by preview and GA builds (see [Staging the Application Insights instance](#staging-the-application-insights-instance-environments)). This subscription is dedicated to telemetry and has nothing to do with the customer's subscription. 
 
### Why Azure Monitor is a defensible metrics backend 

Azure Monitor gives us concrete, documented limits that are easier to defend than a custom logs-based pipeline. Application Insights has a financially backed 99.9% availability SLA.

For retention, workspace-based Application Insights data can be retained for up to 730 days, which matches the two-year requirement. Existing Application Insights guidance also documents a default daily cap of 100 GB/day, with increase paths up to 500 GB/day. This gives a concrete starting point for expected volume while leaving room to scale if adoption grows. 
 
Azure Monitor does not guarantee zero loss under unlimited burst traffic. Throttling can occur when ingestion limits are exceeded.

The Azure Monitor platform exposes utilization metrics and recommended alerts at 75% and 95% of ingestion limits, supports retry and offline storage behavior in the exporter, and gives us time to react before sustained throttling. If we stay within documented limits and monitor utilization, we can be reasonably sure Azure Monitor will not lose data.

### Scaling & service limits

Publically document Azure Monitor service limits - Azure Monitor | Microsoft Learn 
 
- Throttling: 32,000 events/second, measured over a minute 
- Total data per day limit: 100 GB 
 
After talking with Zaki Maksyutov from the Application Insights team, these publicly documented limits do not really apply to us and Application Insights is almost infinitely scalable for us: 
 
> Zaki: 100 GB - this limit you can bump yourself or completely remove. For 32k DPS - if you need more and have sustained ingestion, then we can bump it almost indefinitely. We will bump to next level only when the app is 10 times less than it, meaning once your app reaches 3.2k DPS your request will be approved. 

Lets run a rough analysis (with assumptions) to see if we will need to request a extension to the limits to begin with. The full derivation is in [Appendix A — Rough capacity analysis](#appendix-a--rough-capacity-analysis); in short, at 100% sampling our estimated volume (~1.5 billion runs/day, ~20k events/sec) would exceed the *default* limits, but those limits are liftable (per Zaki above) and we launch at **1% sampling**, which sits comfortably within them.

### Cost estimates for Azure Monitor
 
https://azure.microsoft.com/en-us/pricing/details/monitor 
 
If we assume 1 billion AzCopy runs/day (see Appendix A for this assumption): 

- Each metric payload is ~1 KB → 1000 GB/day → ~30 TB/month of ingested data 
- Pay-as-you-go at $2.30/GB: ~$24,150/month 
- Best commitment tier, 500 GB/day at $1.73/GB: ~$18,165/month 
- Storage and other costs are negligible in comparison to ingestion

With 1% sampling rate, this will be about $4000/month

### Subscription
 
We can use a single global non-customer-specific subscription in Azure to receive and store the metric data. Seanmcc@microsoft.com is the contact person for this subscription (called Xclient).

Within that subscription we will provision **two Application Insights instances**: one **Test** instance that only receives synthetic telemetry from the E2E pipeline, and one **Prod** instance that receives real (sampled) telemetry from both preview/early-adopter and GA builds. Each instance gets its own connection string. The Prod connection string is embedded into official builds, while the E2E pipeline supplies the Test connection string at runtime, so test telemetry never mixes with real customer telemetry. See [Staging the Application Insights instance (environments)](#staging-the-application-insights-instance-environments) for the detailed rationale and isolation options.
 
We have to do a privacy review with related teams with PM help and agree that no privacy-sensitive data is saved. 
 
### Staging the Application Insights instance (environments) 

Official AzCopy artifacts embed the Prod Application Insights connection string (see Authorization). E2E binaries do not embed the Test connection string; the E2E pipeline retrieves it from ARM and sets `AZCOPY_TELEMETRY_CONNECTION_STRING` for the test process. The existing runtime override takes precedence over an embedded value. This gives us a clean, low-cost way to separate test traffic from real customer traffic without producing a specially linked E2E binary. Recommended approach:

- **Provision two App Insights resources**, each with its own connection string / instrumentation key, both defined in the same Bicep/ARM template and deployed with an environment parameter: 
  - **Test** — target of the E2E pipeline. Receives only synthetic telemetry from automated test runs. Lets us validate ingestion, schema, and dashboards without polluting real data.
  - **Prod** — target of both the **preview/early-adopter** build and the **GA** build. Receives real (sampled) telemetry. The preview build exposure is bounded by the small preview audience plus the 1% sampling rate — and the GA build then ramps the same instance to the full installed base but still with 1% sampling.

The E2E pipeline identifies the Test resources with `AZCOPY_TELEMETRY_SUBSCRIPTION_ID_TEST`, `AZCOPY_TELEMETRY_RESOURCE_GROUP_TEST`, and `AZCOPY_TELEMETRY_APP_NAME_TEST`. It retrieves the connection string and Log Analytics workspace customer ID from ARM, then exposes them only to the E2E process as `AZCOPY_TELEMETRY_CONNECTION_STRING` and `NEW_E2E_APP_INSIGHTS_WORKSPACE_ID`. Official Linux, Windows, and macOS build templates continue to inject the Prod connection string through Go `-ldflags`. Local builds contain no connection string and keep telemetry disabled unless `AZCOPY_TELEMETRY_CONNECTION_STRING` is explicitly set at runtime. The all-zero placeholder is rejected.

We can have have separate resource groups but on the same subscription for Test vs Prod (cleaner cost attribution and access control).

### Provisioning approach (IaC + pipeline) 

The two Application Insights instances are provisioned with a **Bicep template driven by a dedicated, manually-triggered Azure DevOps pipeline** — not as part of CI or the release pipeline. 

**Why this shape:** 
- The repo has **no existing IaC**; all Azure interaction is via ADO pipelines authenticated by **workload-identity service connections** (e.g. `azcopy-release`, `azcopytestworkloadidentity`). This approach follows that established convention. 
- Provisioning is a **rare, near-one-time** infrastructure task. Putting it in `azure-pipelines.yml` (runs on every PR/push) or `build-1es-pipeline.yaml` (build → sign → publish) would risk accidental re-deploys and couple infra lifecycle to code lifecycle. A separate `trigger: none, pr: none` pipeline (mirroring `e2e-cleanup.yml`) keeps it isolated and explicit. 
- **Bicep over raw ARM JSON:** Bicep is the modern Azure IaC, compiles to ARM, and is far less verbose for ~2 resources. 
- **Bicep without EV2:** These are two low-churn, single-region telemetry backends. A manually triggered Azure DevOps pipeline validates, previews, and deploys the Bicep template directly.

### Azure Monitor OpenTelemetry Exporter 
 
AzCopy is written in Go. Use the industry-standard, vendor-neutral OpenTelemetry SDK. Add an exporter as a code component in the same binary which receives OTel data and forwards it to Application Insights. It ultimately does a POST request to the `/v2.1/track` endpoint of App Insights. 
 
POC standalone Go binary sending metrics to Application Insights: go-scratchpad/metrics/metrics.go at main · sharankur_microsoft/go-scratchpad
 
### Authorization 
 
Authorization to Application Insights can be done in two ways: standard AAD and connection-string based. 
 
- Microsoft Entra ID / Azure AD token-based authentication 
 - The client obtains an OAuth2 bearer token from Entra ID and presents it on ingestion, so only authenticated callers can write telemetry 
 - Requires an identity, such as managed identity or service principal, granted the Monitoring Metrics Publisher role on the App Insights resource 
- Connection-string / Instrumentation-key 
 - Microsoft states the ikey is not a secret/security token; it only routes telemetry to a resource and does not gate who can write 
 - Simplest to deploy: no identity, no role assignment. Works for anonymous/OSS binaries 

The proposed solution is to hardcode/embed the **Prod** Application Insights connection string hosted in the XClient subscription into preview and GA AzCopy binaries. E2E runs use the same telemetry implementation but override the destination at runtime with the dedicated **Test** connection string retrieved from ARM (see [Staging the Application Insights instance](#staging-the-application-insights-instance-environments)).
 
The pitfall is that anyone who can extract the connection string will be able to send bogus metrics to Application Insights. However, they will not be able to read any metrics data just based on a connection string.

With security consultation, we concluded that this approach is viable as long as we are able to 1) Maintain our rate limits so that the ingestion and storage costs dont explode because of bogus data 2) detect and filter out bogus data using heuristics

Azure CLI and Azurite are two other open-source projects using publicly accessible connection strings for sending metrics to Application Insights. 
 
### How to differentiate between bogus data and genuine data? 
  
- Compare that we received a corresponding number of requests on Azure Storage data plane APIs for the duration of the job run 
- Count and relative timings of start job metric events and finish job metric events should make sense 
- Discard wrong schema 

### Exact metrics being sent 

Each lifecycle event is sent to Application Insights as one `Microsoft.ApplicationInsights.Event` envelope. The numeric data points live under `data.baseData.measurements`, and all resource attributes + job dimensions are sent once as a shared `data.baseData.properties` bag (all property values are strings). The `job.started` and `job.finished` events for one attempt share the same `JobID` and `InvocationID` so they can be correlated. Application Insights stores these envelopes as one `customEvents` row per lifecycle event.

Every property value is bounded before either exporter receives it. The fallback limit is 1,024 UTF-8 bytes. Known identifiers and categorical values use limits from 5 to 128 bytes, host and account descriptions use 256 bytes, endpoint identities and compact error/advice lists use 512 bytes, and normalized option values default to 512 bytes. `OptFlagsSet` remains at 1,024 bytes, while `OptEnvVarsSet` uses 512 bytes. Truncation preserves valid UTF-8 and appends `...(truncated)` whenever the property's limit can contain the marker.

The example below is generated directly from the AzCopy serialization code (an on-prem-style block-blob upload from local disk to a public-cloud Blob account, with a few failed transfers).

#### Event 2 — `azcopy.job.finished`

The finished event carries the same property bag as started event plus `JobStatus`, `TerminalStage`, and (when there were transfer failures) `FailureErrorCodes`, and adds all the numeric measurements.

For both backends, one event is sent in one `/v2.1/track` HTTP batch containing one packed envelope. A finished event has 50 entries in its `measurements` map without repeating the property bag. The exact production-shaped samples are in `SampleTelemetryPayloads.md`.

Compact serialized sizes from the production serializer are approximately 1.7 KB for `job.started`, 3.9 KB for `job.finished`, and 5.6 KB for the paired transfer attempt. The previous metric-row representation was approximately 98-100 KB per paired attempt because it repeated dimensions across 51 rows.

Current packed shape:

```json
{
  "name": "Microsoft.ApplicationInsights.Event",
  "time": "2026-07-26T12:00:00Z",
  "iKey": "00000000-0000-0000-0000-000000000000",
  "data": {
    "baseType": "EventData",
    "baseData": {
      "ver": 2,
      "name": "azcopy.job.finished",
      "properties": {
        "Command": "copy",
        "JobStatus": "Completed",
        "JobID": "b3f2c1a4-9d5e-4f8a-bc12-3456789abcde",
        "AzCopyVersion": "10.32.2"
      },
      "measurements": {
        "azcopy.job.finished": 1,
        "azcopy.bytes_transferred": 5368709120,
        "azcopy.objects_completed": 128,
        "azcopy.job_duration_seconds": 42.5
      }
    }
  }
}
```

The older metric-envelope example below is retained only to document the superseded wire shape and must not be used for new queries.

```json
{
  "name": "Microsoft.ApplicationInsights.Metric",
  "time": "2026-06-25T14:42:18Z",
  "iKey": "00000000-0000-0000-0000-000000000000",
  "data": {
    "baseType": "MetricData",
    "baseData": {
      "metrics": [
        {
          "name": "azcopy.job.finished",
          "value": 1,
          "count": 1
        },
        {
          "name": "azcopy.bytes_enumerated",
          "value": 5410652160,
          "count": 1
        },
        {
          "name": "azcopy.bytes_expected",
          "value": 5381296128,
          "count": 1
        },
        {
          "name": "azcopy.bytes_transferred",
          "value": 5368709120,
          "count": 1
        },
        {
          "name": "azcopy.bytes_over_wire",
          "value": 5402263552,
          "count": 1
        },
        {
          "name": "azcopy.objects_scheduled",
          "value": 1295,
          "count": 1
        },
        {
          "name": "azcopy.objects_completed",
          "value": 1280,
          "count": 1
        },
        {
          "name": "azcopy.objects_failed",
          "value": 3,
          "count": 1
        },
        {
          "name": "azcopy.objects_skipped",
          "value": 12,
          "count": 1
        },
        {
          "name": "azcopy.transfers_completed",
          "value": 1280,
          "count": 1
        },
        {
          "name": "azcopy.transfers_failed",
          "value": 3,
          "count": 1
        },
        {
          "name": "azcopy.transfers_skipped",
          "value": 12,
          "count": 1
        },
        {
          "name": "azcopy.transfers_total",
          "value": 1295,
          "count": 1
        },
        {
          "name": "azcopy.job_duration_seconds",
          "value": 738,
          "count": 1
        },
        {
          "name": "azcopy.transfer_phase_duration_seconds",
          "value": 700,
          "count": 1
        },
        {
          "name": "azcopy.job_throughput_mbps",
          "value": 58.2,
          "count": 1
        },
        {
          "name": "azcopy.transfer_phase_throughput_mbps",
          "value": 61.3,
          "count": 1
        },
        {
          "name": "azcopy.average_storage_http_attempt_e2e_ms",
          "value": 42,
          "count": 1
        },
        {
          "name": "azcopy.avg_iops",
          "value": 173,
          "count": 1
        },
        {
          "name": "azcopy.storage_http_attempt_count",
          "value": 127674,
          "count": 1
        },
        {
          "name": "azcopy.network_error_attempt_count",
          "value": 255,
          "count": 1
        },
        {
          "name": "azcopy.server_busy_503_count",
          "value": 1021,
          "count": 1
        },
        {
          "name": "azcopy.server_busy_throughput_count",
          "value": 600,
          "count": 1
        },
        {
          "name": "azcopy.server_busy_iops_count",
          "value": 300,
          "count": 1
        },
        {
          "name": "azcopy.server_busy_other_count",
          "value": 121,
          "count": 1
        },
        {
          "name": "azcopy.server_busy_pct",
          "value": 0.8,
          "count": 1
        },
        {
          "name": "azcopy.network_error_pct",
          "value": 0.2,
          "count": 1
        }
      ],
      "properties": {
        "BlobType": "BlockBlob",
        "SourceCloudType": "",
        "DestCloudType": "public",
        "Command": "copy",
        "DestAuthMechanism": "OAuthToken",
        "DestEndpointKind": "public",
        "DestProtocol": "https",
        "DestStorageAccount": "mystorageacct",
        "DestType": "Blob",
        "FailureErrorCodes": "403:2,500:1",
        "FromTo": "LocalBlob",
        "HostArch": "amd64",
        "HostCPUModel": "Intel(R) Xeon(R) Platinum 8370C CPU @ 2.80GHz",
        "HostMemoryTotalGB": "32",
        "HostNICSpeedMbps": "10000",
        "HostNumCPU": "8",
        "AzureVMDetected": "true",
        "InstallationID": "8f14e45fceea167a5a36dedd4bea2543",
        "InvocationContext": "ci",
        "JobStatus": "CompletedWithErrors",
        "JobErrorCategory": "transfer",
        "JobErrorCode": "transfer-failures",
        "OSType": "linux",
        "OSVersion": "Ubuntu 22.04.3 LTS",
        "OptBlockSizeMB": "8",
        "OptCapMbps": "false",
        "OptConcurrency": "32",
        "OptFlagsSet": "recursive,put-md5,block-size-mb,block-blob-tier",
        "OptOverwrite": "true",
        "OptPreserveSMBPermissions": "false",
        "OptPutMD5": "true",
        "OptRecursive": "true",
        "RequestedAccessTier": "Cool",
        "JobID": "b3f2c1a4-9d5e-4f8a-bc12-3456789abcde",
        "AzCopyVersion": "10.32.2",
        "SourceAuthMechanism": "Anonymous",
        "SourceMountType": "local-disk",
        "SourceProtocol": "local",
        "SourceStorageAccount": "",
        "SourceType": "Local",
        "FromTo": "LocalBlob"
      }
    }
  }
}
```

Byte metric definitions:

- `bytes_enumerated` is the sum of source sizes for transfers that AzCopy added to job plans after filtering and scheduling decisions. It measures scheduled work, not every object merely scanned. Folder-property transfers normally contribute zero bytes.
- `bytes_expected` is the current progress denominator. A live in-process job initializes it from all scheduled bytes, so it may still include work that later fails. A summary reconstructed from job plans excludes failed and skipped transfers according to their persisted statuses. Interpret this metric together with `JobStatus`, transfer counts, and whether the event came from a live attempt. On a clean completed job it normally equals `bytes_transferred`.
- `bytes_transferred` is logical payload progress: bytes successfully completed plus successfully processed bytes in active transfers when a live summary is taken. It excludes failed transfers and does not double-count bytes retransmitted during retries.
- `bytes_over_wire` is physical payload traffic observed by the transfer engine. It includes duplicate bytes sent or received during retries and traffic for transfers that later fail.

Useful relationships and limitations:

- `bytes_enumerated - bytes_expected` approximates scheduled bytes that are no longer expected to succeed, such as failed or skipped work. Interpret it only after enumeration is complete.
- `bytes_transferred / bytes_expected` is the basis of percent complete, capped at 100%.
- `bytes_over_wire - bytes_transferred` is **not** a pure retry-byte metric because it also includes traffic from failed transfers and may reflect protocol behavior. Comprehensive retry-byte accounting needs separate instrumentation.
- For deletion or folder-only jobs, all byte metrics may legitimately be zero even when many transfers occur.

Object metric definitions:

- A **payload object** is a regular file/cloud object, a preserved symlink, or a hardlink converted and scheduled as a file transfer.
- A **folder-property transfer** represents creating/preserving a folder's existence and properties. It does not include files contained by that folder and is excluded from payload-object counts.
- `objects_scheduled` is `total transfers - folder-property transfers` using saturating subtraction.
- `objects_completed`, `objects_failed`, and `objects_skipped` similarly exclude the corresponding folder-property outcomes.
- `regular_files_scheduled`, `symlinks_scheduled`, and `hardlinks_converted_scheduled` provide the scheduled payload-object type breakdown. Their sum should equal `objects_scheduled` for a complete, internally consistent summary.
- `folder_properties_scheduled/completed/failed/skipped` are emitted separately.
- Existing `transfers_*` metrics remain for compatibility and include both payload objects and folder-property transfers.
- These are scheduled/transfer outcome counts, not every source or destination object merely scanned. Sync scans both sides and schedules only differences.

Source dataset-shape metrics:

- Shape metrics cover **filtered source payload objects** only: regular files/cloud objects, preserved symlinks, and converted hardlinks. Folder-property entries and destination objects are excluded.
- `source_objects_scanned` is the denominator for the source shape measurements. For sync, this is the filtered source side regardless of whether source or destination is enumerated first.
- `source_bytes_scanned` is the exact sum of source sizes for that population, and `source_average_object_size_bytes` is `source_bytes_scanned / source_objects_scanned`.
- `source_object_size_p50_bytes_approx`, `p90`, and `p95` are approximate percentile upper bounds computed from a fixed histogram with upper bounds at 0 B, 1 KiB, 16 KiB, 256 KiB, 1 MiB, 16 MiB, 256 MiB, 1 GiB, 16 GiB, and the largest observed value. Individual sizes are never retained.
- `source_objects_under_1_mib` counts source payload objects whose size is strictly less than 1 MiB. `source_objects_under_1_mib_ratio_pct` is that count divided by `source_objects_scanned` times 100.
- `source_max_directory_depth` is the maximum number of containing segments in a filtered source object's relative path. A root-level object has depth 0; `a/b/file` has depth 2. Both slash styles are recognized, and paths are never emitted.

Source scope metrics:

- `containers_scanned` / `buckets_scanned` count distinct source scopes selected for traversal. A direct single-container/bucket source counts as one; account/service traversal counts the selected scopes returned by the traverser.
- `containers_touched` / `buckets_touched` count distinct source scopes from which at least one payload object was successfully appended to a job part after compatibility and filtering decisions.
- Azure Blob containers, ADLS filesystems, and Azure Files shares use the generic container counters. S3 and GCS use bucket counters.
- Scope names are held only in per-attempt in-memory sets for deduplication and are not emitted by these count metrics.
- A scope selected for scanning can fail during traversal or schedule no transfers, so `touched` can be lower than `scanned`.

Additional bounded dimensions and diagnostics:

- `SourceScope` and `DestScope` identify service, container/share/bucket, object-or-prefix, local directory/object, stream, benchmark, or none without emitting paths.
- Source and destination identity fields contain only normalized Azure storage account names. S3/GCS, custom domains, emulators, and unparseable endpoints emit no client identity.
- Authentication distinguishes `SAS` from `PublicAnonymous`; local, pipe, benchmark, and none endpoints use `NotApplicable`.
- `InvocationID` is a random 128-bit identifier for one command/job attempt. Paired start/finish events share it while `JobID` remains the resumable job key.
- `PerformanceConstraint` and up to eight deduplicated `PerformanceAdviceCodes` are emitted in priority order; the first code is primary. Human-readable advice text is not emitted.
- Main benchmark jobs use the same paired lifecycle with `Command=bench`. `BenchmarkMode`, `BenchmarkFileCount`, `BenchmarkFileSizeBytes`, `BenchmarkFolderCount`, `BenchmarkCleanupRequested`, and `BenchmarkIsCleanup` carry effective inputs on both events.
- Benchmark results reuse `JobStatus`, `TerminalStage`, job-error fields, and generic finish diagnostics. No benchmark description or human-readable advice title/reason is emitted.
- Automatic benchmark cleanup stays outside the benchmark event lifecycle. `BenchmarkIsCleanup=false` on main events makes the exclusion explicit; cleanup transfers emit no benchmark result.
- `FailureErrorOtherCount` reports failed-transfer occurrences omitted when the error histogram is capped to ten distinct codes.
- `HostNICSpeedBucket` makes unknown link speed explicit and supports low-cardinality aggregation.
- `enumeration_phase_duration_seconds` measures progress-tracker start through final job-part dispatch. It overlaps transfer and must not be summed with transfer-phase duration.

Network metric semantics:

- `HostNICSpeedMbps` is the highest advertised link speed AzCopy can discover. It is best-effort and may not be the interface used for the transfer; `-1` means unknown.
- `AzureVMDetected` is `true` when Azure Instance Metadata Service responds and `false` otherwise. A `false` value does not prove that the host is on-premises or non-virtualized.
- Job duration/throughput cover the entire attempt. Transfer-phase duration begins when the first job part is ordered and ends at terminal wait; it may overlap enumeration because AzCopy pipelines scanning and transfer.
- `average_storage_http_attempt_e2e_ms` is the mean duration of a Storage HTTP attempt observed by the SDK per-retry policy. Retries are separate attempts; this is not logical-operation latency or network RTT.
- Raw operation, network-error, and categorized HTTP 503 counts are emitted alongside percentages so aggregate rates can be calculated correctly.
- `server_busy_503_count` is a throttling/server-busy signal, not a comprehensive retry count. SDK retries, body-read retries, and retry bytes require additional instrumentation.
- `DestEndpointKind` describes endpoint configuration only. Actual public/private routing and byte attribution are deferred because Private DNS, proxies, and VPNs require transport-level inspection.

Sampling and correlation:

- `SchemaVersion=3`, the effective `SamplingRate`, and `SamplerVersion=job-id-sha256-v1` are attached to every emitted event. The sampler version defines JobID as the fixed sampling key, so a separate `SamplingUnit` property is not emitted. The default sampling rate is `0.01`; test and diagnostic runs can override it with the hidden `--telemetry-sampling-rate` flag using a value from `0.0` through `1.0`. The same effective value drives the JobID inclusion decision and the emitted metadata. Azure cloud environments are recorded independently as `SourceCloudType` and `DestCloudType`.
- Inclusion is a deterministic SHA-256 threshold decision over `SamplerVersion + JobID`. The original and every resumed attempt sharing a JobID are therefore included or excluded together.
- `InvocationID` and timestamps are not part of the sampling key. Raising the threshold creates a nested cohort without reshuffling existing JobIDs.
- `InvocationID` identifies one command/job attempt; paired start/finish events share it. `JobID` remains the resumable job key across the original and resumed invocations.
- `Command=jobs.resume` identifies resume attempts without a duplicate attempt-type property.
- Resume events emit `SummaryCounterScope=job-cumulative` because job-plan byte/object/outcome and Storage-operation summary counters retain cumulative totals. Attempt and phase durations always describe the current invocation. Original attempts omit `SummaryCounterScope` because their summary counters and durations both describe that attempt.
- `FromTo` is the canonical source/destination classification. Dashboards derive transfer direction and provider topology from `FromTo`, `SourceType`, and `DestType` instead of receiving duplicate properties.

Terminal lifecycle:

- Every non-dry-run copy/sync/benchmark attempt that emits `job.started` defers exactly one `job.finished`, including enumeration failure, manager failure, and explicit context cancellation paths. Accepted resume attempts use the same lifecycle around the resume request and terminal wait.
- `TerminalStage` is a bounded lifecycle value (`initialization`, `enumeration`, `transfer`, `completion`, or `completed`). Successful and partial-success finishes use `completed`; failures and cancellations retain the active stage.
- The finish event is emitted before job-manager cleanup so the finalizer can read the live summary and progress counters. Cancellation classification also checks the attempt context, because a terminal wait can return a different error after cancellation.
- Process crashes, forced termination, and machine loss cannot run the finalizer. Ingestion must classify a sampled start without a matching finish after a defined timeout as probable abandonment, not explicit cancellation.

Job-level error classification:

- Failed and partial-success finish events can include `JobErrorCategory` and `JobErrorCode`. Completed and cancelled events leave both empty; cancellation is represented by `JobStatus=Cancelled` rather than treated as an error.
- Categories are bounded to authentication, authorization, throttling, timeout, network, local I/O, conflict, not found, service, AzCopy internal, lifecycle-stage fallback, or unknown.
- Typed Azure `ResponseError` values retain a service `ErrorCode` only when it is at most 64 ASCII characters and contains only letters, digits, `_`, `-`, or `.`. When no safe service code exists, the classifier emits a fixed HTTP-status or generic code.
- Context deadlines, network errors, local path errors, and AzCopy internal errors use fixed machine-readable codes. Untyped failures fall back to the bounded terminal stage, such as `enumeration-error` or `transfer-error`.
- Raw error messages, paths, URLs, request IDs, and response bodies are never emitted. The separate `FailureErrorCodes` histogram continues to represent per-transfer numeric HTTP outcomes.

### Timing of metrics being sent 

We can send metrics in two batches: one at the start of the process with details about source, target, platform, CLI options, subcommand, etc.; and one at the end with number of objects, number of bytes transferred, number of failures, latency stats, etc. 
 
This allows us to get some information even if the AzCopy command does not run to completion, for example due to crash or being killed. 
 
### Error handling and failure mode 

HTTP 429 and 503 can be retried asynchronously a couple of times with exponential backoff and jittering. Permanent failures like DNS error, bad configuration, or authorization error should not be retried.

### Does AzCopy stall if Azure Monitor throttles? 
 
No. The exporter path should run in a separate asynchronous pipeline. Go has the concept of goroutines, which should make this easier to program.
 
### How the error is surfaced to the customer 
 
Non-fatal warning in debug/verbose logs, but it should not fail the AzCopy command. Making the error more visible may confuse customers about whether there was a critical issue with their data transfer versus a metrics issue. Developers may rely on E2E validation to detect telemetry pipeline issues. 

### Do we ask customers to turn off telemetry? 
 
We can expose a command-line option to turn off telemetry, but it should be on by default. Otherwise, customer adoption and data will be low. Retry policy, bounded buffering, and dropping telemetry when needed should make it so customers do not have to think about this. 
 
### Competing for resources with AzCopy 
 
Investigate how much additional memory usage is added by metrics collection and whether it can be turned on without significant impact, for example no more than 100 KB. 


### Infra & pipelines

We will have one prod subscription and one corp subscription. Corp subscription will be used for e2e testing.

The following resources will have to be provisioned as part of ADO pipeline associated with azcopy.

- **Test backend resource group**
  - Log Analytics workspace
  - Workspace-based Application Insights
  - 30-day retention
  - Low daily cap
  - Query alerts
  - Workbook
- **Production backend resource group**
  - Separate Log Analytics workspace and Application Insights resource
  - 365-day retention
  - Measured daily cap
  - Budget alerts
  - Workbook
  - Delete lock
- **Monitors**
  - Action group for ingestion, cap, schema, and no-data alerts
- **E2E run resource group**
  - Small StorageV2 account
  - Source and destination containers
  - Optional file share
  - Run-specific test data
- **Identities**
  - Corp-tenant workload identities for infrastructure, E2E execution and query, and release builds
- **Governance**
  - Tags
  - Resource locks
  - Azure Policy compliance
  - Deployment history
  - Azure DevOps Environment approval for production

### E2E pipeline

1. Authorize with corp tenant
• Register required resource providers during initial bootstrap.
2. Validate infrastructure
• Run Bicep build/lint.
• Validate parameter files and naming.
• Run ARM/Bicep  what-if .
• Scan for accidental deletion or retention/cap reductions.
3. Approval
• Test deploys automatically on manual/nightly runs.
• Prod uses an ADO Environment with owner approval and change evidence.
4. Deploy persistent backend (use bicep template)
• Create the environment-specific resource group.
• Deploy workspace, App Insights, action group, alerts, workbook, RBAC, budget, and Prod lock.
• Output resource IDs and the workspace customer ID for deployment validation.
5. Backend health validation
• Confirm provisioning state, retention, cap, public ingestion, local-auth settings, and query permissions.
• Prod stops here; never send synthetic telemetry to Prod.
6. Provision E2E fixtures — Test only
• Create a run-scoped resource group and storage account/containers.
• Assign the E2E identity  Storage Blob Data Contributor .
• Output account/container names to later stages.
7. Configure and build telemetry-enabled AzCopy
• Retrieve the Test Application Insights connection string and workspace customer ID from ARM using the E2E workload identity.
• Verify that the selected Application Insights component is tagged `environment=test`.
• Export `AZCOPY_TELEMETRY_CONNECTION_STRING`, `NEW_E2E_APP_INSIGHTS_WORKSPACE_ID`, and a unique `AZCOPY_E2E_TELEMETRY_RUN_ID` only to the E2E process.
• Build the normal E2E binary without telemetry-specific `-ldflags`.
• The E2E launcher forces the hidden `--telemetry-sampling-rate=1` option while validation is enabled.
• Release builds separately retrieve and embed the Prod connection string.
8. Run synthetic scenarios
• Successful local-to-Blob copy.
• Successful sync or benchmark smoke test.
• One controlled terminal failure or cancellation.
• Capture every terminal AzCopy Job ID from structured output and count repeated IDs from resume operations.
9. Validate App Insights ingestion
• During suite teardown, poll for up to five minutes to account for ingestion latency.
• Acquire a Log Analytics token using the workload identity and query `AppEvents`.
• Filter by the suite start time, `azcopy.job.finished`, and `E2ETestRunID`; then count events by `JobID`.
• Assert:
• At least the expected number of terminal `azcopy.job.finished` events arrived for every captured Job ID.
• Authentication, authorization, malformed query, and malformed response failures fail immediately.
• Network errors and HTTP 408, 429, 500, 502, 503, and 504 are retried within the bounded polling window.
10. Teardown
• Use  condition: always() .
• Delete only the run-scoped E2E resource group.
• Support a Storage-Mover-style manual pause to retain failed fixtures temporarily.
• Never delete the persistent Test/Prod telemetry backends.

Implemented safeguards for reliable E2E:

- The hidden sampling override is forced to 100% only when live E2E validation is fully configured.
- Partial configuration fails framework setup rather than silently skipping validation.
- `E2ETestRunID` is emitted only when explicitly configured and is bounded to the same identifier-size limit as other telemetry dimensions.
- Query access uses workload identity and least-privilege RBAC. Bicep can optionally assign Log Analytics Reader on the workspace and Reader on the Application Insights component.
- The persistent Test/Prod telemetry backends are never deleted by run cleanup.

TODOs after MVP:
- add testing for non azure scenarios as well
- add testing with file shares involved
- add testing with gcp or aws as the source or destinations

## Timelines

Action items:

M1 - emitting in PPE - by end of the month
M2 - Provisioning resources in Prod

- Go over Production timelines for Mover - Sync up with Shilpa
- Single meeting for Azcopy with relevant people - 
- Too aggressive timelines
- New subscription in AME

- Separate the timlines from design doc. Dont publish timelines publically yet.
- Written confirmation for security review approval
- Focus on regressions - Azcopy - Sashank & Adele - Common forum (create a recurring meeting and teams space)
- Preview release might not be necessary
- Security review with Thread Model updated


| Category | Activity | Owner | ETA | Comments |
|---|---|---|---|---|
| Spec Closure | Security review | Raj, Sridhar, Daniel | 17/06/2026 | DONE |
| Spec Closure | Present design spec in Scrum group | Ankur | 01/07/2026 |  |
| Spec Closure | Present design in Tuesday XDataManagement review meeting | Ankur | --/06/2026 |  |
| Spec Closure | Privacy review | Daniel Falkner | 02/07/2026 |  |
| Development and Integration | Provision the two Application Insights instances (Test + Prod) in XClient subscription using ARM or Bicep template in AzCopy pipeline | Ankur | 02/07/2026 |  |
| Development and Integration | Implement sending of metrics with configurable sampling rate in AzCopy, default 1% | Ankur | 04/07/2026 |  |
| Development and Integration | Create the dashboards and metrics on Application Insights Azure portal, and integrate with other server-side Grafana dashboard to filter out bogus data | Ankur | 07/07/2026 |  |
| Development and Integration | Add E2E tests for receiving the metrics in Azure Monitor from AzCopy | Ankur | 09/07/2026 |  |
| Milestone | M1 — Code Complete (telemetry implemented, E2E tests implemented, merged to main, unit tests green, design/security/privacy reviews closed) | Ankur | 09/07/2026 |  |
| Development and Integration | Open PR against the AzCopy `main` branch (CI runs build + E2E across Linux/Windows/macOS) | Ankur | 10/07/2026 |  |
| Development and Integration | Feature sign-off for AzCopy | Sridhar | 14/07/2026 |  |
| Development and Integration | Merge PR into `main` after approvals and required checks pass | Ankur | 15/07/2026 |  |
| Development and Integration | R2D Checklist sign-off |  | - |  |
| Deployment | Publish a prerelease / early-adopter build of AzCopy (emitting to the Prod Application Insights instance, bounded by the small preview audience + 1% sampling) | Ankur | 17/07/2026 |  |
| Deployment | Monitor the metrics ingestion rates and bogus metric percentage |  | Throughout |  |
| Milestone | M2 — E2E testing validated in Test environment / preview build (Canary SignOff) — telemetry backend validated end-to-end via E2E pipeline; client shipped as a GitHub **prerelease/preview** build with sampling at 1% as the controlled-exposure knob | Ankur | 23/07/2026 |  |
| Milestone | M3 — Production deployment and signoff — promote prerelease to GA release across all channels (download/version-check container, GitHub release, Docker/ACR, Linux PMC repos) (date depends on AzCopy's release cadence — stable minors ship ~quarterly, with a ~1–2 month preview→GA soak window) | Ankur | --/--/2026 |  |
|

### Staggered rollout 
 
Start deployment with a minimum sampling rate of 1%. Observe ingestion rates and costs. We can also do this by geography to start with, for example roll it out only for EMEA. Depending on costs and confidence in metrics, increase sampling rate to 5% or 10% in the next rollout, and then add more geographies. 

# Appendix

## Appendix A — Rough capacity analysis

This is the back-of-the-envelope capacity math referenced under [Analysis of service limits](#analysis-of-service-limits). It is preserved for reference; the practical conclusion is that we launch at 1% sampling, well within limits, and the default limits are liftable if/when we ramp.

Max AzCopy runs per day or per second a single instance of Azure Monitor can support: 
 
- Number of metric events per AzCopy run that will be ingested: 2 
- Amount of metrics data per AzCopy run that will be transferred: ~2 KB 
- Assume 100% sampling rate
- Max AzCopy runs supported per day in terms of data limit: 400 million 
- Max AzCopy runs supported per second in terms of events throttling: 16,000

Screenshot from https://aka.ms/BlobClientTools: 
 
- Number of requests from AzCopy towards Azure Storage per week: ~200 billion, which is ~1 billion per hour, or ~300k/sec 
- Number of unique storage accounts: ~1 billion per week 
- Number of AzCopy job runs in a week: not known 
- If we assume number of AzCopy runs to be 10x the number of unique storage accounts, we get 10 billion AzCopy runs per week, 1.5 billion per day, or 20k per second
 
1.5 billion is higher than 400 million calculated earlier. 20k per second is higher than the 16k per second limit calculated earlier. 

So based on the above stated estimates, assumptions and at at 100% sampling rate, we will need to request extension to the service limits.

But we are planning to release with 1% sampling rate which will be comfortably within the limits and will also give us a more accurate estimate of how much metrics we will expect to ingest with a higher sampling rate.
