# AzCopy Business Metrics ADX Dashboard

This directory contains an importable ADX dashboard and its source KQL for the business metrics requested in `StorageMoverAzCopyComparison.md`.

## Import The Dashboard

1. Open [ADX Dashboards](https://dataexplorer.azure.com/dashboards) in a browser that supports your Microsoft certificate authentication.
2. Select the arrow next to **New dashboard**.
3. Select **Import dashboard from file**.
4. Select `azcopy-business-metrics.dashboard.json` from this directory.
5. Name the dashboard `AzCopy Business Metrics` and select **Create**.

The imported dashboard contains 17 tiles across six pages and uses one XStore data source. Client telemetry and XDataAnalytics queries use explicit cross-cluster references, so no additional dashboard data sources are required.

If the imported XStore source needs reconnecting, set it to:

```text
Cluster: https://azcore.centralus.kusto.windows.net
Database: Xstore
```

Regenerate the dashboard after changing a source `.kql` file:

```powershell
./dashboards/azcopy-business-metrics/generate-dashboard.ps1
```

The generator uses Microsoft's documented ADX dashboard schema version 60 and stable IDs, so regeneration produces reviewable output rather than unrelated ID churn.

## Data Sources

The generated dashboard uses XStore as its single dashboard data source and references the other sources through cross-cluster KQL. It includes a 24-hour default time-range parameter that defines `_startTime` and `_endTime`. The source details are documented below for troubleshooting and manual use.

### Client Telemetry

Name: `AzCopyClientTelemetry`

```text
https://ade.applicationinsights.io/subscriptions/31347be8-d066-464e-9866-7e58d85027b7/resourcegroups/sharankur_playground/providers/microsoft.insights/components/sharankur_insights1
```

Database: `sharankur_insights1`

Table: `customEvents` (numeric `customMeasurements` are expanded into a virtual metric name/value shape by each query)

For cross-service queries initiated from a native ADX cluster, use `https://adx.monitor.azure.com/...` as shown in `queries/server/04_client_server_account_hour.kql`.

### XStore User-Agent Telemetry

Name: `XStoreUserAgent`

```text
https://azcore.centralus.kusto.windows.net
```

Database: `Xstore`

Function: `XAggUserAgentTelemetryMetric()`

### Storage Transaction And Account Data

Name: `XDataAnalytics`

```text
https://xdataanalytics.westcentralus.kusto.windows.net
```

Database: `XDataAnalytics`

Tables:

- `XStoreAccountTransactionsHourly`
- `XStoreAccountPropertiesDaily`

### Subscription Tenant Enrichment

ARG 1P is required to map `SubscriptionId` to the owning Microsoft Entra tenant. This query is intentionally not placed directly on the dashboard until ARG access and privacy controls are approved. Use `InternalSubscriptionResources` with a 60-hour lookback and latest-record `arg_max` as documented by ARG 1P.

## Parameters

Every ADX dashboard has the default time-range parameter. The queries use:

- `_startTime`
- `_endTime`

Add one free-text parameter:

| Label | Variable | Type | Default | Pages |
| --- | --- | --- | --- | --- |
| Storage account | `_account` | String | Empty | All pages; used by Customer Drilldown, Data Quality, and Server Correlation queries |

## Page And Tile Manifest

### Overview

| Tile | Query | Data source | Visual |
| --- | --- | --- | --- |
| Jobs, bytes, completion and unmatched starts | `queries/client/01_overview_cards.kql` | AzCopyClientTelemetry | Multi stat |
| Volume by topology | `queries/client/02_volume_topology_trend.kql` | AzCopyClientTelemetry | Stacked column or time chart |
| Outcomes by command | `queries/client/03_outcome_distribution.kql` | AzCopyClientTelemetry | Stacked bar |
| Weekly command and version trend | `queries/client/10_weekly_command_version_trend.kql` | AzCopyClientTelemetry | Stacked column chart |

### Performance

| Tile | Query | Data source | Visual |
| --- | --- | --- | --- |
| Throughput and completion-time percentiles | `queries/client/04_performance_percentiles.kql` | AzCopyClientTelemetry | Table |
| Platform and version mix | `queries/client/07_platform_mix.kql` | AzCopyClientTelemetry | Table or bar chart |

### Reliability

| Tile | Query | Data source | Visual |
| --- | --- | --- | --- |
| Reliability rates | `queries/client/05_reliability_cards.kql` | AzCopyClientTelemetry | Multi stat |
| Top job error categories | `queries/client/06_error_distribution.kql` | AzCopyClientTelemetry | Bar chart |

### Data Quality

| Tile | Query | Data source | Visual |
| --- | --- | --- | --- |
| Telemetry acceptance | `queries/client/08_telemetry_quality.kql` | AzCopyClientTelemetry | Multi stat |
| Rejected and suspect attempts | `queries/client/12_telemetry_rejections.kql` | AzCopyClientTelemetry | Table |
| Per-attempt Storage request evidence | `queries/server/05_job_storage_evidence.kql` | AzCopyClientTelemetry and XStoreUserAgent | Table |

### Customer Drilldown

| Tile | Query | Data source | Visual |
| --- | --- | --- | --- |
| Observed job attempts | `queries/client/09_account_drilldown.kql` | AzCopyClientTelemetry | Table |
| Top source/destination movers | `queries/client/11_top_account_movers.kql` | AzCopyClientTelemetry | Table or bar chart |
| Account ownership | `queries/server/03_account_ownership.kql` | XDataAnalytics | Table |

### Server Correlation

| Tile | Query | Data source | Visual |
| --- | --- | --- | --- |
| Server-observed AzCopy requests | `queries/server/01_xagg_azcopy_requests.kql` | XStoreUserAgent | Time chart |
| Storage API and error mix | `queries/server/02_storage_operation_mix.kql` | XDataAnalytics | Bar chart or table |
| Requests per estimated job by account/hour | `queries/server/04_client_server_account_hour.kql` | XStoreUserAgent | Table or time chart |

Neither Storage correlation panel is a job-to-request join. The aggregate panel correlates client jobs and server requests by normalized destination account and hour. The per-attempt evidence panel narrows correlation to each recognized source or destination account and padded job time window.

## Panel Descriptions

### Overview KPIs

Summarizes the selected time range with observed sampled job attempts, sampling-adjusted estimated attempts and transferred TB, estimated successful completion rate, and starts with no matching finish event. Estimated values weight each finished attempt by `1 / SamplingRate`; unmatched starts remain an observed telemetry-quality count.

### Volume by topology

Shows daily sampling-adjusted job attempts and transferred TB for each transfer topology, such as local-to-Azure, Azure-to-local, intra-Azure, AWS-to-Azure, and GCS-to-Azure. Only finished attempts contribute; missing topology values appear as `unknown`.

### Outcomes by command

Breaks job outcomes down by command (`copy`, `sync`, `bench`, or other values) and terminal status, including completed, completed with skipped items, completed with errors, failed, and cancelled. It shows both observed sampled attempts and inverse-sampling estimated attempts.

### Weekly command and version trend

Shows estimated weekly command usage by AzCopy version. It combines finished job-producing commands with standalone `command.invoked` telemetry, then applies each event's inverse sampling weight. Use it to track command and version adoption over time rather than transfer performance.

### Performance percentiles

Reports sampled-job performance by transfer topology: average and P50/P90/P95 job throughput, P90/P95 completion hours per TB, and average enumeration and transfer-phase durations. Percentiles are calculated directly over observed sampled jobs and are not inverse-sampling weighted; `SampleSize` indicates the population behind each row. Jobs with no transferred bytes or nonpositive duration are excluded.

### Platform and version mix

Ranks the top 30 combinations of source type, destination type, transfer topology, host operating system, and AzCopy version. `ObservedAttempts` is the sampled count and `EstimatedAttempts` applies inverse sampling. This is a multidimensional usage mix, so totals across overlapping filtered views should not be treated as distinct customers.

### Reliability rates

Shows sampling-adjusted completion, partial-success, failure, cancellation, failed-object, server-busy, network-error, and resume-success rates. Job outcome rates use estimated job attempts; failed-object rate uses scheduled objects; server-busy and network-error rates use Storage HTTP attempts. These percentages have different denominators and are not intended to add to 100%.

### Top job errors

Ranks the top 20 terminal job error category/code combinations by estimated attempts and includes observed attempt counts plus example bounded failed-transfer code histograms. It describes why jobs ended unsuccessfully or with errors; it is not a count of individual HTTP failures or failed files.

### Telemetry acceptance

Classifies observed attempts using independently visible schema, lifecycle, field-completeness, and timing checks. Schema versions 2 and 3 are accepted; other or mixed versions are `InvalidSchema` and are excluded from the business-metric queries. An accepted attempt has exactly one start and finish, valid required dimensions and sampling rate, at least 50 finish measurements, a finish at or after its start, and an emitted duration within 60 seconds or 20 percent of the observed elapsed time.

### Rejected and suspect attempts

Lists the latest attempts that failed acceptance, with explicit reasons such as unsupported or mixed schema, missing or duplicate lifecycle events, missing required fields, incomplete finish measurements, finish-before-start ordering, and implausible duration differences. `InvalidSchema` is a hard rejection; the other reasons classify the attempt as `Suspect` so it can be investigated rather than silently discarded.

### Per-attempt Storage request evidence

For accepted client attempts, expands each Azure Storage source and destination account into five-minute buckets covering the observed job interval with five minutes of padding, then compares it with server-observed AzCopy requests. `SupportingEvidence` means at least one matching request was observed, `Suspect` means no request was found, and `InsufficientEvidence` means the job had no recognized Azure Storage account. The panel shows both server requests and the client-emitted Storage HTTP-attempt count plus their ratio; it does not require equality.

### Observed job attempts

Lists up to the 200 most recent sampled finished attempts, including IDs, command, status, topology, source and destination Azure storage accounts, version, bytes, duration, throughput, failed objects, and inverse-sampling weight. The optional storage-account parameter matches either Azure endpoint. Rows are observed records; `EstimatedWeight` is context, not a duplicated estimated row count.

### Top source and destination movers

Ranks up to 50 Azure storage accounts by sampling-adjusted transferred TB, keeping source and destination roles separate. S3/GCS bucket names are never included. It also shows observed and estimated attempts. An Azure service-to-service job contributes once to its source account and once to its destination account, so source and destination rows must not be summed to derive a global volume total.

### Account ownership

Shows the latest available server-side metadata for storage accounts: subscription, billed subscription, resource group, Storage logical tenant, live status, and snapshot time. The data comes from `XStoreAccountPropertiesDaily`, is not client-sampled, and uses a three-day lookback to locate the latest snapshot. Storage logical tenant is not the customer's Microsoft Entra tenant ID.

### Server-observed AzCopy requests

Shows five-minute Azure Storage request counts where the server-observed user agent identifies AzCopy, broken down by AzCopy tool version and Storage SDK/version. These are server-side observed requests rather than estimated client jobs. The optional account parameter filters the server account name.

### Storage API operation mix

Ranks the top 30 hourly Storage service, request-type, and authentication-type combinations, including total requests, successes, throttles, authorization errors, network errors, and ingress/egress bytes. This server-side account traffic is not client-sampled, but it can include non-AzCopy callers because the source table is account transaction telemetry rather than an exact AzCopy request join.

### Requests per estimated job

Correlates destination-account/hour client telemetry with server-observed AzCopy request counts. It shows observed jobs, inverse-sampling estimated jobs, requests, and requests per estimated job. This is an aggregate account/time correlation, not exact attribution of requests to a JobID; a full outer join intentionally exposes hours present on only one side.

## Interpretation Rules

- `ObservedAttempts` is the sampled row count.
- `EstimatedAttempts` and estimated byte totals use inverse-probability weighting from each event's `SamplingRate`.
- Percentiles are calculated over sampled jobs. Always show `SampleSize`; do not weight percentile values.
- Aggregate retry/throttle rates from raw numerators and denominators, never by averaging per-job percentages.
- Keep source and destination account roles separate. Do not duplicate S2S jobs in global totals.
- Treat unmatched starts as probable abandonment only after an agreed ingestion timeout.
- Treat `InvalidSchema` as a hard exclusion. Treat lifecycle, timing, and missing Storage evidence as investigation signals, not proof that a client fabricated telemetry.
- Do not require client HTTP-attempt counts to equal server request counts. Chunking, retries, service-to-service transfers, sampling, concurrent jobs, and aggregation boundaries can all change the ratio.
- XStore `Tenant`/`LogicalTenant` values are Storage deployment tenants, not customer Entra tenant IDs.

## Unsupported Or Partial Business Metrics

Do not create authoritative tiles for these until their dependencies exist:

- First-ever job success, time to first success, and second-job conversion: requires an account/subscription cohort sampler.
- Exact last-active date per customer: JobID sampling can omit the actual latest job.
- Finalization duration: not emitted separately.
- Comprehensive retries and retry overhead: current counters do not cover all SDK/body-read retries.
- Public/private network bytes: configured endpoint kind is not actual route attribution.
- Support cases, diagnosis, mitigation, resolution, repeat contact, and escalation: require support-system joins.
- Exact job-to-Storage-request attribution: requires a job/run correlation value on Storage requests.

## Validate Client Tiles

The validator substitutes a two-day time range and an empty account filter, then executes every client query against the configured Application Insights resource.

```powershell
./dashboards/azcopy-business-metrics/validate-client-queries.ps1
```

Override the defaults when needed:

```powershell
./dashboards/azcopy-business-metrics/validate-client-queries.ps1 `
  -App sharankur_insights1 `
  -ResourceGroup sharankur_playground `
  -Offset 7d
```

## Production Evolution

The MVP can use live cross-service queries. For production, persist curated attempt facts and daily/hourly aggregates in a central ADX database before client and XAgg retention expires. Point the final Azure Managed Grafana dashboard at those curated tables rather than repeatedly joining raw telemetry in every panel.

References:

- [ADX dashboards](https://learn.microsoft.com/azure/data-explorer/azure-data-explorer-dashboards)
- [ADX dashboard parameters](https://learn.microsoft.com/azure/data-explorer/dashboard-parameters)
- [ADX and Azure Monitor cross-service queries](https://learn.microsoft.com/azure/data-explorer/query-monitor-data)