# AzCopy Business Metrics ADX Dashboard

This directory contains an importable ADX dashboard and its source KQL for the business metrics requested in `StorageMoverAzCopyComparison.md`.

## Import The Dashboard

1. Open [ADX Dashboards](https://dataexplorer.azure.com/dashboards) in a browser that supports your Microsoft certificate authentication.
2. Select the arrow next to **New dashboard**.
3. Select **Import dashboard from file**.
4. Select `azcopy-business-metrics.dashboard.json` from this directory.
5. Name the dashboard `AzCopy Business Metrics` and select **Create**.

The imported dashboard contains 37 tiles across seven pages and uses one XStore data source. Client telemetry, XDataAnalytics, and ARG 1P queries use explicit cross-cluster references, so no additional dashboard data sources are required.

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

Name: `AzureResourceGraph`

```text
https://argeusarm1pone.eastus.kusto.windows.net
```

Database: `AzureResourceGraph`

Table: `InternalSubscriptionResources`

The Top customer tenants panel maps `SubscriptionId` to the owning Microsoft Entra tenant and shows example ARG subscription display names. It uses the required 60-hour lookback and latest non-deleted record. Dashboard viewers need access to the ARG 1P inventory in addition to the other cross-cluster sources. A subscription display name labels a subscription, not its tenant/customer, and can change. Friendly tenant names require a separately approved tenant-name source and are intentionally not inferred from subscription names or XStore deployment-tenant fields.

#### Optional Friendly Tenant-Name Cache

Do not call Microsoft Graph from dashboard KQL and do not add tenant display names to AzCopy client telemetry. Run a scheduled Azure worker under a user-assigned managed identity, resolve only missing or stale tenant IDs, and write the results to an ADX dimension that the dashboard can left-join.

Grant the managed identity the least-privileged Microsoft Graph application permission, `CrossTenantInformation.ReadBasic.All`. This is a Microsoft Graph app-role assignment, not an Azure ARM RBAC role assignment. A Microsoft Entra administrator who can grant application permissions must run the following once:

```powershell
Connect-MgGraph -Scopes "Application.Read.All","AppRoleAssignment.ReadWrite.All"

$managedIdentityObjectId = "<managed-identity-service-principal-object-id>"
$graphServicePrincipal = Get-MgServicePrincipal `
    -Filter "appId eq '00000003-0000-0000-c000-000000000000'" `
    -Property "Id,AppRoles"
$appRole = $graphServicePrincipal.AppRoles | Where-Object {
    $_.Value -eq "CrossTenantInformation.ReadBasic.All" -and
    $_.AllowedMemberTypes -contains "Application"
}

if (-not $appRole) {
    throw "Microsoft Graph application role CrossTenantInformation.ReadBasic.All was not found."
}

New-MgServicePrincipalAppRoleAssignment `
    -ServicePrincipalId $graphServicePrincipal.Id `
    -PrincipalId $managedIdentityObjectId `
    -ResourceId $graphServicePrincipal.Id `
    -AppRoleId $appRole.Id
```

At runtime, request a managed-identity token for `https://graph.microsoft.com/.default` and call:

```http
GET https://graph.microsoft.com/v1.0/tenantRelationships/findTenantInformationByTenantId(tenantId='{tenant-id}')
```

The response can supply `displayName`, `defaultDomainName`, and `federationBrandName`. Store only fields approved for this dashboard's audience. Batch with `POST https://graph.microsoft.com/v1.0/$batch`, with no more than 20 lookups per batch. Inspect every item in `responses`: the batch can return HTTP 200 while an individual lookup returns 404, 403, 429, or 5xx. Treat 404 as `NotFound`; honor each 429 `Retry-After` value and retry those items explicitly; use exponential backoff when `Retry-After` is absent. Preserve the last successful name as stale on transient refresh failures rather than replacing it with an error.

The dimension should retain `TenantId`, `TenantDisplayName`, `Source`, `FirstResolvedTime`, `LastResolvedTime`, `LastAttemptTime`, `ResolutionStatus`, `HttpStatus`, and `IsCurrent`. Recommended statuses are `Resolved`, `NotFound`, `Forbidden`, `Throttled`, `TransientFailure`, and `StaleCached`. Managed-identity role changes can take time to appear because managed-identity tokens are cached.

## Parameters

Every ADX dashboard has the default time-range parameter. The queries use:

- `_startTime`
- `_endTime`

Add one free-text parameter:

| Label | Variable | Type | Default | Pages |
| --- | --- | --- | --- | --- |
| Storage account | `_account` | String | Empty | All pages; used by Customer Drilldown, Reliability, Data Quality, and Server Correlation queries |

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
| Source and destination platform mix | `queries/client/18_endpoint_platform_mix.kql` | AzCopyClientTelemetry | Bar chart |

### Reliability

| Tile | Query | Data source | Visual |
| --- | --- | --- | --- |
| Reliability rates | `queries/client/05_reliability_cards.kql` | AzCopyClientTelemetry | Multi stat |
| Terminal errors (one reason per job) | `queries/client/06_error_distribution.kql` | AzCopyClientTelemetry | Bar chart |
| Failed transfer-item error codes (per item) | `queries/client/17_failed_object_error_codes.kql` | AzCopyClientTelemetry | Bar chart or table |
| Newly observed error codes | `queries/client/19_new_error_codes_30d.kql` | AzCopyClientTelemetry | Table |
| Job lifecycle gaps and cancellation progress | `queries/client/16_abandonment_and_cancellation.kql` | AzCopyClientTelemetry | Table |

### Adoption

| Tile | Query | Data source | Visual |
| --- | --- | --- | --- |
| Weekly job frequency and change | `queries/client/14_weekly_job_frequency.kql` | AzCopyClientTelemetry | Time chart or table |
| Observed installation behavior (range proxy) | `queries/client/15_observed_installation_funnel.kql` | AzCopyClientTelemetry | Multi stat |

### Data Quality

| Tile | Query | Data source | Visual |
| --- | --- | --- | --- |
| Telemetry acceptance | `queries/client/08_telemetry_quality.kql` | AzCopyClientTelemetry | Multi stat |
| Rejected and suspect attempts | `queries/client/12_telemetry_rejections.kql` | AzCopyClientTelemetry | Table |
| Per-attempt Storage request evidence | `queries/server/05_job_storage_evidence.kql` | AzCopyClientTelemetry and XStoreUserAgent | Table |

### Customer Drilldown

| Tile | Query | Data source | Visual |
| --- | --- | --- | --- |
| Selected account business summary | `queries/client/13_account_business_summary.kql` | AzCopyClientTelemetry | Multi stat |
| Observed job attempts | `queries/client/09_account_drilldown.kql` | AzCopyClientTelemetry | Table |
| Top source/destination movers | `queries/client/11_top_account_movers.kql` | AzCopyClientTelemetry | Table or bar chart |
| Account ownership | `queries/server/03_account_ownership.kql` | XDataAnalytics | Table |
| Top customer tenants (resource-owning IDs) | `queries/server/06_top_customer_tenants.kql` | AzCopyClientTelemetry, XDataAnalytics, and ARG 1P | Table |

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

Reports sampled-job performance by transfer topology: average and P50/P90/P95 job throughput, average/P90/P95 completion hours per TB, and average/P90/P95 enumeration and transfer-phase durations. Percentiles are calculated directly over observed sampled jobs and are not inverse-sampling weighted; `SampleSize` indicates the population behind each row. Jobs with no transferred bytes or nonpositive duration are excluded.

### Platform and version mix

Ranks the top 30 combinations of source type, destination type, transfer topology, host operating system, and AzCopy version. `ObservedAttempts` is the sampled count and `EstimatedAttempts` applies inverse sampling. This is a multidimensional usage mix, so totals across overlapping filtered views should not be treated as distinct customers.

### Source and destination platform mix

Shows sampling-adjusted endpoint platform and protocol distribution with source and destination roles kept separate. It answers the source/target platform-mix question directly without treating the two endpoints of a service-to-service job as independent jobs.

### Reliability rates

Shows sampling-adjusted completion, partial-success, failure, cancellation, failed-object, server-busy, network-error, and resume-success rates. Job outcome rates use estimated job attempts; failed-object rate uses scheduled objects; server-busy and network-error rates use Storage HTTP attempts. These percentages have different denominators and are not intended to add to 100%.

### Terminal errors (one reason per job)

Ranks the top 20 terminal job error category/code combinations by estimated attempts and includes observed attempt counts plus example bounded failed-transfer code histograms. Each unsuccessful job contributes one terminal or primary failure reason. It is not a count of individual HTTP failures or failed transfer items.

### Failed transfer-item error codes (per item)

Expands the bounded `FailureErrorCodes` histogram and ranks error codes by observed and sampling-adjusted failed transfer-item occurrences. It complements terminal job errors: one job can contain multiple failed items and codes. Transfer items are normally files/objects and can include failed folder-property transfers.

### Newly observed error codes

Lists terminal-job and failed-object error codes whose first event in the retained 730-day lookback falls within the last 30 days. "New" means newly observed in retained telemetry, not proof that the code has never occurred anywhere before.

### Job lifecycle gaps and cancellation progress

Shows starts at least 30 minutes old, how many still lack a finish event after that grace period, plus explicit cancellation count and average/P50/P90 percent complete at cancellation. A missing finish is an abandonment proxy only; ingestion delay, process interruption, and telemetry delivery failure can produce the same shape.

### Weekly job frequency and change

Shows observed and inverse-sampling estimated finished attempts by week, the prior week's estimate, and week-over-week percentage change. Missing weeks are not backfilled, so compare only adjacent rows that represent adjacent calendar weeks.

### Observed installation behavior (range proxy)

Groups finished jobs in the selected range by `InstallationID`. It shows the number of unique installations observed, the percentage whose first observed attempt succeeded, the percentage with at least two observed attempts, the percentage with any observed success, and the median/P90 time from first observed attempt to first observed success.

This is a behavior summary, not a true sequential funnel. The rates share observed installations as their denominator but describe overlapping outcomes. The selected time range can hide earlier or later jobs, so "first observed" and "2+ observed" do not mean first-ever customer job or true second-job conversion.

### Telemetry acceptance

Classifies observed attempts using independently visible schema, lifecycle, field-completeness, and timing checks. Schema versions 2 and 3 are accepted; other or mixed versions are `InvalidSchema` and are excluded from the business-metric queries. An accepted attempt has exactly one start and finish, valid required dimensions and sampling rate, at least 50 finish measurements, a finish at or after its start, and an emitted duration within 60 seconds or 20 percent of the observed elapsed time.

### Rejected and suspect attempts

Lists the latest attempts that failed acceptance, with explicit reasons such as unsupported or mixed schema, missing or duplicate lifecycle events, missing required fields, incomplete finish measurements, finish-before-start ordering, and implausible duration differences. `InvalidSchema` is a hard rejection; the other reasons classify the attempt as `Suspect` so it can be investigated rather than silently discarded.

### Per-attempt Storage request evidence

For accepted client attempts, expands each Azure Storage source and destination account into five-minute buckets covering the observed job interval with five minutes of padding, then compares it with server-observed AzCopy requests. `SupportingEvidence` means at least one matching request was observed, `Suspect` means no request was found, and `InsufficientEvidence` means the job had no recognized Azure Storage account. The panel shows both server requests and the client-emitted Storage HTTP-attempt count plus their ratio; it does not require equality.

### Observed job attempts

Lists up to the 200 most recent sampled finished attempts, including IDs, command, status, topology, source and destination Azure storage accounts, version, bytes, duration, throughput, failed objects, and inverse-sampling weight. The optional storage-account parameter matches either Azure endpoint. Rows are observed records; `EstimatedWeight` is context, not a duplicated estimated row count.

### Selected account business summary

Summarizes the selected interval and optional Azure Storage account filter with observed and estimated attempts, transferred TB, average throughput, P90/P95 hours per TB, failures, cancellations, resume attempts, and observed activity age. Versions remain available in the observed-attempt detail panel. A storage account is an endpoint/resource proxy rather than a unique customer, and deterministic JobID sampling can omit the actual latest job.

### Top source and destination movers

Ranks up to 50 Azure storage accounts by sampling-adjusted transferred TB, keeping source and destination roles separate. S3/GCS bucket names are never included. It also shows observed and estimated attempts. An Azure service-to-service job contributes once to its source account and once to its destination account, so source and destination rows must not be summed to derive a global volume total.

### Account ownership

Shows the latest available server-side metadata for storage accounts: subscription, billed subscription, resource group, Storage logical tenant, live status, and snapshot time. The data comes from `XStoreAccountPropertiesDaily`, is not client-sampled, and uses a three-day lookback to locate the latest snapshot. Storage logical tenant is not the customer's Microsoft Entra tenant ID.

### Top customer tenants

Ranks Microsoft Entra tenant IDs by sampling-adjusted transferred TB after mapping each Azure Storage endpoint to its resource-owning subscription and then to that subscription's owning tenant. The table also shows up to five example owning subscription IDs and ARG subscription display names for each tenant row. Source and destination roles remain separate. Account ownership is selected from `XStoreAccountPropertiesDaily` as of each activity day, allowing up to three days for a missing daily snapshot. Subscription-to-tenant mapping and subscription names use the latest non-deleted `InternalSubscriptionResources` record in ARG 1P's required 60-hour inventory window.

`Mapped` rows completed both joins. `MissingAccountOwnership` means no suitable account snapshot was found. `MissingTenantInventory` means account ownership was found but ARG did not return a current tenant mapping. `ExampleOwningSubscriptionNames` contains mutable subscription labels, not tenant/customer names. Friendly tenant names are not shown because no approved tenant-name source is configured; subscription display names and XStore logical tenants are not valid substitutes. This is a resource-ownership grouping, not an authoritative commercial or contractual customer master.

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
- Installation-funnel and latest-activity fields are observed sampled proxies; inverse weighting cannot reconstruct a customer cohort or the actual latest job.
- Aggregate retry/throttle rates from raw numerators and denominators, never by averaging per-job percentages.
- Keep source and destination account roles separate. Do not duplicate S2S jobs in global totals.
- Keep source and destination customer-tenant roles separate. The same S2S job can contribute its full bytes to both endpoint owners, so do not sum roles for a global total.
- Treat unmatched starts as probable abandonment only after an agreed ingestion timeout.
- Treat `InvalidSchema` as a hard exclusion. Treat lifecycle, timing, and missing Storage evidence as investigation signals, not proof that a client fabricated telemetry.
- Do not require client HTTP-attempt counts to equal server request counts. Chunking, retries, service-to-service transfers, sampling, concurrent jobs, and aggregation boundaries can all change the ratio.
- XStore `Tenant`/`LogicalTenant` values are Storage deployment tenants, not customer Entra tenant IDs.

## Business-Metric Coverage

The source document describes desired business outcomes rather than a finalized telemetry contract. Coverage below distinguishes authoritative telemetry-backed calculations from useful but explicitly limited proxies.

| Document metric | Coverage | Dashboard implementation or gap |
| --- | --- | --- |
| Jobs and transferred data over N days | Covered | Overview and selected-account summary use observed counts plus inverse-sampling estimates. |
| Average/P90/P95 migration time per TB | Covered | Performance percentiles globally/by topology and selected-account P90/P95. |
| Average network speed | Partial | Job throughput is covered; it is end-to-end transfer throughput, not physical network-link speed. |
| Failures and error codes | Covered | Reliability rates, terminal job errors, failed-object error codes, and newly observed retained-history codes. |
| Command distribution and version | Covered | Outcomes by command plus weekly command/version trend. |
| Days since last active job | Partial | Days since the latest observed sampled account job/success; the actual latest job may be sampled out. |
| Week-over-week job frequency | Covered | Weekly frequency and week-over-week change. |
| Retry and resume/restart count | Partial | Resume attempts are covered. Storage HTTP attempts, 503 attempts, and network-error attempts are available, but they are not comprehensive retries; a newly started job cannot be reliably classified as a restart. |
| Abandonment and percent complete at kill | Partial | Mature unmatched starts are an abandonment proxy; explicit cancellations include percent-complete statistics. |
| Volume and time per TB by transfer category | Covered | Volume by topology and performance percentiles by topology. |
| P50/P90/P95 throughput | Covered | Performance percentiles. |
| Enumeration and transfer time | Covered | Average/P90/P95 durations by topology. |
| Finalization time and retry overhead | Missing | Neither phase is emitted separately. |
| Top movers | Partial | Azure Storage source/destination account endpoints and their resource-owning Entra tenant IDs are covered. Friendly/canonical customer names, S3/GCS ownership, and commercial-account identity remain unavailable. |
| First-job success, time to first success, second-job conversion | Partial | The Adoption page provides selected-range installation proxies only. Authoritative metrics require retained cohort state spanning the installation's full history. |
| Completion, outcome, failed-object, throttling, cancellation, and resume-success rates | Covered | Reliability rates. |
| Repeated and newly observed error codes | Covered | Failed-object occurrences and first-observed-in-retained-730-day-history query. |
| Source and target platform distribution | Covered | Source/destination role, endpoint type, and protocol distribution. |
| Public/private network byte attribution | Missing | `DestEndpointKind` is configured endpoint intent, not proof of the actual network route. |
| Cases per active customer/job | Missing | Requires support-system case data and a governed customer/job join. |
| Diagnostic bundle and self-service rates | Missing | Diagnostic/support workflow events are not emitted. |
| Diagnosis, mitigation, and resolution time | Missing | Requires support-system workflow timestamps. |
| Repeat-contact and escalation rates | Missing | Requires support-system case history. |

Additional scope limits:

- "Per customer" is not globally authoritative. The customer-tenant panel enriches recognized Azure Storage accounts to the Entra tenant owning the resource subscription, but this is not a universal commercial customer identifier. A tenant may own many accounts, an account may be shared, and billed ownership may differ from resource ownership.
- Local-only, S3, GCS, and sampled-out activity cannot be assigned to an Azure account proxy. S3/GCS bucket names are intentionally excluded for privacy minimization.
- Exact job-to-Storage-request attribution remains unavailable because server requests do not carry AzCopy Job IDs. Existing server panels provide account/time-window evidence only.

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