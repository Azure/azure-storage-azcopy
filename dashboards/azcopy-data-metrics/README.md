# AzCopy Data Metrics ADX dashboard

This dashboard adapts the useful data-volume views from the Storage Mover `Data` Grafana dashboard to AzCopy job telemetry. It also enriches destination Azure Storage account names at query time with governed inventory data. The AzCopy client does not call ARM or emit subscription, tenant, offer, SKU, kind, redundancy, or resource-region dimensions.

The dashboard is destination-centric because Storage Mover's comparable views describe the target account and because applying one account's metadata to both sides of an arbitrary AzCopy transfer would be ambiguous.

## Files

- `generate-dashboard.ps1`: deterministic schema-60 Azure Data Explorer dashboard generator.
- `validate-queries.ps1`: authenticates to central ADX and live-validates parameter and tile queries in efficient batches.
- `queries/common`: shared job-rollup, enrichment, and filtering logic.
- `queries/parameters`: query-backed multi-select dropdown values.
- `queries/tiles`: dashboard panel queries.
- `azcopy-data-metrics.dashboard.json`: default Application Insights variant.
- `azcopy-data-metrics.telemetry-test.dashboard.json`: telemetry-test Application Insights variant.

## Generate

```powershell
.\dashboards\azcopy-data-metrics\generate-dashboard.ps1

.\dashboards\azcopy-data-metrics\generate-dashboard.ps1 `
  -AppInsightsSubscriptionId 31347be8-d066-464e-9866-7e58d85027b7 `
  -AppInsightsResourceGroup azcopy-telemetry-test-rg `
  -AppInsightsName azcopy-telemetry-test-ai `
  -OutputPath .\dashboards\azcopy-data-metrics\azcopy-data-metrics.telemetry-test.dashboard.json

# Live ARG is enabled by default. Use this explicit opt-out when needed.
.\dashboards\azcopy-data-metrics\generate-dashboard.ps1 `
  -EnableLiveArgEnrichment:$false
```

Import the generated JSON from the Azure Data Explorer dashboard UI.

## Validate

```powershell
.\dashboards\azcopy-data-metrics\validate-queries.ps1 `
  -App azcopy-telemetry-test-ai `
  -ResourceGroup azcopy-telemetry-test-rg `
  -Offset 30d
```

The validator runs from `https://azcore.centralus.kusto.windows.net`, database `Xstore`, so the queries can join Application Insights, XStore, and Azure Resource Graph. It validates the default ARG-enabled dashboard queries, all-values behavior, selected multi-value syntax, every panel query, an explicit ARG-disabled path, and a forced ARG-unavailable path. The caller needs access to all referenced data sources for the live-ARG checks.

## Server-side enrichment

The shared query first materializes the small set of observed destination
account names and activity days. Historical XStore enrichment filters its
bounded time window by the observed account set before the broadcast join.
Live ARG enrichment is enabled in generated artifacts by default. It converts
only the observed account and subscription keys to scalar sets, then filters
the remote tables before projection and aggregation. Concurrent dashboard
fan-out can still cause remote streaming disconnects and partial-query
warnings, so this live path should be replaced by a local snapshot before
production-scale use.

| Data source | Time semantics | Fields used |
| --- | --- | --- |
| AzCopy `azcopy.job.finished` events | Event time in the selected range | Destination account name, topology, counters, and sender geography. |
| `XDataAnalytics.XStoreAccountPropertiesDaily` | Historical snapshot selected for the AzCopy activity day | Owning subscription, redundancy, hierarchical namespace, access tier, and billing/account class. |
| Azure Resource Graph `Resources` | Current snapshot from the last 60 hours | ARM account kind, SKU name/tier, Storage resource region, subscription, and tenant join key. |
| Azure Resource Graph `InternalSubscriptionResources` | Current snapshot from the last 60 hours | Subscription display name, offer type, and billing channel used to classify internal versus external scope. |

Historical XStore ownership remains available in the default mode. When live
ARG is enabled, historical XStore ownership takes precedence over current ARG
ownership, and current ARG is the fallback for accounts without a historical
match. Subscription labels display the current name when available and retain
the subscription ID as the filter key.

Default artifacts query live ARG. To disable that dependency, generate with
`-EnableLiveArgEnrichment:$false`; current-only resource and subscription
dimensions then report `Live ARG disabled`. Enabled enrichment remains best
effort: each remote ARG input is a `union isfuzzy=true` leg with a typed empty
fallback under `set best_effort=true`. If ARG has a transport, connectivity, or
resolution failure, the core telemetry and historical XStore panels still
return results, but the Kusto response can contain partial-query warnings. The
mapping-coverage panel exposes disabled or missing current inventory rather
than silently dropping jobs.

### Recommended local ARG snapshot

Kusto query-results caching is not a durable fix for this dashboard. It only
reuses successful results for byte-for-byte identical queries against the same
database and client request properties, and cacheable results must be at most
16 MB. Each dashboard panel has a different full query, so the common
enrichment prefix is not shared, and concurrent cold requests still reach ARG.
The table hot-cache policy is also unrelated: it controls which already
ingested extents stay on local SSD.

For production, run one scheduled refresh outside the dashboard and publish a
small account dimension into a writable ADX database:

1. Query ARG once per refresh for Storage accounts and their subscriptions.
2. Normalize one row per account with `SnapshotTime`, account/resource ID,
   subscription and tenant IDs, resource region, ARM kind, SKU name/tier,
   subscription display name, offer type, and billing channel.
3. Ingest the complete result as an immutable snapshot with a refresh-run ID
   and success status. Retain 7-30 days for diagnostics.
4. Have dashboard queries select the latest successful snapshot with
   `arg_max(SnapshotTime, *) by Account` and join it locally.
5. Expose snapshot age in the dashboard and alert when it exceeds the agreed
   freshness target.

Use an Azure Function, Container Apps Job, Data Factory/Synapse pipeline, Logic
App, Automation runbook, or an existing scheduled ingestion service with
managed identity. The identity needs ARG read access and ADX ingestion access.
A one- to four-hour cadence is usually sufficient for resource metadata.
Materialized views and update policies can process data after it is ingested,
but they do not schedule or execute periodic remote ARG queries themselves.
For a small snapshot, atomic replacement is simple; immutable snapshots are
preferred when refresh history and last-known-good fallback matter.

### Coverage and missing data

- Deleted short-lived test accounts commonly exist in historical XStore but not current ARG.
- Current ARM kind, SKU, resource region, offer, and subscription-channel metadata cannot be reconstructed for accounts absent from current ARG.
- Fully historical kind, SKU, and offer attribution would require a curated effective-dated dimension table; this dashboard does not imply that current values were true at transfer time.
- Non-Storage destinations use `Not applicable`.
- Disabled live fields use `Live ARG disabled`; enabled-but-missing current kind, SKU, and region use explicit `Unmapped` values.
- `Destination enrichment mapping coverage` reports jobs and bytes for mapped and missing states instead of silently dropping unmatched telemetry.

## Dashboard filters

| Dashboard filter | Source | Meaning and limitation |
| --- | --- | --- |
| Time range | ADX dashboard parameter | Controls selected-range panels and dropdown values. The calendar-month comparison intentionally uses the current and previous calendar months. |
| Source type | `SourceType` | AzCopy source service type. |
| Destination type | `DestType` | AzCopy destination service type. |
| From-to | `FromTo` | Exact AzCopy source/destination pairing. |
| Source mount type | `SourceMountType` | Source classification such as local disk, NAS SMB/NFS, or cloud. |
| Destination endpoint kind | `DestEndpointKind` | Destination hostname classification. It detects explicit `.privatelink.` hosts but cannot prove every private-network path. |
| Client country/region | Application Insights `client_CountryOrRegion` | Telemetry-sender IP geography, not source, destination, or Azure resource region. |
| Storage account | `SourceStorageAccount` and `DestStorageAccount` | Optional exact account-name filter across either transfer side. Enriched dimensions remain destination-only. |
| Destination owning subscription | Historical XStore ownership with current ARG label | Available for Storage destinations when account inventory maps. |
| Destination subscription offer type | Current ARG subscription inventory | Current offer classification, not an effective-dated historical value. |
| Destination subscription scope | Current ARG subscription billing channel | `Internal`, `External`, `Unknown`, or `Not applicable`. `CustomerLed`, `FieldLed`, and `PartnerLed` are classified as external. |

### Original Storage Mover dropdown decisions

| Storage Mover dropdown | Decision | AzCopy treatment |
| --- | --- | --- |
| `SubscriptionId` | Included through trusted enrichment | `Destination owning subscription`; no raw subscription identifier was added to client telemetry. |
| `OfferType` | Included through trusted enrichment | `Destination subscription offer type` from current ARG subscription inventory. |
| `Region` | Adapted, not conflated | Client country/region remains available from telemetry. Current destination Storage resource region is shown in detailed job output but is not presented as historical attribution. |
| `InternalSubIds` | Included with generalized semantics | `Destination subscription scope` replaces a Storage Mover-specific ID list with current channel-based `Internal`/`External` classification. |
| `Source` | Included | Mapped to `SourceType`; `SourceMountType` adds AzCopy-specific detail. |
| `Target` | Included | Mapped to `DestType`. |
| `PrivateNetworkEnabled` | Partially adapted | Replaced by destination `DestEndpointKind`; private DNS using public hostnames remains indistinguishable, and the signal says nothing about source connectivity. |

## Included panels

| AzCopy panel | Source and purpose |
| --- | --- |
| How to read this dashboard | Visible interpretation and enrichment guidance. |
| Selected-range data totals | Unique jobs, logical payload bytes, and completed payload objects. |
| Current versus previous month | Calendar-month comparison while honoring topology and enrichment filters. |
| Monthly data transferred | Monthly logical successful payload volume. |
| Monthly completed objects | Monthly completed payload objects excluding folder-property operations. |
| Data by source-target pair | Volume grouped by AzCopy source and destination service type. |
| Completed objects by source-target pair | Completed objects grouped by source and destination service type. |
| Data by destination service type | Blob, BlobFS, File, FileNFS, Local, and other AzCopy destination types. |
| Data by source mount type | Local disk, NAS SMB/NFS, cloud, and other source classifications. |
| Data by destination endpoint kind | Public-hostname versus explicit private-link endpoint classification. |
| Data by telemetry-sender country/region | Client IP geography, explicitly separate from Storage resource region. |
| Recent observed finished jobs | Latest 200 unique jobs with destination enrichment values and mapping statuses. |
| Data by destination Storage account kind | Current ARM kind such as `StorageV2`; unmatched accounts remain visible. |
| Data by destination Storage SKU | Current ARM SKU such as `Standard_RAGRS`; unmatched accounts remain visible. |
| Data by destination redundancy | Historical XStore redundancy for the activity day. |
| Data by destination namespace | Historical HNS/FNS state for the activity day. |
| Data by destination access tier | Historical XStore access tier for the activity day. |
| Data by destination account class | Historical XStore billing/account class, deliberately distinct from ARM kind and SKU. |
| Destination enrichment mapping coverage | Mapping completeness for account, resource, and subscription inventories. |

## Excluded or consolidated Storage Mover panels

| Storage Mover panel or row | Decision | Reason |
| --- | --- | --- |
| Empty text panel | Excluded | Grafana layout spacer with no analytical content. |
| `README` row | Adapted | Replaced by the visible guidance card and this README. |
| `Metrics` row | Adapted | ADX tile layout provides the section directly. |
| `Backup` row | Excluded | Grafana organizational row with no distinct AzCopy metric. |
| Data transferred since May 2025 | Consolidated | The time-range picker and monthly trend provide the same analysis without a Storage Mover schema-cutover date. |
| Current and previous semester cards | Consolidated | The time-range picker can select either semester without fixed duplicate cards. |
| Separate current-month and previous-month cards | Consolidated | Combined into `Current versus previous month`. |
| Duplicate target account-type panels | Consolidated and clarified | Replaced by distinct ARM kind, ARM SKU, historical redundancy, namespace, access-tier, and account-class panels instead of copying ambiguous duplicate labels. |
| Legacy monthly source pairing through April 2025 | Excluded | Storage Mover schema workaround; AzCopy emits current source and destination dimensions directly. |
| Raw-plus-aggregate recent-job union | Excluded | AzCopy reads raw finished events directly and does not depend on a lagging Storage Mover aggregate table. |

## Counting and resume semantics

- `azcopy.bytes_transferred` is logical successful payload bytes. It does not duplicate retries; `azcopy.bytes_over_wire` is the physical-traffic metric when retry overhead is desired.
- `azcopy.objects_completed` counts completed payload objects and excludes folder-property transfers.
- Totals use all received finished-job telemetry records.
- Job counts and the recent-jobs table represent received telemetry records.
- A resumed job can emit multiple cumulative summaries. Queries group by `JobID`, take the latest observed dimensions, and use maximum cumulative counters so the same job is not summed repeatedly.
- If a job started before the selected range and its latest finish is inside the range, its cumulative volume is assigned to that finish. A job whose latest finish is outside the selected range is not represented.

## Privacy and performance constraints

- Enrichment occurs only in trusted server-side KQL. The AzCopy client remains free of ARM calls and does not emit new subscription or tenant identifiers.
- Account names already emitted by the approved telemetry schema are used as join keys; panels do not attempt to infer missing identities.
- Queries filter telemetry by time first, materialize only observed
  account/account-day keys, use a bounded XStore broadcast join, and push
  observed account/subscription sets into ARG filters.
- Do not replace the bounded join pattern with global `summarize` operations over XStore or ARG; earlier broad forms exceeded memory or timed out.
- Live ARG remains a repeated interactive dependency rather than a shared
  cross-panel cache. Best-effort fallbacks keep the dashboard usable during ARG
  outages; the governed local snapshot described above is the preferred
  long-term replacement.
