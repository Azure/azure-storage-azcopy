# AzCopy Top Movers

For the additional Data, Customer Drilldown, Trends, and No Observed Success dashboards on both platforms, see [SUITE.md](SUITE.md). Those dashboards share saved frontend functions over the same persisted aggregates.

This directory mirrors the active panel layout and persisted-aggregation architecture of the Storage Mover Top Movers dashboard without using Storage Mover transfer telemetry. Customer identity enrichment uses XStore and AIPDD.

- [AzCopy Top Movers](https://azcopy-telemetry-ankur-cbbcech2ecd9gad6.eus.grafana.azure.com/d/azcopy-top-movers/azcopy-top-movers?orgId=1&from=now-90d&to=now)
- [AzCopy Top Movers in ADX](https://dataexplorer.azure.com/dashboards/182e989f-c43d-4937-af35-9ea4a6492f08)
- [Storage Mover reference](https://storagemoverprod-aaa9f9c6eyh6e7ex.eus.grafana.azure.com/d/fe2vkjmjke800c/top-movers): active Metrics section, version 87 at inspection.
- Kusto cluster: `https://sm-prd-westeurope.westeurope.kusto.windows.net`, database: `usage-analytics`, function folder: `AzCopyQueries`.

## Query model

- `AzCopy_KO_UsageAggregates` is the aggregate producer. It normalizes schema-version 3 terminal events, deduplicates delivery by invocation, collapses resumed attempts by job, enriches storage-account ownership through XStore and AIPDD, and emits Storage Mover-style daily name/count rows.
- `AzCopyUsageAggregates1` persists those daily rows. Grafana never scans raw Application Insights telemetry.
- `AzCopy_FilterValues` and `AzCopy_TopMovers` are the dashboard-facing helpers.
- Semester panels use the Storage Mover dashboard's April-September and October-March boundaries.

Panels rank AIPDD customer names, as the Storage Mover dashboard does. Attribution uses the destination Azure Storage account when present and otherwise the source Azure Storage account. Service-to-service transfers are therefore attributed to the destination owner. `InstallationID` remains available only for counting distinct AzCopy installations. Raw storage-account names and paths are not exposed by the frontend functions.

Unmapped customer rows remain in the aggregate table but are excluded from the named-customer rankings and dropdowns. Consequently, dashboard bytes can be lower than the table's total bytes. Ownership uses an XStore daily snapshot up to three days before the job's reporting date; customer names use the latest available AIPDD subscription snapshot.

New builds from this branch restore `SourceStorageAccount` and `DestStorageAccount` as job-event `customDimensions`, retaining schema version 3 and the existing property names. The producer can use those new events after the normal persisted refresh. August 26-27 test events omitted both properties; their 24,639,914,248 unmapped bytes cannot be attributed simply by rebuilding the client or refreshing subscription metadata. This code change does not modify previously ingested events or refresh the live aggregate table.

Schema-version 3 `azcopy.job.finished` events carry cumulative `TotalBytesTransferred` from the job summary, not per-attempt network traffic. The producer deduplicates `InvocationID`, takes the maximum byte/object totals across attempts sharing `JobID`, and attributes the logical job to its latest observed terminal day. It searches 30 days before the requested start to include earlier attempts. This is completion-day accounting, not a time series of bytes physically moved each day. Older attempts outside source retention or that lookback cannot be reconstructed.

Time filters use UTC daily slices, including the full day containing the range start. Hour-level precision is not available in these persisted aggregates. Transfers without delivered terminal telemetry are not represented; telemetry is best-effort and can be disabled by users.

The dashboard reproduces the active Storage Mover panels: all time, current month, previous month, previous semester, current semester, selected range, and selected-range Top 100 details. Backup and Archive rows from the source dashboard are intentionally excluded.

## Deploy and publish

```powershell
.\test-deployment.ps1
.\deploy-functions.ps1 -BackfillDays 120
.\generate-dashboard.ps1 -Database usage-analytics
.\generate-adx-dashboard.ps1 -Database usage-analytics
.\validate-functions.ps1
.\publish-dashboard.ps1 -Database usage-analytics
```

The default aggregate source is the AzCopy test Application Insights resource. Override `AppInsightsResourceId` and `AppInsightsDatabase` for another source.

Deployment creates or updates `AzCopyUsageAggregates1`, deploys only `AzCopy_*` functions in the `AzCopyQueries` Kusto folder, and enriches ownership from XStore and AIPDD. It builds a uniquely named staging table, rejects empty data, invalid counts, or missing historical reporting days, then atomically replaces the persisted backfill and removes staging. Function updates occur before backfill and are not part of that table replacement transaction. Generate and publish with the same database value. Publishing creates or verifies the separate `azcopy-usage-analytics` current-user OAuth datasource and refuses to overwrite a mismatched datasource. Only the dashboard with UID `azcopy-top-movers` is overwritten.

Because the datasource uses delegated current-user OAuth, viewing panel data requires an interactive Grafana login.

## ADX JSON upload

`generate-adx-dashboard.ps1` translates the Grafana panel queries into the native ADX dashboard schema and writes `azcopy-top-movers.adx.dashboard.json`. It creates six horizontal bar charts, a Top 100 details table, and native time-range, Top N, customer, command, source, and target parameters. Charts use decimal GB; the details table retains raw bytes. Both dashboards use the same persisted Kusto frontend helpers.

In the existing ADX Top Movers dashboard, enter edit mode and choose **Additional options > File > Replace dashboard with file**. Upload `azcopy-top-movers.adx.dashboard.json`, verify the preview, then **Save**. The same file can be imported as a new dashboard. The ADX file is not interchangeable with the Grafana dashboard JSON.

The ADX dashboard linked above was created through Playwright MCP by uploading this JSON file, saved, and verified after reload. Existing business/data/telemetry dashboards were preserved.

## Refresh and permissions

Refreshes now block missing or reduced historical keys at the full persisted dimensional grain, not only missing dates. Resume date/status moves and enrichment corrections can legitimately trip this guard. Reconcile the change first, then explicitly pass `-AllowHistoricalReductions` for that rebuild. The switch never bypasses empty/invalid data checks. Aggregated totals cannot detect all compensating job-level losses; retain source/job-level history for that guarantee.

Stored functions do not schedule themselves. No recurring job is installed by these scripts. Run `deploy-functions.ps1` and `validate-functions.ps1` from an approved scheduler when fresh aggregates are needed; do not append overlapping producer results, which would double-count jobs and resumed attempts. Use a single writer and retain enough raw history to rebuild the entire reporting period. The 120-day default is an initial backfill window, not an all-time retention policy. If source retention removes historical events, archive/reconcile job-level history before attempting another full rebuild; the missing-day guard is not a substitute for durable job-level history.

The refresh identity needs function/table management and ingestion permissions in `usage-analytics`, read access to the selected Application Insights resource, and access to the XStore/AIPDD enrichment sources. Dashboard viewers need Grafana access and read access to the persisted aggregate database; they do not need raw telemetry access. No credentials are written to the repository.

## Verification

`test-aggregate-replay.ps1` runs 18 read-only synthetic assertions against the actual producer body, including duplicate delivery, cumulative resumes, late events, schema-3 account-field migration, and enrichment gaps. It needs a current Azure CLI login and Kusto query permission; `-QueryOnly` produces the query offline. It does not read or modify live source/aggregate tables or saved functions. See [telemetry testing boundaries](../../telemetry/TESTING.md).

`test-deployment.ps1` tests publication guards with mocked Azure calls and makes no network requests. `validate-functions.ps1` executes all seven generated Grafana panel queries, all four query variables, all eleven ADX queries, representative selections for all four filters, no-match/empty-range cases, sequential top-N rankings, duplicate aggregate keys, byte/job reconciliation, and April/October semester boundaries. It also checks custom-variable `text`/`value` columns and explicit customer names on Grafana bar gauges.

On September 7, 2026, Grafana dashboard version 7 and the five functions were published and verified. The refreshed table contains 24,428 rows, 16,814 logical jobs, and 47,830,502,509 bytes, with reporting dates August 18-27, 2026. Of those bytes, 24,639,914,248 are unmapped; the named-customer ranking contains one customer and 23,190,588,261 bytes. These are test telemetry observations, not production adoption statistics. Empty current-month and previous-semester panels are expected for this coverage.

Standalone Playwright MCP authenticated through the existing work account without requiring SmartKey. Browser verification found and corrected two Grafana issues: custom variables must return `text` and `value` rather than SQL-style `__text`/`__value`, and a single-customer bar gauge needs an explicit display name. Selected-range panels now send the customer key and render 23.2 GB, with the customer label visible. The ADX dashboard renders the same total as 23.190588261 GB and its details table shows the matching raw byte count. The Storage Mover reference dashboard and unrelated AzCopy dashboards were not modified.
