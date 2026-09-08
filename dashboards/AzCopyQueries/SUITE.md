# AzCopy dashboard suite

These four dashboards adapt the active layouts of the requested Storage Mover dashboards. All use the same persisted AzCopy telemetry as Top Movers: `AzCopyUsageAggregates1` in `usage-analytics` on `https://sm-prd-westeurope.westeurope.kusto.windows.net`. All saved frontend functions are in the `AzCopyQueries` folder.

## Published dashboards

| Dashboard | Grafana | ADX | Data panels per platform |
|---|---|---|---:|
| Data Metrics | [Open](https://azcopy-telemetry-ankur-cbbcech2ecd9gad6.eus.grafana.azure.com/d/azcopy-data-metrics) | [Open](https://dataexplorer.azure.com/dashboards/8dfca9b8-386e-426d-860c-c3dd6e24848d) | 13 |
| Customer Drilldown | [Open](https://azcopy-telemetry-ankur-cbbcech2ecd9gad6.eus.grafana.azure.com/d/azcopy-customer-drilldown) | [Open](https://dataexplorer.azure.com/dashboards/d285b287-e566-4354-8422-2ad8d6289b29) | 20 |
| Recurring Customer Trends | [Open](https://azcopy-telemetry-ankur-cbbcech2ecd9gad6.eus.grafana.azure.com/d/azcopy-recurring-trends) | [Open](https://dataexplorer.azure.com/dashboards/21fb48af-f26a-457f-9fe6-eb9718b5830c) | 4 |
| Customers with No Observed Success | [Open](https://azcopy-telemetry-ankur-cbbcech2ecd9gad6.eus.grafana.azure.com/d/azcopy-no-observed-success) | [Open](https://dataexplorer.azure.com/dashboards/6b1b9577-23a2-4ae7-ac2e-1c6802d81bf7) | 5 |

Published September 7, 2026. Grafana versions: Data 2, Customer Drilldown 3, Trends 2, No Observed Success 2. The previous four Grafana dashboards were backed up under `generated/suite/backups` before replacement. Existing ADX telemetry-test dashboards, the Observed Funnel dashboard, Top Movers, and all Storage Mover dashboards were preserved.

## Source mapping

| Source and inspected version | Structure retained | AzCopy adaptations and omissions |
|---|---|---|
| [Data](https://storagemoverprod-aaa9f9c6eyh6e7ex.eus.grafana.azure.com/d/be3i4q07jl5vka/data), v161; destination of the supplied short link | Successful/selected-range/semester/month headline totals, monthly chart, full-width source-target byte and object charts, service distributions | Recent means the last seven UTC calendar days. Target account SKU/class charts become target-service, source-service, and ownership-coverage distributions. No unsupported account metadata, artificial May 2025 cutoff, pre-April legacy chart, business targets, or duplicate Backup panels. |
| [Customer Drilldown](https://storagemoverprod-aaa9f9c6eyh6e7ex.eus.grafana.azure.com/d/aedhihs9max34c/customer-drilldown), v125 | Customer/subscription header tables, summary metric block, paired daily/topology charts, engineering outcome tables | Observed installations, attempts, successful/cancelled jobs, and completed objects replace unavailable control-plane resource/project/agent counts. Tables are explicitly labeled daily activity, not individual runs. No authoritative in-progress jobs, endpoint registrations, current subscription lifecycle, support cases, or incidents. ADX summary cards use two rows of three to satisfy runtime minimum dimensions. |
| [Trends](https://storagemoverprod-aaa9f9c6eyh6e7ex.eus.grafana.azure.com/d/eea4p1kr7v5dse/trends), v21 | Three headline metrics followed by recurring-customer details | Recurrence uses mapped customer keys, at least two active calendar months in the selected range, and all observed logical bytes from that cohort. No lifetime-retention inference. Empty-cohort average is null, not NaN or a fabricated zero-month average. |
| [Customers with no success](https://storagemoverprod-aaa9f9c6eyh6e7ex.eus.grafana.azure.com/d/afeix0jmzcxkwd/customers-with-no-success), v30 | Summary, failure distribution, customer cohort table, drilldown section and failure details | Selected-range **no observed success** instead of lifetime failure. Terminal status replaces detailed status code because error codes are not persisted in this aggregate table. Customer/subscription selectors replace the source's failed-subscription variable. Incidents and support cases are omitted. |

Grafana retains section rows. Native ADX uses the same ordered panel groups and spacing, with its own card/column/table visualizations. Grafana displays decimal byte units; ADX headline and chart byte metrics use explicitly labeled decimal GB. ADX detail tables retain bytes.

## Saved-query reuse

```text
Application Insights + XStore/AIPDD
  -> AzCopy_KO_UsageAggregates (refresh only)
  -> AzCopyUsageAggregates1 (persisted daily metrics)
     -> AzCopy_Usage (shared filters, metric pivot, terminal-outcome classification)
        -> AzCopy_UsageSummary
        -> AzCopy_Activity
        -> AzCopy_Breakdown
        -> AzCopy_CustomerSubscriptions
        -> AzCopy_UsageDetails
        -> AzCopy_RecurringCustomers -> AzCopy_RecurringSummary
        -> AzCopy_NoSuccessCustomers -> AzCopy_NoSuccessDetails -> AzCopy_NoSuccessSummary
     -> AzCopy_DashboardFilters (dropdown inventory)
```

Both platforms are generated from one panel manifest in `generate-suite.ps1`. Every data-panel and query-variable expression begins with a saved `AzCopy_*` function. Dashboard expressions contain only arguments, final column projections, presentation-unit conversions, and final status-row selection. No dashboard contains `customEvents`, direct aggregate-table access, cross-cluster enrichment, `summarize`, `join`, or `union`. Calendar periods reuse the existing semester functions.

The suite directly calls eleven frontend helpers; their shared `AzCopy_Usage` dependency makes twelve new saved functions. The aggregate producer and existing semester functions are reused rather than copied. Repeated metrics may still issue separate lightweight persisted-data queries; reuse does not imply shared execution or cached results across panels.

`validate-suite.ps1` verifies all 59 unique rendered query strings across the two formats, variable `text`/`value` columns, customer-link key columns, total reconciliation, and exact normalized readback of all twelve deployed function bodies. It rejects raw-source scans and embedded business aggregation in dashboard queries. ADX formats the empty numeric recurrence average as `N/A` to avoid its card renderer displaying null as `NaN`; the shared metric remains null.

## Semantics and filters

- Daily records inherit the producer's invocation deduplication, maximum cumulative job-byte accounting across resumes, and latest-terminal-day attribution. See [README.md](README.md) for retention, enrichment, and refresh limitations.
- `Completed` and `CompletedWithSkipped` are successful logical jobs. `Failed`, `CompletedWithErrors`, and `CompletedWithErrorsAndSkipped` are failures. Cancelled-only customers are not classified as failure-only customers.
- Data and Customer Drilldown retain the explicit unmapped bucket. Customer recurrence and no-success cohorts exclude it because unrelated unmapped accounts must not be treated as one customer.
- No-success eligibility requires failed jobs and zero successful jobs across the selected time, customer, subscription, source, and target scope. Status filtering happens **after** that classification. The all-status cohort table is labeled separately from status-filtered summary/details.
- Ownership is attributed to the destination Azure account when available, otherwise the source. It is an enriched account-owner identity, not necessarily the person invoking AzCopy.
- No source status-code filter, account-SKU/region/offer filter, or registered-resource metric is fabricated from unrelated data. Adding those requires extending the persisted source model.
- Detail activity is limited to the latest 500 aggregate rows in `AzCopy_UsageDetails`; it is not a complete job-record export. Other cohort tables are not arbitrarily top-N truncated.
- Grafana customer/subscription table links preserve the selected time range and pass the actual key into Customer Drilldown. Suite navigation links retain variables/time. ADX dashboards expose corresponding native filters; cross-dashboard field links are not configured there.

## Deploy and update

```powershell
.\deploy-functions.ps1 -FunctionsOnly
.\generate-suite.ps1
.\validate-suite.ps1
.\publish-suite.ps1
```

`-FunctionsOnly` deploys definitions without refreshing persisted rows. The existing aggregate refresh workflow and scheduler limitation are unchanged. `publish-suite.ps1` allows only the four suite UIDs, backs up prior Grafana JSON, and uses the verified `azcopy-usage-analytics` datasource.

Upload each `generated/suite/<uid>.adx.json` through ADX **Import dashboard from file** for initial creation. For future updates, open the corresponding dashboard above, switch to Editing, select **Additional options > File > Replace dashboard with file**, confirm, and Save. Wait for Save to finish before navigating. Do not import a new copy when updating an existing dashboard.

The older `dashboards/azcopy-usage-dashboards` generator is retained as a legacy implementation. Its four overlapping artifacts no longer represent the deployed dashboards and should not be republished over this suite. The Observed Funnel remains separate and outside this request.

## Verification results

- All 42 Grafana data panels executed successfully through the authenticated Grafana ADX datasource.
- All 42 ADX tiles were scrolled into view and checked for query/layout errors after publication.
- Named-customer selection rendered 23.2 GB in Grafana and 23.190588261 GB in ADX. Data totals include unmapped activity and reconcile to 47,830,502,509 bytes and 16,814 logical jobs.
- Synthetic tests cover two-month recurrence, one-month nonrecurrence, empty-cohort null average, failure plus success in the same customer, cancelled-only customers, skipped successes, unmapped ownership, and status filtering after cohort selection.
- Current test telemetry spans August 18-27, 2026. Zero recurring customers and an empty named-customer no-success cohort are expected for this data, not query failures.
- The suite is backed by test telemetry and manual persisted refresh, not production adoption data or a newly installed scheduler.