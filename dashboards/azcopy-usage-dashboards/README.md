# AzCopy usage dashboards

This directory generates ADX and Azure Managed Grafana dashboards adapted from
the Storage Mover monitoring dashboards. The dashboards use real AzCopy client
telemetry and the existing server-side destination-account enrichment model.

## Dashboards

| Dashboard | ADX | Grafana |
|---|---:|---:|
| AzCopy Data Metrics | Existing `..\azcopy-data-metrics` artifact | `azcopy-data-metrics*.grafana.json` |
| AzCopy Customer Drilldown | Yes | Yes |
| AzCopy Observed Funnel | Yes | Yes |
| AzCopy Installations and Accounts with No Observed Success | Yes | Yes |
| AzCopy Recurring Usage Trends | Yes | Yes |

Generated artifacts are written to `generated`. Files with
`.telemetry-test` use `azcopy-telemetry-test-ai`; files without that suffix use
the default Application Insights component accepted by the generator.

## Generate

```powershell
.\generate-dashboards.ps1

.\generate-dashboards.ps1 `
  -AppInsightsResourceId '/subscriptions/31347be8-d066-464e-9866-7e58d85027b7/resourceGroups/azcopy-telemetry-test-rg/providers/Microsoft.Insights/components/azcopy-telemetry-test-ai' `
  -AppInsightsDatabase 'azcopy-telemetry-test-ai' `
  -OutputSuffix '.telemetry-test'

# Live ARG is enabled by default. Use this explicit opt-out when needed.
.\generate-dashboards.ps1 -EnableLiveArgEnrichment:$false
```

The Grafana artifacts reference datasource UID `azcopy-xstore`, which targets
`https://azcore.centralus.kusto.windows.net/Xstore` with current-user OAuth
passthrough.

## Published test dashboards

The telemetry-test Grafana variants are published to
`azcopy-telemetry-ankur`:

| Dashboard | URL |
|---|---|
| AzCopy Data Metrics | [Open](https://azcopy-telemetry-ankur-cbbcech2ecd9gad6.eus.grafana.azure.com/d/azcopy-data-metrics/azcopy-data-metrics) |
| AzCopy Customer Drilldown | [Open](https://azcopy-telemetry-ankur-cbbcech2ecd9gad6.eus.grafana.azure.com/d/azcopy-customer-drilldown/azcopy-customer-drilldown) |
| AzCopy Observed Funnel | [Open](https://azcopy-telemetry-ankur-cbbcech2ecd9gad6.eus.grafana.azure.com/d/azcopy-observed-funnel/azcopy-observed-funnel) |
| AzCopy Installations and Accounts with No Observed Success | [Open](https://azcopy-telemetry-ankur-cbbcech2ecd9gad6.eus.grafana.azure.com/d/azcopy-no-observed-success/566d4c7) |
| AzCopy Recurring Usage Trends | [Open](https://azcopy-telemetry-ankur-cbbcech2ecd9gad6.eus.grafana.azure.com/d/azcopy-recurring-trends/azcopy-recurring-usage-trends) |

## Validate

```powershell
.\validate-generated-queries.ps1
```

The validator executes every unique query from the generated telemetry-test
ADX artifacts against the central ADX cluster. Final validation passed all 25
unique usage-dashboard queries and the complete Data Metrics validation suite.
An authenticated query through the published Grafana `azcopy-xstore`
datasource also succeeded.

The four new ADX dashboard artifacts are generated and query-validated, but
they still need to be imported into the intended ADX dashboard workspace.

## Semantics

- `InstallationID` is a durable pseudonymous installation identifier, not a
  verified customer, tenant, or person.
- Storage account names are resource proxies. Destination account metadata is
  enriched server-side through XStore account history. Live Azure Resource
  Graph enrichment is enabled by default.
- Concurrent live-ARG fan-out can produce remote streaming disconnects and
  Kusto partial-query warnings. Generate with
  `-EnableLiveArgEnrichment:$false` to disable it; current-only fields then
  explicitly report `Live ARG disabled`.
- Enabled Azure Resource Graph enrichment is best effort. If its remote query
  leg fails, telemetry and historical XStore results remain available, but the
  Kusto response can still contain partial-query warnings.
- The recommended production replacement is a periodically refreshed local ADX
  account/subscription snapshot. Query-results caching only helps identical
  complete queries and does not share the enrichment prefix between panels.
  See the Data Metrics README for the proposed schema, refresh flow, and
  freshness handling.
- Terminal jobs are deduplicated by `JobID`.
- Additive values and distinct installation counts use received finished-job
  telemetry.
- Funnel, no-success, and recurrence classifications apply only to the selected
  time range.
- No new subscription IDs, tenant IDs, or ARM calls are added to client
  telemetry.

See [migration-report.md](migration-report.md) for the source-panel,
filter, and drilldown decision matrix.
