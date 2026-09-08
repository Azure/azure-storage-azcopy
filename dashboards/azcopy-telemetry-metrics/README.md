# AzCopy Telemetry Metrics ADX Dashboard

This dashboard displays the complete numeric telemetry inventory emitted by `telemetry/events.go`. It is an operational schema dashboard, separate from the business-metrics dashboard: values are received telemetry observations.

## Import

1. Open [ADX Dashboards](https://dataexplorer.azure.com/dashboards).
2. Select the arrow next to **New dashboard**.
3. Select **Import dashboard from file**.
4. Select `azcopy-telemetry-metrics.dashboard.json` from this directory.
5. Name the dashboard `AzCopy Telemetry Metrics` and select **Create**.

To query the provisioned E2E telemetry resource instead, import
`azcopy-telemetry-metrics.telemetry-test.dashboard.json`. It has distinct
dashboard and data-source IDs, so it can coexist with the default dashboard.

The import contains 39 tiles across six pages. It uses an XStore dashboard data source to execute explicit cross-cluster queries against Application Insights. If the source needs reconnecting, use:

```text
Cluster: https://azcore.centralus.kusto.windows.net
Database: Xstore
```

## Pages

| Page | Panels |
| --- | --- |
| Overview | Lifecycle counters, complete metric catalog, transfer dimension mix |
| Data Volume | Bytes, byte definitions, scheduled object composition, object/folder outcomes, visible outcome relationships, transfer outcomes |
| Source Profile | Scanned/touched inventory, scanned source object-size statistics, scanned source small-object share and directory depth |
| Performance | Phase durations, throughput, Storage HTTP latency and IOPS, performance constraints, advice-code distributions, configured concurrency, buffer size, and throughput cap |
| Reliability | Visible panel guide, HTTP/network/error counts, server-busy counts, error/completion percentages, job errors |
| Environment | Individual version, OS, architecture, CPU, memory, NIC, Azure VM detection, geography, and invocation distributions; combined detail table; recent finished attempts |

The complete metric catalog provides sample count, nonzero count, minimum, average, p50, p95, and maximum for every metric observed in the selected time range. Metrics not emitted during that range do not have rows, while the grouped panels remain valid and return empty results.

## Generate And Validate

Regenerate the deterministic schema-v60 import artifact:

```powershell
./dashboards/azcopy-telemetry-metrics/generate-dashboard.ps1
```

Generate the provisioned E2E telemetry variant:

```powershell
./dashboards/azcopy-telemetry-metrics/generate-dashboard.ps1 `
  -OutputPath ./dashboards/azcopy-telemetry-metrics/azcopy-telemetry-metrics.telemetry-test.dashboard.json `
  -AppInsightsSubscriptionId 31347be8-d066-464e-9866-7e58d85027b7 `
  -AppInsightsResourceGroup azcopy-telemetry-test-rg `
  -AppInsightsApp azcopy-telemetry-test-ai `
  -DashboardVariant "Telemetry Test"
```

The generator validates ADX's minimum tile size, rejects overlapping tiles, and compares the queries with the metric names in `telemetry/events.go`. It fails when a newly emitted metric is not represented in the query set.

Execute every source query against the live Application Insights component:

```powershell
./dashboards/azcopy-telemetry-metrics/validate-queries.ps1
```

The default dashboard time range is 24 hours. The default artifact reads the
`customEvents` table in `sharankur_insights1`; the telemetry-test artifact reads
it from `azcopy-telemetry-test-ai`. Each query expands the packed
`customMeasurements` bag into a virtual metric name/value shape.