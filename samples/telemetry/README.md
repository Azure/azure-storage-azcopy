# AzCopy Telemetry Sample

This sample emits telemetry through the same `telemetry.Reporter`, event models, direct Application Insights backend, and `/v2.1/track` envelope path used by AzCopy. It does not enumerate or transfer data.

Each run creates:

- One `azcopy.command.invoked` event.
- Five paired `azcopy.job.started` and `azcopy.job.finished` copy attempts.
- Five paired `azcopy.job.started` and `azcopy.job.finished` sync attempts.
- Every numeric measurement defined by `JobFinishedEvent` in each finish payload. Statuses vary across completed, completed-with-errors, failed, cancelled, and completed-with-skipped samples.
- Measurement values are randomized on every run while preserving relationships between totals, outcomes, percentages, bytes, and durations. This produces visible variation in dashboard trends; run timestamps provide the random seed.
- Each command covers five dimension variants: local-to-Azure upload, Azure-to-local download, intra-Azure service-to-service, AWS S3-to-Azure, and Google Cloud Storage-to-Azure. Endpoint protocols, scopes, auth mechanisms, cloud types, and public/private endpoint kinds vary with the topology.

Byte measurements always satisfy `bytes_transferred <= bytes_expected <= bytes_enumerated` and `bytes_over_wire > bytes_transferred`. Fully successful attempts have equal enumerated, expected, and transferred bytes. Terminal attempts with failed, skipped, or cancelled work set expected bytes to the successful payload; over-wire bytes add retry and partial failed/incomplete traffic.

Sample events use `SamplingRate=1`, `SamplerVersion=telemetry-sample-v1`, `InstallationID=telemetry-sample`, and sample-prefixed job IDs so they can be excluded from production analysis.

## Dry Run

```powershell
go run ./samples/telemetry -dry-run
```

The dry run builds and counts the events without making network requests.

## Capture JSON Payloads

```powershell
go run ./samples/telemetry -write-payloads SampleTelemetryPayloads.md
```

This runs each representative event through the production direct Application Insights serializer using an in-memory HTTP client. It writes one command, copy start/finish, and sync start/finish request body without contacting ingestion.

## Send to Application Insights

The sample currently contains a hardcoded Application Insights connection string for temporary testing.

```powershell
go run ./samples/telemetry
```

Use `-timeout` to change the default two-minute overall send timeout. Remove the hardcoded connection string or restore environment-based configuration before committing or using this sample beyond temporary testing.