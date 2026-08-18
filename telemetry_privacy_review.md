# AzCopy telemetry privacy review

## Verdict

The implementation minimizes collection and does not intentionally emit file
contents, file/object names, local paths, full URLs, credentials, SAS tokens,
tenant/client IDs, free-form benchmark descriptions, or raw error messages.
However, the dataset is **pseudonymous and potentially identifying, not
anonymous**. Production use should receive formal privacy approval.

## Data collected

- Random installation, invocation, and job correlation IDs.
- Azure Storage account names for recognized Azure endpoints.
- Exact OS/CPU/memory/NIC characteristics and Azure VM detection.
- Command/option categories and aggregate workload, performance, and failure
  measurements.
- Application Insights may derive coarse geolocation from the sender IP; by
  default, it discards the raw IP and stores `0.0.0.0`.

Users can disable telemetry with `--disable-telemetry` or
`AZCOPY_DISABLE_TELEMETRY=true`. Default 1% sampling reduces volume, not the
sensitivity of an included event.

## Residual risks and required actions

- Stable installation IDs, job IDs, account names, and host details can be
  linked across events and can fingerprint a customer resource or machine.
- Replace user-facing claims that the telemetry is anonymous or "contains no
  PII" with a precise disclosure of the pseudonymous data above.
- Review whether account names and exact host fields can be removed, hashed,
  rotated, or coarsened.
- Justify or reduce the current 730-day retention period, enforce least-privilege
  query access, and audit access to production telemetry.
- Confirm that IP masking remains enabled and that derived geolocation is
  required; otherwise suppress or discard those fields.
