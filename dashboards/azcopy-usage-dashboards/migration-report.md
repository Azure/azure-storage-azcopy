# Storage Mover to AzCopy dashboard migration report

## Decision rules

Panels are included only when AzCopy telemetry measures the same behavior or a
clearly labeled AzCopy analogue. Storage Mover control-plane entities are not
renamed into unrelated AzCopy concepts. `InstallationID` and Storage account
names are used as pseudonymous installation and resource proxies, never as
verified customer identity.

## Data dashboard

Source: `be3i4q07jl5vka` (`Data`)

### Filters

| Source filter | Decision | AzCopy implementation |
|---|---|---|
| SubscriptionId | Adapted | Destination owning subscription from ARG/XStore enrichment |
| OfferType | Included | Destination subscription offer type from ARG enrichment |
| Region | Adapted | Client country/region plus destination Storage region |
| InternalSubIds | Adapted | Destination subscription scope: Internal, External, Unknown, Not applicable |
| Source | Included | `SourceType` and `FromTo` |
| Target | Included | `DestType` and `FromTo` |
| PrivateNetworkEnabled | Replaced | `DestEndpointKind`; AzCopy has endpoint evidence, not an authoritative account private-network setting |
| Account | Added | Exact source or destination Storage account name |

### Panels

| Storage Mover panel | Decision | AzCopy panel or reason |
|---|---|---|
| README | Adapted | AzCopy semantics and enrichment notice |
| Data Transferred through completed jobs | Included | Selected-range logical bytes from deduplicated terminal jobs |
| Data Transferred (May 2025 onwards) | Adapted | Selected-range totals; no artificial product launch cutoff |
| Current semester / Previous semester | Replaced | Current versus previous calendar month; avoids Storage Mover business targets |
| Recent data transferred, 15 min refresh | Adapted | Recent terminal jobs table; dashboard refresh remains user controlled |
| Current month / Last month | Included | Current versus previous month multistat |
| Monthly Data transferred | Included | Monthly logical payload trend |
| Data by source-target pair | Included | `FromTo` payload distribution |
| Files by source-target pair | Adapted | Completed payload objects by `FromTo` |
| Target storage account type panels | Adapted | Destination service type, account kind, SKU, redundancy, namespace, access tier, and account class |
| Data split by account type | Adapted | Destination account-class enrichment |
| Until-April-2025 legacy monthly chart | Excluded | Storage Mover schema-transition artifact has no AzCopy equivalent |
| Backup: active-job data | Excluded | AzCopy telemetry has no reliable authoritative in-progress byte counter |
| Backup: semester goal panels | Excluded | Storage Mover-specific business goals |
| Backup: all-time data/files | Excluded | Unbounded all-time scans are expensive and misleading under telemetry retention |
| Backup: C2C total | Adapted | Available through source/target filters instead of a duplicate panel |
| Backup: duplicate data/monthly/files charts | Excluded | Superseded by selected-range panels |
| Industry category/segment | Excluded | No authoritative AzCopy-to-industry mapping |
| Subscription Offer Type | Included | Offer-type filter and enriched distributions |

The AzCopy dashboard adds mapping coverage so missing XStore, ARG resource, and
ARG subscription joins remain visible rather than silently disappearing.

## Customer Drilldown dashboard

Source: `aedhihs9max34c` (`Customer Drilldown`)

### Filters

| Source filter | Decision | AzCopy implementation |
|---|---|---|
| OnlyActiveSubscriptions | Excluded | No authoritative client-side subscription lifecycle state |
| CustomerName | Replaced | Exact pseudonymous `InstallationID` or Storage account proxy |
| SubscriptionId | Adapted | Destination owning subscription through enrichment |
| MonthRange | Included | Grafana/ADX time range |
| Source | Included | `SourceType` |
| Target | Included | `DestType` |

### Panels

| Storage Mover panel | Decision | AzCopy panel or reason |
|---|---|---|
| Customer Name | Replaced | Selected installation/account summary |
| Properties | Adapted | Destination account profile |
| Subscriptions | Adapted | Destination subscription, offer, and scope columns |
| Total Data transferred | Included | Selected-range logical payload |
| Resources created | Excluded | Storage Mover ARM lifecycle has no AzCopy equivalent |
| Job runs | Adapted | Observed deduplicated terminal jobs |
| InProgress Job runs | Excluded | Started-minus-finished is not authoritative across range boundaries or when lifecycle events are missing |
| Failed Job runs | Included | Terminal outcome distribution and errors |
| Registered Agents | Excluded | AzCopy has no registered-agent control-plane entity |
| Recent data transferred | Adapted | Recent terminal jobs |
| Projects | Excluded | No AzCopy migration-project entity |
| Job definitions created | Excluded | No AzCopy job-definition entity |
| NFS endpoints | Adapted | Source/target topology and mount-type filters |
| SMB endpoints | Adapted | Source/target topology and mount-type filters |
| Activity (DataTransferred) | Included | Daily activity and data by topology |
| Activity split by Source Target | Included | Data by `FromTo` |
| Activity (File Transferred) | Adapted | Completed object metrics |
| Activity (Job Runs) | Included | Daily terminal job activity |
| Failed job runs | Included | Error distribution and recent terminal jobs |
| Successful runs | Included | Outcome distribution and recent terminal jobs |
| Recent runs | Included | Recent terminal jobs |
| Support cases | Excluded | No authoritative AzCopy telemetry-to-support-case relationship |
| Incidents | Excluded | No authoritative AzCopy telemetry-to-IcM relationship |

AzCopy-specific additions are version/OS mix, terminal error codes, and enriched
destination-account metadata.

## Funnel dashboard

Source: `ce2vqxnpbvfnke` (`Funnel (OnPrem)`)

### Filters

| Source filter | Decision | AzCopy implementation |
|---|---|---|
| SubscriptionId | Adapted | Destination subscription enrichment is available on terminal stages |
| OfferType | Adapted | Destination offer enrichment is available on terminal stages |
| Region | Adapted | Client/destination region is available after a terminal job |
| InvocationContext | Added | Separates E2E, test, and ordinary invocation contexts |

### Panels

| Storage Mover panel/group | Decision | AzCopy panel or reason |
|---|---|---|
| README | Adapted | Explicit selected-range and pseudonymous-identity limitations |
| Funnel - Customers with valid TPID | Excluded | AzCopy telemetry does not contain an authoritative TPID/customer identity |
| Storage Mover Resources | Replaced | Any `azcopy.command.invoked` event |
| Registered Agent | Replaced | Transfer job started |
| Migration Project | Excluded | No AzCopy equivalent |
| Job Definition | Excluded | No AzCopy equivalent |
| Job Run | Adapted | Terminal job observed |
| Successful Job Run | Included | Successful terminal job |
| Non-zero Bytes Transferred | Included | Successful terminal job with nonzero bytes |
| More than 1 GB Transferred | Included | Successful terminal job over 1 GiB |
| Funnel - Unique customers | Adapted | Distinct observed `InstallationID` values |
| Concise format for PPT | Excluded | Duplicate presentation-only panels |
| Customer drop after resource creation | Replaced | Stage retention/drop-off table |
| Support cases after each early stage | Excluded | No authoritative support-case relationship |
| Customer drop after agent registration | Excluded | No registered-agent stage |
| Customer drop after migration project | Excluded | No migration-project stage |
| Customer drop after job definition | Excluded | No job-definition stage |
| Zero successful runs with failures/cancels | Adapted | Separate no-observed-success dashboard |
| Successful runs but no data | Included | Captured by retention from success to nonzero-data stage |
| Successful runs under 1 GB | Included | Captured by retention from nonzero data to over-1-GiB stage |

The AzCopy funnel is intentionally only four dashboard rows high: one
explanation, one compact six-stage gauge, one retention table, and one weekly
trend. It does not imply acquisition or lifetime conversion.

## Customers with no success dashboard

Source: `afeix0jmzcxkwd` (`Customers with no success`)

### Filters

| Source filter | Decision | AzCopy implementation |
|---|---|---|
| SourceType | Included | `SourceType` |
| TargetType | Included | `DestType` |
| StatusCode | Adapted | `JobErrorCode` in the distribution and detail tables |
| FailedSubscriptionIds | Replaced | Storage account/installation text filters and destination subscription enrichment |

### Panels

| Storage Mover panel | Decision | AzCopy panel or reason |
|---|---|---|
| Unique Active Customer count with no successful job run | Adapted | Selected-range installations and destination accounts with terminal activity and zero success |
| Status codes of terminal job failures | Included | `JobErrorCode` distribution |
| Terminal job failure | Included | Installation and destination-account summaries |
| Incidents | Excluded | No authoritative IcM relationship |
| Support cases | Excluded | No authoritative support-case relationship |
| Drilldown: Terminal job failure | Included | Clickable failed terminal-job detail |
| Drilldown: All Failed jobs | Included | Latest failed or partially failed jobs |

The title deliberately says **No Observed Success** because the selected range
cannot establish lifetime behavior.

## Trends dashboard

Source: `eea4p1kr7v5dse` (`Trends`)

| Storage Mover panel | Decision | AzCopy panel |
|---|---|---|
| Recurring customers | Adapted | Recurring pseudonymous installations and destination-account proxies active in at least two selected-range months |
| Total data by recurring customers | Included with corrected identity wording | Data from recurring installations |
| Average recurring months | Included | Average active months among recurring installations |
| Recurring customers detail | Included | Recurring installation detail with activity, jobs, payload, and account count |

Monthly recurring-installation, recurring-account, and recurring-data trends are
added because they expose the behavior over time rather than only a headline
number.

## Drilldowns

| Source behavior | Decision | AzCopy implementation |
|---|---|---|
| Trends to Customer Drilldown | Included | Grafana `InstallationID` field links preserve time range |
| No-success to Customer Drilldown | Included | Grafana installation and destination-account links preserve time range |
| Data recent jobs to Customer Drilldown | Included | Grafana installation and destination-account links preserve time range |
| Customer-name and subscription drilldowns | Adapted | Pseudonymous installation and resource-proxy filters |
| Support-case and IcM drilldowns | Excluded | No authoritative join key |
| ADX click-through | Adapted | Shared installation/account parameters are present on each ADX dashboard; imported dashboard links are intentionally not hard-coded because ADX dashboard IDs are deployment-specific |

## Known limitations

- Current ARG metadata can differ from the account state when the job ran.
- Historical XStore and current ARG joins are destination-centric because the
  destination account has the strongest exact ownership relationship.
- Missing inventory joins are shown as `Unknown`, `Unmapped`, or explicit
  coverage states.
- Additive metrics and distinct installation counts use received finished-job
  telemetry.
- Telemetry retention bounds all trend, no-success, and funnel conclusions.

## Delivery and validation status

- All five telemetry-test Grafana dashboards are published to
  `azcopy-telemetry-ankur`.
- All 25 unique queries from the four new telemetry-test ADX dashboards passed
  live execution against the central ADX cluster.
- The complete AzCopy Data Metrics validation suite passed.
- The published dashboards were read back and matched the generated panel,
  variable, datasource, and query models.
- Every published query-backed variable uses `azcopy-xstore`; textbox variables
  are intentionally datasource-free.
- Published field-link overrides match the generated models: 10 links preserve
  `${__url_time_range}`, including five installation and five account
  drilldowns.
- An authenticated query through Grafana's `azcopy-xstore` datasource returned
  live `azcopy.command.invoked`, `azcopy.job.started`, and
  `azcopy.job.finished` telemetry.
- The four new ADX dashboard artifacts have not yet been imported into an ADX
  dashboard workspace.

Published Grafana dashboards:

- [AzCopy Data Metrics](https://azcopy-telemetry-ankur-cbbcech2ecd9gad6.eus.grafana.azure.com/d/azcopy-data-metrics/azcopy-data-metrics)
- [AzCopy Customer Drilldown](https://azcopy-telemetry-ankur-cbbcech2ecd9gad6.eus.grafana.azure.com/d/azcopy-customer-drilldown/azcopy-customer-drilldown)
- [AzCopy Observed Funnel](https://azcopy-telemetry-ankur-cbbcech2ecd9gad6.eus.grafana.azure.com/d/azcopy-observed-funnel/azcopy-observed-funnel)
- [AzCopy Installations and Accounts with No Observed Success](https://azcopy-telemetry-ankur-cbbcech2ecd9gad6.eus.grafana.azure.com/d/azcopy-no-observed-success/566d4c7)
- [AzCopy Recurring Usage Trends](https://azcopy-telemetry-ankur-cbbcech2ecd9gad6.eus.grafana.azure.com/d/azcopy-recurring-trends/azcopy-recurring-usage-trends)
