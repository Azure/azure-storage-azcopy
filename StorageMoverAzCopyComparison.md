# AzCopy vs Storage Mover Comparison

> Converted from the internal [AzCopy vs Storage Mover Comparison Word document](https://microsoft.sharepoint.com/:w:/t/AzureStorage/cQrFZHozS7WdRZ4R60u6GNNEEgUCZethAXT0giQdHMceOXh9uw) on 2026-07-16. The review comments present at conversion time are preserved at the end of this file.

## Change Log

| Change | Date | Owner |
| --- | --- | --- |
| Initial draft | 7/1 | Raj Singh (singra) |

## Introduction

AzCopy ([Azure/azure-storage-azcopy](https://github.com/Azure/azure-storage-azcopy)) is an open-source command-line utility for copying data to, from, and between supported storage locations. Depending on the scenario, supported endpoints include the local file system, Azure Blob Storage, Azure Files, Azure Data Lake Storage Gen2, Amazon S3, and Google Cloud Storage.

AzCopy works by calling Azure Storage APIs, typically via the Azure Storage SDKs for Go. We add a header with every request of the form `User-Agent: AzCopy/10.24.0 azsdk-go-azblob/v1.3.1 (go1.19.12; Windows_NT)`, which helps collect some data about AzCopy usage on the server side. This can be accessed at [BlobClientTools](https://aka.ms/BlobClientTools).

This document outlines:

- A comparison of capabilities between AzCopy and Storage Mover, to analyze any future investments in Storage Mover based on AzCopy.
- A new enhancement in AzCopy command output, toward go-to-market for Storage Mover.
- Details about the development of a telemetry pipeline.

See the [AzCopy wiki](https://github.com/Azure/azure-storage-azcopy/wiki).

## Comparing AzCopy and Storage Mover

### Source-Target Pairs

| Source-target pair supported by AzCopy | Storage Mover parity |
| --- | --- |
| Local `<->` Azure Blob / Files SMB / Files NFS / ADLS | Storage Mover does not support egress to local on-premises storage. |
| Azure Blob / ADLS `->` Azure Blob / ADLS | Yes. Cross-tenant support is under development; cross-cloud intra-Azure migration is on the roadmap. |
| Azure Blob `<->` Azure Files SMB | No. |
| Azure Files SMB / NFS `->` Azure Files SMB / NFS | Under development. |
| AWS S3 (access key) `->` Azure Block Blob | Yes, with the multi-cloud connector. |
| GCS (service account key) `->` Azure Blob | Yes, with HMAC. |
| Full storage account moves (all blobs and file shares) | No; on the roadmap. |

- AzCopy supports cross-tenant migration and cross-cloud intra-Azure migration using SAS tokens.
- AzCopy does not support egress from Azure to other clouds (AWS or GCP). It only supports egress to local on-premises storage.
- AzCopy cannot transfer data from other compatible S3 object storage, such as DigitalOcean or Wasabi.

### Migration Features in AzCopy

The following table compares AzCopy commands and flags with the corresponding Storage Mover behavior. The Priority column indicates how these features could be introduced into Storage Mover.

| AzCopy capability | Storage Mover behavior | Priority |
| --- | --- | --- |
| Include specific paths, regular expressions, attributes, wildcard patterns (`*`), and directory stubs. | Only allows including a specific path in endpoints. | P0 |
| Exclude specific attributes, blob types (Block Blob, Page Blob, Append Blob), containers, paths/directories, wildcard patterns (`*`), and regular expressions. | Not exposed. | P0 |
| Include a subset of files before or after a specific last-modified date/time. | Not exposed. | P1 |
| Set tags on target files on Blob Storage. | Not exposed. | P1 |
| Place folder sources as subdirectories under the destination. | Not exposed. | P1 |
| Define the destination blob type: `Detect` (same as source), `BlockBlob`, `PageBlob`, or `AppendBlob`; additionally choose the Page Blob tier. | Uses `Detect` by default. The development team enabled Hydro Quebec to choose Block Blob as the target tier in a one-off scenario. | P0 |
| Define the destination blob tier: `Hot`, `Cold`, `Cool`, or `Archive`. | Preserves the source tier by default. The development team enabled PicTime to land data directly on the Cold tier with a feature flag. | P0 |
| Check destination file length after transfer and mark the transfer failed if it differs from the source. | To be confirmed. | TBD |
| Set `content-disposition`, `content-encoding`, and `content-language` headers. | To be confirmed. | TBD |
| Use a client-provided key for encryption of data in Azure. | Not used in Storage Mover. | P0 |
| Follow symlinks and copy their contents. | Same behavior. | - |
| Overwrite destination (`true`, `false`, `prompt`, or `ifSourceNewer`). | Uses `ifSourceNewer`: destination files are overwritten when source files are newer. | P1 |
| Choose the maximum size for uploading a blob as a single PUT. | Not configurable. The development team enabled PicTime to increase the maximum to 100 MB and use Put Blob for all files with a feature flag. | P0 |
| Create an MD5 for each file. | Defaults to Storage API behavior. Put Blob includes a checksum in `Content-MD5`; Put Block does not. | P0 |
| Handle differences in object naming rules between AWS and Azure. | Same behavior. | - |
| Handle invalid object metadata with `ExcludeIfInvalid`, `FailIfInvalid`, or `RenameIfInvalid`. | Uses `RenameIfInvalid`. | - |
| Increase concurrency with `AZCOPY_CONCURRENCY_VALUE`. | To be confirmed. | TBD |
| Optimize memory use with `AZCOPY_BUFFER_GB`. | To be confirmed. | TBD |
| Synchronize by comparing last-modified time and MD5 hash. | Relies only on last-modified time. | P1 |

### Summary

Storage Mover covers most source/target pairs supported by AzCopy except egress to local on-premises storage and migration between Azure Blob and Azure Files SMB.

AzCopy exposes many flags that could be added to the Storage Mover portal to give customers more flexibility. Priorities are shown above; P0 items have previously been requested by customers or partners.

## In-Product Storage Mover Nudge

To increase the reach and visibility of Storage Mover, introduce a nudge inside compatible AzCopy commands that directs customers toward Storage Mover for migrations.

Display the nudge as a `NOTE` or `INFO` line in the terminal:

> For large transfers beyond 100 TB, consider using Storage Mover. Storage Mover is a fully managed data migration service that will make large data transfers convenient. Visit `https://aka.ms/XXXX` for more information.

The nudge must only be displayed for scenarios supported by Storage Mover. It should not be displayed in the following scenarios:

| Source | Destination |
| --- | --- |
| Any | Local on-premises storage |
| Azure Files SMB | Azure Blob |
| Azure Blob | Azure Files SMB |
| Full storage account | Any |

The nudge should also be suppressed for intra-Azure, cross-cloud transfers until that feature is released.

Confirm with the development team how long the nudge will take to develop and deploy. The exclusion table may change because Azure Files-to-Files migration is currently under development.

## AzCopy Telemetry Pipeline

Today, AzCopy usage can be tracked only through limited Storage server-side data in [BlobClientTools](https://aka.ms/BlobClientTools). This limits analysis of adoption, reliability, performance, migration outcomes, and supportability.

The tables below define the data points to capture for every AzCopy run and the business metrics to derive from those signals.

### Data Points Captured Per Run

| Scope | Data points captured |
| --- | --- |
| All commands | Command metadata with secrets redacted: command type, flags used, and AzCopy version. |
| Per copy, sync, or benchmark job: host and environment | CPU, memory, network, NIC speed, file protocol, and platform (Windows or Linux). |
| Per copy, sync, or benchmark job: endpoints and data | Endpoint type and transfer direction: local, Blob, ADLS, Azure Files, AWS S3, GCS, or full storage account.<br><br>Transfer shape: total bytes, object count, average and percentile object size, small-file ratio, and directory-depth bucket.<br><br>Blob/ADLS: number of blobs and number and size of containers.<br><br>S3/GCP: number and size of buckets. |
| Per copy, sync, or benchmark job: identity and customer per endpoint | Storage account, subscription ID, tenant ID, Azure region, and cloud type.<br><br>S3/GCS bucket name for the source/target pair.<br><br>Cloud type (Public or Government).<br><br>Authentication mechanism (Entra or SAS). |
| Per copy or sync job: execution and outcome | Job status (`InProgress`, `Completed`, `CompletedWithErrors`, `Failed`, or `Canceled`).<br><br>Error code on failure; files transferred, passed, and failed; error-code distribution; amount of data transferred over public versus private networks; start/end times and duration; latency and average network speed; retry count; resume/restart count; and percentage complete at cancellation. |
| Per benchmark job | Result code, description, reason, and diagnostic statistics. |
| Per supportability signal | Diagnostic bundle generated; log bundle upload status; redaction status; support case or ICM linkage; command invocation ID; job ID; top error category; remediation shown and attempted; and whether the issue was resolved without escalation. |

### Business Metrics Derived

| Scope | Metrics derived |
| --- | --- |
| Per customer | Jobs and amount of data transferred in the past N days; average migration time per TB (P90/P95); average network speed; failures and error codes; command distribution; days since the last job; week-over-week frequency; AzCopy version; retries and resumptions per job; and abandonment rate with completion percentage at termination. |
| AzCopy-wide: volume and categories | Data transferred across on-premises-to-Azure, Azure-to-Azure, AWS-to-Azure, and GCS-to-Azure categories; average migration time per TB; throughput P50/P90/P95; completion time per TB; enumeration, transfer, and finalization times; retry overhead; and top movers. |
| AzCopy-wide: adoption and funnel | First-job success rate, time to first successful job, percentage of customers running a second job, and week-over-week job-frequency trend. |
| AzCopy-wide: reliability | Completion rate; success, partial-success, and failure rates; error distribution; failed-object rate; throttling and cancellation rates; resume success; abandonment and completion at termination; repeated error codes; and new error codes in the last 30 days. |
| AzCopy-wide: platform mix | Distribution of source and target platforms. |
| AzCopy-wide: supportability | Support cases per active customer and per 1,000 jobs; diagnostic-bundle, support-ready-case, and self-service-resolution rates; time to diagnosis, mitigation, and resolution; repeat-contact rate; and escalation rate. |

## Review Comments

At conversion time the Word document contained 44 comments in 23 unresolved threads and no tracked revisions. Comment numbers below preserve the Word comment indexes.

### General

**#1 - Anchor: `AzCopy`**

- Manu Aery, 2026-06-26: Thank you for putting this together, Raj. Appreciate it.

### Source-Target Pairs

**#2 - Anchor: `Yes (with HMAC)`**

- Anusha Subramanian, 2026-06-26: We can switch this to Arc connector in the next semester now that support is available, based on customer feedback and demand.

**#3 - Anchor: compatible S3 object storage**

- Mohit Kumar Garg, 2026-07-08: Asked Sridhar what limitation prevents support for DigitalOcean, Wasabi, and similar storage.
- **#4 reply - Sridhar Lanka, 2026-07-10:** Former AzCopy team members said those providers were never tested because the team wanted to limit the support matrix.

### Migration Features

**#5 - Anchor: `Exclude`**

- Manu Aery, 2026-06-26: Asked which customer scenarios exclusion controls would serve.
- **#6 reply - Raj Singh, 2026-06-26:** No known customer use case, but Shekhar asked whether folder selection and deselection was available in Storage Mover for CloudContext.

**#7 - Anchor: `subset`**

- Manu Aery, 2026-06-26: Suggested this could support the mirror scenario that XMS does not support.
- **#8 reply - Raj Singh, 2026-06-26:** AzCopy sync already has equivalents of merge and mirror modes. Because this would have a similar internal implementation, waiting for XMS may be better.

**#9 - Anchor: `folder sources`**

- Manu Aery, 2026-06-26: Asked for clarification.
- **#10 reply - Raj Singh, 2026-06-26:** Explained that the flag places the source folder beneath a new root at the destination rather than preserving it directly at the destination root. The flag was included for completeness but was not considered especially useful.

**#11 - Anchor: empty Storage Mover behavior cells**

- Raj Singh, 2026-06-25: The empty cells in the Storage Mover column need confirmation from the Storage Mover development team.

**#12 - Anchor: blob type priority `P0`**

- Manu Aery, 2026-06-26: Asked whether telemetry shows how often the source and destination blob types differ.
- **#13 reply - Raj Singh, 2026-06-26:** AzCopy client telemetry did not exist. Limited Storage-side telemetry is available through the AzCopy request header and BlobClientTools dashboard.

**#14 - Anchor: destination tier**

- Anusha Subramanian, 2026-06-26: Asked whether AzCopy supports Smart Tier and whether Storage Mover has been tested with it.
- **#15 reply - Manu Aery, 2026-06-26:** Asked why Storage Mover chose its current fixed settings and suggested exposing some options under advanced copy settings.
- **#16 reply - Raj Singh, 2026-06-26:** The flag supports Hot, Cold, Cool, and Archive, but not Smart. After migration, `azcopy set-properties` can apply tags used for Smart Tier classification.
- **#17 reply - Raj Singh, 2026-06-26:** Proposed a combined feature release exposing many of these flags in advanced portal settings while preserving the current experience through defaults.

**#18 - Anchor: client-provided encryption key**

- Anusha Subramanian, 2026-06-26: Asked whether AzCopy supports different encryption methods on source and target.
- **#19 reply - Manu Aery, 2026-06-26:** Asked for clarification about source encryption and suggested customer-managed key support in Storage Mover should be P0.
- **#20 reply - Anusha Subramanian, 2026-06-26:** Cited an Axon cross-tenant scenario where one customer uses HSM encryption and another uses cross-tenant CMK through a remote HSM.
- **#21 reply - Anusha Subramanian, 2026-06-26:** Confirmed that storage-account CMK support should be P0.
- **#22 reply - Raj Singh, 2026-06-26:** AzCopy can separately download with source-key decryption and upload with target-key encryption, but support for one managed command using different keys was unclear and needed discussion with Ankur.

**#23 - Anchor: symlinks**

- Anusha Subramanian, 2026-06-26: AzCopy support for copying symlinks and hardlinks was believed to be forthcoming, but needs confirmation and a Storage Mover parity evaluation.

**#24 - Anchor: single PUT upload size**

- Manu Aery, 2026-06-26: Suggested collecting usage telemetry and potentially exposing or recommending the setting based on its benefit and effectiveness.

### Storage Mover Nudge

**#25 - Anchor: in-product Storage Mover nudge**

- Mohit Kumar Garg, 2026-07-08: Asked whether AzCopy documentation could recommend Storage Mover beyond a certain data size while implementation is pending.
- **#26 reply - Raj Singh, 2026-07-09:** Confirmed the documentation work was being tracked.

**#27 - Anchor: proposed 100 TB message**

- Manu Aery, 2026-06-26: Suggested softening the assertion and recommending Storage Mover above a threshold such as 1 PB because AzCopy may not know how much data the customer intends to move.
- **#28 reply - Raj Singh, 2026-06-26:** Agreed.
- **#29 reply - Mohit Kumar Garg, 2026-07-08:** Suggested 1 PB may be too high; the recommendation agent in Copilot may already use 1 TB or 10 TB, which could be reused.

**#30 - Anchor: `aka.ms/XXXX`**

- Raj Singh, 2026-06-25: Replace the placeholder with a source/target comparison link.
- **#31 reply - Manu Aery, 2026-06-26:** Suggested updating AzCopy documentation to direct users to Storage Mover.
- **#32 reply - Raj Singh, 2026-06-26:** Agreed to update it.

**#33 - Anchor: nudge eligibility**

- Anusha Subramanian, 2026-06-26: Asked whether source/target should be the only eligibility test. Storage Mover may not support other selected parameters, and AzCopy may remain simpler for a 1 TB transfer.
- **#34 reply - Raj Singh, 2026-06-26:** Suggested showing the nudge for all situations and linking to a comprehensive capability comparison.
- **#35 reply - Raj Singh, 2026-06-26:** Suggested displaying the nudge regardless of transfer size so customers see it before investing in VM setup, rather than waiting until a run exceeds 100 TB.

**#36 - Anchor: supported-scenario restriction**

- Mohit Kumar Garg, 2026-07-08: The supported-scenario list will keep changing. Use a generic nudge that points to an `aka.ms` page containing the current Storage Mover scenarios.

### Telemetry Pipeline

**#37 - Anchor: host and environment data**

- Raj Pathak, 2026-07-08: Asked Ankur Sharma to confirm feasibility because telemetry is planned as only a few metrics at job start and aggregate values at job end.

**#38 - Anchor: error-code distribution**

- Raj Pathak, 2026-07-08: Asked Ankur Sharma to confirm feasibility.

**#39 - Anchor: business metrics**

- Mohit Kumar Garg, 2026-07-08: The list is very large and should be prioritized.

**#40 - Anchor: per customer**

- Raj Pathak, 2026-07-08: Assumed that "per customer" means per subscription.

**#41 - Anchor: per-customer metrics under sampling**

- Raj Pathak, 2026-07-08: With sampling, the listed values will not be fully accurate and can only be best-effort estimates.
- **#42 reply - Raj Singh, 2026-07-09:** Asked what sampling means in this telemetry context.
- **#43 reply - Raj Pathak, 2026-07-09:** Sampling means collecting data from only a small percentage of daily AzCopy jobs, probably starting at 1%.

**#44 - Anchor: AzCopy-wide reliability metrics**

- Raj Pathak, 2026-07-08: These metrics also cannot be derived with 100% accuracy under sampling.