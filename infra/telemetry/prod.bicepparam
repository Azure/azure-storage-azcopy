using './main.bicep'

// Prod instance, deployed only through Ev2 (ev2/telemetry). The region comes from
// the Ev2 rollout via the resource group, so location is intentionally not set.
// The workspace keeps AzCopy events a year: 90 days searchable, which the Application Insights
// ingestion price includes, then long-term retention to 365 days, read with search jobs or restore
// at about a fifth of the per-GB-month price. The 2-year history the design requires lives in the
// Kusto export (kustoRetentionDays), which stores an event in about 300 bytes against ~2.4 KB here.
param environmentName = 'prod'
param retentionInDays = 90
param totalRetentionInDays = 365
// Expected volume is ~13.2 GB/day (5.5M events at ~2.4 KB billed each), so 100 GB/day leaves ~7.6x headroom.
param dailyQuotaGb = '100'
// At least twice the workspace cap: the caps reset at different hours, so one Application Insights
// day can span two workspace days. This keeps the workspace cap, which raises the alert, binding.
param appInsightsDailyCapGb = 200
// Pay-as-you-go: expected ingestion is about $0.9k/month, and the 100 GB/day cap allows about $7k
// ($2.30/GB), so the budget alerts only when spending approaches the cap's pace every day.
param monthlyBudget = 7000
// Cap and budget alerts go only to sharankur for now; add rajpathak, shnayak, srilanka and
// azcopydev@microsoft.com once the new caps and budget have settled.
param capAlertEmails = [
  'sharankur@microsoft.com'
]
param lockResources = true
// Set to true to stop AzCopy telemetry in an emergency.
param disableIngestion = false

// Raw-event export to Storage Mover's prod Kusto cluster. Before the first Ev2 rollout with this:
// - Privacy review: the approved design covered aggregates only; a raw copy needs sign-off.
// - Managed SDP deploys as the release approver, who needs Owner (JIT) on both XDataMove-AzCopy-Prod
//   and XDataMove-Prod while the rollout runs, since the Kusto module deploys into the cluster's
//   resource group.
// - After the first rollout, the completeness alert identity (output exportCompletenessAlertPrincipalId)
//   needs Reader on the cluster; that alert's evaluations fail until it has it.
// - After the first rollout, copy the history into Kusto with kusto/backfill.ps1 while the workspace can
//   still query it (90 days; events from 2026-10-20 leave on about 2027-01-18).
// Confirmed in the AME portal (2026-11-18): the cluster's system-assigned identity, which reads the
// Event Hub, is enabled (principal 95233fe0-b157-4f3f-a65c-246d75078ab3).
param kustoClusterResourceId = '/subscriptions/ccd84cb7-896e-441a-8981-8b48ac987b1c/resourceGroups/rg-sm-ur-prd-common/providers/Microsoft.Kusto/clusters/sm-prd-westeurope'
// TODO: replace with a security group of the release approvers: the database holds raw per-job events,
// and Kusto schema scripts need the deploying approver to be an admin. The approver whose rollout
// creates the database becomes an admin automatically, and can then run backfill.ps1.
param kustoDatabaseAdmins = [
  {
    // sharankur@microsoft.com
    principalId: '234aee49-ff5b-4d36-9228-3c261158d363'
    principalType: 'User'
    tenantId: '72f988bf-86f1-41af-91ab-2d7cd011db47'
  }
]
// Kusto keeps the 2-year history: about 1.2 TB compressed at expected volume, about 9 TB at the
// 100 GB/day cap's pace. Seven days stay in the cluster's SSD cache; older data is read from storage.
param kustoRetentionDays = 730
param kustoHotCacheDays = 7
// Expected traffic is ~17 GB/day on the hub (13.2 GB billed x 1.31), about 0.2 MB/s, which one
// throughput unit carries. The diagnostic stream ignores the workspace cap, so only the 200 GB/day
// Application Insights cap bounds it: ~3 MB/s on average, with peaks perhaps three times that.
// A Standard partition takes about 1 MB/s and the count can't change after creation, so use the
// maximum (no extra cost). Auto-inflate never scales back down, so each throughput unit it reaches
// (about $22/month) stays until lowered by hand.
param exportEventHubPartitionCount = 32
param exportEventHubMaxThroughputUnits = 10
// Three days covers a weekend Kusto outage. At expected volume it fits in the 84 GB of storage each
// throughput unit includes; at the cap's pace the excess costs a few dollars a month.
param exportEventHubRetentionHours = 72
// Alerts go only to sharankur while the export stabilizes; send them to azcopydev@microsoft.com once it has.
param exportAlertEmails = [
  'sharankur@microsoft.com'
]
// Normal traffic is ~0.7 GB/hour on the hub; 5 GB/hour is roughly the 100 GB/day workspace cap's pace,
// which this stream ignores.
param exportHourlyVolumeAlertMb = 5000
