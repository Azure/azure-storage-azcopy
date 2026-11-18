using './main.bicep'

// Prod instance, deployed only through Ev2 (ev2/telemetry). The region comes from
// the Ev2 rollout via the resource group, so location is intentionally not set.
// AzCopy events are kept 730 days (2 years) to satisfy the design requirement: the
// first 90 days are searchable (included for Application Insights data), and days
// 91-730 are in long-term retention, which costs about a fifth as much per GB-month.
param environmentName = 'prod'
param retentionInDays = 90
param totalRetentionInDays = 730
// Expected volume is ~13.2 GB/day (5.5M events at ~2.4 KB billed each), so 100 GB/day leaves ~7.6x headroom.
param dailyQuotaGb = '100'
// At least twice the workspace cap: the caps reset at different hours, so one Application Insights
// day can span two workspace days. This keeps the workspace cap, which raises the alert, binding.
param appInsightsDailyCapGb = 200
// Expected ingestion is about $0.9k/month; the 100 GB/day cap allows about $7k.
param monthlyBudget = 5000
param capAlertEmails = [
  'sharankur@microsoft.com'
  'rajpathak@microsoft.com'
  'shnayak@microsoft.com'
  'srilanka@microsoft.com'
  'azcopydev@microsoft.com'
]
param lockResources = true
// Set to true to stop AzCopy telemetry in an emergency.
param disableIngestion = false

// Raw-event export to Storage Mover's prod Kusto cluster. Before the first Ev2 rollout with this:
// - Privacy review: the approved design covered aggregates only; a raw copy needs sign-off.
// - The Ev2 deployment identity needs Contributor on the cluster and Microsoft.Resources/deployments/*
//   on its resource group (Monitoring Contributor has it), granted by a XDataMove-Prod owner, as in PPE.
//   The cluster also needs a system-assigned identity, which reads the Event Hub (PPE's has one).
// - After the first rollout, the completeness alert identity (output exportCompletenessAlertPrincipalId)
//   needs Reader on the cluster; that alert's evaluations fail until it has it.
// TODO: confirm the cluster ID with Storage Mover. It is inferred from sm-ppe-westeurope, which sits in
// rg-sm-ur-ppe-common, and from rg-sm-ur-prd-common being the prod common resource group in XDataMove-Prod.
param kustoClusterResourceId = '/subscriptions/ccd84cb7-896e-441a-8981-8b48ac987b1c/resourceGroups/rg-sm-ur-prd-common/providers/Microsoft.Kusto/clusters/sm-prd-westeurope'
// TODO: replace with a security group before enabling, since the database holds raw per-job events.
param kustoDatabaseAdmins = [
  {
    // sharankur@microsoft.com
    principalId: '234aee49-ff5b-4d36-9228-3c261158d363'
    principalType: 'User'
    tenantId: '72f988bf-86f1-41af-91ab-2d7cd011db47'
  }
]
// Kusto is the queryable copy; history beyond 90 days stays in the workspace's 730-day retention.
param kustoRetentionDays = 90
// Expected traffic is ~17 GB/day on the hub (13.2 GB billed x 1.31), about 0.2 MB/s, which one
// throughput unit carries. The 200 GB/day Application Insights cap bounds it at ~3 MB/s, within the
// 4 partitions and 4 throughput units auto-inflate may reach.
param exportEventHubPartitionCount = 4
param exportEventHubMaxThroughputUnits = 4
// Three days covers a weekend Kusto outage; at expected volume it fits in the storage one throughput unit includes.
param exportEventHubRetentionHours = 72
// Alerts go only to sharankur while the export stabilizes; send them to azcopydev@microsoft.com once it has.
param exportAlertEmails = [
  'sharankur@microsoft.com'
]
// Normal traffic is ~0.7 GB/hour on the hub; 5 GB/hour is roughly the 100 GB/day workspace cap's pace,
// which this stream ignores.
param exportHourlyVolumeAlertMb = 5000
