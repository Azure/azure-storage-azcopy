using './main.bicep'

// Test instance: target of the E2E pipeline only (synthetic telemetry).
// 0.1 GB (approximately 100 MB) is a last-resort safeguard against runaway
// synthetic ingestion. Normal E2E traffic should remain well below this cap.
param environmentName = 'test'
param location = 'eastus'
param retentionInDays = 90
param totalRetentionInDays = 90
param dailyQuotaGb = '0.1'
// Application Insights caps are whole GB; 1 GB is well over twice the 0.1 GB workspace cap, which applies first.
param appInsightsDailyCapGb = 1
// The 0.1 GB/day cap keeps ingestion under about $7/month.
param monthlyBudget = 50
// Hitting the cap means E2E telemetry is being dropped, which can fail telemetry validation.
// Alerts go only to sharankur while the test setup stabilizes; add azcopydev@microsoft.com once it has.
param capAlertEmails = [
  'sharankur@microsoft.com'
]
// Azure.azure-storage-azcopy pipeline workload identity.
param e2eQueryPrincipalId = '28a5f214-22ad-42fc-833f-019f71f9bf60'
// Mover=Dev is enforced by a Modify policy on the Corp subscription.
param extraTags = {
  Mover: 'Dev'
  team: 'Discovery'
}

// Raw-event export to Storage Mover's PPE Kusto cluster (XDataMove-Dev). The pipeline identity has
// Contributor on the cluster and Monitoring Contributor (for ARM deployments) on its resource group.
param kustoClusterResourceId = '/subscriptions/8e146654-89da-4a29-b7bc-ad1fa6175fec/resourceGroups/rg-sm-ur-ppe-common/providers/Microsoft.Kusto/clusters/sm-ppe-westeurope'
param kustoDatabaseAdmins = [
  {
    // sharankur@microsoft.com
    principalId: '234aee49-ff5b-4d36-9228-3c261158d363'
    principalType: 'User'
    tenantId: '72f988bf-86f1-41af-91ab-2d7cd011db47'
  }
]
// Test traffic is tiny; batch briefly so E2E events reach Kusto within a minute or two.
param kustoIngestionBatchingTime = '00:00:30'
param kustoStagingRetentionDays = 3
// One throughput unit (no auto-inflate) is ample for E2E traffic, which the 1 GB/day Application Insights cap also bounds.
param exportEventHubPartitionCount = 2
param exportEventHubMaxThroughputUnits = 1
// Alerts go only to sharankur while the export stabilizes; send them to azcopydev@microsoft.com once it has.
param exportAlertEmails = [
  'sharankur@microsoft.com'
]
param exportHourlyVolumeAlertMb = 100
// E2E runs produce tens of events an hour, so a handful missing is already a real gap.
param exportCompletenessMinMissingEvents = 5
