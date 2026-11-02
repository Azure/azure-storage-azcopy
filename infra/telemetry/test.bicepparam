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
// Hitting the cap means E2E telemetry is being dropped, which can fail telemetry validation.
param capAlertEmails = [
  'sharankur@microsoft.com'
  'rajpathak@microsoft.com'
  'shnayak@microsoft.com'
  'srilanka@microsoft.com'
  'azcopydev@microsoft.com'
]
// Azure.azure-storage-azcopy pipeline workload identity.
param e2eQueryPrincipalId = '28a5f214-22ad-42fc-833f-019f71f9bf60'
// Mover=Dev is enforced by a Modify policy on the Corp subscription.
param extraTags = {
  Mover: 'Dev'
  team: 'Discovery'
}
