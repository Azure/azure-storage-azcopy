using './main.bicep'

// Prod instance, deployed only through Ev2 (ev2/telemetry). The region comes from
// the Ev2 rollout via the resource group, so location is intentionally not set.
// AzCopy events are kept 730 days (2 years) to satisfy the design requirement: the
// first 90 days are searchable (included for Application Insights data), and days
// 91-730 are in long-term retention, which costs about a fifth as much per GB-month.
param environmentName = 'prod'
param retentionInDays = 90
param totalRetentionInDays = 730
// Expected volume is ~13.2 GB/day (5.5M events at ~2.4 KB billed each), so 50 GB/day leaves ~3.8x headroom.
param dailyQuotaGb = '50'
// At least twice the workspace cap: the caps reset at different hours, so one Application Insights
// day can span two workspace days. This keeps the workspace cap, which raises the alert, binding.
param appInsightsDailyCapGb = 100
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
