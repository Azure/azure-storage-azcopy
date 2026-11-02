using './main.bicep'

// Prod instance, deployed only through Ev2 (ev2/telemetry). The region comes from
// the Ev2 rollout via the resource group, so location is intentionally not set.
// 730-day (2 year) retention satisfies the design requirement.
param environmentName = 'prod'
param retentionInDays = 730
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
