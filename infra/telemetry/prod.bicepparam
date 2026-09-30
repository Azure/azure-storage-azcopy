using './main.bicep'

// Prod instance, deployed only through Ev2 (ev2/telemetry). The region comes from
// the Ev2 rollout via the resource group, so location is intentionally not set.
// 730-day (2 year) retention satisfies the design requirement.
param environmentName = 'prod'
param retentionInDays = 730
param dailyQuotaGb = '10'
