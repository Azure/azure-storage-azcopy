using './main.bicep'

// Prod instance: target of preview/early-adopter and GA builds (real, sampled
// telemetry). 730-day (2 year) retention satisfies the design requirement.
// Start the daily cap conservatively for the 1% sampling launch; raise it as
// the sampling rate / geographies are ramped.
param environmentName = 'prod'
param location = 'eastus'
param retentionInDays = 730
param dailyQuotaGb = '10'
