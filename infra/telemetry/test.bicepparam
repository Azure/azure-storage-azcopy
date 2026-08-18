using './main.bicep'

// Test instance: target of the E2E pipeline only (synthetic telemetry).
// Lower daily cap since test traffic is small and bounded.
param environmentName = 'test'
param location = 'eastus'
param retentionInDays = 90
param dailyQuotaGb = 1
// Azure.azure-storage-azcopy pipeline workload identity.
param e2eQueryPrincipalId = '28a5f214-22ad-42fc-833f-019f71f9bf60'
