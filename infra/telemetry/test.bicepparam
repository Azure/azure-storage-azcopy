using './main.bicep'

// Test instance: target of the E2E pipeline only (synthetic telemetry).
// 0.1 GB (approximately 100 MB) is a last-resort safeguard against runaway
// synthetic ingestion. Normal E2E traffic should remain well below this cap.
param environmentName = 'test'
param location = 'eastus'
param retentionInDays = 90
param dailyQuotaGb = '0.1'
// Azure.azure-storage-azcopy pipeline workload identity.
param e2eQueryPrincipalId = '28a5f214-22ad-42fc-833f-019f71f9bf60'
