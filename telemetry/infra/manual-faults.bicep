targetScope = 'resourceGroup'

param location string = resourceGroup().location

@minLength(1)
@maxLength(12)
param suffix string = 'manual'

var name = 'azcopy-telfault-${suffix}-shutdown'
// Daily caps in GB. Bicep can't set the Application Insights cap; telemetry-live.ps1 applies it.
var workspaceCapGb = '0.024'
var applicationCapGb = '0.0323'
// The tests read both caps from these tags.
var tags = {
  environment: 'test'
  purpose: 'manual-telemetry-faults'
  scenario: 'shutdown'
  managedBy: 'bicep'
  workspaceCapGb: workspaceCapGb
  applicationCapGb: applicationCapGb
}

resource workspace 'Microsoft.OperationalInsights/workspaces@2023-09-01' = {
  name: '${name}-law'
  location: location
  tags: tags
  properties: {
    sku: {
      name: 'PerGB2018'
    }
    retentionInDays: 30
    features: {
      disableLocalAuth: true
    }
    workspaceCapping: {
      dailyQuotaGb: json(workspaceCapGb)
    }
    publicNetworkAccessForIngestion: 'Enabled'
    publicNetworkAccessForQuery: 'Enabled'
  }
}

resource component 'Microsoft.Insights/components@2020-02-02' = {
  name: '${name}-ai'
  location: location
  kind: 'other'
  tags: tags
  properties: {
    Application_Type: 'other'
    WorkspaceResourceId: workspace.id
    IngestionMode: 'LogAnalytics'
    SamplingPercentage: 100
    DisableLocalAuth: false
    publicNetworkAccessForIngestion: 'Enabled'
    publicNetworkAccessForQuery: 'Enabled'
  }
}

output target object = {
  componentId: component.id
  workspaceId: workspace.id
  workspaceCapGb: workspaceCapGb
  applicationCapGb: applicationCapGb
}
