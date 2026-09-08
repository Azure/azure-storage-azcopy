targetScope = 'resourceGroup'

param location string = resourceGroup().location

@minLength(1)
@maxLength(12)
param suffix string = 'manual'

var prefix = 'azcopy-telfault-${suffix}'
var scenarios = [
  {
    name: 'workspace'
    workspaceCap: json('0.024')
    applicationCap: '1'
  }
  {
    name: 'application'
    workspaceCap: 1
    applicationCap: '0.0323'
  }
  {
    name: 'shutdown'
    workspaceCap: json('0.024')
    applicationCap: '0.0323'
  }
]

resource workspaces 'Microsoft.OperationalInsights/workspaces@2023-09-01' = [for scenario in scenarios: {
  name: '${prefix}-${scenario.name}-law'
  location: location
  tags: {
    environment: 'test'
    purpose: 'manual-telemetry-faults'
    scenario: scenario.name
    managedBy: 'bicep'
  }
  properties: {
    sku: {
      name: 'PerGB2018'
    }
    retentionInDays: 30
    features: {
      disableLocalAuth: true
    }
    workspaceCapping: {
      dailyQuotaGb: scenario.workspaceCap
    }
    publicNetworkAccessForIngestion: 'Enabled'
    publicNetworkAccessForQuery: 'Enabled'
  }
}]

resource components 'Microsoft.Insights/components@2020-02-02' = [for (scenario, index) in scenarios: {
  name: '${prefix}-${scenario.name}-ai'
  location: location
  kind: 'other'
  tags: {
    environment: 'test'
    purpose: 'manual-telemetry-faults'
    scenario: scenario.name
    managedBy: 'bicep'
  }
  properties: {
    Application_Type: 'other'
    WorkspaceResourceId: workspaces[index].id
    IngestionMode: 'LogAnalytics'
    SamplingPercentage: 100
    DisableLocalAuth: false
    publicNetworkAccessForIngestion: 'Enabled'
    publicNetworkAccessForQuery: 'Enabled'
  }
}]

output targets array = [for (scenario, index) in scenarios: {
  scenario: scenario.name
  componentId: components[index].id
  workspaceId: workspaces[index].id
  applicationCapGb: scenario.applicationCap
}]