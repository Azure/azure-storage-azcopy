// Provisions the Application Insights backend for AzCopy client-side telemetry.
//
// Deploys, per environment, a workspace-based Application Insights component
// (the only supported type now that classic App Insights is retired) bound to a
// dedicated Log Analytics workspace. Two environments are expected:
//   - test : fed only by the E2E pipeline (synthetic telemetry)
//   - prod : fed by released AzCopy builds; deployed through Ev2 (ev2/telemetry)

@description('Deployment environment. Drives resource names and is stamped as a tag.')
@allowed([
  'test'
  'prod'
])
param environmentName string

@description('Azure region for all resources.')
param location string = resourceGroup().location

@description('Base name used to derive resource names.')
param baseName string = 'azcopy-telemetry'

@description('Data retention in days. 730 (2 years) satisfies the telemetry design requirement.')
@minValue(30)
@maxValue(730)
param retentionInDays int = 730

@description('Daily ingestion cap in GB. Decimal values are passed as strings because Bicep has no floating-point type.')
param dailyQuotaGb string = '10'

@description('Optional object ID of the E2E workload identity. When provided, grants only the read/query roles required by telemetry validation.')
param e2eQueryPrincipalId string = ''

@description('Additional tags required by the target subscription, for example policy-enforced tags.')
param extraTags object = {}

@description('Emergency kill switch. Rejects connection-string ingestion, so AzCopy stops sending telemetry; the connection string does not change.')
param disableIngestion bool = false

@description('Adds CanNotDelete locks to the workspace and component. A recreated component gets a new connection string, which released AzCopy builds cannot pick up.')
param lockResources bool = false

var workspaceName = '${baseName}-${environmentName}-law'
var appInsightsName = '${baseName}-${environmentName}-ai'
var logAnalyticsReaderRoleDefinitionId = subscriptionResourceId('Microsoft.Authorization/roleDefinitions', '73c42c96-874c-492b-b04d-ab87d138a893')
var readerRoleDefinitionId = subscriptionResourceId('Microsoft.Authorization/roleDefinitions', 'acdd72a7-3385-48ef-bd42-f606fba81ae7')

var commonTags = union(extraTags, {
  service: 'azcopy-telemetry'
  environment: environmentName
  managedBy: 'bicep'
})

resource workspace 'Microsoft.OperationalInsights/workspaces@2023-09-01' = {
  name: workspaceName
  location: location
  tags: commonTags
  properties: {
    sku: {
      name: 'PerGB2018'
    }
    retentionInDays: retentionInDays
    workspaceCapping: {
      dailyQuotaGb: json(dailyQuotaGb)
    }
    features: {
      enableLogAccessUsingOnlyResourcePermissions: true
    }
    publicNetworkAccessForIngestion: 'Enabled'
    publicNetworkAccessForQuery: 'Enabled'
  }
}

resource appInsights 'Microsoft.Insights/components@2020-02-02' = {
  name: appInsightsName
  location: location
  kind: 'web'
  tags: commonTags
  properties: {
    Application_Type: 'web'
    WorkspaceResourceId: workspace.id
    IngestionMode: 'LogAnalytics'
    // Connection-string ingestion stays open except in an emergency: the
    // connection string is embedded in the open-source AzCopy binary and is not a secret.
    DisableLocalAuth: disableIngestion
    RetentionInDays: retentionInDays
    publicNetworkAccessForIngestion: 'Enabled'
    publicNetworkAccessForQuery: 'Enabled'
  }
}

resource workspaceLock 'Microsoft.Authorization/locks@2020-05-01' = if (lockResources) {
  name: 'do-not-delete'
  scope: workspace
  properties: {
    level: 'CanNotDelete'
    notes: 'Stores telemetry from released AzCopy builds.'
  }
}

resource appInsightsLock 'Microsoft.Authorization/locks@2020-05-01' = if (lockResources) {
  name: 'do-not-delete'
  scope: appInsights
  properties: {
    level: 'CanNotDelete'
    notes: 'Released AzCopy builds embed this component\'s connection string; a recreated component gets a new one.'
  }
}

resource workspaceQueryReader 'Microsoft.Authorization/roleAssignments@2022-04-01' = if (!empty(e2eQueryPrincipalId)) {
  name: guid(workspace.id, e2eQueryPrincipalId, logAnalyticsReaderRoleDefinitionId)
  scope: workspace
  properties: {
    principalId: e2eQueryPrincipalId
    principalType: 'ServicePrincipal'
    roleDefinitionId: logAnalyticsReaderRoleDefinitionId
  }
}

resource appInsightsReader 'Microsoft.Authorization/roleAssignments@2022-04-01' = if (!empty(e2eQueryPrincipalId)) {
  name: guid(appInsights.id, e2eQueryPrincipalId, readerRoleDefinitionId)
  scope: appInsights
  properties: {
    principalId: e2eQueryPrincipalId
    principalType: 'ServicePrincipal'
    roleDefinitionId: readerRoleDefinitionId
  }
}

@description('The Application Insights connection string for runtime E2E injection or production build embedding.')
output connectionString string = appInsights.properties.ConnectionString

@description('The Application Insights instrumentation key (routing only; not a secret).')
output instrumentationKey string = appInsights.properties.InstrumentationKey

output appInsightsResourceId string = appInsights.id
output appInsightsName string = appInsights.name
output workspaceResourceId string = workspace.id
output workspaceName string = workspace.name
output workspaceCustomerId string = workspace.properties.customerId
