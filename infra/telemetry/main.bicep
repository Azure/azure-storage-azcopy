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

@description('Interactive (searchable) retention in days for every workspace table. Application Insights tables include 90 days at no retention charge.')
@minValue(30)
@maxValue(730)
param retentionInDays int = 90

@description('Total retention in days for AzCopy events (the AppEvents table). Days beyond retentionInDays are kept in low-cost long-term retention and are read with search jobs or restore. 730 (2 years) satisfies the telemetry design requirement.')
@minValue(30)
@maxValue(4383)
param totalRetentionInDays int = 730

@description('Log Analytics workspace daily ingestion cap in GB. Decimal values are passed as strings because Bicep has no floating-point type.')
param dailyQuotaGb string = '10'

@description('Application Insights daily cap in whole GB. Keep it at least twice dailyQuotaGb so the workspace cap, which raises the daily cap alert, is the one that stops ingestion: the two caps reset at different hours, so one Application Insights day can span two workspace days.')
@minValue(1)
param appInsightsDailyCapGb int = 20

@description('Email addresses notified when the workspace daily cap stops ingestion. Leave empty to see the alert only in the Azure portal.')
param capAlertEmails array = []

@description('Existing action group resource IDs, for example an IcM connector, also notified when the workspace daily cap stops ingestion.')
param capAlertActionGroupIds array = []

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

// AzCopy sends only custom events, which land in AppEvents.
resource appEventsTable 'Microsoft.OperationalInsights/workspaces/tables@2022-10-01' = {
  parent: workspace
  name: 'AppEvents'
  properties: {
    plan: 'Analytics'
    retentionInDays: retentionInDays
    totalRetentionInDays: totalRetentionInDays
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

resource appInsightsDailyCap 'Microsoft.Insights/components/pricingPlans@2017-10-01' = {
  parent: appInsights
  name: 'current'
  properties: {
    planType: 'Basic'
    cap: appInsightsDailyCapGb
    stopSendNotificationWhenHitCap: false
  }
}

resource capAlertActionGroup 'Microsoft.Insights/actionGroups@2023-01-01' = if (!empty(capAlertEmails)) {
  name: '${baseName}-${environmentName}-cap-alerts'
  location: 'global'
  tags: commonTags
  properties: {
    groupShortName: environmentName == 'prod' ? 'azcopytelprd' : 'azcopyteltst'
    enabled: true
    emailReceivers: [for (email, i) in capAlertEmails: {
      name: 'email-${i}'
      emailAddress: email
      useCommonAlertSchema: true
    }]
  }
}

// Query and settings follow https://learn.microsoft.com/azure/azure-monitor/logs/daily-cap#alert-when-daily-cap-is-reached
resource dailyCapAlert 'Microsoft.Insights/scheduledQueryRules@2023-12-01' = {
  name: '${baseName}-${environmentName}-daily-cap-reached'
  location: location
  tags: commonTags
  properties: {
    displayName: 'AzCopy telemetry (${environmentName}): daily data limit reached'
    description: 'The ${workspaceName} daily cap stopped ingestion; AzCopy telemetry is dropped until the daily reset.'
    severity: 2
    enabled: true
    scopes: [
      workspace.id
    ]
    evaluationFrequency: 'PT5M'
    windowSize: 'PT5M'
    criteria: {
      allOf: [
        {
          query: '_LogOperation | where Category =~ "Ingestion" | where Detail contains "OverQuota"'
          timeAggregation: 'Count'
          operator: 'GreaterThan'
          threshold: 0
          failingPeriods: {
            numberOfEvaluationPeriods: 1
            minFailingPeriodsToAlert: 1
          }
        }
      ]
    }
    autoMitigate: false
    actions: {
      actionGroups: empty(capAlertEmails) ? capAlertActionGroupIds : concat(capAlertActionGroupIds, [capAlertActionGroup.id])
    }
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
