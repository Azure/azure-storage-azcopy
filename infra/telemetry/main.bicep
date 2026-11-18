// Provisions the Application Insights backend for AzCopy client-side telemetry.
//
// Deploys, per environment, a workspace-based Application Insights component
// (the only supported type now that classic App Insights is retired) bound to a
// dedicated Log Analytics workspace. Two environments are expected:
//   - test : fed only by the E2E pipeline (synthetic telemetry)
//   - prod : fed by released AzCopy builds; deployed through Ev2 (ev2/telemetry)
//
// When kustoClusterResourceId is set, kusto-export.bicep also streams the raw AzCopy events to a
// database on that Kusto cluster and alerts on the health of the export.

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

@description('Total retention in days for AzCopy events (the AppEvents table). Days beyond retentionInDays are kept in low-cost long-term retention and are read with search jobs or restore. Prod keeps 365 days here (90 searchable) and its 2-year history in the Kusto export.')
@minValue(30)
@maxValue(4383)
param totalRetentionInDays int = 730

@description('Log Analytics workspace daily ingestion cap in GB. Decimal values are passed as strings because Bicep has no floating-point type.')
param dailyQuotaGb string = '10'

@description('Application Insights daily cap in whole GB. Keep it at least twice dailyQuotaGb so the workspace cap, which raises the daily cap alert, is the one that stops ingestion: the two caps reset at different hours, so one Application Insights day can span two workspace days.')
@minValue(1)
param appInsightsDailyCapGb int = 20

@description('Email addresses notified when the workspace daily cap stops ingestion or the monthly budget is crossed. Leave empty to see the cap alert only in the Azure portal.')
param capAlertEmails array = []

@description('Existing action group resource IDs, for example an IcM connector, also notified when the workspace daily cap stops ingestion or the monthly budget is crossed.')
param capAlertActionGroupIds array = []

@description('Monthly cost budget for this resource group, in the billing currency (USD). 0 deploys no budget. The budget needs capAlertEmails or capAlertActionGroupIds to notify.')
@minValue(0)
param monthlyBudget int = 0

@description('Start of the budget. A monthly budget rejects a start date before the current month, so this defaults to the current month and redeploys keep working.')
param budgetStartDate string = '${utcNow('yyyy')}-${utcNow('MM')}-01T00:00:00Z'

@description('Optional object ID of the E2E workload identity. When provided, grants only the read/query roles required by telemetry validation.')
param e2eQueryPrincipalId string = ''

@description('Additional tags required by the target subscription, for example policy-enforced tags.')
param extraTags object = {}

@description('Emergency kill switch. Rejects connection-string ingestion, so AzCopy stops sending telemetry; the connection string does not change.')
param disableIngestion bool = false

@description('Adds CanNotDelete locks to the workspace and component. A recreated component gets a new connection string, which released AzCopy builds cannot pick up.')
param lockResources bool = false

@description('Resource ID of the Kusto cluster that receives the raw AzCopy events. Empty deploys no export. The deploying identity needs Contributor on the cluster and Microsoft.Resources/deployments/* on its resource group.')
param kustoClusterResourceId string = ''

@description('Kusto database created for AzCopy telemetry.')
param kustoDatabaseName string = 'azcopy-telemetry'

@description('Days of Kusto data kept in the hot cache.')
@minValue(1)
param kustoHotCacheDays int = 31

@description('Days the de-duplicated Kusto view (AppEventsDedup), the table to query, keeps events.')
@minValue(1)
param kustoRetentionDays int = 90

@description('Days the Kusto raw and typed staging tables keep data. Must exceed the view\'s 1-day de-duplication lookback.')
@minValue(2)
param kustoStagingRetentionDays int = 2

@description('How long Kusto batches Event Hub messages before ingesting them (hh:mm:ss). Shorter means fresher data and more, smaller extents.')
param kustoIngestionBatchingTime string = '00:05:00'

@description('Kusto database admins: objects with principalId, principalType (User, Group or App; App uses the application ID) and tenantId.')
param kustoDatabaseAdmins array = []

@description('Partitions of the export Event Hub. Fixed at creation on the Standard tier.')
@minValue(1)
@maxValue(32)
param exportEventHubPartitionCount int = 4

@description('Throughput units the export Event Hub namespace may auto-inflate to (1 TU = 1 MB/s or 1,000 events/s in). 1 disables auto-inflate.')
@minValue(1)
@maxValue(40)
param exportEventHubMaxThroughputUnits int = 4

@description('Hours the export Event Hub keeps events, which bounds how long Kusto can fall behind without losing data.')
@minValue(1)
@maxValue(168)
param exportEventHubRetentionHours int = 24

@description('Email addresses notified by the export health alerts.')
param exportAlertEmails array = []

@description('Alert when the export Event Hub receives more than this many MB in an hour.')
@minValue(1)
param exportHourlyVolumeAlertMb int = 10000

@description('Completeness alert: minimum number of events missing from Kusto before it fires.')
@minValue(1)
param exportCompletenessMinMissingEvents int = 100

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

// Everyone notified by the daily cap alert and the monthly budget.
var alertActionGroupIds = empty(capAlertEmails) ? capAlertActionGroupIds : concat(capAlertActionGroupIds, [capAlertActionGroup.id])

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
      actionGroups: alertActionGroupIds
    }
  }
}

// Cost data reaches budgets within 8-24 hours and budgets are evaluated once a day, so this is a monthly
// spend check behind the daily cap alert, and it never stops ingestion:
// https://learn.microsoft.com/azure/cost-management-billing/costs/tutorial-acm-create-budgets
resource monthlyCostBudget 'Microsoft.Consumption/budgets@2024-08-01' = if (monthlyBudget > 0 && !empty(alertActionGroupIds)) {
  name: '${baseName}-${environmentName}-monthly-budget'
  properties: {
    category: 'Cost'
    amount: monthlyBudget
    timeGrain: 'Monthly'
    timePeriod: {
      startDate: budgetStartDate
    }
    notifications: {
      actual80Percent: {
        enabled: true
        operator: 'GreaterThan'
        threshold: 80
        thresholdType: 'Actual'
        contactEmails: []
        contactGroups: alertActionGroupIds
      }
      actual100Percent: {
        enabled: true
        operator: 'GreaterThan'
        threshold: 100
        thresholdType: 'Actual'
        contactEmails: []
        contactGroups: alertActionGroupIds
      }
      forecast100Percent: {
        enabled: true
        operator: 'GreaterThan'
        threshold: 100
        thresholdType: 'Forecasted'
        contactEmails: []
        contactGroups: alertActionGroupIds
      }
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

module kustoExport 'kusto-export.bicep' = if (!empty(kustoClusterResourceId)) {
  name: '${baseName}-${environmentName}-kusto-export'
  params: {
    environmentName: environmentName
    baseName: baseName
    location: location
    tags: commonTags
    appInsightsName: appInsights.name
    workspaceName: workspace.name
    kustoClusterResourceId: kustoClusterResourceId
    kustoDatabaseName: kustoDatabaseName
    kustoHotCacheDays: kustoHotCacheDays
    kustoRetentionDays: kustoRetentionDays
    kustoStagingRetentionDays: kustoStagingRetentionDays
    kustoIngestionBatchingTime: kustoIngestionBatchingTime
    kustoDatabaseAdmins: kustoDatabaseAdmins
    eventHubPartitionCount: exportEventHubPartitionCount
    eventHubMaxThroughputUnits: exportEventHubMaxThroughputUnits
    eventHubRetentionHours: exportEventHubRetentionHours
    alertEmails: exportAlertEmails
    hourlyVolumeAlertMb: exportHourlyVolumeAlertMb
    completenessMinMissingEvents: exportCompletenessMinMissingEvents
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

@description('Export Event Hub namespace; empty when the Kusto export is not deployed.')
output exportEventHubNamespaceName string = kustoExport.?outputs.eventHubNamespaceName ?? ''

@description('Kusto database receiving the export; empty when the Kusto export is not deployed.')
output exportKustoDatabaseResourceId string = kustoExport.?outputs.kustoDatabaseResourceId ?? ''

@description('Principal ID of the completeness alert identity, which needs Reader on the Kusto cluster; empty when the export is not deployed.')
output exportCompletenessAlertPrincipalId string = kustoExport.?outputs.completenessAlertIdentityPrincipalId ?? ''
