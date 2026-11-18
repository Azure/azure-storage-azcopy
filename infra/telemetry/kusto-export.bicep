// Raw-event export of AzCopy telemetry to Kusto:
//   Application Insights diagnostic setting (AppEvents) -> Event Hub -> Kusto data connection
//   -> AppEventsRaw -> update policy -> AppEvents (typed) -> AppEventsDedup (materialized view).
//
// Diagnostic-setting export of AppEvents has no Azure Monitor export charge, unlike a Log Analytics
// data export rule. It also keeps flowing when the workspace daily cap stops workspace ingestion;
// only the Application Insights cap, the Event Hub throughput ceiling, and the volume alert bound it.
//
// Deployed into the telemetry resource group by main.bicep when kustoClusterResourceId is set.

@description('Deployment environment; used in resource names.')
param environmentName string

@description('Base name used to derive resource names.')
param baseName string

@description('Azure region for the Event Hub, identity and log alert.')
param location string

@description('Tags applied to every resource created in the telemetry resource group.')
param tags object

@description('Name of the Application Insights component whose AppEvents are exported.')
param appInsightsName string

@description('Name of the Log Analytics workspace behind the component; the completeness alert queries it.')
param workspaceName string

@description('Resource ID of the existing Kusto cluster that receives the events.')
param kustoClusterResourceId string

@description('Database created on the cluster for AzCopy telemetry.')
param kustoDatabaseName string

@description('Days of Kusto data kept in the hot cache.')
param kustoHotCacheDays int

@description('Days AppEventsDedup, the table to query, keeps data.')
param kustoRetentionDays int

@description('Days the raw and typed staging tables keep data.')
param kustoStagingRetentionDays int

@description('How long Kusto batches Event Hub messages before ingesting them (hh:mm:ss).')
param kustoIngestionBatchingTime string

@description('Kusto database admins: objects with principalId, principalType (User, Group or App) and tenantId.')
param kustoDatabaseAdmins array

@description('Event Hub partitions. Fixed at creation on the Standard tier.')
param eventHubPartitionCount int

@description('Throughput units the namespace may auto-inflate to (1 TU = 1 MB/s or 1,000 events/s in). 1 disables auto-inflate.')
@minValue(1)
@maxValue(40)
param eventHubMaxThroughputUnits int

@description('Hours the Event Hub retains events, which bounds how long Kusto can fall behind without data loss.')
param eventHubRetentionHours int

@description('Email addresses notified by the export alerts.')
param alertEmails array

@description('Alert when the Event Hub receives more than this many MB in an hour.')
param hourlyVolumeAlertMb int

@description('Completeness alert: minimum number of events missing from Kusto before it fires. Keeps small test volumes from alerting on a single late event.')
param completenessMinMissingEvents int

var clusterIdParts = split(kustoClusterResourceId, '/')
var clusterSubscriptionId = clusterIdParts[2]
var clusterResourceGroupName = clusterIdParts[4]
var clusterName = clusterIdParts[8]
var dataConnectionName = '${baseName}-${environmentName}-appevents'
var eventHubsDataReceiverRoleDefinitionId = subscriptionResourceId('Microsoft.Authorization/roleDefinitions', 'a638d3c7-ab3a-418d-83e6-5f17a39d4fde')
var logAnalyticsReaderRoleDefinitionId = subscriptionResourceId('Microsoft.Authorization/roleDefinitions', '73c42c96-874c-492b-b04d-ab87d138a893')
var metricCriteriaType = 'Microsoft.Azure.Monitor.SingleResourceMultipleMetricCriteria'

resource appInsights 'Microsoft.Insights/components@2020-02-02' existing = {
  name: appInsightsName
}

resource workspace 'Microsoft.OperationalInsights/workspaces@2023-09-01' existing = {
  name: workspaceName
}

resource kustoCluster 'Microsoft.Kusto/clusters@2023-08-15' existing = {
  name: clusterName
  scope: resourceGroup(clusterSubscriptionId, clusterResourceGroupName)
}

resource eventHubNamespace 'Microsoft.EventHub/namespaces@2024-01-01' = {
  name: '${baseName}-${environmentName}-export'
  location: location
  tags: tags
  sku: {
    name: 'Standard'
    tier: 'Standard'
    capacity: 1
  }
  properties: {
    isAutoInflateEnabled: eventHubMaxThroughputUnits > 1
    maximumThroughputUnits: eventHubMaxThroughputUnits > 1 ? eventHubMaxThroughputUnits : 0
    // Azure Monitor and Kusto both reach the hub through trusted-service managed identities.
    disableLocalAuth: true
    minimumTlsVersion: '1.2'
    publicNetworkAccess: 'Enabled'
  }
}

resource exportHub 'Microsoft.EventHub/namespaces/eventhubs@2024-01-01' = {
  parent: eventHubNamespace
  name: 'appevents'
  properties: {
    partitionCount: eventHubPartitionCount
    retentionDescription: {
      cleanupPolicy: 'Delete'
      retentionTimeInHours: eventHubRetentionHours
    }
  }
}

resource kustoConsumerGroup 'Microsoft.EventHub/namespaces/eventhubs/consumergroups@2024-01-01' = {
  parent: exportHub
  name: 'kusto'
}

// Diagnostic settings must name an authorization rule (Manage, Send and Listen) even though local
// auth is disabled; delivery still works because Azure Monitor sends as a trusted service.
resource diagnosticSettingsRule 'Microsoft.EventHub/namespaces/authorizationRules@2024-01-01' = {
  parent: eventHubNamespace
  name: 'azure-monitor-diagnostic-settings'
  properties: {
    rights: [
      'Listen'
      'Manage'
      'Send'
    ]
  }
}

resource appEventsExport 'Microsoft.Insights/diagnosticSettings@2021-05-01-preview' = {
  name: 'kusto-export'
  scope: appInsights
  properties: {
    eventHubAuthorizationRuleId: diagnosticSettingsRule.id
    eventHubName: exportHub.name
    logs: [
      {
        category: 'AppEvents'
        enabled: true
      }
    ]
  }
}

resource kustoReadsExportHub 'Microsoft.Authorization/roleAssignments@2022-04-01' = {
  name: guid(exportHub.id, kustoCluster.id, eventHubsDataReceiverRoleDefinitionId)
  scope: exportHub
  properties: {
    principalId: kustoCluster.identity.principalId
    principalType: 'ServicePrincipal'
    roleDefinitionId: eventHubsDataReceiverRoleDefinitionId
  }
}

// Identity of the completeness alert. Besides the grants below, it needs Reader on the Kusto cluster
// for adx() queries; that is granted outside this template by an owner of the cluster's subscription.
resource completenessAlertIdentity 'Microsoft.ManagedIdentity/userAssignedIdentities@2023-01-31' = {
  name: '${baseName}-${environmentName}-export-alert-id'
  location: location
  tags: tags
}

resource completenessAlertReadsWorkspace 'Microsoft.Authorization/roleAssignments@2022-04-01' = {
  name: guid(workspace.id, completenessAlertIdentity.id, logAnalyticsReaderRoleDefinitionId)
  scope: workspace
  properties: {
    principalId: completenessAlertIdentity.properties.principalId
    principalType: 'ServicePrincipal'
    roleDefinitionId: logAnalyticsReaderRoleDefinitionId
  }
}

module kustoDatabase 'kusto-database.bicep' = {
  name: '${baseName}-${environmentName}-kusto-database'
  scope: resourceGroup(clusterSubscriptionId, clusterResourceGroupName)
  params: {
    clusterName: clusterName
    clusterLocation: kustoCluster.location
    databaseName: kustoDatabaseName
    hotCacheDays: kustoHotCacheDays
    retentionDays: kustoRetentionDays
    stagingRetentionDays: kustoStagingRetentionDays
    ingestionBatchingTime: kustoIngestionBatchingTime
    databaseAdmins: kustoDatabaseAdmins
    viewerAppIds: [
      completenessAlertIdentity.properties.clientId
    ]
    eventHubResourceId: exportHub.id
    consumerGroupName: kustoConsumerGroup.name
    dataConnectionName: dataConnectionName
  }
  // The data connection validates the cluster's access to the hub when it is created.
  dependsOn: [
    kustoReadsExportHub
  ]
}

resource exportAlertActionGroup 'Microsoft.Insights/actionGroups@2023-01-01' = if (!empty(alertEmails)) {
  name: '${baseName}-${environmentName}-export-alerts'
  location: 'global'
  tags: tags
  properties: {
    groupShortName: environmentName == 'prod' ? 'azcpyexpprd' : 'azcpyexptst'
    enabled: true
    emailReceivers: [for (email, i) in alertEmails: {
      name: 'email-${i}'
      emailAddress: email
      useCommonAlertSchema: true
    }]
  }
}

var metricAlertActions = empty(alertEmails) ? [] : [
  {
    actionGroupId: exportAlertActionGroup.id
  }
]

resource throttledAlert 'Microsoft.Insights/metricAlerts@2018-03-01' = {
  name: '${baseName}-${environmentName}-export-throttled'
  location: 'global'
  tags: tags
  properties: {
    description: 'The export Event Hub is at its throughput-unit ceiling, so Application Insights delivery to Kusto is delayed and can drop events. Raise eventHubMaxThroughputUnits or look for a volume surge.'
    severity: 2
    enabled: true
    scopes: [
      eventHubNamespace.id
    ]
    evaluationFrequency: 'PT5M'
    windowSize: 'PT15M'
    criteria: {
      'odata.type': metricCriteriaType
      allOf: [
        {
          criterionType: 'StaticThresholdCriterion'
          name: 'throttled'
          metricNamespace: 'Microsoft.EventHub/namespaces'
          metricName: 'ThrottledRequests'
          operator: 'GreaterThan'
          threshold: 0
          timeAggregation: 'Total'
        }
      ]
    }
    autoMitigate: true
    actions: metricAlertActions
  }
}

resource serverErrorsAlert 'Microsoft.Insights/metricAlerts@2018-03-01' = {
  name: '${baseName}-${environmentName}-export-server-errors'
  location: 'global'
  tags: tags
  properties: {
    description: 'The export Event Hub returned server errors; check Event Hubs service health.'
    severity: 3
    enabled: true
    scopes: [
      eventHubNamespace.id
    ]
    evaluationFrequency: 'PT5M'
    windowSize: 'PT15M'
    criteria: {
      'odata.type': metricCriteriaType
      allOf: [
        {
          criterionType: 'StaticThresholdCriterion'
          name: 'serverErrors'
          metricNamespace: 'Microsoft.EventHub/namespaces'
          metricName: 'ServerErrors'
          operator: 'GreaterThan'
          threshold: 10
          timeAggregation: 'Total'
        }
      ]
    }
    autoMitigate: true
    actions: metricAlertActions
  }
}

resource volumeSurgeAlert 'Microsoft.Insights/metricAlerts@2018-03-01' = {
  name: '${baseName}-${environmentName}-export-volume-surge'
  location: 'global'
  tags: tags
  properties: {
    description: 'The export Event Hub received more than ${hourlyVolumeAlertMb} MB in an hour. The workspace daily cap does not stop this stream, so a runaway client reaches Kusto.'
    severity: 2
    enabled: true
    scopes: [
      eventHubNamespace.id
    ]
    evaluationFrequency: 'PT15M'
    windowSize: 'PT1H'
    criteria: {
      'odata.type': metricCriteriaType
      allOf: [
        {
          criterionType: 'StaticThresholdCriterion'
          name: 'incomingBytes'
          metricNamespace: 'Microsoft.EventHub/namespaces'
          metricName: 'IncomingBytes'
          operator: 'GreaterThan'
          threshold: hourlyVolumeAlertMb * 1000000
          timeAggregation: 'Total'
        }
      ]
    }
    autoMitigate: true
    actions: metricAlertActions
  }
}

resource eventsDroppedAlert 'Microsoft.Insights/metricAlerts@2018-03-01' = {
  name: '${baseName}-${environmentName}-export-kusto-dropped'
  location: 'global'
  tags: tags
  properties: {
    description: 'Kusto dropped events from the AzCopy export data connection (for example a mapping or update-policy failure).'
    severity: 2
    enabled: true
    scopes: [
      kustoCluster.id
    ]
    evaluationFrequency: 'PT5M'
    windowSize: 'PT15M'
    criteria: {
      'odata.type': metricCriteriaType
      allOf: [
        {
          criterionType: 'StaticThresholdCriterion'
          name: 'eventsDropped'
          metricNamespace: 'Microsoft.Kusto/clusters'
          metricName: 'EventsDropped'
          dimensions: [
            {
              name: 'ComponentName'
              operator: 'Include'
              values: [
                dataConnectionName
              ]
            }
          ]
          operator: 'GreaterThan'
          threshold: 0
          timeAggregation: 'Total'
        }
      ]
    }
    autoMitigate: true
    actions: metricAlertActions
  }
  dependsOn: [
    kustoDatabase
  ]
}

resource discoveryLatencyAlert 'Microsoft.Insights/metricAlerts@2018-03-01' = {
  name: '${baseName}-${environmentName}-export-kusto-lag'
  location: 'global'
  tags: tags
  properties: {
    description: 'Kusto is more than 15 minutes behind the export Event Hub. Events are lost if the lag exceeds the hub retention.'
    severity: 2
    enabled: true
    scopes: [
      kustoCluster.id
    ]
    evaluationFrequency: 'PT5M'
    windowSize: 'PT15M'
    criteria: {
      'odata.type': metricCriteriaType
      allOf: [
        {
          criterionType: 'StaticThresholdCriterion'
          name: 'discoveryLatency'
          metricNamespace: 'Microsoft.Kusto/clusters'
          metricName: 'DiscoveryLatency'
          dimensions: [
            {
              name: 'ComponentName'
              operator: 'Include'
              values: [
                dataConnectionName
              ]
            }
          ]
          operator: 'GreaterThan'
          threshold: 900
          timeAggregation: 'Average'
        }
      ]
    }
    autoMitigate: true
    actions: metricAlertActions
  }
  dependsOn: [
    kustoDatabase
  ]
}

resource viewAgeAlert 'Microsoft.Insights/metricAlerts@2018-03-01' = {
  name: '${baseName}-${environmentName}-export-view-age'
  location: 'global'
  tags: tags
  properties: {
    description: 'AppEventsDedup is more than an hour behind AppEvents. The age does not grow while no data arrives, so idle test periods do not fire this.'
    severity: 3
    enabled: true
    scopes: [
      kustoCluster.id
    ]
    evaluationFrequency: 'PT5M'
    windowSize: 'PT30M'
    criteria: {
      'odata.type': metricCriteriaType
      allOf: [
        {
          criterionType: 'StaticThresholdCriterion'
          name: 'viewAge'
          metricNamespace: 'Microsoft.Kusto/clusters'
          metricName: 'MaterializedViewAgeMinutes'
          dimensions: [
            {
              name: 'Database'
              operator: 'Include'
              values: [
                kustoDatabaseName
              ]
            }
            {
              name: 'MaterializedViewName'
              operator: 'Include'
              values: [
                'AppEventsDedup'
              ]
            }
          ]
          operator: 'GreaterThan'
          threshold: 60
          timeAggregation: 'Average'
        }
      ]
    }
    autoMitigate: true
    actions: metricAlertActions
  }
  dependsOn: [
    kustoDatabase
  ]
}

// Compares AzCopy events in Application Insights with events that reached Kusto, for the hour that
// ended an hour ago. Both sides filter on the client timestamp (the diagnostic stream's time equals
// TimeGenerated), so clock skew affects both equally. Kusto can hold more than the workspace on
// workspace-cap days, so only a shortfall alerts; the minimum keeps one late event in a small test
// window from firing it.
var completenessQuery = '''
let endTime = bin(now() - 1h, 5m);
let startTime = endTime - 1h;
let workspaceEvents = toscalar(
    AppEvents
    | where TimeGenerated >= startTime and TimeGenerated < endTime and Name startswith 'azcopy.'
    | distinct Name, InvocationID = tostring(Properties.InvocationID)
    | count);
let kustoEvents = toscalar(
    adx('{0}/{1}').AppEventsDedup
    | where TimeGenerated >= startTime and TimeGenerated < endTime and Name startswith 'azcopy.'
    | count);
print WorkspaceEvents = workspaceEvents, KustoEvents = kustoEvents, MissingEvents = workspaceEvents - kustoEvents
| where MissingEvents >= {2} and KustoEvents < 0.98 * WorkspaceEvents
'''

resource completenessAlert 'Microsoft.Insights/scheduledQueryRules@2023-12-01' = {
  name: '${baseName}-${environmentName}-export-completeness'
  location: location
  tags: tags
  identity: {
    type: 'UserAssigned'
    userAssignedIdentities: {
      '${completenessAlertIdentity.id}': {}
    }
  }
  properties: {
    displayName: 'AzCopy telemetry (${environmentName}): events missing from Kusto'
    description: 'Fewer than 98% of the AzCopy events Application Insights accepted (and at least ${completenessMinMissingEvents}) reached ${kustoDatabaseName}.AppEventsDedup. Check the diagnostic setting, the Event Hub, the Kusto data connection and its update policy.'
    severity: 2
    enabled: true
    scopes: [
      workspace.id
    ]
    evaluationFrequency: 'PT1H'
    windowSize: 'PT2H'
    criteria: {
      allOf: [
        {
          query: format(completenessQuery, kustoDatabase.outputs.clusterUri, kustoDatabaseName, completenessMinMissingEvents)
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
    // The identity's Reader role on the Kusto cluster is granted after the first deployment.
    skipQueryValidation: true
    autoMitigate: true
    actions: {
      actionGroups: empty(alertEmails) ? [] : [
        exportAlertActionGroup.id
      ]
    }
  }
  dependsOn: [
    completenessAlertReadsWorkspace
  ]
}

output eventHubNamespaceName string = eventHubNamespace.name
output kustoDatabaseResourceId string = kustoDatabase.outputs.databaseResourceId
output completenessAlertIdentityPrincipalId string = completenessAlertIdentity.properties.principalId
