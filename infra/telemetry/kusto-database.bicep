// Kusto side of the AzCopy telemetry export: a database on an existing (Storage Mover) cluster,
// its schema (raw landing table, typed table, de-duplicated view), and the Event Hub data
// connection that ingests the Application Insights diagnostic-setting stream.
//
// Deployed as a module into the cluster's resource group by kusto-export.bicep. The deploying
// identity needs Contributor on the cluster and Microsoft.Resources/deployments/* on its resource
// group; it becomes the database Admin when it creates the database, which the schema script needs.

@description('Name of the existing Kusto cluster, in this resource group.')
param clusterName string

@description('Region of the cluster; its databases and data connections must use it.')
param clusterLocation string

@description('Database to create for AzCopy telemetry.')
param databaseName string

@description('Days of data kept in the hot cache.')
param hotCacheDays int

@description('Database retention in days. AppEventsDedup, the table to query, inherits it.')
param retentionDays int

@description('Retention in days for the raw landing table and the typed table that feeds the view. Must exceed the view\'s 1-day de-duplication lookback.')
@minValue(2)
param stagingRetentionDays int

@description('How long Kusto batches Event Hub messages before ingesting them, as a timespan (hh:mm:ss).')
param ingestionBatchingTime string

@description('Database admins: objects with principalId, principalType (User, Group or App) and tenantId. App principals use the application (client) ID.')
param databaseAdmins array

@description('Application (client) IDs of managed identities that may query the database.')
param viewerAppIds array

@description('Resource ID of the Event Hub that carries the diagnostic-setting stream.')
param eventHubResourceId string

@description('Event Hub consumer group reserved for this data connection.')
param consumerGroupName string

@description('Name of the Event Hub data connection. Alerts filter Kusto metrics on it.')
param dataConnectionName string

resource cluster 'Microsoft.Kusto/clusters@2023-08-15' existing = {
  name: clusterName
}

resource database 'Microsoft.Kusto/clusters/databases@2023-08-15' = {
  parent: cluster
  name: databaseName
  location: clusterLocation
  kind: 'ReadWrite'
  properties: {
    hotCachePeriod: 'P${hotCacheDays}D'
    softDeletePeriod: 'P${retentionDays}D'
  }
}

var schemaScript = replace(replace(replace(loadTextContent('kusto/schema.kql'),
  '{{RawRetention}}', '${stagingRetentionDays}d'),
  '{{TableRetention}}', '${stagingRetentionDays}d'),
  '{{BatchingTime}}', ingestionBatchingTime)

// Idempotent commands only; a changed script re-runs on the next deployment.
resource schema 'Microsoft.Kusto/clusters/databases/scripts@2023-08-15' = {
  parent: database
  name: 'azcopy-telemetry-schema'
  properties: {
    // The script is schema DDL with no secrets.
    #disable-next-line use-secure-value-for-secure-inputs
    scriptContent: schemaScript
    forceUpdateTag: uniqueString(schemaScript)
    continueOnErrors: false
  }
}

// Roles are granted with .add commands rather than principalAssignments resources: before the
// database exists, the Kusto RP answers GETs for its principal assignments with 400 instead of
// 404, which fails ARM what-if. Like incremental deployments, .add never removes a principal.
var principalPrefixes = {
  User: 'aaduser'
  Group: 'aadgroup'
  App: 'aadapp'
}
var tenantId = tenant().tenantId
var principalCommands = concat(
  map(databaseAdmins, admin => '.add database ["${databaseName}"] admins ("${principalPrefixes[admin.principalType]}=${admin.principalId};${admin.tenantId}")'),
  map(viewerAppIds, appId => '.add database ["${databaseName}"] viewers ("aadapp=${appId};${tenantId}")'))
var principalScript = join(principalCommands, '\n\n')

resource principals 'Microsoft.Kusto/clusters/databases/scripts@2023-08-15' = if (!empty(principalCommands)) {
  parent: database
  name: 'azcopy-telemetry-principals'
  properties: {
    #disable-next-line use-secure-value-for-secure-inputs
    scriptContent: principalScript
    forceUpdateTag: uniqueString(principalScript)
    continueOnErrors: false
  }
  dependsOn: [
    schema
  ]
}

resource dataConnection 'Microsoft.Kusto/clusters/databases/dataConnections@2023-08-15' = {
  parent: database
  name: dataConnectionName
  location: clusterLocation
  kind: 'EventHub'
  properties: {
    eventHubResourceId: eventHubResourceId
    consumerGroup: consumerGroupName
    tableName: 'AppEventsRaw'
    mappingRuleName: 'AppEventsRawMapping'
    dataFormat: 'MULTIJSON'
    compression: 'None'
    // The enqueue time stands in for Log Analytics' _TimeReceived, which the diagnostic stream lacks.
    eventSystemProperties: [
      'x-opt-enqueued-time'
      'x-opt-sequence-number'
      'x-opt-offset'
    ]
    // The cluster's system-assigned identity reads the Event Hub; kusto-export.bicep grants it the role.
    managedIdentityResourceId: cluster.id
    databaseRouting: 'Single'
  }
  // Waiting for both scripts also gives the cluster's new Event Hub role time to propagate; the
  // connection checks its access to the hub when it is created.
  dependsOn: [
    schema
    principals
  ]
}

output databaseResourceId string = database.id
output clusterUri string = cluster.properties.uri
