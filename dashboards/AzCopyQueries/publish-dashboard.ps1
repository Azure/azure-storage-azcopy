[CmdletBinding()]
param(
    [string]$GrafanaUrl = 'https://azcopy-telemetry-ankur-cbbcech2ecd9gad6.eus.grafana.azure.com',
    [string]$DatasourceName = 'AzCopy Usage Analytics',
    [string]$DatasourceUid = 'azcopy-usage-analytics',
    [string]$ClusterUri = 'https://sm-prd-westeurope.westeurope.kusto.windows.net',
    [string]$Database = 'usage-analytics',
    [string]$DashboardPath = (Join-Path $PSScriptRoot 'azcopy-top-movers.dashboard.json')
)

$ErrorActionPreference = 'Stop'
if ($args.Count -gt 0) { throw "Unrecognized arguments: $($args -join ' ')" }
if (-not (Test-Path -LiteralPath $DashboardPath -PathType Leaf)) { throw "Dashboard not found: $DashboardPath. Run generate-dashboard.ps1 first." }

$resource = 'ce34e7e5-485f-4d76-964f-b3d2b16d1e4f'
$token = az account get-access-token --resource $resource --query accessToken -o tsv
if ($LASTEXITCODE -ne 0 -or [string]::IsNullOrWhiteSpace($token)) { throw 'Could not acquire an Azure Managed Grafana token.' }
$headers = @{ Authorization = "Bearer $token" }

$desiredDatasource = [ordered]@{
    name = $DatasourceName
    uid = $DatasourceUid
    type = 'grafana-azure-data-explorer-datasource'
    access = 'proxy'
    isDefault = $false
    jsonData = [ordered]@{
        clusterUrl = $ClusterUri
        defaultDatabase = $Database
        dataConsistency = 'strongconsistency'
        queryTimeout = '30s'
        defaultEditorMode = 'raw'
        oauthPassThru = $true
        azureCredentials = [ordered]@{ authType = 'currentuser' }
    }
}

$datasourceUri = "$GrafanaUrl/api/datasources/uid/$DatasourceUid"
$existing = $null
try {
    $existing = Invoke-RestMethod -Method Get -Uri $datasourceUri -Headers $headers
} catch {
    if ($_.Exception.Response.StatusCode.value__ -ne 404) { throw }
}

if ($null -eq $existing) {
    $null = Invoke-RestMethod -Method Post -Uri "$GrafanaUrl/api/datasources" -Headers $headers -ContentType 'application/json' -Body ($desiredDatasource | ConvertTo-Json -Depth 20 -Compress)
    Write-Host "Created datasource $DatasourceUid"
} else {
    $mismatches = @()
    if ($existing.name -ne $DatasourceName) { $mismatches += "name '$($existing.name)'" }
    if ($existing.type -ne $desiredDatasource.type) { $mismatches += "type '$($existing.type)'" }
    if ($existing.jsonData.clusterUrl.TrimEnd('/') -ne $ClusterUri.TrimEnd('/')) { $mismatches += "cluster '$($existing.jsonData.clusterUrl)'" }
    if ($existing.jsonData.defaultDatabase -ne $Database) { $mismatches += "database '$($existing.jsonData.defaultDatabase)'" }
    if (-not [bool]$existing.jsonData.oauthPassThru -or $existing.jsonData.azureCredentials.authType -ne 'currentuser') { $mismatches += 'current-user OAuth disabled' }
    if ($mismatches.Count -gt 0) { throw "Datasource UID $DatasourceUid already exists with different settings: $($mismatches -join ', '). Refusing to overwrite it." }
    Write-Host "Verified datasource $DatasourceUid"
}

$dashboard = Get-Content -Raw $DashboardPath | ConvertFrom-Json
$embeddedDatasourceUids = @(@(
    $dashboard.panels | ForEach-Object {
        if ($_.datasource.uid) { $_.datasource.uid }
        $_.targets | ForEach-Object { if ($_.datasource.uid) { $_.datasource.uid } }
    }
    $dashboard.templating.list | ForEach-Object { if ($_.datasource.uid) { $_.datasource.uid } }
) | Sort-Object -Unique)
$embeddedDatabases = @(@($dashboard.panels.targets.database) | Where-Object { $_ } | Sort-Object -Unique)
if ($embeddedDatasourceUids.Count -ne 1 -or $embeddedDatasourceUids[0] -ne $DatasourceUid) {
    throw "Dashboard datasource references do not match requested UID '$DatasourceUid': $($embeddedDatasourceUids -join ', ')."
}
if ($embeddedDatabases.Count -ne 1 -or $embeddedDatabases[0] -ne $Database) {
    throw "Dashboard database references do not match requested database '$Database': $($embeddedDatabases -join ', ')."
}
$payload = [ordered]@{ dashboard = $dashboard; folderUid = ''; overwrite = $true; message = 'Publish generated AzCopy Top Movers dashboard' }
$result = Invoke-RestMethod -Method Post -Uri "$GrafanaUrl/api/dashboards/db" -Headers $headers -ContentType 'application/json' -Body ($payload | ConvertTo-Json -Depth 100 -Compress)
if ($result.status -ne 'success') { throw "Grafana did not confirm publication: $($result | ConvertTo-Json -Depth 10 -Compress)" }
Write-Host "Published $($result.url) (version $($result.version))"
