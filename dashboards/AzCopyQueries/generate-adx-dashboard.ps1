[CmdletBinding()]
param(
    [string]$OutputPath = (Join-Path $PSScriptRoot 'azcopy-top-movers.adx.dashboard.json'),
    [string]$GrafanaDashboardPath = (Join-Path $PSScriptRoot 'azcopy-top-movers.dashboard.json'),
    [string]$ClusterUri = 'https://sm-prd-westeurope.westeurope.kusto.windows.net',
    [string]$Database = 'usage-analytics'
)

$ErrorActionPreference = 'Stop'
if ($args.Count -gt 0) { throw "Unrecognized arguments: $($args -join ' ')" }

function Get-ArtifactId([int]$Index) { return 'a2c00000-0000-4000-8000-{0:d12}' -f $Index }

$datasourceId = Get-ArtifactId 1
$pageId = Get-ArtifactId 2
$queries = [System.Collections.Generic.List[object]]::new()
$tiles = [System.Collections.Generic.List[object]]::new()
$parameters = [System.Collections.Generic.List[object]]::new()
$parameters.Add([ordered]@{
    kind = 'duration'; id = (Get-ArtifactId 3); displayName = 'Time range'; description = ''
    beginVariableName = '_startTime'; endVariableName = '_endTime'
    defaultValue = @{ kind = 'dynamic'; count = 90; unit = 'days' }
    showOnPages = @{ kind = 'all' }
})
$parameters.Add([ordered]@{
    kind = 'long'; id = (Get-ArtifactId 4); displayName = 'Top N'; description = ''
    variableName = '_topN'; selectionType = 'scalar'; includeAllOption = $false
    defaultValue = @{ kind = 'value'; value = 10 }; showOnPages = @{ kind = 'all' }
    dataSource = @{ kind = 'static'; values = @(10, 25, 50, 100 | ForEach-Object { @{ value = $_; displayText = "$_" } }) }
})

$filterVariables = [ordered]@{ Customer = '_customer'; Command = '_command'; SourceType = '_source'; TargetType = '_target' }
$filterIndex = 0
foreach ($filterName in $filterVariables.Keys) {
    $queryId = Get-ArtifactId (100 + $filterIndex)
    $queries.Add([ordered]@{
        id = $queryId; dataSource = @{ kind = 'inline'; dataSourceId = $datasourceId }; usedVariables = @()
        text = "AzCopy_FilterValues() | where FilterName == '$filterName' | project FilterValue, FilterLabel | order by FilterLabel asc"
    })
    $parameters.Add([ordered]@{
        kind = 'string'; id = (Get-ArtifactId (10 + $filterIndex)); displayName = $filterName; description = ''
        variableName = $filterVariables[$filterName]; selectionType = 'array'; includeAllOption = $true; allIsNull = $false
        defaultValue = @{ kind = 'all' }; showOnPages = @{ kind = 'all' }
        dataSource = @{
            kind = 'query'; queryRef = @{ kind = 'query'; queryId = $queryId }
            columns = @{ value = 'FilterValue'; label = 'FilterLabel' }
        }
    })
    $filterIndex++
}

$grafana = Get-Content -Raw $GrafanaDashboardPath | ConvertFrom-Json
$panelIndex = 0
foreach ($panel in $grafana.panels | Where-Object type -in @('bargauge', 'table')) {
    $query = $panel.targets[0].query.Replace('$__timeFrom', '_startTime').Replace('$__timeTo', '_endTime').Replace('tolong(${TopN:json})', '_topN')
    foreach ($filterName in $filterVariables.Keys) {
        $query = $query.Replace(('dynamic(${'+ $filterName + ':json})'), $filterVariables[$filterName])
    }
    $isBar = $panel.type -eq 'bargauge'
    if ($isBar) { $query = $query.Replace('| project Customer, DataTransferred', '| project Customer, DataTransferredGB = DataTransferred / 1e9') }
    $usedVariables = @(@('_startTime', '_endTime', '_topN') + @($filterVariables.Values) | Where-Object { $query.Contains($_) })
    $queryId = Get-ArtifactId (200 + $panelIndex)
    $queries.Add([ordered]@{ id = $queryId; dataSource = @{ kind = 'inline'; dataSourceId = $datasourceId }; usedVariables = $usedVariables; text = $query })
    $visualOptions = if ($isBar) {
        [ordered]@{ xColumn = 'Customer'; yColumns = @('DataTransferredGB'); xColumnTitle = 'Customer'; yColumnTitle = 'Data transferred (GB)'; hideLegend = $true; yAxisMinimumValue = 0; selectedDataOnLoad = @{ all = $true; limit = 100 } }
    } else {
        [ordered]@{ selectedDataOnLoad = @{ all = $true; limit = 100 } }
    }
    $tiles.Add([ordered]@{
        id = (Get-ArtifactId (300 + $panelIndex)); title = $panel.title; pageId = $pageId
        queryRef = @{ kind = 'query'; queryId = $queryId }
        layout = if ($isBar) { @{ x = ($panelIndex % 3) * 4; y = [int][Math]::Floor($panelIndex / 3) * 6; width = 4; height = 6 } } else { @{ x = 0; y = 12; width = 12; height = 8 } }
        visualType = if ($isBar) { 'bar' } else { 'table' }; visualOptions = $visualOptions
    })
    $panelIndex++
}

$dashboard = [ordered]@{
    '$schema' = 'https://dataexplorer.azure.com/static/d/schema/84/dashboard.json'
    title = 'AzCopy Top Movers'; schema_version = 84
    tiles = @($tiles.ToArray()); baseQueries = @(); parameters = @($parameters.ToArray())
    dataSources = @(@{ id = $datasourceId; kind = 'manual-kusto'; name = 'AzCopy Usage Analytics'; clusterUri = $ClusterUri; database = $Database })
    pages = @(@{ name = 'Top Movers'; id = $pageId }); queries = @($queries.ToArray()); embeddedApps = @()
}
if ($tiles.Count -ne 7 -or $queries.Count -ne 11 -or $parameters.Count -ne 6) { throw 'ADX dashboard must have seven tiles, eleven queries, and six parameters.' }
if (@($queries | Where-Object { $_.text -match '\$\{|\$__time|customEvents|AzCopy_KO_UsageAggregates' }).Count -gt 0) { throw 'ADX queries contain unresolved macros or raw telemetry sources.' }
$json = $dashboard | ConvertTo-Json -Depth 100
[System.IO.File]::WriteAllText($OutputPath, $json + [Environment]::NewLine, [System.Text.UTF8Encoding]::new($false))
Write-Host "Generated $OutputPath"