[CmdletBinding()]
param(
    [string]$OutputDirectory = (Join-Path $PSScriptRoot 'generated\suite'),
    [string]$DatasourceUid = 'azcopy-usage-analytics',
    [string]$ClusterUri = 'https://sm-prd-westeurope.westeurope.kusto.windows.net',
    [string]$Database = 'usage-analytics'
)

$ErrorActionPreference = 'Stop'
if ($args.Count -gt 0) { throw "Unrecognized arguments: $($args -join ' ')" }
$null = [System.IO.Directory]::CreateDirectory($OutputDirectory)
$datasource = @{ type = 'grafana-azure-data-explorer-datasource'; uid = $DatasourceUid }
$parameters = '_startTime, _endTime, _customer, _subscription, _source, _target'
$summary = "AzCopy_UsageSummary($parameters)"
$subscriptions = "AzCopy_CustomerSubscriptions($parameters)"
$recurring = "AzCopy_RecurringSummary($parameters)"
$noSuccess = "AzCopy_NoSuccessSummary($parameters, _status)"

function New-Panel([int]$Id, [string]$Title, [string]$Type, [string]$Query, [string]$Unit, [int]$X, [int]$Y, [int]$Width, [int]$Height) {
    return @{ Id = $Id; Title = $Title; Type = $Type; Query = $Query; Unit = $Unit; X = $X; Y = $Y; Width = $Width; Height = $Height }
}

$definitions = @(
    @{
        Uid = 'azcopy-data-metrics'; Title = 'AzCopy Data Metrics'; SourceUid = 'be3i4q07jl5vka'; SourceVersion = 161
        Description = 'Storage Mover Data layout adapted to daily AzCopy terminal telemetry. Bytes are logical payload, not wire traffic. Account SKU/region/private-network metadata is not available in the persisted source.'
        Panels = @(
            (New-Panel 24 'Metrics' 'row' '' '' 0 0 24 1),
            (New-Panel 27 'Data transferred through successful jobs' 'stat' "$summary | project SuccessfulBytes" 'decbytes' 0 1 6 10),
            (New-Panel 26 'Data transferred (selected range)' 'stat' "$summary | project BytesTransferred" 'decbytes' 6 1 6 5),
            (New-Panel 29 'Data transferred (current semester)' 'stat' "AzCopy_UsageSummary(AzCopy_CurrentSemesterStart(), now(), _customer, _subscription, _source, _target) | project BytesTransferred" 'decbytes' 12 1 6 5),
            (New-Panel 31 'Data transferred (previous semester)' 'stat' "AzCopy_UsageSummary(AzCopy_PreviousSemesterStart(), AzCopy_CurrentSemesterStart(), _customer, _subscription, _source, _target) | project BytesTransferred" 'decbytes' 18 1 6 5),
            (New-Panel 37 'Data transferred (last 7 calendar days)' 'stat' "AzCopy_UsageSummary(startofday(ago(6d)), now(), _customer, _subscription, _source, _target) | project BytesTransferred" 'decbytes' 6 6 6 5),
            (New-Panel 30 'Data transferred (current month)' 'stat' "AzCopy_UsageSummary(startofmonth(now()), now(), _customer, _subscription, _source, _target) | project BytesTransferred" 'decbytes' 12 6 6 5),
            (New-Panel 14 'Data transferred (previous month)' 'stat' "AzCopy_UsageSummary(startofmonth(now(), -1), startofmonth(now()), _customer, _subscription, _source, _target) | project BytesTransferred" 'decbytes' 18 6 6 5),
            (New-Panel 36 'Monthly data transferred' 'barchart' "AzCopy_Activity($parameters, 'month') | project Period, BytesTransferred" 'decbytes' 0 11 24 12),
            (New-Panel 25 'Data transferred by source-target pair' 'barchart' "AzCopy_Breakdown($parameters, 'topology') | project Category, BytesTransferred" 'decbytes' 0 23 24 12),
            (New-Panel 41 'Completed objects by source-target pair' 'barchart' "AzCopy_Breakdown($parameters, 'topology') | project Category, ObjectsCompleted" 'short' 0 35 24 12),
            (New-Panel 40 'Data transferred by target service' 'barchart' "AzCopy_Breakdown($parameters, 'target') | project Category, BytesTransferred" 'decbytes' 0 47 24 10),
            (New-Panel 39 'Data transferred by source service' 'barchart' "AzCopy_Breakdown($parameters, 'source') | project Category, BytesTransferred" 'decbytes' 0 57 24 10),
            (New-Panel 38 'Data transferred by customer mapping coverage' 'barchart' "AzCopy_Breakdown($parameters, 'mapping') | project Category, BytesTransferred" 'decbytes' 0 67 24 10)
        )
    },
    @{
        Uid = 'azcopy-customer-drilldown'; Title = 'AzCopy Customer Drilldown'; SourceUid = 'aedhihs9max34c'; SourceVersion = 125
        Description = 'Storage Mover customer/subscription sections adapted to enriched daily AzCopy activity. Installations are pseudonymous, not registered agents. Recent/outcome tables contain daily aggregates rather than individual job records.'
        Panels = @(
            (New-Panel 33 'Customer and subscription details' 'row' '' '' 0 0 24 1),
            (New-Panel 1 'Customer names' 'table' "$subscriptions | project Customer, CustomerKey" '' 0 1 14 6),
            (New-Panel 37 'Observed properties' 'table' "$subscriptions | project Customer, CustomerKey, SubscriptionId, Sources, Targets, Scenarios, FirstSeen, LastSeen, LastRefresh" '' 14 1 10 21),
            (New-Panel 36 'Subscriptions' 'table' "$subscriptions | project Customer, CustomerKey, SubscriptionId, BytesTransferred, Jobs, Installations" '' 0 7 14 15),
            (New-Panel 32 'Customer activity' 'row' '' '' 0 22 24 1),
            (New-Panel 26 'Total data transferred' 'stat' "$summary | project BytesTransferred" 'decbytes' 0 23 6 5),
            (New-Panel 21 'Observed installations' 'stat' "$summary | project Installations" 'short' 6 23 3 5),
            (New-Panel 22 'Logical jobs' 'stat' "$summary | project Jobs" 'short' 9 23 3 5),
            (New-Panel 38 'Successful logical jobs' 'stat' "$summary | project SuccessfulJobs" 'short' 12 23 3 5),
            (New-Panel 25 'Failed logical jobs' 'stat' "$summary | project FailedJobs" 'short' 15 23 3 5),
            (New-Panel 7 'Subscriptions' 'stat' "$summary | project Subscriptions" 'short' 18 23 6 5),
            (New-Panel 28 'Data transferred (last 7 calendar days)' 'stat' "AzCopy_UsageSummary(startofday(ago(6d)), now(), _customer, _subscription, _source, _target) | project BytesTransferred" 'decbytes' 0 28 6 5),
            (New-Panel 11 'Completed objects' 'stat' "$summary | project ObjectsCompleted" 'short' 6 28 6 5),
            (New-Panel 12 'Observed attempts' 'stat' "$summary | project Attempts" 'short' 12 28 6 5),
            (New-Panel 10 'Cancelled logical jobs' 'stat' "$summary | project CancelledJobs" 'short' 18 28 6 5),
            (New-Panel 2 'Activity (data transferred)' 'barchart' "AzCopy_Activity($parameters, 'day') | project Period, BytesTransferred" 'decbytes' 0 33 12 10),
            (New-Panel 35 'Activity by source-target pair' 'barchart' "AzCopy_Breakdown($parameters, 'topology') | project Category, BytesTransferred" 'decbytes' 12 33 12 10),
            (New-Panel 3 'Activity (completed objects)' 'barchart' "AzCopy_Activity($parameters, 'day') | project Period, ObjectsCompleted" 'short' 0 43 12 10),
            (New-Panel 5 'Activity (logical jobs)' 'barchart' "AzCopy_Activity($parameters, 'day') | project Period, Jobs" 'short' 12 43 12 10),
            (New-Panel 34 'Engineering metrics' 'row' '' '' 0 53 24 1),
            (New-Panel 6 'Failed daily job activity' 'table' "AzCopy_UsageDetails($parameters, 'failure')" '' 0 54 24 9),
            (New-Panel 24 'Successful daily job activity' 'table' "AzCopy_UsageDetails($parameters, 'success')" '' 0 63 24 9),
            (New-Panel 23 'Recent daily job activity' 'table' "AzCopy_UsageDetails($parameters, 'all')" '' 0 72 24 11)
        )
    },
    @{
        Uid = 'azcopy-recurring-trends'; Title = 'AzCopy Recurring Customer Trends'; SourceUid = 'eea4p1kr7v5dse'; SourceVersion = 21
        Description = 'Storage Mover Trends layout. Recurring means a mapped customer has terminal activity in at least two calendar months inside the selected range; not lifetime retention. Unmapped ownership is excluded.'
        Panels = @(
            (New-Panel 3 'Recurring customers' 'stat' "$recurring | project RecurringCustomers" 'short' 0 0 6 7),
            (New-Panel 1 'Total data by recurring customers' 'stat' "$recurring | project BytesTransferred" 'decbytes' 6 0 10 7),
            (New-Panel 4 'Average active months' 'stat' "$recurring | project AverageActiveMonths" 'short' 16 0 8 7),
            (New-Panel 2 'Recurring customers' 'table' "AzCopy_RecurringCustomers($parameters)" '' 0 7 24 24)
        )
    },
    @{
        Uid = 'azcopy-no-observed-success'; Title = 'AzCopy Customers with No Observed Success'; SourceUid = 'afeix0jmzcxkwd'; SourceVersion = 30
        Description = 'Mapped customers with failed logical jobs and no observed successful job in the selected time/source/target scope. Status is filtered only after cohort classification. No lifetime claim, incident join, or support-case join is made.'
        StatusFilter = $true
        Panels = @(
            (New-Panel 10 'Summary' 'row' '' '' 0 0 24 1),
            (New-Panel 7 'Customers with no observed successful job' 'stat' "$noSuccess | where JobStatus == 'All' | project Customers" 'short' 0 1 8 6),
            (New-Panel 8 'Failed logical jobs in cohort' 'stat' "$noSuccess | where JobStatus == 'All' | project FailedJobs" 'short' 8 1 8 6),
            (New-Panel 3 'Terminal failure statuses' 'table' "$noSuccess | where JobStatus != 'All' | project JobStatus, Customers, FailedJobs, BytesTransferred" '' 0 7 24 10),
            (New-Panel 11 'Customers with failed jobs and no success (all statuses)' 'table' "AzCopy_NoSuccessCustomers($parameters)" '' 0 17 24 12),
            (New-Panel 9 'Drilldown' 'row' '' '' 0 29 24 1),
            (New-Panel 13 'Failed daily job activity in cohort' 'table' "AzCopy_NoSuccessDetails($parameters, _status)" '' 0 30 24 14)
        )
    }
)

function New-StableId([int]$DashboardIndex, [int]$Index) { return 'a2c10000-0000-4000-{0:d4}-{1:d12}' -f (8000 + $DashboardIndex), $Index }

function Convert-GrafanaQuery([string]$Query) {
    $result = $Query.Replace('_startTime', '$__timeFrom').Replace('_endTime', '$__timeTo')
    foreach ($entry in $variableNames.GetEnumerator()) { $result = $result.Replace($entry.Key, ('dynamic(${'+$entry.Value+':json})')) }
    return $result
}

$variableNames = [ordered]@{ _customer = 'Customer'; _subscription = 'SubscriptionId'; _source = 'SourceType'; _target = 'TargetType'; _status = 'JobStatus' }
$links = @($definitions | ForEach-Object { @{ title = $_.Title; url = "/d/$($_.Uid)"; type = 'link'; includeVars = $true; keepTime = $true; targetBlank = $false } })
$manifest = [System.Collections.Generic.List[object]]::new()
$dashboardIndex = 0
foreach ($definition in $definitions) {
    $dashboardIndex++
    $datasourceId = New-StableId $dashboardIndex 1
    $pageId = New-StableId $dashboardIndex 2
    $adxQueries = [System.Collections.Generic.List[object]]::new()
    $adxTiles = [System.Collections.Generic.List[object]]::new()
    $adxParameters = [System.Collections.Generic.List[object]]::new()
    $grafanaVariables = [System.Collections.Generic.List[object]]::new()
    $grafanaPanels = [System.Collections.Generic.List[object]]::new()
    $adxParameters.Add(@{ id = (New-StableId $dashboardIndex 3); kind = 'duration'; displayName = 'Time range'; description = ''; beginVariableName = '_startTime'; endVariableName = '_endTime'; defaultValue = @{ kind = 'dynamic'; count = 90; unit = 'days' }; showOnPages = @{ kind = 'all' } })
    $parameterIndex = 0
    foreach ($entry in $variableNames.GetEnumerator()) {
        if ($entry.Key -eq '_status' -and -not $definition.StatusFilter) { continue }
        $parameterIndex++
        $queryId = New-StableId $dashboardIndex (100 + $parameterIndex)
        $query = "AzCopy_DashboardFilters() | where FilterName == '$($entry.Value)' | project text=FilterLabel, value=FilterValue"
        $grafanaVariables.Add(@{
            name = $entry.Value; label = $entry.Value; type = 'query'; multi = $true; includeAll = $true; refresh = 1; sort = 1; hide = 0
            datasource = $datasource; definition = $query; query = @{ query = $query; database = $Database; refId = "Variable-$($entry.Value)" }
            current = @{ selected = $true; text = @('All'); value = @('$__all') }; options = @(); skipUrlSync = $false
        })
        $adxQueries.Add(@{ id = $queryId; text = $query; usedVariables = @(); dataSource = @{ kind = 'inline'; dataSourceId = $datasourceId } })
        $adxParameters.Add(@{
            id = (New-StableId $dashboardIndex (10 + $parameterIndex)); kind = 'string'; displayName = $entry.Value; description = ''
            variableName = $entry.Key; selectionType = 'array'; includeAllOption = $true; allIsNull = $true
            defaultValue = @{ kind = 'all' }; showOnPages = @{ kind = 'all' }
            dataSource = @{ kind = 'query'; queryRef = @{ kind = 'query'; queryId = $queryId }; columns = @{ value = 'value'; label = 'text' } }
        })
    }
    foreach ($panel in $definition.Panels) {
        $grafanaPanel = [ordered]@{
            id = $panel.Id; title = $panel.Title; type = $panel.Type
            gridPos = @{ x = $panel.X; y = $panel.Y; w = $panel.Width; h = $panel.Height }
        }
        if ($panel.Type -eq 'row') {
            $grafanaPanel.collapsed = $false; $grafanaPanel.panels = @(); $grafanaPanels.Add($grafanaPanel)
            continue
        }
        $grafanaPanel.description = $definition.Description
        $grafanaPanel.datasource = $datasource
        $grafanaPanel.pluginVersion = '12.4.6'
        $grafanaPanel.targets = @(@{ refId = 'A'; queryType = 'KQL'; resultFormat = 'table'; query = (Convert-GrafanaQuery $panel.Query); database = $Database; datasource = $datasource })
        $defaults = @{ unit = $panel.Unit; color = @{ mode = 'palette-classic' }; thresholds = @{ mode = 'absolute'; steps = @(@{ color = 'green'; value = $null }) }; mappings = @() }
        $overrides = [System.Collections.Generic.List[object]]::new()
        if ($panel.Type -eq 'stat') {
            $grafanaPanel.options = @{ reduceOptions = @{ values = $false; calcs = @('lastNotNull'); fields = '' }; orientation = 'auto'; textMode = 'value'; colorMode = 'value'; graphMode = 'none'; justifyMode = 'center' }
        } elseif ($panel.Type -eq 'barchart') {
            $grafanaPanel.options = @{ orientation = 'auto'; xTickLabelRotation = -30; showValue = 'auto'; stacking = 'none'; barWidth = 0.8; groupWidth = 0.7; legend = @{ showLegend = $false }; tooltip = @{ mode = 'single'; sort = 'none' } }
        } else {
            $defaults.custom = @{ align = 'auto'; cellOptions = @{ type = 'auto' }; filterable = $true; inspect = $false }
            $grafanaPanel.options = @{ showHeader = $true; cellHeight = 'sm'; footer = @{ enablePagination = $true; show = $false }; sortBy = @() }
            $overrides.Add(@{ matcher = @{ id = 'byRegexp'; options = '.*Bytes.*' }; properties = @(@{ id = 'unit'; value = 'decbytes' }) })
            $overrides.Add(@{ matcher = @{ id = 'byRegexp'; options = '.*(Seen|Date|Refresh)$' }; properties = @(@{ id = 'unit'; value = 'dateTimeAsIso' }) })
            $overrides.Add(@{ matcher = @{ id = 'byName'; options = 'Customer' }; properties = @(@{ id = 'links'; value = @(@{ title = 'Customer drilldown'; url = '/d/azcopy-customer-drilldown?${__url_time_range}&var-Customer=${__data.fields.CustomerKey:percentencode}&var-SubscriptionId=$__all&var-SourceType=$__all&var-TargetType=$__all'; targetBlank = $false }) }) })
            $overrides.Add(@{ matcher = @{ id = 'byName'; options = 'SubscriptionId' }; properties = @(@{ id = 'links'; value = @(@{ title = 'Subscription drilldown'; url = '/d/azcopy-customer-drilldown?${__url_time_range}&var-Customer=$__all&var-SubscriptionId=${__data.fields.SubscriptionId:percentencode}&var-SourceType=$__all&var-TargetType=$__all'; targetBlank = $false }) }) })
        }
        $grafanaPanel.fieldConfig = @{ defaults = $defaults; overrides = @($overrides.ToArray()) }
        $grafanaPanels.Add($grafanaPanel)

        $queryId = New-StableId $dashboardIndex (200 + $panel.Id)
        $query = $panel.Query
        $usedVariables = @(@('_startTime', '_endTime') + @($variableNames.Keys) | Where-Object { $query.Contains($_) })
        $adxQuery = $query
        if ($query.EndsWith('| project AverageActiveMonths')) {
            $adxQuery = $query.Replace('| project AverageActiveMonths', "| project AverageActiveMonths = iff(isnull(AverageActiveMonths), 'N/A', tostring(round(AverageActiveMonths, 1)))")
        }
        if ($panel.Unit -eq 'decbytes') {
            $field = [regex]::Match($query, '\| project (\w+)$').Groups[1].Value
            if ($field) { $adxQuery = $query -replace ('\| project ' + $field + '$'), "| project DataTransferredGB = $field / 1e9" }
            else { $adxQuery = $query.Replace('project Period, BytesTransferred', 'project Period, DataTransferredGB = BytesTransferred / 1e9').Replace('project Category, BytesTransferred', 'project Category, DataTransferredGB = BytesTransferred / 1e9') }
        }
        $adxQueries.Add(@{ id = $queryId; text = $adxQuery; usedVariables = $usedVariables; dataSource = @{ kind = 'inline'; dataSourceId = $datasourceId } })
        $visualOptions = @{ selectedDataOnLoad = @{ all = $true; limit = 500 } }
        if ($panel.Type -eq 'stat') { $visualOptions.multiStat__textSize = 'small' }
        if ($panel.Type -eq 'barchart') { $visualOptions.hideLegend = $true; $visualOptions.yAxisMinimumValue = 0 }
        $adxLayout = @{ x = [Math]::Floor($panel.X / 2); y = $panel.Y; width = [Math]::Max(2, [Math]::Floor($panel.Width / 2)); height = $panel.Height }
        if ($definition.Uid -eq 'azcopy-customer-drilldown' -and $panel.Y -eq 23) {
            $summaryIndex = [array]::IndexOf(@(26, 21, 22, 38, 25, 7), $panel.Id)
            $adxLayout.x = ($summaryIndex % 3) * 4
            $adxLayout.y = 23 + [Math]::Floor($summaryIndex / 3) * 5
            $adxLayout.width = 4
        } elseif ($definition.Uid -eq 'azcopy-customer-drilldown' -and $panel.Y -ge 28) {
            $adxLayout.y += 5
        }
        $adxTiles.Add(@{
            id = (New-StableId $dashboardIndex (300 + $panel.Id)); title = $panel.Title + $(if ($panel.Unit -eq 'decbytes') { ' (GB)' } else { '' }); pageId = $pageId
            queryRef = @{ kind = 'query'; queryId = $queryId }
            layout = $adxLayout
            visualType = $(switch ($panel.Type) { 'stat' { 'card' }; 'barchart' { 'column' }; default { 'table' } }); visualOptions = $visualOptions
        })
    }
    $grafana = [ordered]@{
        id = $null; uid = $definition.Uid; title = $definition.Title; description = $definition.Description; version = 1; schemaVersion = 42
        timezone = 'browser'; fiscalYearStartMonth = 3; editable = $true; refresh = ''; tags = @('azcopy', 'telemetry', 'saved-queries'); links = $links
        time = @{ from = 'now-90d'; to = 'now' }; timepicker = @{}; annotations = @{ list = @() }; templating = @{ list = @($grafanaVariables.ToArray()) }; panels = @($grafanaPanels.ToArray())
    }
    $adx = [ordered]@{
        '$schema' = 'https://dataexplorer.azure.com/static/d/schema/84/dashboard.json'; schema_version = 84; title = $definition.Title
        tiles = @($adxTiles.ToArray()); parameters = @($adxParameters.ToArray()); queries = @($adxQueries.ToArray()); baseQueries = @(); embeddedApps = @()
        pages = @(@{ id = $pageId; name = $definition.Title.Replace('AzCopy ', '') }); dataSources = @(@{ id = $datasourceId; kind = 'manual-kusto'; name = 'AzCopy Usage Analytics'; clusterUri = $ClusterUri; database = $Database })
    }
    for ($tileIndex = 0; $tileIndex -lt $adxTiles.Count; $tileIndex++) {
        $layout = $adxTiles[$tileIndex].layout
        if ($layout.width -lt 2 -or $layout.x + $layout.width -gt 12) { throw 'ADX tile violates grid bounds.' }
        if ($adxTiles[$tileIndex].visualType -eq 'card' -and ($layout.width -lt 3 -or $layout.height -lt 3)) { throw 'ADX stat cards require at least a 3 by 3 layout.' }
        for ($otherIndex = $tileIndex + 1; $otherIndex -lt $adxTiles.Count; $otherIndex++) {
            $other = $adxTiles[$otherIndex].layout
            if ($layout.x -lt $other.x + $other.width -and $layout.x + $layout.width -gt $other.x -and
                $layout.y -lt $other.y + $other.height -and $layout.y + $layout.height -gt $other.y) { throw 'ADX tiles overlap.' }
        }
    }
    foreach ($format in @('grafana', 'adx')) {
        $artifact = if ($format -eq 'grafana') { $grafana } else { $adx }
        $path = Join-Path $OutputDirectory "$($definition.Uid).$format.json"
        [System.IO.File]::WriteAllText($path, ($artifact | ConvertTo-Json -Depth 100) + [Environment]::NewLine, [System.Text.UTF8Encoding]::new($false))
    }
    $manifest.Add(@{ uid = $definition.Uid; title = $definition.Title; sourceUid = $definition.SourceUid; sourceVersion = $definition.SourceVersion; grafanaPanels = $grafanaPanels.Count; adxTiles = $adxTiles.Count; queryFunctions = @($definition.Panels.Query | ForEach-Object { if ($_ -match '^(AzCopy_\w+)\(') { $Matches[1] } } | Sort-Object -Unique) })
    Write-Host "Generated $($definition.Title): $($grafanaPanels.Count) Grafana panels, $($adxTiles.Count) ADX tiles"
}
[System.IO.File]::WriteAllText((Join-Path $OutputDirectory 'manifest.json'), ($manifest.ToArray() | ConvertTo-Json -Depth 20) + [Environment]::NewLine, [System.Text.UTF8Encoding]::new($false))