[CmdletBinding()]
param(
    [string]$ClusterUri = 'https://sm-prd-westeurope.westeurope.kusto.windows.net',
    [string]$Database = 'usage-analytics',
    [string]$DashboardPath = (Join-Path $PSScriptRoot 'azcopy-top-movers.dashboard.json'),
    [string]$AdxDashboardPath = (Join-Path $PSScriptRoot 'azcopy-top-movers.adx.dashboard.json')
)

$ErrorActionPreference = 'Stop'
if ($args.Count -gt 0) { throw "Unrecognized arguments: $($args -join ' ')" }

function Test-NearlyEqual([double]$Left, [double]$Right) {
    $tolerance = [Math]::Max(0.001, [Math]::Max([Math]::Abs($Left), [Math]::Abs($Right)) * 1e-10)
    return [Math]::Abs($Left - $Right) -le $tolerance
}

function Invoke-KustoQuery([string]$Query) {
    $token = az account get-access-token --resource 'https://api.kusto.windows.net' --query accessToken -o tsv
    if ($LASTEXITCODE -ne 0 -or [string]::IsNullOrWhiteSpace($token)) { throw 'Could not acquire an Azure Data Explorer token.' }
    $headers = @{ Authorization = "Bearer $token"; 'x-ms-app' = 'AzCopyTopMoversValidation' }
    $body = @{ db = $Database; csl = $Query; properties = @{ Options = @{ queryconsistency = 'strongconsistency' } } } | ConvertTo-Json -Depth 6 -Compress
    $rawResponse = Invoke-RestMethod -Method Post -Uri "$ClusterUri/v2/rest/query" -Headers $headers -ContentType 'application/json' -Body $body
    $response = @($rawResponse.GetEnumerator())
    $completion = $response | Where-Object { $_.FrameType -eq 'DataSetCompletion' } | Select-Object -First 1
    if ($null -ne $completion -and [bool]$completion.HasErrors) { throw 'Kusto reported an in-data query failure.' }
    $table = $response | Where-Object { $_.TableKind -eq 'PrimaryResult' } | Select-Object -First 1
    if ($null -eq $table) { throw 'Kusto did not return a primary result table.' }
    $rows = @()
    foreach ($row in @($table.Rows)) {
        $result = [ordered]@{}
        for ($index = 0; $index -lt $table.Columns.Count; $index++) { $result[$table.Columns[$index].ColumnName] = $row[$index] }
        $rows += [pscustomobject]$result
    }
    return $rows
}

if (-not (Test-Path -LiteralPath $DashboardPath -PathType Leaf)) { throw "Dashboard not found: $DashboardPath. Run generate-dashboard.ps1 first." }
$dashboard = Get-Content -Raw $DashboardPath | ConvertFrom-Json
if ($dashboard.panels.Count -ne 10) { throw 'Dashboard must contain two source rows, six bar gauges, one details table, and one visible field guide.' }
if (@($dashboard.panels | Where-Object { $_.type -eq 'text' -and $_.title -eq 'Field definitions' -and $_.options.mode -eq 'markdown' }).Count -ne 1) { throw 'Dashboard must contain a visible Markdown field guide.' }
if (@($dashboard.panels | Where-Object type -eq 'bargauge').Count -ne 6) { throw 'Dashboard must contain six Storage Mover-style bar gauges.' }
if (@($dashboard.panels | Where-Object type -eq 'table').Count -ne 1) { throw 'Dashboard must contain one Top 100 detail table.' }
$invalidBarGauges = @($dashboard.panels | Where-Object { $_.type -eq 'bargauge' -and ($_.options.reduceOptions.values -ne $true -or $_.options.valueMode -ne 'text' -or $_.fieldConfig.defaults.unit -ne 'decbytes') })
if ($invalidBarGauges.Count -ne 0) { throw 'Bar gauges must use all-values text mode with decimal-byte units so customer labels are visible.' }
foreach ($bar in $dashboard.panels | Where-Object type -eq 'bargauge') {
    $mappings = @($bar.transformations | Where-Object id -eq 'rowsToFields' | ForEach-Object { $_.options.mappings })
    if ($bar.fieldConfig.defaults.displayName -ne '${__field.name}' -or
        @($mappings | Where-Object { $_.fieldName -eq 'Customer' -and $_.handlerKey -eq 'field.name' }).Count -ne 1 -or
        @($mappings | Where-Object { $_.fieldName -eq 'DataTransferred' -and $_.handlerKey -eq 'field.value' }).Count -ne 1) {
        throw 'Bar gauges must retain customer names even when only one customer is returned.'
    }
}
$detailsUnit = ($dashboard.panels | Where-Object type -eq 'table').fieldConfig.overrides |
    Where-Object { $_.matcher.options -eq 'Data Transferred' } |
    ForEach-Object { $_.properties | Where-Object id -eq 'unit' | Select-Object -ExpandProperty value }
if ($detailsUnit -ne 'decbytes') { throw 'Details table and bar gauges must use the same decimal-byte units.' }
$expectedTitles = @(
    'README',
    'Metrics',
    'Top data movers (All time) (Filters: None)',
    'Top AzCopy movers (Current month) (Filters: Source, Target)',
    'Top AzCopy movers (Previous month) (Filters: Source, Target)',
    'Top AzCopy movers (Previous semester) (Filters: None)',
    'Top AzCopy movers (Current semester) (Filters: Source, Target)',
    'Top AzCopy movers (selected range) (Filters: All)',
    'Top 100 AzCopy movers (selected range) (Filters: All)',
    'Field definitions'
)
if (($expectedTitles -join '|') -ne (@($dashboard.panels.title) -join '|')) { throw 'Dashboard panel layout does not match the active Storage Mover dashboard.' }
$dashboardText = Get-Content -Raw $DashboardPath
if ($dashboardText -match 'customEvents|AzCopy_KO_UsageAggregates') { throw 'Dashboard queries raw telemetry instead of frontend helpers.' }
if ($dashboardText -notmatch 'AzCopy_TopMovers') { throw 'Dashboard does not use the AzCopy frontend helper.' }

$summary = @(Invoke-KustoQuery "AzCopyUsageAggregates1 | summarize Rows=count(), FirstSlice=min(SliceDate), LastSlice=max(SliceDate), Bytes=sumif(Count, Name == 'JobRunBytesTransferred'), Jobs=sumif(Count, Name == 'JobRunResourcesCreated'), NamedCustomerRows=countif(CustomerName !in ('Unmapped customer', '') and not(CustomerName startswith 'Customer unavailable')), Names=make_set(Name, 10)")[0]
if ([long]$summary.Rows -le 0 -or [double]$summary.Bytes -le 0 -or [double]$summary.Jobs -le 0) { throw 'Persisted usage aggregate table is empty or has no mover activity.' }
if ([long]$summary.NamedCustomerRows -le 0) { throw 'Customer enrichment produced no displayable customer names.' }
$names = @($summary.Names | Sort-Object)
$expectedNames = @('JobRunAttempts', 'JobRunBytesTransferred', 'JobRunObjectsCompleted', 'JobRunResourcesCreated') | Sort-Object
if (($names -join ',') -ne ($expectedNames -join ',')) { throw "Unexpected aggregate metric names: $($names -join ', ')" }

$duplicateKeys = @(Invoke-KustoQuery "AzCopyUsageAggregates1 | summarize Rows=count() by InstallationID, Command, FromTo, SourceType, TargetType, JobStatus, Name, SliceDate, CustomerKey, CustomerName, SubscriptionId | summarize DuplicateKeys=countif(Rows != 1)")[0]
if ([long]$duplicateKeys.DuplicateKeys -ne 0) { throw 'Persisted aggregate table has duplicate daily dimension keys.' }

$live = @(Invoke-KustoQuery "AzCopy_KO_UsageAggregates(startofday(ago(30d)), now() + 1m) | summarize Bytes=sumif(Count, Name == 'JobRunBytesTransferred'), Jobs=sumif(Count, Name == 'JobRunResourcesCreated')")[0]
$persisted = @(Invoke-KustoQuery "AzCopyUsageAggregates1 | where SliceDate >= startofday(ago(30d)) | summarize Bytes=sumif(Count, Name == 'JobRunBytesTransferred'), Jobs=sumif(Count, Name == 'JobRunResourcesCreated')")[0]
if (-not (Test-NearlyEqual ([double]$live.Bytes) ([double]$persisted.Bytes)) -or -not (Test-NearlyEqual ([double]$live.Jobs) ([double]$persisted.Jobs))) { throw 'Persisted aggregates do not reconcile with the aggregate producer over the last 30 days.' }

$all = "dynamic(['`$__all'])"
$topMovers = @(Invoke-KustoQuery "AzCopy_TopMovers(datetime(1970-01-01), now() + 1m, 10, $all, $all, $all, $all)")
if ($topMovers.Count -le 0 -or $topMovers.Count -gt 10) { throw 'All-time Top Movers returned an invalid row count.' }
for ($index = 0; $index -lt $topMovers.Count; $index++) {
    if ([long]$topMovers[$index].Rank -ne ($index + 1)) { throw 'Top Movers ranks are not sequential.' }
    if ($index -gt 0 -and [double]$topMovers[$index - 1].DataTransferred -lt [double]$topMovers[$index].DataTransferred) { throw 'Top Movers are not ordered by transferred bytes.' }
}
$top100 = @(Invoke-KustoQuery "AzCopy_TopMovers(datetime(1970-01-01), now() + 1m, 100, $all, $all, $all, $all)")
if ($top100.Count -lt $topMovers.Count -or $top100.Count -gt 100) { throw 'Top 100 helper returned an invalid row count.' }

$filterValues = @(Invoke-KustoQuery 'AzCopy_FilterValues()')
$filterNames = @($filterValues.FilterName | Sort-Object -Unique)
$expectedFilterNames = @('Command', 'Customer', 'SourceType', 'TargetType') | Sort-Object
if (($filterNames -join ',') -ne ($expectedFilterNames -join ',')) { throw "Unexpected filter categories: $($filterNames -join ', ')" }
foreach ($filterName in $expectedFilterNames) {
    if (@($filterValues | Where-Object FilterName -eq $filterName).Count -eq 0) { throw "Filter '$filterName' has no values." }
}
if (@($filterValues | Where-Object { $_.FilterName -eq 'Customer' -and ($_.FilterValue -eq 'unmapped' -or $_.FilterLabel -like 'Customer unavailable*') }).Count -gt 0) {
    throw 'Customer filter contains values that the Top Movers helper excludes.'
}

$validatedPanels = 0
foreach ($panel in $dashboard.panels | Where-Object type -in @('bargauge', 'table')) {
    foreach ($target in $panel.targets) {
        $query = $target.query.Replace('$__timeFrom', 'startofday(ago(30d))').Replace('$__timeTo', 'now()')
        $query = $query.Replace('${TopN:json}', '10')
        foreach ($variableName in @('Customer', 'Command', 'SourceType', 'TargetType')) {
            $query = $query.Replace(('${' + $variableName + ':json}'), '["$__all"]')
        }
        if ($query -match '\$\{|\$__time') { throw "Unresolved Grafana macro in panel '$($panel.title)'." }
        $null = Invoke-KustoQuery $query
        $validatedPanels++
    }
}
foreach ($variable in $dashboard.templating.list | Where-Object type -eq 'query') {
    $options = @(Invoke-KustoQuery $variable.query.query)
    if ($options.Count -eq 0 -or $options[0].PSObject.Properties.Name -notcontains 'text' -or $options[0].PSObject.Properties.Name -notcontains 'value') {
        throw "Grafana custom variable '$($variable.name)' must return text and value columns."
    }
}

$adxDashboard = Get-Content -Raw $AdxDashboardPath | ConvertFrom-Json
if ($adxDashboard.tiles.Count -ne 8 -or $adxDashboard.queries.Count -ne 11 -or $adxDashboard.parameters.Count -ne 6) {
    throw 'ADX dashboard must contain seven data tiles, one field guide, eleven queries, and six parameters.'
}
if (@($adxDashboard.tiles | Where-Object { $_.visualType -eq 'markdownCard' -and $_.title -eq 'Field definitions' }).Count -ne 1) { throw 'ADX dashboard must contain a visible Markdown field guide.' }
$adxSubstitutions = @{ _startTime = 'startofday(ago(90d))'; _endTime = 'now()'; _topN = '10'; _customer = $all; _command = $all; _source = $all; _target = $all }
foreach ($adxQuery in $adxDashboard.queries) {
    $query = $adxQuery.text
    foreach ($variableName in $adxQuery.usedVariables) { $query = $query.Replace($variableName, $adxSubstitutions[$variableName]) }
    $null = Invoke-KustoQuery $query
}

foreach ($filterName in $expectedFilterNames) {
    $selected = $filterValues | Where-Object FilterName -eq $filterName | Select-Object -First 1
    $selectedArray = ConvertTo-Json -InputObject @($selected.FilterValue) -Compress
    $filters = @{ Customer = $all; Command = $all; SourceType = $all; TargetType = $all }
    $filters[$filterName] = "dynamic($selectedArray)"
    $filtered = @(Invoke-KustoQuery "AzCopy_TopMovers(datetime(1970-01-01), now() + 1m, 100, $($filters.Customer), $($filters.Command), $($filters.SourceType), $($filters.TargetType))")
    if ($filtered.Count -eq 0) { throw "Selecting '$filterName' value '$($selected.FilterValue)' returned no customers." }
    $column = @{ Customer = 'CustomerKey'; Command = 'Commands'; SourceType = 'Sources'; TargetType = 'Targets' }[$filterName]
    foreach ($row in $filtered) {
        $values = @($row.$column)
        if ($values.Count -ne 1 -or $values[0] -ne $selected.FilterValue) { throw "Filter '$filterName' leaked another dimension value." }
    }
}
$noMatch = @(Invoke-KustoQuery "AzCopy_TopMovers(datetime(1970-01-01), now(), 10, dynamic(['__nonexistent_customer__']), $all, $all, $all)")
if ($noMatch.Count -ne 0) { throw 'Unknown customer filter must return no rows.' }
$emptyRange = @(Invoke-KustoQuery "AzCopy_TopMovers(datetime(1969-01-01), datetime(1970-01-01), 10, $all, $all, $all, $all)")
if ($emptyRange.Count -ne 0) { throw 'A range before telemetry collection must return no rows.' }

$semesters = @(Invoke-KustoQuery 'print CurrentStart=AzCopy_CurrentSemesterStart(), PreviousStart=AzCopy_PreviousSemesterStart()')[0]
$currentStart = [datetime]$semesters.CurrentStart
$previousStart = [datetime]$semesters.PreviousStart
if ($currentStart.Month -notin @(4, 10) -or $currentStart.Day -ne 1 -or $previousStart -ne $currentStart.AddMonths(-6)) { throw 'Semester helpers do not match Storage Mover April/October boundaries.' }

[pscustomobject]@{
    AggregateRows = [long]$summary.Rows
    FirstSlice = $summary.FirstSlice
    LastSlice = $summary.LastSlice
    Bytes = [double]$summary.Bytes
    Jobs = [long]$summary.Jobs
    MoversReturned = $topMovers.Count
    DetailMoversReturned = $top100.Count
    FilterValues = $filterValues.Count
    NamedCustomerRows = [long]$summary.NamedCustomerRows
    BarGaugePanels = @($dashboard.panels | Where-Object type -eq 'bargauge').Count
    ValidatedPanelQueries = $validatedPanels
    ValidatedAdxQueries = $adxDashboard.queries.Count
    ValidatedFilterSelections = $expectedFilterNames.Count
    CurrentSemesterStart = $currentStart.ToString('o')
    Status = 'PASS'
} | Format-List
