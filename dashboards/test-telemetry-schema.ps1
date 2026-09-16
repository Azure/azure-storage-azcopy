[CmdletBinding()]
param(
    [switch]$RunQueries,
    [string]$Cluster = 'https://sm-prd-westeurope.westeurope.kusto.windows.net',
    [string]$Database = 'usage-analytics'
)

$ErrorActionPreference = 'Stop'
$root = Split-Path $PSScriptRoot -Parent
$checkedGates = 0

function Test-SchemaGates([string]$Text, [string]$Context) {
    foreach ($line in ($Text -split "`n")) {
        if ($line -notmatch "SchemaVersion\)?\s+!?in\s+\(([^)]+)\)") { continue }
        $versions = @([regex]::Matches($Matches[1], "'([0-9]+)'") | ForEach-Object { $_.Groups[1].Value })
        if ('1' -notin $versions -or '3' -notin $versions -or @($versions | Where-Object { $_ -notin @('1', '2', '3') }).Count) {
            throw "Incorrect schema compatibility gate: $Context"
        }
        $script:checkedGates++
    }
    if ($Text -match "SchemaVersion\)\s*==\s*'3'|SchemaVersion\s+!?in\s+\('2', '3'\)") {
        throw "Schema 1 would be excluded: $Context"
    }
}

foreach ($file in (Get-ChildItem $PSScriptRoot -Recurse -Filter '*.kql')) {
    $text = Get-Content $file.FullName -Raw
    Test-SchemaGates $text $file.FullName
    if ($text -match 'mv-expand\s+MetricName\s*=\s*bag_keys\(customMeasurements\)' -and
        $text -match 'azcopy\.(job\.(started|finished)|command\.invoked)') {
        throw "Occurrence measurement dependency remains: $($file.FullName)"
    }
    if ($text -match 'FinishMetricCount\s*<\s*50') { throw "Obsolete metric-count guard: $($file.FullName)" }
}
$artifacts = @(& git -C $root ls-files -- 'dashboards/*.json' 'dashboards/**/*.json')
if ($LASTEXITCODE) { throw 'Cannot list generated artifacts' }
foreach ($path in $artifacts) {
    $text = Get-Content (Join-Path $root $path) -Raw
    $document = $text | ConvertFrom-Json -AsHashtable
    $dashboard = if ($document.dashboard) { $document.dashboard } else { $document }
    if (-not $dashboard.tiles -and -not $dashboard.panels) { continue }
    $queries = if ($dashboard.tiles) { @($dashboard.queries.text) } else { @($dashboard.panels.targets.query) + @($dashboard.templating.list.query.query) }
    foreach ($query in $queries) {
        Test-SchemaGates $query $path
        if ($query -match 'mv-expand\s+MetricName\s*=\s*bag_keys\(customMeasurements\)' -and
            $query -match 'azcopy\.(job\.(started|finished)|command\.invoked)') { throw "Generated occurrence dependency: $path" }
        if ($query -match 'FinishMetricCount\s*<\s*50') { throw "Generated obsolete metric-count guard: $path" }
    }
}

$dataArtifacts = @(
    'azcopy-data-metrics/azcopy-data-metrics.dashboard.json',
    'azcopy-data-metrics/azcopy-data-metrics.telemetry-test.dashboard.json',
    'azcopy-usage-dashboards/generated/azcopy-data-metrics.grafana.json',
    'azcopy-usage-dashboards/generated/azcopy-data-metrics.telemetry-test.grafana.json'
)
foreach ($path in $dataArtifacts) {
    $document = Get-Content (Join-Path $PSScriptRoot $path) -Raw | ConvertFrom-Json -AsHashtable
    $dashboard = if ($document.dashboard) { $document.dashboard } else { $document }
    $isAdx = [bool]$dashboard.tiles
    $parameters = if ($isAdx) { $dashboard.parameters } else { $dashboard.templating.list }
    $source = @($parameters | Where-Object { $_.variableName -eq '_sourceEndpointKind' -or $_.name -eq 'SourceEndpointKind' })
    $dest = @($parameters | Where-Object { $_.variableName -eq '_destEndpointKind' -or $_.name -eq 'DestEndpointKind' })
    if ($source.Count -ne 1 -or $dest.Count -ne 1) { throw "Independent endpoint filters missing: $path" }
    $panels = if ($isAdx) { $dashboard.tiles } else { $dashboard.panels }
    $panel = @($panels | Where-Object title -eq 'Data by source endpoint kind')
    if ($panel.Count -ne 1) { throw "Source endpoint chart missing: $path" }
    $query = if ($isAdx) { ($dashboard.queries | Where-Object id -eq $panel[0].queryRef.queryId).text } else { $panel[0].targets[0].query }
    if ($query -notmatch "SourceEndpointKind = iif\(isempty\(SourceEndpointKindValue\), 'unknown'" -or $query -notmatch 'by SourceEndpointKind') {
        throw "Historical missing endpoint classification is not preserved: $path"
    }
    if ($isAdx) {
        foreach ($item in ($dashboard.queries | Where-Object { $_.text.Contains('_sourceEndpointKind') })) {
            if ('_sourceEndpointKind' -notin $item.usedVariables) { throw "Source parameter not declared: $path" }
        }
    } elseif ($query -match '_sourceEndpointKind' -or -not $query.Contains('${SourceEndpointKind}')) {
        throw "Grafana source filter substitution failed: $path"
    }
}
$replay = & (Join-Path $PSScriptRoot 'AzCopyQueries/test-aggregate-replay.ps1') -QueryOnly
if ($replay -notmatch "SchemaVersion\) in \('1', '3'\)" -or $replay -notmatch 'Schema 1 account names enrich customer' -or
    [regex]::Matches($replay, 'print Test=').Count -ne 19) { throw 'Aggregate schema-1 replay coverage missing' }
if ($checkedGates -lt 100) { throw "Unexpectedly few schema checks: $checkedGates" }
Write-Host "PASS: $checkedGates schema gates; four ADX/Grafana data variants have independent endpoint filters and source charts; 19-case replay generated offline."

if (-not $RunQueries) { return }
$eventSource = Get-Content (Join-Path $root 'telemetry/events.go') -Raw
$metricNames = @([regex]::Matches($eventSource, 'Name: "(azcopy\.[a-z0-9_]+)"') | ForEach-Object { $_.Groups[1].Value } | Sort-Object -Unique)
if ($metricNames.Count -ne 50) { throw 'Expected 50 finished numeric observations.' }
$measurementValues = [ordered]@{}
foreach ($name in $metricNames) { $measurementValues[$name] = 0 }
foreach ($entry in @{
    'azcopy.bytes_transferred' = 1000000000000; 'azcopy.bytes_enumerated' = 1000000000000; 'azcopy.bytes_expected' = 1000000000000
    'azcopy.objects_completed' = 1; 'azcopy.objects_scheduled' = 1; 'azcopy.transfers_completed' = 1; 'azcopy.transfers_total' = 1
    'azcopy.job_duration_seconds' = 60; 'azcopy.job_throughput_mbps' = 10; 'azcopy.transfer_phase_throughput_mbps' = 20
    'azcopy.storage_http_attempt_count' = 100; 'azcopy.percent_complete' = 100
}.GetEnumerator()) { $measurementValues[$entry.Key] = $entry.Value }
$metricJson = $measurementValues | ConvertTo-Json -Compress
$fixture = @'
let _startTime = datetime(2026-09-01T00:00:00Z);
let _endTime = datetime(2026-09-02T00:00:00Z);
let _account = '';
let CompleteMeasurements = dynamic(__MEASUREMENTS__);
let Fixture = datatable(Scenario:string, Invocation:string, Job:string, EventName:string, Command:string, Status:string, Offset:timespan)
[
    'success', 'success', 'job1', 'azcopy.job.started', 'copy', '', 1h,
    'success', 'success', 'job1', 'azcopy.job.finished', 'copy', 'Completed', 61m,
    'failed', 'failed', 'job2', 'azcopy.job.started', 'copy', '', 2h,
    'failed', 'failed', 'job2', 'azcopy.job.finished', 'copy', 'Failed', 121m,
    'resume', 'resumed', 'job2', 'azcopy.job.started', 'jobs.resume', '', 3h,
    'resume', 'resumed', 'job2', 'azcopy.job.finished', 'jobs.resume', 'Completed', 181m,
    'missing finish', 'missing', 'job3', 'azcopy.job.started', 'copy', '', 4h,
    'command', 'command', '', 'azcopy.command.invoked', 'jobs.list', '', 5h,
    'duplicate', 'duplicate', 'job4', 'azcopy.job.started', 'copy', '', 6h,
    'duplicate', 'duplicate', 'job4', 'azcopy.job.finished', 'copy', 'Completed', 361m,
    'duplicate', 'duplicate', 'job4', 'azcopy.job.finished', 'copy', 'Completed', 361m,
    'incomplete scope', 'scope', 'job5', 'azcopy.job.started', 'copy', '', 7h,
    'incomplete scope', 'scope', 'job5', 'azcopy.job.finished', 'copy', 'Completed', 421m,
    'missing required', 'invalid', 'job6', 'azcopy.job.started', 'copy', '', 8h,
    'missing required', 'invalid', 'job6', 'azcopy.job.finished', 'copy', 'Completed', 481m
]
| serialize Sequence = row_number()
| extend customMeasurements = iff(EventName == 'azcopy.job.finished', CompleteMeasurements, dynamic({}))
| extend customMeasurements = case(
    EventName != 'azcopy.job.finished', customMeasurements,
    Scenario == 'resume', bag_remove_keys(customMeasurements, dynamic(['azcopy.job_throughput_mbps', 'azcopy.transfer_phase_throughput_mbps'])),
    Scenario == 'incomplete scope', bag_remove_keys(customMeasurements, dynamic(['azcopy.containers_scanned', 'azcopy.containers_touched', 'azcopy.buckets_scanned', 'azcopy.buckets_touched'])),
    Scenario == 'missing required', bag_remove_keys(customMeasurements, dynamic(['azcopy.bytes_transferred'])),
    customMeasurements)
| extend customDimensions = bag_pack(
    'SchemaVersion', '1', 'InvocationID', Invocation, 'JobID', Job, 'InstallationID', 'installation',
    'Command', Command, 'JobStatus', Status, 'TerminalStage', 'completed', 'FromTo', 'LocalBlob',
    'SourceType', 'Local', 'DestType', 'Blob', 'DestStorageAccount', 'account',
    'SourceStorageAccount', '', 'AzCopyVersion', 'test', 'OSType', 'linux', 'InvocationContext', 'ci',
    'SummaryCounterScope', iff(Scenario == 'resume', 'job-cumulative', ''),
    'SourceScopeCountsStatus', iff(Scenario == 'incomplete scope', 'incomplete', ''),
    'JobErrorCategory', iff(Status == 'Failed', 'transfer', ''), 'JobErrorCode', iff(Status == 'Failed', 'failed', ''),
    'FailureErrorCodes', iff(Status == 'Failed', '403:1', ''), 'PerformanceAdviceCodes', 'NetworkErrors')
| project Scenario, timestamp = _startTime + Offset, name = EventName, customDimensions, customMeasurements,
    itemId = tostring(Sequence), client_CountryOrRegion = 'US';
'@.Replace('__MEASUREMENTS__', $metricJson)
$token = az account get-access-token --resource https://api.kusto.windows.net --query accessToken -o tsv
if ($LASTEXITCODE -ne 0 -or -not $token) { throw 'Kusto authentication failed.' }
function Invoke-ContractQuery([string]$Query) {
    $response = Invoke-WebRequest -Method Post -Uri ($Cluster.TrimEnd('/') + '/v1/rest/query') `
        -Headers @{ Authorization = "Bearer $token"; Accept = 'application/json' } -ContentType 'application/json' `
        -Body (@{ db = $Database; csl = $Query; properties = @{ Options = @{ servertimeout = '00:00:45'; queryconsistency = 'strongconsistency' } } } | ConvertTo-Json -Depth 6 -Compress)
    $result = $response.Content | ConvertFrom-Json -AsHashtable
    if ($result.error -or $result.Exceptions -or $result.HasErrors) { throw 'Kusto query returned an error.' }
    $contents = @($result.Tables | Where-Object { 'Ordinal' -in $_.Columns.ColumnName -and 'Kind' -in $_.Columns.ColumnName })
    if ($contents.Count -ne 1) { throw 'Expected the Kusto result table of contents.' }
    $columns = @($contents[0].Columns.ColumnName)
    $kindIndex = [array]::IndexOf($columns, 'Kind')
    $nameIndex = [array]::IndexOf($columns, 'Name')
    $ordinalIndex = [array]::IndexOf($columns, 'Ordinal')
    $primary = @($contents[0].Rows | Where-Object { $_[$kindIndex] -eq 'QueryResult' -and $_[$nameIndex] -eq 'PrimaryResult' })
    if ($primary.Count -ne 1) { throw 'Expected one primary result table.' }
    foreach ($status in $contents[0].Rows | Where-Object { $_[$kindIndex] -eq 'QueryStatus' }) {
        $statusTable = $result.Tables[[int]$status[$ordinalIndex]]
        $statusCodeIndex = [array]::IndexOf(@($statusTable.Columns.ColumnName), 'StatusCode')
        if ($statusCodeIndex -lt 0 -or @($statusTable.Rows | Where-Object { $_[$statusCodeIndex] -ne 0 }).Count) {
            throw 'Kusto reported a partial query failure.'
        }
    }
    return $result.Tables[[int]$primary[0][$ordinalIndex]]
}
function Get-FixtureQuery([string]$Relative, [string]$Filter = '') {
    $query = (Get-Content (Join-Path $PSScriptRoot $Relative) | Where-Object { $_ -notmatch '^\s*//' }) -join "`n"
    $source = if ($Filter) { "Fixture | where $Filter" } else { 'Fixture' }
    return $fixture + "`nlet customEvents = $source;`n" + $query
}
function Assert-Value($Table, [string]$Key, [string]$ValueColumn, $Expected, [string]$KeyColumn = 'Metric') {
    $keyIndex = [array]::IndexOf(@($Table.Columns.ColumnName), $KeyColumn)
    $valueIndex = [array]::IndexOf(@($Table.Columns.ColumnName), $ValueColumn)
    $rows = @($Table.Rows | Where-Object { $_[$keyIndex] -eq $Key })
    if ($keyIndex -lt 0 -or $valueIndex -lt 0 -or $rows.Count -ne 1 -or [math]::Abs([double]$rows[0][$valueIndex] - [double]$Expected) -gt 0.001) {
        throw "Unexpected synthetic result: $Key expected $Expected."
    }
}
$lifecycle = Invoke-ContractQuery (Get-FixtureQuery 'azcopy-telemetry-metrics/queries/01_lifecycle_trend.kql' "Scenario in ('success', 'failed', 'resume', 'missing finish', 'command')")
foreach ($entry in @{'azcopy.job.started'=4; 'azcopy.job.finished'=3; 'azcopy.command.invoked'=1}.GetEnumerator()) {
    $index = [array]::IndexOf(@($lifecycle.Columns.ColumnName), 'Metric')
    $valueIndex = [array]::IndexOf(@($lifecycle.Columns.ColumnName), 'Value')
    $actual = ($lifecycle.Rows | Where-Object { $_[$index] -eq $entry.Key } | ForEach-Object { $_[$valueIndex] } | Measure-Object -Sum).Sum
    if ($actual -ne $entry.Value) { throw "Lifecycle count mismatch: $($entry.Key)" }
}
$overview = Invoke-ContractQuery (Get-FixtureQuery 'azcopy-business-metrics/queries/client/01_overview_cards.kql' "Scenario in ('success', 'failed', 'resume', 'missing finish', 'command', 'duplicate')")
Assert-Value $overview 'Attempts' 'Value' 4
Assert-Value $overview 'Data (TB)' 'Value' 4
Assert-Value $overview 'Completion rate (%)' 'Value' 75
Assert-Value $overview 'Unmatched starts' 'Value' 1
$quality = Invoke-ContractQuery (Get-FixtureQuery 'azcopy-business-metrics/queries/client/08_telemetry_quality.kql')
Assert-Value $quality 'Observed attempts' 'Value' 7
Assert-Value $quality 'Accepted attempts' 'Value' 4
Assert-Value $quality 'Lifecycle suspects' 'Value' 2
Assert-Value $quality 'Incomplete finish metrics' 'Value' 1
$rejections = Invoke-ContractQuery (Get-FixtureQuery 'azcopy-business-metrics/queries/client/12_telemetry_rejections.kql')
Assert-Value $rejections 'missing' 'StartEvents' 1 'InvocationID'
Assert-Value $rejections 'duplicate' 'FinishEvents' 2 'InvocationID'
Assert-Value $rejections 'invalid' 'FinishMetricCount' 48 'InvocationID'
if ($rejections.Rows.Count -ne 3) { throw 'Valid resume or incomplete-scope events were incorrectly rejected.' }
$testedQueries = 0
foreach ($queryFile in @(Get-ChildItem (Join-Path $PSScriptRoot 'azcopy-telemetry-metrics/queries') -Filter '*.kql') +
    @(Get-ChildItem (Join-Path $PSScriptRoot 'azcopy-business-metrics/queries/client') -Filter '*.kql')) {
    $query = Get-Content $queryFile.FullName -Raw
    if ($query -notmatch '(?m)^\s*customEvents\b') { continue }
    $relative = [IO.Path]::GetRelativePath($PSScriptRoot, $queryFile.FullName)
    Invoke-ContractQuery (Get-FixtureQuery $relative) | Out-Null
    $testedQueries++
}
$serverFixtures = @'
let XAggUserAgentTelemetryMetric = () {
    datatable(TimeWindow:datetime, Tool:string, RequestCount:long, Account:string)
    [datetime(2026-09-01T01:00:00Z), 'azcopy', 100, 'account',
     datetime(2026-09-01T02:00:00Z), 'azcopy', 100, 'account',
     datetime(2026-09-01T03:00:00Z), 'azcopy', 100, 'account',
     datetime(2026-09-01T06:00:00Z), 'azcopy', 100, 'account']
};
let XStoreAccountPropertiesDaily = datatable(TimePeriod:datetime, AccountNameWithoutVersion:string, Subscription:string, BilledSubscription:string, ResourceGroup:string, IsLive:bool)
    [datetime(2026-09-01), 'account', 'subscription', 'subscription', 'fixture', true];
let InternalSubscriptionResources = datatable(subscriptionId:string, tenantId:string, properties:dynamic, deleted:bool)
    ['subscription', 'tenant', dynamic({'displayName':'Fixture subscription'}), false]
    | extend timestamp = now();
'@
foreach ($file in @('04_client_server_account_hour.kql', '06_top_customer_tenants.kql')) {
    $query = Get-FixtureQuery "azcopy-business-metrics/queries/server/$file" "Scenario in ('success', 'failed', 'resume', 'missing finish', 'command', 'duplicate')"
    $query = [regex]::Replace($query, "cluster\('[^']+'\)\.database\('[^']+'\)\.(customEvents|XStoreAccountPropertiesDaily|InternalSubscriptionResources)", '$1')
    if ($query.Contains('cluster(')) { throw "Unexpected remote input in synthetic server query: $file" }
    $result = Invoke-ContractQuery ($serverFixtures + "`n" + $query)
    if ($file -eq '04_client_server_account_hour.kql') {
        foreach ($entry in @{ Jobs = 4; Requests = 400 }.GetEnumerator()) {
            $index = [array]::IndexOf(@($result.Columns.ColumnName), $entry.Key)
            if ($index -lt 0 -or ($result.Rows | ForEach-Object { $_[$index] } | Measure-Object -Sum).Sum -ne $entry.Value) {
                throw "Server correlation count mismatch: $($entry.Key)"
            }
        }
    } else {
        Assert-Value $result 'tenant' 'Attempts' 4 'CustomerTenantId'
        Assert-Value $result 'tenant' 'DataTB' 4 'CustomerTenantId'
    }
}
Write-Host "PASS: occurrence-free lifecycle counts, deduplicated attempt totals, resume and omission rules; $testedQueries raw/client and two server KQL sources executed with synthetic events."