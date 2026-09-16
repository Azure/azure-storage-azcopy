[CmdletBinding()]
param(
    [string]$DashboardPath = (Join-Path $PSScriptRoot 'generated/azcopy-engineering-no-success.grafana.json'),
    [switch]$StructureOnly
)

$ErrorActionPreference = 'Stop'
$dashboard = Get-Content -LiteralPath $DashboardPath -Raw | ConvertFrom-Json
if ($dashboard.uid -ne 'azcopy-engineering-no-success') { throw 'Unexpected dashboard UID.' }
$panels = @($dashboard.panels)
$dataPanels = @($panels | Where-Object targets)
if ($dataPanels.Count -ne 22 -or $panels.Count -ne 28) { throw 'Expected twenty-two query panels, three headings and three definitions panels.' }
$definitionTerms = @{
    30 = @('JobID', 'InvocationID', '1 job, 2 attempts and 1 failed attempt', 'HTTP retries', 'not directly comparable')
    31 = @('Category', 'Stage', 'Code', 'initialization', 'enumeration', 'transfer', 'completion', 'completed', 'CompletedWithErrors', 'transfer-failures', 'completion / completion', 'transfer / transfer', 'transfer / completed')
    32 = @('HTTP attempt', 'Request error rates', 'Server busy (503)', 'Progress', 'Reported errors', 'Unknown / other')
}
if (@($panels | Where-Object type -eq 'text').Count -ne 3) { throw 'Expected exactly three text panels.' }
foreach ($id in $definitionTerms.Keys) {
    $definition = @($panels | Where-Object { $_.id -eq $id -and $_.type -eq 'text' })
    if ($definition.Count -ne 1 -or $definition[0].options.mode -ne 'markdown') { throw "Missing Markdown definitions panel: $id" }
    foreach ($term in $definitionTerms[$id]) {
        if (-not $definition[0].options.content.Contains($term)) { throw "Missing metric definition in panel ${id}: $term" }
    }
}
foreach ($placement in @(@{ Definition = 30; After = 12; Before = 20 }, @{ Definition = 31; After = 26; Before = 40 }, @{ Definition = 32; After = 45 })) {
    $definition = $panels | Where-Object id -eq $placement.Definition
    $previous = $panels | Where-Object id -eq $placement.After
    if ($definition.gridPos.y -ne $previous.gridPos.y + $previous.gridPos.h -or [array]::IndexOf($panels, $definition) -ne [array]::IndexOf($panels, $previous) + 1) { throw 'Definitions must follow their relevant panels.' }
    if ($placement.ContainsKey('Before')) {
        $next = $panels | Where-Object id -eq $placement.Before
        if ($next.gridPos.y -ne $definition.gridPos.y + $definition.gridPos.h) { throw 'Unexpected gap after definitions.' }
    }
}
if ($dashboard.title -notmatch 'Customer Errors.*Test' -or $dashboard.refresh -ne '') { throw 'Expected explicit test scope and no automatic raw-query refresh.' }
$errorChart = $panels | Where-Object id -eq 45
if ($errorChart.type -ne 'barchart' -or $errorChart.options.xField -ne 'ErrorCode' -or $errorChart.options.showValue -ne 'always') { throw 'Expected an error-code frequency chart with visible counts.' }
if (($panels | ConvertTo-Json -Depth 100 -Compress) -match 'WithOverflowCounter|ExecutionsWithOmissions|OmittedErrorEntries') { throw 'Obsolete error-detail coverage fields remain visible.' }
if (($dataPanels.targets.query -join "`n") -match 'AzCopy_NoSuccess') { throw 'The no-success cohort must not be used.' }
if (@($dashboard.templating.list).Count -ne 3) { throw 'Expected source, target and raw-run filters.' }
if (@($dashboard.templating.list | Where-Object { $_.type -eq 'query' -and $_.allValue -ne '[]' }).Count -ne 0) { throw 'All must remain unfiltered, not expand to stale aggregate values.' }
if ((($dashboard.templating.list | Where-Object name -eq 'RawRunScope').options.value -join ',') -ne '0,1,2') { throw 'Raw scope must use numeric values compatible with Grafana interpolation.' }
foreach ($panel in $panels) {
    if ($panel.gridPos.x -lt 0 -or $panel.gridPos.x + $panel.gridPos.w -gt 24) { throw "Invalid panel width: $($panel.id)" }
    foreach ($other in $panels | Where-Object { $_.id -gt $panel.id }) {
        if ($panel.gridPos.x -lt $other.gridPos.x + $other.gridPos.w -and $other.gridPos.x -lt $panel.gridPos.x + $panel.gridPos.w -and
            $panel.gridPos.y -lt $other.gridPos.y + $other.gridPos.h -and $other.gridPos.y -lt $panel.gridPos.y + $panel.gridPos.h) { throw "Overlapping panels: $($panel.id), $($other.id)" }
    }
}

if ($StructureOnly) {
    [pscustomobject]@{ Status = 'PASS'; QueryPanels = $dataPanels.Count; DefinitionPanels = 3; Layout = 'Non-overlapping, adjacent sections' }
    return
}

$token = az account get-access-token --resource https://api.kusto.windows.net --query accessToken -o tsv
if ($LASTEXITCODE -ne 0 -or -not $token) { throw 'Kusto authentication failed.' }
function Invoke-ReadOnlyQuery([string]$Query) {
    $response = Invoke-WebRequest -Method Post -Uri 'https://sm-prd-westeurope.westeurope.kusto.windows.net/v2/rest/query' `
        -Headers @{ Authorization = "Bearer $token" } -ContentType 'application/json' `
        -Body (@{ db = 'usage-analytics'; csl = $Query; properties = @{ Options = @{ servertimeout = '00:00:45' } } } | ConvertTo-Json -Depth 6 -Compress)
    $content = $response.Content
    if ($content -is [byte[]]) { $content = [Text.Encoding]::UTF8.GetString($content) }
    $frames = $content | ConvertFrom-Json
    if (($frames | Where-Object FrameType -eq 'DataSetCompletion').HasErrors) { throw 'Kusto reported a partial query failure.' }
    $table = $frames | Where-Object TableKind -eq 'PrimaryResult' | Select-Object -First 1
    if (-not $table) { throw 'No primary result.' }
    return ,$table
}
function Render-Query([string]$Query, [string]$Source = '[]', [string]$Target = '[]', [string]$RunScope = 'all') {
    $rendered = $Query.Replace('$__timeFrom', 'startofday(ago(90d))').Replace('$__timeTo', 'now()')
    $scopeValue = @{ all = '0'; e2e = '1'; untagged = '2' }[$RunScope]
    if ($null -eq $scopeValue) { throw 'Unknown raw run scope.' }
    $rendered = $rendered.Replace('${SourceType:json}', $Source).Replace('${TargetType:json}', $Target).Replace('${RawRunScope:json}', $scopeValue)
    if ($rendered -match '\$\{|\$__time') { throw 'Unresolved macro.' }
    return $rendered
}
$fixture = @'
let AzCopy_Usage = (startTime:datetime, endTime:datetime, customerFilter:dynamic, subscriptionFilter:dynamic, sourceFilter:dynamic, targetFilter:dynamic) {
    let IsAll = (values:dynamic) { array_length(values) == 0 or '$__all' in (values) or '__all' in (values) };
    datatable(CustomerKey:string, CustomerName:string, SubscriptionId:string, FromTo:string, JobStatus:string, Jobs:long)
    [
        'A', 'Alpha', 'sub-a', 'LocalBlob', 'Failed', 2,
        'A', 'Alpha', 'sub-a', 'LocalBlob', 'Completed', 8,
        'B', 'Beta', 'sub-b', 'LocalBlob', 'CompletedWithErrors', 3,
        'B', 'Beta', 'sub-c', 'S3Blob', 'Failed', 2,
        'C', 'Cancelled only', 'sub-d', 'LocalBlob', 'Cancelled', 1,
        'D', 'Skipped success', 'sub-e', 'LocalBlob', 'CompletedWithSkipped', 4,
        'unmapped', 'Unmapped customer', 'unmapped', 'LocalBlob', 'Failed', 9
    ]
    | extend SliceDate=startofday(ago(1d)), LastRefresh=now(), Command='copy',
        SourceType=iff(FromTo == 'S3Blob', 'S3', 'Local'), TargetType='Blob',
        IsMapped=CustomerKey != 'unmapped', IsSuccess=JobStatus in ('Completed', 'CompletedWithSkipped'),
        IsFailure=JobStatus in ('Failed', 'CompletedWithErrors', 'CompletedWithErrorsAndSkipped')
    | where IsAll(sourceFilter) or SourceType in (sourceFilter)
    | where IsAll(targetFilter) or TargetType in (targetFilter)
};
let RawFixture = datatable(Offset:timespan, Invocation:string, Job:string, Status:string, Category:string, Code:string, Schema:string, RunID:string, Source:string, Target:string)
[
    3h, 'failed-resume', 'resumed-job', 'Failed', 'authorization', 'AuthorizationFailure', '3', 'build-leg/one', 'Local', 'Blob',
    2h, 'failed-resume', 'resumed-job', 'Failed', 'authorization', 'AuthorizationFailure', '3', 'build-leg/one', 'Local', 'Blob',
    1h, 'successful-resume', 'resumed-job', 'Completed', '', '', '3', 'build-leg/two', 'Local', 'Blob',
    1h, 'partial', 'partial-job', 'CompletedWithErrors', 'transfer', 'transfer-error', '1', '', 'S3', 'Blob',
    1h, 'cancel', 'cancel-job', 'Cancelled', '', '', '3', '', 'Local', 'Blob',
    1h, 'legacy', 'legacy-job', 'Failed', 'unknown', 'unknown', '2', '', 'Local', 'Blob',
    1h, '', 'missing-id-1', 'Failed', '', '', '1', '', 'Local', 'Blob',
    1h, '', 'missing-id-2', 'Failed', '', '', '1', '', 'Local', 'Blob'
]
| project timestamp=bin(now(), 1h)-1h+iff(Offset == 3h, 0s, 1s), name='azcopy.job.finished',
    customMeasurements=case(
        Invocation == 'failed-resume', dynamic({'azcopy.storage_http_attempt_count':100, 'azcopy.network_error_attempt_count':10,
            'azcopy.server_busy_503_count':2, 'azcopy.server_busy_throughput_count':1, 'azcopy.server_busy_iops_count':1,
            'azcopy.server_busy_other_count':0, 'azcopy.percent_complete':20, 'azcopy.failure_error_other_count':3}),
        Invocation == 'successful-resume', dynamic({'azcopy.storage_http_attempt_count':900, 'azcopy.network_error_attempt_count':0,
            'azcopy.server_busy_503_count':0, 'azcopy.server_busy_throughput_count':0, 'azcopy.server_busy_iops_count':0,
            'azcopy.server_busy_other_count':0, 'azcopy.percent_complete':100, 'azcopy.failure_error_other_count':0}),
        Invocation == 'partial', dynamic({'azcopy.storage_http_attempt_count':0, 'azcopy.network_error_attempt_count':0,
            'azcopy.server_busy_503_count':0, 'azcopy.server_busy_throughput_count':0, 'azcopy.server_busy_iops_count':0,
            'azcopy.server_busy_other_count':0, 'azcopy.percent_complete':80, 'azcopy.failure_error_other_count':0}),
        Invocation == 'cancel', dynamic({'azcopy.storage_http_attempt_count':0, 'azcopy.network_error_attempt_count':0,
            'azcopy.server_busy_503_count':0, 'azcopy.server_busy_throughput_count':0, 'azcopy.server_busy_iops_count':0,
            'azcopy.server_busy_other_count':0, 'azcopy.percent_complete':25}),
        Job == 'missing-id-1', dynamic({'azcopy.percent_complete':120}), dynamic({})),
    customDimensions=bag_pack('InvocationID', Invocation, 'JobID', Job, 'JobStatus', Status, 'JobErrorCategory', Category,
        'JobErrorCode', Code, 'SchemaVersion', Schema, 'E2ETestRunID', RunID, 'SourceType', Source, 'DestType', Target,
        'FailureErrorCodes', iff(Invocation == 'failed-resume', '403:2', ''),
        'TerminalStage', 'transfer', 'Command', 'copy', 'FromTo', strcat(Source, Target), 'OSType', 'linux', 'AzCopyVersion', 'test');
'@
$rawSource = "cluster('https://adx.monitor.azure.com/subscriptions/31347be8-d066-464e-9866-7e58d85027b7/resourcegroups/azcopy-telemetry-test-rg/providers/microsoft.insights/components/azcopy-telemetry-test-ai').database('azcopy-telemetry-test-ai').customEvents"
$cases = @(
    @{ Panel = 1; Assert = 'summarize Valid=count()==1 and take_any(AffectedCustomers)==2' },
    @{ Panel = 2; Assert = 'summarize Valid=count()==1 and take_any(AffectedPercent)==50.0' },
    @{ Panel = 3; Assert = 'summarize Valid=count()==1 and take_any(FailedJobs)==7' },
    @{ Panel = 4; Assert = 'summarize Valid=count()==1 and take_any(FailureRate)==35.0' },
    @{ Panel = 7; Assert = 'summarize Valid=count()==2 and countif(Customer=="Alpha" and FailedJobs==2 and SuccessfulJobs==8)==1 and sum(FailedJobs)==7' },
    @{ Panel = 8; Assert = 'summarize Valid=count()==1 and take_any(AffectedCustomers)==2' },
    @{ Panel = 9; Assert = 'summarize Valid=count()==2 and sum(FailedJobs)==7 and countif(FromTo=="LocalBlob" and Jobs==18 and AffectedCustomers==2)==1' },
    @{ Panel = 11; Assert = 'summarize Valid=sum(Jobs)==20 and countif(JobStatus=="Cancelled" and Jobs==1)==1' },
    @{ Panel = 12; Assert = 'summarize Valid=count()==2 and sum(Jobs)==29 and countif(Attribution=="Unmapped" and FailedJobs==9)==1' },
    @{ Panel = 1; Source = '["S3"]'; Assert = 'summarize Valid=take_any(AffectedCustomers)==1' },
    @{ Panel = 2; Target = '["Absent"]'; Assert = 'summarize Valid=count()==1 and isnull(take_any(AffectedPercent))' },
    @{ Panel = 4; Target = '["Absent"]'; Assert = 'summarize Valid=count()==1 and isnull(take_any(FailureRate))' },
    @{ Panel = 1; Source = '["$__all"]'; Assert = 'summarize Valid=take_any(AffectedCustomers)==2' },
    @{ Panel = 21; Assert = 'summarize Valid=take_any(FailedAttempts)==4' },
    @{ Panel = 22; Assert = 'summarize Valid=take_any(E2EFailedAttempts)==1' },
    @{ Panel = 24; Assert = 'summarize Valid=count()==3 and sum(FailedAttempts)==4 and countif(Category=="Not reported" and FailedAttempts==2)==1' },
    @{ Panel = 25; Assert = 'summarize Valid=count()==3 and countif(Code=="AuthorizationFailure" and FailedAttempts==1)==1' },
    @{ Panel = 26; Assert = 'summarize Valid=count()==3 and sum(FailedAttempts)==4' },
    @{ Panel = 21; RunScope = 'e2e'; Assert = 'summarize Valid=take_any(FailedAttempts)==1' },
    @{ Panel = 21; RunScope = 'untagged'; Assert = 'summarize Valid=take_any(FailedAttempts)==3' },
    @{ Panel = 21; Source = '["S3"]'; Assert = 'summarize Valid=take_any(FailedAttempts)==1' },
    @{ Panel = 21; Target = '["Absent"]'; Assert = 'summarize Valid=take_any(FailedAttempts)==0' },
    @{ Panel = 21; Source = '["$__all"]'; Assert = 'summarize Valid=take_any(FailedAttempts)==4' },
    @{ Panel = 41; Assert = 'summarize Valid=count()==4 and sum(ReportingExecutions)==4 and sum(HTTPAttempts)==1000 and sum(NetworkErrors)==10 and sum(Busy503)==2 and countif(JobStatus=="Completed" and HTTPAttempts==900)==1' },
    @{ Panel = 41; Source = '["S3"]'; Assert = 'summarize Valid=count()==1 and take_any(HTTPAttempts)==0 and isnull(take_any(NetworkErrorPercent)) and isnull(take_any(ServerBusyPercent))' },
    @{ Panel = 42; Assert = 'summarize Valid=count()==1 and take_any(NetworkErrorPercent)==1.0 and take_any(ServerBusyPercent)==0.2' },
    @{ Panel = 42; RunScope = 'untagged'; Assert = 'summarize Valid=count()==1 and isnull(take_any(NetworkErrorPercent))' },
    @{ Panel = 43; Assert = 'summarize Valid=count()==1 and take_any(ReportingExecutions)==4 and take_any(Total503)==2 and take_any(Throughput)==1 and take_any(IOPS)==1 and take_any(Other)==0 and take_any(MismatchedExecutions)==0' },
    @{ Panel = 43; Target = '["Absent"]'; Assert = 'summarize Valid=count()==0' },
    @{ Panel = 44; Assert = 'summarize Valid=count()==3 and countif(JobStatus=="Failed" and Executions==3 and WithProgress==1 and AveragePercent==20)==1 and countif(JobStatus=="Cancelled" and AveragePercent==25)==1 and countif(JobStatus=="CompletedWithErrors" and AveragePercent==80)==1' },
    @{ Panel = 45; Assert = 'summarize Valid=count()==2 and sum(FailedAttempts)==4 and countif(ErrorCode=="AuthorizationFailure" and FailedAttempts==1)==1 and countif(ErrorCode=="Unknown / other" and FailedAttempts==3)==1' },
    @{ Panel = 45; Target = '["Absent"]'; Assert = 'summarize Valid=count()==0' },
    @{ Panel = 41; Mutation = ' | extend customMeasurements=dynamic({})'; Assert = 'summarize Valid=count()==0' },
    @{ Panel = 44; Mutation = ' | extend customMeasurements=dynamic({})'; Assert = 'summarize Valid=count()==3 and sum(WithProgress)==0 and countif(isnotnull(AveragePercent))==0' },
    @{ Panel = 45; Mutation = ' | extend customMeasurements=dynamic({})'; Assert = 'summarize Valid=count()==2 and sum(FailedAttempts)==4' },
    @{ Panel = 41; Mutation = ' | extend customMeasurements=bag_set_key(customMeasurements, "azcopy.network_error_attempt_count", 2000)'; Assert = 'summarize Valid=count()==0' },
    @{ Panel = 43; Mutation = ' | extend customMeasurements=bag_set_key(customMeasurements, "azcopy.server_busy_other_count", 1)'; Assert = 'summarize Valid=take_any(MismatchedExecutions)==4' },
    @{ Panel = 45; Source = '["S3"]'; Assert = 'summarize Valid=count()==1 and take_any(ErrorCode)=="Unknown / other" and sum(FailedAttempts)==1' },
    @{ Panel = 45; RunScope = 'e2e'; Assert = 'summarize Valid=count()==1 and take_any(ErrorCode)=="AuthorizationFailure" and sum(FailedAttempts)==1' },
    @{ Panel = 45; Mutation = ' | extend customDimensions=bag_set_key(customDimensions, "JobErrorCode", " Completion-Error ")'; Assert = 'summarize Valid=count()==1 and take_any(ErrorCode)=="Unknown / other" and sum(FailedAttempts)==4' },
    @{ Panel = 45; Mutation = ' | extend customDimensions=bag_set_key(customDimensions, "JobErrorCode", "http-403")'; Assert = 'summarize Valid=count()==1 and take_any(ErrorCode)=="http-403" and sum(FailedAttempts)==4' },
    @{ Panel = 45; Mutation = ' | extend customDimensions=bag_set_key(customDimensions, "JobErrorCode", "azcopy-1001")'; Assert = 'summarize Valid=count()==1 and take_any(ErrorCode)=="azcopy-1001" and sum(FailedAttempts)==4' },
    @{ Panel = 45; Mutation = ' | extend customDimensions=bag_set_key(customDimensions, "JobErrorCode", "FutureServiceError")'; Assert = 'summarize Valid=count()==1 and take_any(ErrorCode)=="FutureServiceError" and sum(FailedAttempts)==4' },
    @{ Panel = 45; Mutation = ' | extend customDimensions=bag_set_key(customDimensions, "JobErrorCode", "200")'; Assert = 'summarize Valid=count()==1 and take_any(ErrorCode)=="Unknown / other" and sum(FailedAttempts)==4' },
    @{ Panel = 45; Mutation = ' | extend customDimensions=bag_set_key(customDimensions, "JobErrorCode", "http-200")'; Assert = 'summarize Valid=count()==1 and take_any(ErrorCode)=="Unknown / other" and sum(FailedAttempts)==4' },
    @{ Panel = 45; Mutation = ' | extend customDimensions=bag_set_key(customDimensions, "JobErrorCode", "403")'; Assert = 'summarize Valid=count()==1 and take_any(ErrorCode)=="403" and sum(FailedAttempts)==4' },
    @{ Panel = 45; Mutation = ' | extend customDimensions=bag_set_key(customDimensions, "JobErrorCode", "not an error code")'; Assert = 'summarize Valid=count()==1 and take_any(ErrorCode)=="Unknown / other" and sum(FailedAttempts)==4' }
)
foreach ($case in $cases) {
    $panel = $dataPanels | Where-Object id -eq $case.Panel
    $parameters = @{ Query = $panel.targets[0].query }
    foreach ($key in @('Source', 'Target', 'RunScope')) { if ($case.ContainsKey($key)) { $parameters[$key] = $case[$key] } }
    $query = (Render-Query @parameters).Replace($rawSource, ('(RawFixture' + $case.Mutation + ')'))
    try {
        $result = Invoke-ReadOnlyQuery ($fixture + "`n" + $query + "`n| " + $case.Assert)
    } catch {
        throw "Fixture query failed: panel $($case.Panel), source $($case.Source), target $($case.Target), raw scope $($case.RunScope): $_"
    }
    if ($result.Rows[0][0] -ne $true) { throw "Regression failed: panel $($case.Panel), source $($case.Source), target $($case.Target), raw scope $($case.RunScope)" }
}
$results = foreach ($panel in $dataPanels) {
    try {
        $table = Invoke-ReadOnlyQuery (Render-Query $panel.targets[0].query)
    } catch {
        throw "Live query failed: panel $($panel.id) ($($panel.title)): $_"
    }
    if ($panel.id -in @(5, 6, 23) -and $table.Columns[0].ColumnType -ne 'real') { throw "Timestamp stat requires numeric epoch milliseconds: $($panel.id)" }
    [pscustomobject]@{ Panel = $panel.id; Title = $panel.title; Rows = @($table.Rows).Count }
}
foreach ($variable in $dashboard.templating.list | Where-Object type -eq 'query') {
    $table = Invoke-ReadOnlyQuery (Render-Query $variable.query.query)
    if ($table.Columns.ColumnName -notcontains 'text' -or $table.Columns.ColumnName -notcontains 'value') { throw "Invalid variable schema: $($variable.name)" }
}
[pscustomobject]@{ Status = 'PASS'; LivePanelQueries = $results; LiveFilterQueries = 2; SyntheticAssertions = $cases.Count }