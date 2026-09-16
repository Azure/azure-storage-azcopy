[CmdletBinding()]
param(
    [string]$ClusterUri = 'https://sm-prd-westeurope.westeurope.kusto.windows.net',
    [string]$Database = 'usage-analytics',
    [string]$ArtifactDirectory = (Join-Path $PSScriptRoot 'generated\suite')
)

$ErrorActionPreference = 'Stop'
$token = az account get-access-token --resource 'https://api.kusto.windows.net' --query accessToken -o tsv
if ($LASTEXITCODE -ne 0 -or [string]::IsNullOrWhiteSpace($token)) { throw 'Could not acquire a Kusto access token.' }

function Invoke-Query([string]$Query) {
    $response = Invoke-RestMethod -Method Post -Uri "$ClusterUri/v2/rest/query" -Headers @{ Authorization = "Bearer $token" } -ContentType 'application/json' -Body (@{ db = $Database; csl = $Query } | ConvertTo-Json -Compress)
    if (($response | Where-Object FrameType -eq 'DataSetCompletion').HasErrors) { throw 'Kusto reported a partial query failure.' }
    $table = $response | Where-Object TableKind -eq 'PrimaryResult' | Select-Object -First 1
    if ($null -eq $table) { throw 'Kusto returned no primary result.' }
    return ,$table
}

$manifest = @(Get-Content -Raw (Join-Path $ArtifactDirectory 'manifest.json') | ConvertFrom-Json)
if ($manifest.Count -ne 4) { throw 'Expected four suite dashboards.' }
$queries = [System.Collections.Generic.HashSet[string]]::new()
$functionCalls = [System.Collections.Generic.HashSet[string]]::new()
$dataPanelCount = 0
$all = 'dynamic([])'
$substitutions = @{ _startTime = 'startofday(ago(90d))'; _endTime = 'now()'; _customer = $all; _subscription = $all; _source = $all; _target = $all; _status = $all }
foreach ($item in $manifest) {
    $grafana = Get-Content -Raw (Join-Path $ArtifactDirectory "$($item.uid).grafana.json") | ConvertFrom-Json
    $adx = Get-Content -Raw (Join-Path $ArtifactDirectory "$($item.uid).adx.json") | ConvertFrom-Json
    if ($grafana.panels.Count -ne $item.grafanaPanels -or $adx.tiles.Count -ne $item.adxTiles) { throw "Unexpected panel count for $($item.uid)." }
    $dataPanelCount += @($grafana.panels | Where-Object { $_.targets }).Count
    $grafanaQueries = @($grafana.panels.targets.query) + @($grafana.templating.list.query.query)
    foreach ($queryText in @($grafanaQueries) + @($adx.queries.text)) {
        if ([string]::IsNullOrWhiteSpace($queryText)) { continue }
        if ($queryText -match 'customEvents|cluster\(|database\(|AzCopyUsageAggregates1|\|\s*(summarize|join|union)\b|AzCopy_KO_') {
            throw "Dashboard embeds source or business aggregation logic: $($item.uid)"
        }
        if ($queryText -notmatch '^(AzCopy_\w+)\(') { throw "Query does not call a saved AzCopy helper: $queryText" }
        $null = $functionCalls.Add($Matches[1])
        $query = $queryText.Replace('$__timeFrom', 'startofday(ago(90d))').Replace('$__timeTo', 'now()')
        foreach ($name in @('Customer', 'SubscriptionId', 'SourceType', 'TargetType', 'JobStatus')) { $query = $query.Replace(('dynamic(${'+$name+':json})'), $all) }
        foreach ($entry in $substitutions.GetEnumerator()) { $query = $query.Replace($entry.Key, $entry.Value) }
        if ($query -match '\$\{|\$__time') { throw 'Unresolved macro.' }
        $null = $queries.Add($query)
    }
    foreach ($variable in $grafana.templating.list) {
        $result = Invoke-Query $variable.query.query
        if (($result.schema -ne $null) -or ($result.Columns.ColumnName -notcontains 'text') -or ($result.Columns.ColumnName -notcontains 'value')) { throw 'Grafana custom variables require text/value fields.' }
    }
}
foreach ($query in $queries) {
    $result = Invoke-Query $query
    if ($result.Columns.ColumnName -contains 'Customer' -and $result.Columns.ColumnName -notcontains 'CustomerKey') {
        throw 'Customer-link tables must include CustomerKey in their query result.'
    }
}

$summary = Invoke-Query "AzCopy_UsageSummary(datetime(1970-01-01), now(), $all, $all, $all, $all) | project BytesTransferred, Jobs"
$raw = Invoke-Query "AzCopyUsageAggregates1 | summarize BytesTransferred=sumif(Count, Name == 'JobRunBytesTransferred'), Jobs=tolong(sumif(Count, Name == 'JobRunResourcesCreated'))"
if ($summary.Rows[0][0] -ne $raw.Rows[0][0] -or $summary.Rows[0][1] -ne $raw.Rows[0][1]) { throw 'Shared suite totals do not reconcile with persisted rows.' }

$fixture = @'
let AzCopy_Usage = (startTime:datetime, endTime:datetime, customerFilter:dynamic, subscriptionFilter:dynamic, sourceFilter:dynamic, targetFilter:dynamic) {
    datatable(SliceDate:datetime, CustomerKey:string, CustomerName:string, SubscriptionId:string, InstallationID:string, FromTo:string, JobStatus:string, Jobs:long, BytesTransferred:real)
    [
        datetime(2026-01-05), 'A', 'Alpha', 'sub-a', 'install-a', 'LocalBlob', 'Failed', 2, 100.0,
        datetime(2026-02-05), 'A', 'Alpha', 'sub-a', 'install-a', 'LocalBlob', 'Completed', 1, 200.0,
        datetime(2026-01-06), 'B', 'Beta', 'sub-b', 'install-b', 'LocalBlob', 'Failed', 3, 0.0,
        datetime(2026-02-06), 'B', 'Beta', 'sub-b', 'install-b', 'LocalBlob', 'CompletedWithErrors', 5, 10.0,
        datetime(2026-01-07), 'C', 'Cancelled only', 'sub-c', 'install-c', 'LocalBlob', 'Cancelled', 1, 0.0,
        datetime(2026-01-08), 'D', 'Skipped success', 'sub-d', 'install-d', 'LocalBlob', 'CompletedWithSkipped', 1, 20.0,
        datetime(2026-01-09), 'unmapped', 'Unmapped customer', 'unmapped', 'install-u', 'LocalBlob', 'Failed', 9, 0.0
    ]
    | where SliceDate >= startTime and SliceDate < endTime
    | extend IsMapped = CustomerKey != 'unmapped', IsSuccess = JobStatus in ('Completed', 'CompletedWithSkipped'),
        IsFailure = JobStatus in ('Failed', 'CompletedWithErrors', 'CompletedWithErrorsAndSkipped'), Attempts = Jobs
};
'@
foreach ($filename in @('11_AzCopy_RecurringCustomers.kql', '12_AzCopy_RecurringSummary.kql', '13_AzCopy_NoSuccessCustomers.kql', '14_AzCopy_NoSuccessDetails.kql', '15_AzCopy_NoSuccessSummary.kql')) {
    $text = Get-Content -Raw (Join-Path $PSScriptRoot "FrontendQueries\$filename")
    $match = [regex]::Match($text, '\)\s+(AzCopy_\w+)\(')
    $body = $text.Substring($match.Index + $match.Value.IndexOf($match.Groups[1].Value))
    $fixture += 'let ' + $body.Replace(($match.Groups[1].Value + '('), ($match.Groups[1].Value + ' = (')) + ";`n"
}
$arguments = 'datetime(2026-01-01), datetime(2026-03-01), dynamic([]), dynamic([]), dynamic([]), dynamic([])'
$assertions = @"
let Recurring = AzCopy_RecurringSummary($arguments);
let NoSuccess = AzCopy_NoSuccessCustomers($arguments);
let FailedOnly = AzCopy_NoSuccessSummary($arguments, dynamic(['Failed'])) | where JobStatus == 'All';
print RecurringCorrect = toscalar(Recurring | project RecurringCustomers == 2 and BytesTransferred == 310.0 and AverageActiveMonths == 2.0),
    CohortCorrect = toscalar(NoSuccess | summarize count() == 1 and take_any(CustomerKey) == 'B' and sum(FailedJobs) == 8),
    StatusDoesNotHideSuccess = toscalar(FailedOnly | project Customers == 1 and FailedJobs == 3)
"@
$test = Invoke-Query ($fixture + $assertions)
if (@($test.Rows[0] | Where-Object { $_ -ne $true }).Count -ne 0) { throw 'Synthetic cohort regression failed.' }
$monthOnly = Invoke-Query ($fixture + "AzCopy_RecurringSummary(datetime(2026-01-01), datetime(2026-02-01), $all, $all, $all, $all) | project RecurringCustomers == 0 and isnull(AverageActiveMonths)")
if ($monthOnly.Rows[0][0] -ne $true) { throw 'One-month activity must not count as recurrence.' }

$functionList = @($functionCalls | Sort-Object)
$listing = Invoke-RestMethod -Method Post -Uri "$ClusterUri/v1/rest/mgmt" -Headers @{ Authorization = "Bearer $token" } -ContentType 'application/json' -Body (@{ db = $Database; csl = ".show functions | where Folder == 'AzCopyQueries' | project Name, Body" } | ConvertTo-Json -Compress)
$deployed = @{}
foreach ($row in $listing.Tables[0].Rows) { $deployed[$row[0]] = $row[1] }
$verifiedBodies = 0
foreach ($file in Get-ChildItem (Join-Path $PSScriptRoot 'FrontendQueries') -Filter '*.kql' | Where-Object { [int]$_.Name.Substring(0, 2) -ge 5 }) {
    $command = Get-Content -Raw $file.FullName
    $name = [regex]::Match($command, '\)\s+(AzCopy_\w+)\(').Groups[1].Value
    $localBody = $command.Substring($command.IndexOf('{')).Trim() -replace '\s+', ' '
    $remoteBody = ([string]$deployed[$name]).Trim() -replace '\s+', ' '
    if ($localBody -cne $remoteBody) { throw "Deployed helper differs from local definition: $name" }
    if ($remoteBody -match 'customEvents|cluster\(|database\(') { throw "Frontend helper scans a raw or cross-cluster source: $name" }
    $verifiedBodies++
}
[pscustomobject]@{
    Dashboards = $manifest.Count
    GrafanaDataPanels = $dataPanelCount
    AdxTiles = ($manifest.adxTiles | Measure-Object -Sum).Sum
    UniqueRenderedQueries = $queries.Count
    SavedHelpers = $functionList -join ', '
    RawSourceQueriesInDashboards = 0
    VerifiedDeployedHelperBodies = $verifiedBodies
    SyntheticCohortTests = 'PASS'
    Bytes = $summary.Rows[0][0]
    Jobs = $summary.Rows[0][1]
    Status = 'PASS'
} | Format-List