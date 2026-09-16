[CmdletBinding()]
param(
    [string]$ClusterUri = 'https://sm-prd-westeurope.westeurope.kusto.windows.net',
    [string]$Database = 'usage-analytics',
    [string]$AppInsightsResourceId = '/subscriptions/31347be8-d066-464e-9866-7e58d85027b7/resourceGroups/azcopy-telemetry-test-rg/providers/Microsoft.Insights/components/azcopy-telemetry-test-ai',
    [string]$AppInsightsDatabase = 'azcopy-telemetry-test-ai',
    [string]$XStoreAccountProperties = "cluster('https://xdataanalytics.westcentralus.kusto.windows.net').database('XDataAnalytics').XStoreAccountPropertiesDaily",
    [string]$AipddSubscriptionSnapshot = "cluster('https://aipddprod.kusto.windows.net').database('AIPDD_Usage').SubscriptionSnapshotV2",
    [ValidateRange(1, 730)]
    [int]$BackfillDays = 120,
    [switch]$FunctionsOnly,
    [switch]$AllowHistoricalReductions
)

$ErrorActionPreference = 'Stop'
if ($args.Count -gt 0) { throw "Unrecognized arguments: $($args -join ' ')" }

$files = @(
    'AggregateQueries\01_AzCopy_KO_UsageAggregates.kql',
    'FrontendQueries\01_AzCopy_FilterValues.kql',
    'FrontendQueries\02_AzCopy_TopMovers.kql',
    'FrontendQueries\03_AzCopy_CurrentSemesterStart.kql',
    'FrontendQueries\04_AzCopy_PreviousSemesterStart.kql',
    'FrontendQueries\05_AzCopy_Usage.kql',
    'FrontendQueries\06_AzCopy_UsageSummary.kql',
    'FrontendQueries\07_AzCopy_Activity.kql',
    'FrontendQueries\08_AzCopy_Breakdown.kql',
    'FrontendQueries\09_AzCopy_CustomerSubscriptions.kql',
    'FrontendQueries\10_AzCopy_UsageDetails.kql',
    'FrontendQueries\11_AzCopy_RecurringCustomers.kql',
    'FrontendQueries\12_AzCopy_RecurringSummary.kql',
    'FrontendQueries\13_AzCopy_NoSuccessCustomers.kql',
    'FrontendQueries\14_AzCopy_NoSuccessDetails.kql',
    'FrontendQueries\15_AzCopy_NoSuccessSummary.kql',
    'FrontendQueries\16_AzCopy_DashboardFilters.kql'
)

function Get-AppInsightsExpression {
    $resourceId = $AppInsightsResourceId.Trim('/').ToLowerInvariant()
    $databaseName = $AppInsightsDatabase.Replace("'", "''")
    return "cluster('https://adx.monitor.azure.com/$resourceId').database('$databaseName').customEvents"
}

function Invoke-KustoManagement([string]$Command) {
    $token = az account get-access-token --resource $ClusterUri --query accessToken -o tsv
    if ($LASTEXITCODE -ne 0 -or [string]::IsNullOrWhiteSpace($token)) { throw "Could not acquire a Kusto token for $ClusterUri." }
    $headers = @{ Authorization = "Bearer $token" }
    $body = @{ db = $Database; csl = $Command } | ConvertTo-Json -Compress
    $endpoint = if ($Command.TrimStart().StartsWith('.')) { 'mgmt' } else { 'query' }
    $result = Invoke-RestMethod -Method Post -Uri "$ClusterUri/v1/rest/$endpoint" -Headers $headers -ContentType 'application/json' -Body $body
    if ($result.Exceptions.Count -gt 0) { throw "Kusto returned a partial failure: $($result.Exceptions | ConvertTo-Json -Compress)" }
    return $result
}

$tableCommand = @"
.create-merge table AzCopyUsageAggregates1 (
    InstallationID:string,
    Command:string,
    FromTo:string,
    SourceType:string,
    TargetType:string,
    JobStatus:string,
    Count:real,
    Name:string,
    SliceDate:datetime,
    ExecutionTime:datetime,
    CustomerKey:string,
    CustomerName:string,
    SubscriptionId:string)
"@
Write-Host 'Creating or merging AzCopyUsageAggregates1'
$null = Invoke-KustoManagement $tableCommand

$sourceExpression = Get-AppInsightsExpression
foreach ($relativePath in $files) {
    $path = Join-Path $PSScriptRoot $relativePath
    $command = (Get-Content -Raw $path).
        Replace('__APP_INSIGHTS_CUSTOM_EVENTS__', $sourceExpression).
        Replace('__XSTORE_ACCOUNT_PROPERTIES__', $XStoreAccountProperties).
        Replace('__AIPDD_SUBSCRIPTION_SNAPSHOT__', $AipddSubscriptionSnapshot).
        Trim()
    $match = [regex]::Match($command, '\)\s+(AzCopy_[A-Za-z0-9_]+)\s*\(')
    if (-not $match.Success) { throw "$relativePath does not declare an AzCopy_ function." }
    if ($command -notmatch "folder\s*=\s*'AzCopyQueries'") { throw "$relativePath does not target the AzCopyQueries folder." }
    if ($command -match '__[A-Z_]+__') { throw "$relativePath contains an unresolved source placeholder." }
    Write-Host "Deploying $($match.Groups[1].Value)"
    $null = Invoke-KustoManagement $command
}

if ($FunctionsOnly) {
    Write-Host 'Functions deployed; persisted aggregate rows were not refreshed.'
    return
}

$backfillStart = "datetime_add('day', -$BackfillDays, startofday(now()))"
$stagingTable = 'AzCopyUsageAggregatesStaging_' + [guid]::NewGuid().ToString('N')
$stagingAttempted = $false
try {
    Write-Host "Staging $BackfillDays days of daily aggregates before publishing"
    $stagingAttempted = $true
    $null = Invoke-KustoManagement ".set $stagingTable <| AzCopy_KO_UsageAggregates($backfillStart, now() + 1m)"
    $validationQuery = @"
set queryconsistency = 'strongconsistency';
let MissingSlices = toscalar(
    AzCopyUsageAggregates1
    | distinct SliceDate
    | join kind=leftanti ($stagingTable | distinct SliceDate) on SliceDate
    | count);
let HistoricalReductions = toscalar(
    AzCopyUsageAggregates1
    | summarize PreviousCount=sum(Count) by InstallationID, Command, FromTo, SourceType, TargetType, JobStatus, Name, SliceDate, CustomerKey, CustomerName, SubscriptionId
    | join kind=leftouter (
        $stagingTable
        | summarize NewCount=sum(Count) by InstallationID, Command, FromTo, SourceType, TargetType, JobStatus, Name, SliceDate, CustomerKey, CustomerName, SubscriptionId
    ) on InstallationID, Command, FromTo, SourceType, TargetType, JobStatus, Name, SliceDate, CustomerKey, CustomerName, SubscriptionId
    | where isnull(NewCount) or NewCount < PreviousCount
    | count);
$stagingTable
| summarize Rows=count(), InvalidRows=countif(isnull(Count) or not(isfinite(Count)) or Count < 0), MissingSlices=take_any(MissingSlices), HistoricalReductions=take_any(HistoricalReductions)
"@
    $validation = Invoke-KustoManagement $validationQuery
    $values = $validation.Tables[0].Rows[0]
    if ($validation.Tables[0].Rows.Count -ne 1 -or $values.Count -ne 4) {
        throw 'Staging validation returned an unexpected result shape. The live aggregate table was not replaced.'
    }
    foreach ($value in $values) {
        $parsedValue = 0L
        if ($null -eq $value -or -not [long]::TryParse([string]$value, [ref]$parsedValue) -or $parsedValue -lt 0) {
            throw 'Staging validation returned a missing or invalid count. The live aggregate table was not replaced.'
        }
    }
    $ingestedRows = [long]$values[0]
    if ($ingestedRows -le 0) { throw 'Staged aggregates are empty. The live aggregate table was not replaced.' }
    if ([long]$values[1] -ne 0) { throw 'Staged aggregates contain invalid counts. The live aggregate table was not replaced.' }
    if (([long]$values[2] -ne 0 -or [long]$values[3] -ne 0) -and -not $AllowHistoricalReductions) {
        throw 'Backfill would remove or reduce historical aggregate data. Reconcile retention loss, resume date/status changes, or enrichment changes before explicitly using -AllowHistoricalReductions. The live aggregate table was not replaced.'
    }
    if ($AllowHistoricalReductions) { Write-Warning "Historical reductions explicitly approved: $($values[3]) aggregate keys; $($values[2]) missing days." }
    $null = Invoke-KustoManagement ".set-or-replace AzCopyUsageAggregates1 <| $stagingTable"
    Write-Host "Published $ingestedRows validated aggregate rows"
} finally {
    if ($stagingAttempted) {
        try { $null = Invoke-KustoManagement ".drop table $stagingTable ifexists" }
        catch { Write-Warning "Could not remove staging table ${stagingTable}: $($_.Exception.Message)" }
    }
}

$listing = Invoke-KustoManagement ".show functions | where Folder == 'AzCopyQueries' | project Name, Folder, DocString | order by Name asc"
$listing.Tables[0].Rows | ForEach-Object {
    [pscustomobject]@{ Name = $_[0]; Folder = $_[1]; DocString = $_[2] }
} | Format-Table -AutoSize

[pscustomobject]@{ TableName = 'AzCopyUsageAggregates1'; IngestedRows = $ingestedRows } | Format-Table -AutoSize
