param(
    [string]$App = "azcopy-telemetry-test-ai",
    [string]$ResourceGroup = "azcopy-telemetry-test-rg",
    [string]$SubscriptionId = "",
    [string]$Offset = "30d",
    [string]$CentralClusterUri = "https://azcore.centralus.kusto.windows.net",
    [string]$CentralDatabase = "Xstore",
    [switch]$DisableAipddEnrichment,
    [ValidateRange(1, 10)] [int]$MaxAttempts = 3,
    [string]$QueryDumpDirectory = ""
)

$ErrorActionPreference = "Stop"
$queryRoot = Join-Path $PSScriptRoot "queries"

if ([string]::IsNullOrWhiteSpace($SubscriptionId)) {
    $SubscriptionId = (& az account show --query id --output tsv).Trim()
    if ($LASTEXITCODE -ne 0 -or [string]::IsNullOrWhiteSpace($SubscriptionId)) {
        throw "Could not determine the active Azure subscription."
    }
}

$resourceId = "/subscriptions/$($SubscriptionId.ToLowerInvariant())/resourcegroups/$($ResourceGroup.ToLowerInvariant())/providers/microsoft.insights/components/$($App.ToLowerInvariant())"
$appInsightsTable = "cluster('https://adx.monitor.azure.com$resourceId').database('$App').customEvents"
$xstoreAccountPropertiesTable = "cluster('https://xdataanalytics.westcentralus.kusto.windows.net').database('XDataAnalytics').XStoreAccountPropertiesDaily"
$argResourcesTable = "cluster('https://argeusarm1pone.eastus.kusto.windows.net').database('AzureResourceGraph').Resources"
$argSubscriptionsTable = "cluster('https://argeusarm1pone.eastus.kusto.windows.net').database('AzureResourceGraph').InternalSubscriptionResources"
$aipddSubscriptionSnapshotTable = "cluster('https://aipddprod.kusto.windows.net').database('AIPDD_Usage').SubscriptionSnapshotV2"
$enrichmentQuery = Get-Content -Path (Join-Path $queryRoot "common\enriched_finished_jobs.kql") -Raw
$enrichmentFilters = Get-Content -Path (Join-Path $queryRoot "common\enrichment_filters.kql") -Raw

function Expand-Tokens(
    [string]$Query,
    [string]$ArgResourcesTable = $argResourcesTable,
    [string]$ArgSubscriptionsTable = $argSubscriptionsTable,
    [bool]$EnableLiveArgEnrichment = $true,
    [string]$AipddSubscriptionSnapshotTable = $aipddSubscriptionSnapshotTable,
    [bool]$EnableAipddEnrichment = (-not $DisableAipddEnrichment)
) {
    $expanded = $Query
    $expanded = $expanded.Replace('{{ENRICHED_FINISHED_JOBS}}', $enrichmentQuery.Trim())
    $expanded = $expanded.Replace('{{ENRICHMENT_FILTERS}}', $enrichmentFilters.Trim())
    $expanded = $expanded -replace "(?m)^customEvents", $appInsightsTable
    $expanded = $expanded.Replace('__APP_INSIGHTS_CUSTOM_EVENTS__', $appInsightsTable)
    $expanded = $expanded.Replace('__XSTORE_ACCOUNT_PROPERTIES__', $xstoreAccountPropertiesTable)
    $expanded = $expanded.Replace('__ARG_RESOURCES__', $ArgResourcesTable)
    $expanded = $expanded.Replace('__ARG_SUBSCRIPTIONS__', $ArgSubscriptionsTable)
    $expanded = $expanded.Replace('__AIPDD_SUBSCRIPTION_SNAPSHOT__', $AipddSubscriptionSnapshotTable)
    $expanded = $expanded.Replace(
        '__ENABLE_LIVE_ARG_ENRICHMENT__',
        $(if ($EnableLiveArgEnrichment) { 'true' } else { 'false' }))
    $expanded = $expanded.Replace(
        '__ENABLE_AIPDD_ENRICHMENT__',
        $(if ($EnableAipddEnrichment) { 'true' } else { 'false' }))
    if ($expanded -match '\{\{[A-Z_]+\}\}' -or $expanded -match '__[A-Z_]+__') {
        throw "An unresolved query template token remains."
    }
    return $expanded
}

function Set-AllParameterValues([string]$Query) {
    $expanded = $Query.Replace('_startTime', "ago($Offset)").Replace('_endTime', 'now()')
    foreach ($variable in @(
        '_sourceType',
        '_destType',
        '_fromTo',
        '_sourceMountType',
        '_destEndpointKind',
        '_clientRegion',
        '_destSubscription',
        '_offerType',
        '_subscriptionScope',
        '_customer',
        '_account')) {
        $expanded = $expanded.Replace($variable, "''")
    }
    return $expanded
}

function Get-BodyAfterMarker([string]$RelativePath, [string]$Marker) {
    $query = Get-Content -Path (Join-Path $PSScriptRoot $RelativePath) -Raw
    $markerIndex = $query.IndexOf($Marker, [System.StringComparison]::Ordinal)
    if ($markerIndex -lt 0) {
        throw "Marker '$Marker' was not found in '$RelativePath'."
    }
    return $query.Substring($markerIndex + $Marker.Length).Trim()
}

function New-ValidationBranch([string]$Name, [string]$Body) {
    $safeName = $Name -replace '[^A-Za-z0-9_]', '_'
    return [pscustomobject]@{
        Definition = @"
let Validate_$safeName = (
$Body
| take 1
| extend ValidationQuery = '$Name', ValidationPayload = tostring(pack_all())
| project ValidationQuery, ValidationPayload
);
"@
        Reference = "Validate_$safeName"
    }
}

$token = (& az account get-access-token --resource https://api.kusto.windows.net --query accessToken --output tsv).Trim()
if ($LASTEXITCODE -ne 0 -or [string]::IsNullOrWhiteSpace($token)) {
    throw "Could not acquire an Azure Data Explorer token."
}

function Get-KustoQueryStatusFailures([object]$Response) {
    $failures = [Collections.Generic.List[string]]::new()
    foreach ($table in @($Response.Tables)) {
        $columnNames = @($table.Columns | ForEach-Object { $_.ColumnName })
        $statusCodeIndex = [array]::IndexOf($columnNames, "StatusCode")
        if ($statusCodeIndex -lt 0) {
            continue
        }

        $severityIndex = [array]::IndexOf($columnNames, "SeverityName")
        $descriptionIndex = [array]::IndexOf($columnNames, "StatusDescription")
        foreach ($row in @($table.Rows)) {
            if ([long]$row[$statusCodeIndex] -eq 0) {
                continue
            }

            $severity = if ($severityIndex -ge 0) { [string]$row[$severityIndex] } else { "Unknown" }
            $description = if ($descriptionIndex -ge 0) { [string]$row[$descriptionIndex] } else { "No description" }
            $failures.Add("$severity`: $description")
        }
    }
    return @($failures)
}

function Test-IsTransientKustoFailure([System.Management.Automation.ErrorRecord]$ErrorRecord) {
    $exception = $ErrorRecord.Exception
    $errorText = "$($ErrorRecord.ErrorDetails.Message)`n$($exception.Message)"
    $failureCodeMatch = [regex]::Match($errorText, '"@failureCode"\s*:\s*"?(\d{3})"?')
    if ($failureCodeMatch.Success) {
        $failureCode = [int]$failureCodeMatch.Groups[1].Value
        return $failureCode -eq 408 -or $failureCode -eq 429 -or $failureCode -ge 500
    }

    for ($current = $exception; $null -ne $current; $current = $current.InnerException) {
        $statusCodeProperty = $current.PSObject.Properties["StatusCode"]
        if ($null -ne $statusCodeProperty -and $null -ne $statusCodeProperty.Value) {
            $statusCode = [int]$statusCodeProperty.Value
            return $statusCode -eq 408 -or $statusCode -eq 429 -or $statusCode -ge 500
        }

        $responseProperty = $current.PSObject.Properties["Response"]
        if ($null -ne $responseProperty -and
            $null -ne $responseProperty.Value -and
            $null -ne $responseProperty.Value.StatusCode) {
            $statusCode = [int]$responseProperty.Value.StatusCode
            return $statusCode -eq 408 -or $statusCode -eq 429 -or $statusCode -ge 500
        }

        if ($current -is [System.TimeoutException] -or
            $current -is [System.Threading.Tasks.TaskCanceledException]) {
            return $true
        }

        if ($current -is [System.Net.Http.HttpRequestException]) {
            return $true
        }
    }

    return $errorText -match '(?i)Unable to read data from the transport connection|connection was forcibly closed by the remote host|E_QUERY_CANCELLED|Request aborted due to an internal service error'
}

function Invoke-KustoValidation(
    [string]$Name,
    [string]$Query,
    [switch]$AllowQueryStatusFailures
) {
    if (-not [string]::IsNullOrWhiteSpace($QueryDumpDirectory)) {
        $safeName = $Name -replace '[^A-Za-z0-9_.-]', '_'
        New-Item -ItemType Directory -Path $QueryDumpDirectory -Force | Out-Null
        Set-Content -Path (Join-Path $QueryDumpDirectory "$safeName.kql") -Value $Query -Encoding utf8
    }

    $body = @{
        db = $CentralDatabase
        csl = $Query
        properties = @{
            Options = @{
                queryconsistency = "strongconsistency"
            }
        }
    } | ConvertTo-Json -Depth 6

    $headers = @{
        Authorization = "Bearer $token"
        Accept = "application/json"
        "Content-Type" = "application/json; charset=utf-8"
        "x-ms-client-request-id" = "AzCopyDataDashboardValidation;$([Guid]::NewGuid())"
    }

    $stopwatch = [System.Diagnostics.Stopwatch]::StartNew()
    for ($attempt = 1; $attempt -le $MaxAttempts; $attempt++) {
        try {
            $response = Invoke-RestMethod `
                -Method Post `
                -Uri "$($CentralClusterUri.TrimEnd('/'))/v1/rest/query" `
                -Headers $headers `
                -Body $body `
                -TimeoutSec 600
            $queryStatusFailures = @(Get-KustoQueryStatusFailures $response)
            if ($queryStatusFailures.Count -gt 0 -and -not $AllowQueryStatusFailures) {
                throw "Kusto returned query status failures: $($queryStatusFailures -join ' | ')"
            }
            $stopwatch.Stop()
            $statusSuffix = if ($queryStatusFailures.Count -gt 0) {
                " with $($queryStatusFailures.Count) expected warning(s)"
            } else {
                ""
            }
            Write-Host "PASS $Name$statusSuffix ($([math]::Round($stopwatch.Elapsed.TotalSeconds, 1))s)"
            return
        }
        catch {
            $isTransient = Test-IsTransientKustoFailure $_
            if (-not $isTransient -or $attempt -eq $MaxAttempts) {
                $stopwatch.Stop()
                Write-Host "FAIL $Name ($([math]::Round($stopwatch.Elapsed.TotalSeconds, 1))s)" -ForegroundColor Red
                throw
            }

            $delaySeconds = [Math]::Pow(2, $attempt)
            Write-Warning "Transient query failure for '$Name'; retrying in $delaySeconds seconds (attempt $($attempt + 1) of $MaxAttempts)."
            Start-Sleep -Seconds $delaySeconds
        }
    }
}

$simpleParameterFiles = @(
    "queries/parameters/01_source_type.kql",
    "queries/parameters/02_destination_type.kql",
    "queries/parameters/03_from_to.kql",
    "queries/parameters/04_source_mount_type.kql",
    "queries/parameters/05_destination_endpoint_kind.kql",
    "queries/parameters/06_client_region.kql",
    "queries/parameters/subscription_scope.kql"
)
$simpleBranches = foreach ($relativePath in $simpleParameterFiles) {
    $body = Get-Content -Path (Join-Path $PSScriptRoot $relativePath) -Raw
    $body = Set-AllParameterValues (Expand-Tokens $body)
    New-ValidationBranch -Name $relativePath -Body $body
}
$simpleQuery = @"
$($simpleBranches.Definition -join "`n")
union $($simpleBranches.Reference -join ', ')
"@
Invoke-KustoValidation -Name "parameter query batch" -Query $simpleQuery

$standardTileFiles = @(
    "queries/tiles/01_selected_range_totals.kql",
    "queries/tiles/03_monthly_data_trend.kql",
    "queries/tiles/04_monthly_objects_trend.kql",
    "queries/tiles/05_data_by_source_target.kql",
    "queries/tiles/06_objects_by_source_target.kql",
    "queries/tiles/07_data_by_destination_type.kql",
    "queries/tiles/08_data_by_source_mount.kql",
    "queries/tiles/09_data_by_destination_endpoint.kql",
    "queries/tiles/10_data_by_client_region.kql",
    "queries/tiles/11_recent_finished_jobs.kql",
    "queries/tiles/12_data_by_storage_kind.kql",
    "queries/tiles/13_data_by_storage_sku.kql",
    "queries/tiles/14_data_by_storage_redundancy.kql",
    "queries/tiles/15_data_by_storage_namespace.kql",
    "queries/tiles/16_data_by_storage_access_tier.kql",
    "queries/tiles/17_data_by_storage_account_class.kql"
)
$standardBranches = foreach ($relativePath in $standardTileFiles) {
    $body = Get-BodyAfterMarker -RelativePath $relativePath -Marker '{{ENRICHMENT_FILTERS}}'
    New-ValidationBranch -Name $relativePath -Body $body
}
foreach ($relativePath in @(
    "queries/parameters/destination_subscription.kql",
    "queries/parameters/customer.kql",
    "queries/parameters/offer_type.kql")) {
    $body = Get-BodyAfterMarker -RelativePath $relativePath -Marker '{{ENRICHED_FINISHED_JOBS}}'
    $standardBranches += New-ValidationBranch -Name $relativePath -Body $body
}

$allValueFilters = Set-AllParameterValues $enrichmentFilters
$selectedValueFilters = $enrichmentFilters.Replace(
    'let FilteredFinishedJobs',
    'let SelectedFilteredFinishedJobs')
$selectedValues = [ordered]@{
    '_sourceType' = "'Local', 'Blob'"
    '_destType' = "'Blob', 'Local'"
    '_fromTo' = "'LocalBlob', 'BlobLocal'"
    '_sourceMountType' = "'LocalDisk', 'Cloud'"
    '_destEndpointKind' = "'Public endpoint', 'Unknown'"
    '_clientRegion' = "'US', 'unknown'"
    '_destSubscription' = "'Unmapped', 'Not applicable'"
    '_offerType' = "'Unknown', 'Not applicable'"
    '_subscriptionScope' = "'Internal', 'External'"
    '_customer' = "'Unmapped'"
}
foreach ($entry in $selectedValues.GetEnumerator()) {
    $selectedValueFilters = $selectedValueFilters.Replace(
        "in ($($entry.Key))",
        "in ($($entry.Value))")
    $selectedValueFilters = $selectedValueFilters.Replace(
        "isempty($($entry.Key))",
        "false")
}
$selectedValueFilters = $selectedValueFilters.Replace(
    '_account',
    "'nonexistent-validation-account'")
$standardBranches += New-ValidationBranch `
    -Name "selected multi-value filter substitution" `
    -Body "SelectedFilteredFinishedJobs | summarize Rows = count()"

$sharedQuery = @"
let QueryStart = ago($Offset);
let QueryEnd = now();
$(Expand-Tokens $enrichmentQuery)
$allValueFilters
$selectedValueFilters
$($standardBranches.Definition -join "`n")
union $($standardBranches.Reference -join ', ')
"@
Invoke-KustoValidation -Name "shared enriched tile and parameter batch" -Query $sharedQuery

foreach ($relativePath in @(
    "queries/tiles/02_current_previous_month.kql",
    "queries/tiles/18_enrichment_mapping_coverage.kql")) {
    $query = Get-Content -Path (Join-Path $PSScriptRoot $relativePath) -Raw
    $query = Set-AllParameterValues (Expand-Tokens $query)
    Invoke-KustoValidation -Name $relativePath -Query $query
}

$liveArgQuery = @"
let QueryStart = ago($Offset);
let QueryEnd = now();
$(Expand-Tokens $enrichmentQuery $argResourcesTable $argSubscriptionsTable $true)
EnrichedFinishedJobs
| summarize
    Rows = count(),
    ResourceMappings = countif(ResourceInventoryStatus == 'Mapped'),
    SubscriptionMappings = countif(SubscriptionInventoryStatus == 'Mapped')
"@
Invoke-KustoValidation -Name "Live ARG enrichment enabled" -Query $liveArgQuery

$liveAipddQuery = @"
let QueryStart = ago($Offset);
let QueryEnd = now();
$(Expand-Tokens $enrichmentQuery $argResourcesTable $argSubscriptionsTable $true $aipddSubscriptionSnapshotTable $true)
EnrichedFinishedJobs
| summarize
    Rows = count(),
    SubscriptionMappings = countif(AipddSubscriptionInventoryStatus == 'Mapped'),
    CustomerMappings = countif(CustomerInventoryStatus == 'Mapped')
"@
Invoke-KustoValidation -Name "AIPDD enrichment enabled" -Query $liveAipddQuery

$disabledAipddQuery = @"
let QueryStart = ago($Offset);
let QueryEnd = now();
$(Expand-Tokens $enrichmentQuery $argResourcesTable $argSubscriptionsTable $true $aipddSubscriptionSnapshotTable $false)
EnrichedFinishedJobs
| summarize
    Rows = count(),
    DisabledSubscriptionStates = countif(AipddSubscriptionInventoryStatus == 'AIPDD enrichment disabled'),
    DisabledCustomerStates = countif(CustomerInventoryStatus == 'AIPDD enrichment disabled')
"@
Invoke-KustoValidation -Name "AIPDD enrichment explicitly disabled" -Query $disabledAipddQuery

$enrichmentInvariantQuery = @"
let QueryStart = ago($Offset);
let QueryEnd = now();
$(Expand-Tokens $enrichmentQuery $argResourcesTable $argSubscriptionsTable $true $aipddSubscriptionSnapshotTable $true)
let RawTotals = RawFinishedJobs | summarize Rows = count(), Bytes = sum(BytesTransferred);
let EnrichedTotals = EnrichedFinishedJobs | summarize Rows = count(), Bytes = sum(BytesTransferred);
print
    RowCountPreserved = assert(toscalar(RawTotals | project Rows) == toscalar(EnrichedTotals | project Rows), 'AIPDD enrichment changed the finished-job row count'),
    ByteTotalPreserved = assert(toscalar(RawTotals | project Bytes) == toscalar(EnrichedTotals | project Bytes), 'AIPDD enrichment changed the finished-job byte total')
"@
Invoke-KustoValidation -Name "Enrichment row and byte invariants" -Query $enrichmentInvariantQuery

$disabledArgQuery = @"
let QueryStart = ago($Offset);
let QueryEnd = now();
$(Expand-Tokens $enrichmentQuery $argResourcesTable $argSubscriptionsTable $false)
EnrichedFinishedJobs
| summarize
    Rows = count(),
    DisabledResourceStates = countif(ResourceInventoryStatus == 'Live ARG disabled'),
    DisabledSubscriptionStates = countif(SubscriptionInventoryStatus == 'Live ARG disabled')
"@
Invoke-KustoValidation -Name "Live ARG enrichment explicitly disabled" -Query $disabledArgQuery

$missingArgResourcesTable = "cluster('https://argeusarm1pone.eastus.kusto.windows.net').database('AzureResourceGraph').AzCopyMissingArgResources"
$missingArgSubscriptionsTable = "cluster('https://argeusarm1pone.eastus.kusto.windows.net').database('AzureResourceGraph').AzCopyMissingArgSubscriptions"
$argFallbackQuery = @"
let QueryStart = ago($Offset);
let QueryEnd = now();
$(Expand-Tokens $enrichmentQuery $missingArgResourcesTable $missingArgSubscriptionsTable $true)
EnrichedFinishedJobs
| summarize
    Rows = count(),
    ResourceMappings = countif(ResourceInventoryStatus == 'Mapped'),
    SubscriptionMappings = countif(SubscriptionInventoryStatus == 'Mapped')
"@
Invoke-KustoValidation `
    -Name "ARG best-effort fallback" `
    -Query $argFallbackQuery `
    -AllowQueryStatusFailures

$missingAipddTable = "cluster('https://aipddprod.kusto.windows.net').database('AIPDD_Usage').AzCopyMissingSubscriptionSnapshot"
$aipddFallbackQuery = @"
let QueryStart = ago($Offset);
let QueryEnd = now();
$(Expand-Tokens $enrichmentQuery $argResourcesTable $argSubscriptionsTable $true $missingAipddTable $true)
EnrichedFinishedJobs
| summarize
    Rows = count(),
    SubscriptionMappings = countif(AipddSubscriptionInventoryStatus == 'Mapped'),
    CustomerMappings = countif(CustomerInventoryStatus == 'Mapped')
"@
Invoke-KustoValidation `
    -Name "AIPDD best-effort fallback" `
    -Query $aipddFallbackQuery `
    -AllowQueryStatusFailures

Write-Host "All AzCopy data dashboard query batches passed."
