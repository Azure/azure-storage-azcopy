[CmdletBinding()]
param(
    [string]$AppInsightsSubscriptionId = "31347be8-d066-464e-9866-7e58d85027b7",
    [string]$AppInsightsResourceGroup = "sharankur_playground",
    [string]$AppInsightsName = "sharankur_insights1",
    [string]$OutputPath = (Join-Path $PSScriptRoot "azcopy-data-metrics.dashboard.json"),
    [switch]$EnableLiveArgEnrichment = $true,
    [switch]$EnableAipddEnrichment = $true
)

$ErrorActionPreference = "Stop"

if ($args.Count -gt 0) {
    throw "Unrecognized arguments: $($args -join ' ')"
}

function New-StableGuid([string]$Seed) {
    $sha256 = [System.Security.Cryptography.SHA256]::Create()
    try {
        $hash = $sha256.ComputeHash([System.Text.Encoding]::UTF8.GetBytes($Seed))
    }
    finally {
        $sha256.Dispose()
    }

    $bytes = New-Object byte[] 16
    [Array]::Copy($hash, $bytes, 16)
    return ([Guid]::new($bytes)).ToString()
}

$normalizedResourceGroup = $AppInsightsResourceGroup.ToLowerInvariant()
$normalizedAppInsightsName = $AppInsightsName.ToLowerInvariant()
$resourceId = "/subscriptions/$AppInsightsSubscriptionId/resourcegroups/$normalizedResourceGroup/providers/microsoft.insights/components/$normalizedAppInsightsName"
$appInsightsTable = "cluster('https://adx.monitor.azure.com$resourceId').database('$AppInsightsName').customEvents"
$xstoreAccountPropertiesTable = "cluster('https://xdataanalytics.westcentralus.kusto.windows.net').database('XDataAnalytics').XStoreAccountPropertiesDaily"
$aipddSubscriptionSnapshotTable = if ($EnableAipddEnrichment) {
    "cluster('https://aipddprod.kusto.windows.net').database('AIPDD_Usage').SubscriptionSnapshotV2"
} else {
    "datatable(SubscriptionGuid:string, FriendlySubscriptionName:string, AI_OfferType:string, TPID:string, TPName:string, CurrentSubscriptionStatus:string, AI_SubscriptionBusinessStatus:string, AI_UpdatedAt:datetime)[]"
}
$argResourcesTable = "cluster('https://argeusarm1pone.eastus.kusto.windows.net').database('AzureResourceGraph').Resources"
$argSubscriptionsTable = "cluster('https://argeusarm1pone.eastus.kusto.windows.net').database('AzureResourceGraph').InternalSubscriptionResources"
$enrichmentQuery = Get-Content -Path (Join-Path $PSScriptRoot 'queries/common/enriched_finished_jobs.kql') -Raw
$enrichmentFilters = Get-Content -Path (Join-Path $PSScriptRoot 'queries/common/enrichment_filters.kql') -Raw
$isTelemetryTest = $AppInsightsName -eq "azcopy-telemetry-test-ai"
$titleSuffix = if ($isTelemetryTest) { " - Telemetry Test" } else { "" }
$prefix = "azcopy-data-metrics:$resourceId"
$dataSourceId = New-StableGuid "${prefix}:datasource"

function Read-Query([string]$RelativePath) {
    $path = Join-Path $PSScriptRoot $RelativePath
    if (-not (Test-Path $path)) {
        throw "Query file not found: $RelativePath"
    }

    $query = Get-Content -Path $path -Raw
    $query = $query.Replace('{{ENRICHED_FINISHED_JOBS}}', $enrichmentQuery.Trim())
    $query = $query.Replace('{{ENRICHMENT_FILTERS}}', $enrichmentFilters.Trim())
    $query = $query -replace "(?m)^customEvents", $appInsightsTable
    $query = $query.Replace('__APP_INSIGHTS_CUSTOM_EVENTS__', $appInsightsTable)
    $query = $query.Replace('__XSTORE_ACCOUNT_PROPERTIES__', $xstoreAccountPropertiesTable)
    $query = $query.Replace('__AIPDD_SUBSCRIPTION_SNAPSHOT__', $aipddSubscriptionSnapshotTable)
    $query = $query.Replace('__ARG_RESOURCES__', $argResourcesTable)
    $query = $query.Replace('__ARG_SUBSCRIPTIONS__', $argSubscriptionsTable)
    $query = $query.Replace(
        '__ENABLE_LIVE_ARG_ENRICHMENT__',
        $(if ($EnableLiveArgEnrichment) { 'true' } else { 'false' }))
    $query = $query.Replace(
        '__ENABLE_AIPDD_ENRICHMENT__',
        $(if ($EnableAipddEnrichment) { 'true' } else { 'false' }))
    if ($query -match '\{\{[A-Z_]+\}\}' -or $query -match '__[A-Z_]+__') {
        throw "Unresolved query template token in '$RelativePath'."
    }
    return $query.Trim()
}

$knownVariables = @(
    "_startTime",
    "_endTime",
    "_sourceType",
    "_destType",
    "_fromTo",
    "_sourceMountType",
    "_destEndpointKind",
    "_clientRegion",
    "_destSubscription",
    "_customer",
    "_offerType",
    "_subscriptionScope",
    "_account"
)

function Get-UsedVariables([string]$QueryText) {
    return @($knownVariables | Where-Object { $QueryText.Contains($_) })
}

function New-VisualOptions([string]$VisualType) {
    switch ($VisualType) {
        "multistat" {
            return [ordered]@{
                multiStat__textSize = "large"
                multiStat__valueColumn = "Value"
                colorRulesDisabled = $true
                colorStyle = "light"
                multiStat__displayOrientation = "horizontal"
                multiStat__labelColumn = "Metric"
                multiStat__slot = [ordered]@{ width = 1; height = 2 }
                colorRules = @()
            }
        }
        "table" {
            return [ordered]@{
                table__enableRenderLinks = $true
                colorRulesDisabled = $true
                colorStyle = "light"
                crossFilterDisabled = $false
                drillthroughDisabled = $false
                crossFilter = @()
                drillthrough = @()
                table__renderLinks = @()
                colorRules = @()
            }
        }
        default {
            return [ordered]@{
                multipleYAxes = [ordered]@{
                    base = [ordered]@{
                        id = "-1"
                        label = ""
                        columns = @()
                        yAxisMaximumValue = $null
                        yAxisMinimumValue = $null
                        yAxisScale = "linear"
                        horizontalLines = @()
                    }
                    additional = @()
                    showMultiplePanels = $false
                }
                hideLegend = $false
                legendLocation = "bottom"
                xColumnTitle = ""
                xColumn = $null
                yColumns = $null
                seriesColumns = $null
                xAxisScale = "linear"
                verticalLine = ""
                crossFilterDisabled = $false
                drillthroughDisabled = $false
                crossFilter = @()
                drillthrough = @()
                selectedDataOnLoad = [ordered]@{ all = $true; limit = 10 }
                dataPointsTooltip = [ordered]@{ all = $false; limit = 1 }
            }
        }
    }
}

$page = [ordered]@{
    name = "Data"
    id = New-StableGuid "${prefix}:page:data"
}

$manifest = @(
    @{
        Title = "How to read this dashboard"
        Visual = "markdownCard"
        X = 0; Y = 0; W = 22; H = 5
        MarkdownText = @"
This dashboard adapts the useful data-volume views from Storage Mover to AzCopy job telemetry, then enriches destination account names server-side. Totals use all received finished-job telemetry. Resumed jobs are rolled up once by `JobID` using the latest observed finish and maximum cumulative counters.

`Completed objects` excludes folder-property operations. Historical subscription, redundancy, namespace, access tier, and account class come from `XStoreAccountPropertiesDaily` as of the activity day. Current offer/scope, ARM kind, SKU, and Storage region come from Azure Resource Graph. Deleted or inaccessible short-lived accounts remain in explicit Unmapped buckets. No subscription or tenant identifier was added to client telemetry.
"@
    },
    @{
        Title = "Selected-range data totals"
        File = "queries/tiles/01_selected_range_totals.kql"
        Visual = "multistat"
        Description = "Unique JobIDs, logical payload bytes, and completed payload objects in the selected range."
        X = 0; Y = 5; W = 22; H = 6
    },
    @{
        Title = "Current versus previous month"
        File = "queries/tiles/02_current_previous_month.kql"
        Visual = "multistat"
        Description = "Calendar-month comparison independent of the dashboard time-range picker. All topology and account filters still apply."
        X = 0; Y = 11; W = 22; H = 6
    },
    @{
        Title = "Monthly data transferred"
        File = "queries/tiles/03_monthly_data_trend.kql"
        Visual = "timechart"
        Description = "Logical successful payload bytes grouped by the latest observed job finish month."
        X = 0; Y = 17; W = 11; H = 8
    },
    @{
        Title = "Monthly completed objects"
        File = "queries/tiles/04_monthly_objects_trend.kql"
        Visual = "timechart"
        Description = "Completed payload objects excluding folder-property operations."
        X = 11; Y = 17; W = 11; H = 8
    },
    @{
        Title = "Data by source-target pair"
        File = "queries/tiles/05_data_by_source_target.kql"
        Visual = "bar"
        Description = "Logical payload volume grouped by AzCopy source and destination service type."
        X = 0; Y = 25; W = 11; H = 8
    },
    @{
        Title = "Completed objects by source-target pair"
        File = "queries/tiles/06_objects_by_source_target.kql"
        Visual = "bar"
        Description = "Completed payload objects grouped by AzCopy source and destination service type."
        X = 11; Y = 25; W = 11; H = 8
    },
    @{
        Title = "Data by destination service type"
        File = "queries/tiles/07_data_by_destination_type.kql"
        Visual = "bar"
        Description = "AzCopy destination service type such as Blob, BlobFS, File, FileNFS, or Local. This is not Storage account SKU or redundancy."
        X = 0; Y = 33; W = 11; H = 8
    },
    @{
        Title = "Data by source mount type"
        File = "queries/tiles/08_data_by_source_mount.kql"
        Visual = "bar"
        Description = "Logical payload volume grouped by source classification such as local disk, NAS SMB/NFS, or cloud."
        X = 11; Y = 33; W = 11; H = 8
    },
    @{
        Title = "Data by destination endpoint kind"
        File = "queries/tiles/09_data_by_destination_endpoint.kql"
        Visual = "bar"
        Description = "Destination-only hostname classification. Explicit .privatelink. hosts are private-endpoint; private DNS using a public hostname remains indistinguishable."
        X = 0; Y = 41; W = 11; H = 8
    },
    @{
        Title = "Data by telemetry-sender country/region"
        File = "queries/tiles/10_data_by_client_region.kql"
        Visual = "bar"
        Description = "Application Insights sender-IP geography. It must not be interpreted as source, destination, or Azure resource region."
        X = 11; Y = 41; W = 11; H = 8
    },
    @{
        Title = "Recent observed finished jobs"
        File = "queries/tiles/11_recent_finished_jobs.kql"
        Visual = "table"
        Description = "Latest 200 unique JobIDs after all filters. Values are cumulative job counters."
        X = 0; Y = 49; W = 22; H = 10
    },
    @{
        Title = "Data by destination Storage account kind"
        File = "queries/tiles/12_data_by_storage_kind.kql"
        Visual = "bar"
        Description = "Current ARM account kind from Azure Resource Graph. Deleted or inaccessible accounts remain Unmapped."
        X = 0; Y = 59; W = 11; H = 8
    },
    @{
        Title = "Data by destination Storage SKU"
        File = "queries/tiles/13_data_by_storage_sku.kql"
        Visual = "bar"
        Description = "Current ARM SKU name from Azure Resource Graph. Deleted or inaccessible accounts remain Unmapped."
        X = 11; Y = 59; W = 11; H = 8
    },
    @{
        Title = "Data by destination redundancy"
        File = "queries/tiles/14_data_by_storage_redundancy.kql"
        Visual = "bar"
        Description = "Historical XStore redundancy as of the AzCopy activity day."
        X = 0; Y = 67; W = 11; H = 8
    },
    @{
        Title = "Data by destination namespace"
        File = "queries/tiles/15_data_by_storage_namespace.kql"
        Visual = "bar"
        Description = "Historical hierarchical-namespace state represented as HNS or FNS."
        X = 11; Y = 67; W = 11; H = 8
    },
    @{
        Title = "Data by destination access tier"
        File = "queries/tiles/16_data_by_storage_access_tier.kql"
        Visual = "bar"
        Description = "Historical XStore access tier as of the AzCopy activity day."
        X = 0; Y = 75; W = 11; H = 8
    },
    @{
        Title = "Data by destination account class"
        File = "queries/tiles/17_data_by_storage_account_class.kql"
        Visual = "bar"
        Description = "Historical XStore billing/account class. This is deliberately distinct from ARM kind and SKU."
        X = 11; Y = 75; W = 11; H = 8
    },
    @{
        Title = "Destination enrichment mapping coverage"
        File = "queries/tiles/18_enrichment_mapping_coverage.kql"
        Visual = "table"
        Description = "Jobs and data retained in mapped, missing, and not-applicable buckets for each inventory."
        X = 0; Y = 83; W = 22; H = 8
    }
)

foreach ($item in $manifest) {
    $minimumWidth = if ($item.Visual -eq "markdownCard") { 2 } else { 3 }
    $minimumHeight = if ($item.Visual -eq "markdownCard") { 1 } else { 6 }
    if ($item.W -lt $minimumWidth -or $item.H -lt $minimumHeight) {
        throw "Tile '$($item.Title)' is $($item.W)x$($item.H); '$($item.Visual)' requires at least ${minimumWidth}x${minimumHeight}."
    }
}

for ($leftIndex = 0; $leftIndex -lt $manifest.Count; $leftIndex++) {
    for ($rightIndex = $leftIndex + 1; $rightIndex -lt $manifest.Count; $rightIndex++) {
        $left = $manifest[$leftIndex]
        $right = $manifest[$rightIndex]
        $overlaps =
            $left.X -lt ($right.X + $right.W) -and
            ($left.X + $left.W) -gt $right.X -and
            $left.Y -lt ($right.Y + $right.H) -and
            ($left.Y + $left.H) -gt $right.Y
        if ($overlaps) {
            throw "Tiles '$($left.Title)' and '$($right.Title)' overlap."
        }
    }
}

$parameterDefinitions = @(
    @{ VariableName = "_sourceType"; DisplayName = "Source type"; File = "queries/parameters/01_source_type.kql"; Description = "AzCopy source service type." },
    @{ VariableName = "_destType"; DisplayName = "Destination type"; File = "queries/parameters/02_destination_type.kql"; Description = "AzCopy destination service type." },
    @{ VariableName = "_fromTo"; DisplayName = "From-to"; File = "queries/parameters/03_from_to.kql"; Description = "Exact AzCopy FromTo pairing." },
    @{ VariableName = "_sourceMountType"; DisplayName = "Source mount type"; File = "queries/parameters/04_source_mount_type.kql"; Description = "AzCopy source mount or cloud classification." },
    @{ VariableName = "_destEndpointKind"; DisplayName = "Destination endpoint kind"; File = "queries/parameters/05_destination_endpoint_kind.kql"; Description = "Destination hostname classification; not a complete private-network signal." },
    @{ VariableName = "_clientRegion"; DisplayName = "Client country/region"; File = "queries/parameters/06_client_region.kql"; Description = "Telemetry-sender IP geography, not Storage resource region." },
    @{ VariableName = "_destSubscription"; DisplayName = "Destination owning subscription"; File = "queries/parameters/destination_subscription.kql"; Description = "Historical owning subscription with AIPDD friendly name, Azure Resource Graph name fallback, or raw subscription ID." },
    @{ VariableName = "_customer"; DisplayName = "Customer"; File = "queries/parameters/customer.kql"; Description = "AIPDD top-parent customer name; the filter value is the corresponding TPID and remains server-side." },
    @{ VariableName = "_offerType"; DisplayName = "Destination subscription offer type"; File = "queries/parameters/offer_type.kql"; Description = "AIPDD offer type with Azure Resource Graph fallback." },
    @{ VariableName = "_subscriptionScope"; DisplayName = "Destination subscription scope"; File = "queries/parameters/subscription_scope.kql"; Description = "Internal, External, Unknown, or Not applicable based on current subscription channel metadata." }
)

$queries = @()
$parameters = @(
    [ordered]@{
        kind = "duration"
        id = New-StableGuid "${prefix}:parameter:time-range"
        displayName = "Time range"
        description = ""
        beginVariableName = "_startTime"
        endVariableName = "_endTime"
        defaultValue = [ordered]@{ kind = "dynamic"; count = 365; unit = "days" }
        showOnPages = [ordered]@{ kind = "all" }
    }
)

foreach ($definition in $parameterDefinitions) {
    $queryId = New-StableGuid "${prefix}:query:$($definition.File)"
    $text = Read-Query $definition.File
    $queries += [ordered]@{
        dataSource = [ordered]@{ kind = "inline"; dataSourceId = $dataSourceId }
        text = $text
        id = $queryId
        usedVariables = @(Get-UsedVariables $text)
    }

    $parameters += [ordered]@{
        kind = "string"
        selectionType = "array"
        id = New-StableGuid "${prefix}:parameter:$($definition.VariableName)"
        displayName = $definition.DisplayName
        variableName = $definition.VariableName
        description = $definition.Description
        includeAllOption = $true
        allIsNull = $true
        defaultValue = [ordered]@{ kind = "all" }
        showOnPages = [ordered]@{ kind = "all" }
        dataSource = [ordered]@{
            kind = "query"
            queryRef = [ordered]@{ kind = "query"; queryId = $queryId }
            columns = [ordered]@{ value = "Value"; label = "Label" }
        }
    }
}

$parameters += [ordered]@{
    kind = "string"
    selectionType = "freetext"
    id = New-StableGuid "${prefix}:parameter:account"
    displayName = "Storage account"
    variableName = "_account"
    description = "Optional exact source or destination Azure Storage account-name filter. Leave empty for all accounts."
    defaultValue = [ordered]@{ kind = "value"; value = "" }
    showOnPages = [ordered]@{ kind = "all" }
}

$tiles = @()
foreach ($item in $manifest) {
    if ($item.Visual -eq "markdownCard") {
        $tiles += [ordered]@{
            id = New-StableGuid "${prefix}:tile:markdown:$($item.Title)"
            title = $item.Title
            description = ""
            visualType = $item.Visual
            pageId = $page.id
            layout = [ordered]@{ x = $item.X; y = $item.Y; width = $item.W; height = $item.H }
            markdownText = $item.MarkdownText
            visualOptions = [ordered]@{}
        }
        continue
    }

    $queryId = New-StableGuid "${prefix}:query:$($item.File)"
    $text = Read-Query $item.File
    $queries += [ordered]@{
        dataSource = [ordered]@{ kind = "inline"; dataSourceId = $dataSourceId }
        text = $text
        id = $queryId
        usedVariables = @(Get-UsedVariables $text)
    }

    $tiles += [ordered]@{
        id = New-StableGuid "${prefix}:tile:$($item.File)"
        title = $item.Title
        description = $item.Description
        visualType = $item.Visual
        pageId = $page.id
        layout = [ordered]@{ x = $item.X; y = $item.Y; width = $item.W; height = $item.H }
        queryRef = [ordered]@{ kind = "query"; queryId = $queryId }
        visualOptions = New-VisualOptions $item.Visual
    }
}

$dashboard = [ordered]@{
    '$schema' = "https://dataexplorer.azure.com/static/d/schema/60/dashboard.json"
    id = New-StableGuid "${prefix}:dashboard"
    eTag = New-StableGuid "${prefix}:etag:v1"
    title = "AzCopy Data Metrics$titleSuffix"
    schema_version = "60"
    tiles = $tiles
    baseQueries = @()
    parameters = $parameters
    dataSources = @(
        [ordered]@{
            id = $dataSourceId
            kind = "manual-kusto"
            scopeId = "kusto"
            name = "AzCopy Analytics$titleSuffix"
            clusterUri = "https://azcore.centralus.kusto.windows.net/"
            database = "Xstore"
        }
    )
    pages = @($page)
    queries = $queries
}

$json = $dashboard | ConvertTo-Json -Depth 30
$serializedDashboard = $json | ConvertFrom-Json
$invalidUsedVariables = @($serializedDashboard.queries | Where-Object { $_.usedVariables -isnot [array] })
if ($invalidUsedVariables.Count -ne 0) {
    throw "Every query usedVariables property must serialize as a JSON array."
}

$json | Set-Content -Path $OutputPath -Encoding utf8
Write-Host "Generated $OutputPath with $($tiles.Count) tiles, $($parameters.Count) parameters, and $($queries.Count) queries."
