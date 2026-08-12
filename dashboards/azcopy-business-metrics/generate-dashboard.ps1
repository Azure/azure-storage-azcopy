param(
    [string]$OutputPath = (Join-Path $PSScriptRoot "azcopy-business-metrics.dashboard.json")
)

$ErrorActionPreference = "Stop"

function New-StableGuid([string]$Seed) {
    $sha = [System.Security.Cryptography.SHA256]::Create()
    try {
        $hash = $sha.ComputeHash([System.Text.Encoding]::UTF8.GetBytes($Seed))
        $bytes = [byte[]]::new(16)
        [Array]::Copy($hash, $bytes, 16)
        return ([Guid]::new($bytes)).ToString()
    } finally {
        $sha.Dispose()
    }
}

function Read-Query([string]$RelativePath) {
    $path = Join-Path $PSScriptRoot $RelativePath
    $text = (Get-Content $path | Where-Object { $_ -notmatch '^\s*//' }) -join "`n"

    if ($RelativePath -like "queries/client/*") {
        $appInsightsTable = "cluster('https://adx.monitor.azure.com/subscriptions/31347be8-d066-464e-9866-7e58d85027b7/resourcegroups/sharankur_playground/providers/microsoft.insights/components/sharankur_insights1').database('sharankur_insights1').customEvents"
        $text = [regex]::Replace($text, '(?m)^customEvents', $appInsightsTable, 1)
    }

    if ($RelativePath -eq "queries/server/02_storage_operation_mix.kql") {
        $text = [regex]::Replace(
            $text,
            '(?m)^XStoreAccountTransactionsHourly',
            "cluster('https://xdataanalytics.westcentralus.kusto.windows.net').database('XDataAnalytics').XStoreAccountTransactionsHourly",
            1)
    }

    if ($RelativePath -eq "queries/server/03_account_ownership.kql") {
        $text = [regex]::Replace(
            $text,
            '(?m)^XStoreAccountPropertiesDaily',
            "cluster('https://xdataanalytics.westcentralus.kusto.windows.net').database('XDataAnalytics').XStoreAccountPropertiesDaily",
            1)
    }

    return $text.Trim()
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
        "pie" {
            return [ordered]@{
                hideLegend = $false
                legendLocation = "bottom"
                xColumn = $null
                yColumns = $null
                seriesColumns = $null
                crossFilterDisabled = $false
                drillthroughDisabled = $false
                labelDisabled = $false
                pie__label = @("name", "percentage")
                tooltipDisabled = $false
                pie__tooltip = @("name", "percentage", "value")
                pie__orderBy = "size"
                pie__kind = "pie"
                pie__topNSlices = $null
                crossFilter = @()
                drillthrough = @()
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

$dataSourceId = New-StableGuid "azcopy-business-metrics:data-source:xstore"
$pages = @(
    [ordered]@{ name = "Overview"; id = New-StableGuid "azcopy-business-metrics:page:overview" },
    [ordered]@{ name = "Performance"; id = New-StableGuid "azcopy-business-metrics:page:performance" },
    [ordered]@{ name = "Reliability"; id = New-StableGuid "azcopy-business-metrics:page:reliability" },
    [ordered]@{ name = "Adoption"; id = New-StableGuid "azcopy-business-metrics:page:adoption" },
    [ordered]@{ name = "Data Quality"; id = New-StableGuid "azcopy-business-metrics:page:data-quality" },
    [ordered]@{ name = "Customer Drilldown"; id = New-StableGuid "azcopy-business-metrics:page:customer" },
    [ordered]@{ name = "Server Correlation"; id = New-StableGuid "azcopy-business-metrics:page:server" }
)
$pageIds = @{}
foreach ($page in $pages) { $pageIds[$page.name] = $page.id }

$manifest = @(
    @{ Page = "Overview"; Title = "Overview KPIs"; File = "queries/client/01_overview_cards.kql"; Visual = "multistat"; X = 0; Y = 0; W = 22; H = 6 },
    @{ Page = "Overview"; Title = "Volume by topology"; File = "queries/client/02_volume_topology_trend.kql"; Visual = "stackedcolumn"; X = 0; Y = 6; W = 12; H = 8 },
    @{ Page = "Overview"; Title = "Outcomes by command"; File = "queries/client/03_outcome_distribution.kql"; Visual = "bar"; X = 12; Y = 6; W = 10; H = 8 },
    @{ Page = "Overview"; Title = "Weekly command and version trend"; File = "queries/client/10_weekly_command_version_trend.kql"; Visual = "stackedcolumn"; X = 0; Y = 14; W = 22; H = 8 },

    @{ Page = "Performance"; Title = "Performance percentiles"; File = "queries/client/04_performance_percentiles.kql"; Visual = "table"; X = 0; Y = 0; W = 22; H = 8 },
    @{ Page = "Performance"; Title = "Platform and version mix"; File = "queries/client/07_platform_mix.kql"; Visual = "bar"; X = 0; Y = 8; W = 22; H = 9 },
    @{ Page = "Performance"; Title = "Source and destination platform mix"; File = "queries/client/18_endpoint_platform_mix.kql"; Visual = "bar"; X = 0; Y = 17; W = 22; H = 9 },

    @{ Page = "Reliability"; Title = "Reliability rates"; File = "queries/client/05_reliability_cards.kql"; Visual = "multistat"; X = 0; Y = 0; W = 22; H = 6 },
    @{ Page = "Reliability"; Title = "Terminal errors (one reason per job)"; Description = "Job-level view. Each unsuccessful job contributes one terminal category/code representing its final or primary failure reason. For multiple failures within a job, use the failed transfer-item panel."; File = "queries/client/06_error_distribution.kql"; Visual = "bar"; X = 0; Y = 6; W = 11; H = 9 },
    @{ Page = "Reliability"; Title = "Failed transfer-item error codes (per item)"; Description = "Transfer-item view. Expands the bounded error histogram across failed items within each job. One job may contribute multiple error codes and counts; items are normally files/objects and can include failed folder-property transfers."; File = "queries/client/17_failed_object_error_codes.kql"; Visual = "bar"; X = 11; Y = 6; W = 11; H = 9 },
    @{ Page = "Reliability"; Title = "Newly observed error codes (30 days)"; File = "queries/client/19_new_error_codes_30d.kql"; Visual = "table"; X = 0; Y = 15; W = 11; H = 9 },
    @{ Page = "Reliability"; Title = "Job lifecycle gaps and cancellation progress"; Description = "Job-level lifecycle view. Eligible starts are at least 30 minutes old. A start without a finish after that grace period is a telemetry/abandonment proxy, not proof the job was abandoned. Cancellation rows show completion percentage for jobs explicitly reported as Cancelled."; File = "queries/client/16_abandonment_and_cancellation.kql"; Visual = "table"; X = 11; Y = 15; W = 11; H = 9 },

    @{ Page = "Adoption"; Title = "Weekly job frequency and change"; File = "queries/client/14_weekly_job_frequency.kql"; Visual = "timechart"; X = 0; Y = 0; W = 22; H = 8 },
    @{ Page = "Adoption"; Title = "Observed sampled-installation funnel (proxy)"; File = "queries/client/15_observed_installation_funnel.kql"; Visual = "multistat"; X = 0; Y = 8; W = 22; H = 7 },

    @{ Page = "Data Quality"; Title = "Telemetry acceptance"; File = "queries/client/08_telemetry_quality.kql"; Visual = "multistat"; X = 0; Y = 0; W = 22; H = 6 },
    @{ Page = "Data Quality"; Title = "Rejected and suspect attempts"; File = "queries/client/12_telemetry_rejections.kql"; Visual = "table"; X = 0; Y = 6; W = 22; H = 10 },
    @{ Page = "Data Quality"; Title = "Per-attempt Storage request evidence"; File = "queries/server/05_job_storage_evidence.kql"; Visual = "table"; X = 0; Y = 16; W = 22; H = 10 },

    @{ Page = "Customer Drilldown"; Title = "Selected account business summary (proxy)"; File = "queries/client/13_account_business_summary.kql"; Visual = "multistat"; X = 0; Y = 0; W = 22; H = 6 },
    @{ Page = "Customer Drilldown"; Title = "Observed job attempts"; File = "queries/client/09_account_drilldown.kql"; Visual = "table"; X = 0; Y = 6; W = 22; H = 10 },
    @{ Page = "Customer Drilldown"; Title = "Top source and destination movers"; File = "queries/client/11_top_account_movers.kql"; Visual = "bar"; X = 0; Y = 16; W = 12; H = 9 },
    @{ Page = "Customer Drilldown"; Title = "Account ownership"; File = "queries/server/03_account_ownership.kql"; Visual = "table"; X = 12; Y = 16; W = 10; H = 9 },

    @{ Page = "Server Correlation"; Title = "Server-observed AzCopy requests"; File = "queries/server/01_xagg_azcopy_requests.kql"; Visual = "timechart"; X = 0; Y = 0; W = 22; H = 8 },
    @{ Page = "Server Correlation"; Title = "Storage API operation mix"; File = "queries/server/02_storage_operation_mix.kql"; Visual = "bar"; X = 0; Y = 8; W = 10; H = 10 },
    @{ Page = "Server Correlation"; Title = "Requests per estimated job"; File = "queries/server/04_client_server_account_hour.kql"; Visual = "table"; X = 10; Y = 8; W = 12; H = 10 }
)

foreach ($item in $manifest) {
    if ($item.W -lt 3 -or $item.H -lt 6) {
        throw "Tile '$($item.Title)' is $($item.W)x$($item.H); ADX requires at least 3x6."
    }
}

foreach ($page in $pages.name) {
    $pageTiles = @($manifest | Where-Object Page -eq $page)
    for ($leftIndex = 0; $leftIndex -lt $pageTiles.Count; $leftIndex++) {
        for ($rightIndex = $leftIndex + 1; $rightIndex -lt $pageTiles.Count; $rightIndex++) {
            $left = $pageTiles[$leftIndex]
            $right = $pageTiles[$rightIndex]
            $overlaps =
                $left.X -lt ($right.X + $right.W) -and
                ($left.X + $left.W) -gt $right.X -and
                $left.Y -lt ($right.Y + $right.H) -and
                ($left.Y + $left.H) -gt $right.Y
            if ($overlaps) {
                throw "Tiles '$($left.Title)' and '$($right.Title)' overlap on page '$page'."
            }
        }
    }
}

$queries = @()
$tiles = @()
foreach ($item in $manifest) {
    $queryId = New-StableGuid "azcopy-business-metrics:query:$($item.File)"
    $tileId = New-StableGuid "azcopy-business-metrics:tile:$($item.File)"
    $text = Read-Query $item.File
    $usedVariables = @("_startTime", "_endTime")
    if ($text.Contains("_account")) { $usedVariables += "_account" }

    $queries += [ordered]@{
        dataSource = [ordered]@{ kind = "inline"; dataSourceId = $dataSourceId }
        text = $text
        id = $queryId
        usedVariables = $usedVariables
    }

    $tiles += [ordered]@{
        id = $tileId
        title = $item.Title
        description = if ($item.ContainsKey("Description")) { $item.Description } else { "" }
        visualType = $item.Visual
        pageId = $pageIds[$item.Page]
        layout = [ordered]@{ x = $item.X; y = $item.Y; width = $item.W; height = $item.H }
        queryRef = [ordered]@{ kind = "query"; queryId = $queryId }
        visualOptions = New-VisualOptions $item.Visual
    }
}

$dashboard = [ordered]@{
    '$schema' = "https://dataexplorer.azure.com/static/d/schema/60/dashboard.json"
    id = New-StableGuid "azcopy-business-metrics:dashboard"
    eTag = New-StableGuid "azcopy-business-metrics:etag:v1"
    title = "AzCopy Business Metrics"
    schema_version = "60"
    tiles = $tiles
    baseQueries = @()
    parameters = @(
        [ordered]@{
            kind = "duration"
            id = New-StableGuid "azcopy-business-metrics:parameter:time-range"
            displayName = "Time range"
            description = ""
            beginVariableName = "_startTime"
            endVariableName = "_endTime"
            defaultValue = [ordered]@{ kind = "dynamic"; count = 24; unit = "hours" }
            showOnPages = [ordered]@{ kind = "all" }
        },
        [ordered]@{
            kind = "string"
            selectionType = "freetext"
            id = New-StableGuid "azcopy-business-metrics:parameter:account"
            displayName = "Storage account"
            variableName = "_account"
            description = "Optional source or destination storage account filter. Leave empty for all accounts."
            defaultValue = [ordered]@{ kind = "value"; value = "" }
            showOnPages = [ordered]@{ kind = "all" }
        }
    )
    dataSources = @(
        [ordered]@{
            id = $dataSourceId
            kind = "manual-kusto"
            scopeId = "kusto"
            name = "AzCopy Analytics"
            clusterUri = "https://azcore.centralus.kusto.windows.net/"
            database = "Xstore"
        }
    )
    pages = $pages
    queries = $queries
}

$dashboard | ConvertTo-Json -Depth 30 | Set-Content -Path $OutputPath -Encoding utf8
Write-Host "Generated $OutputPath with $($tiles.Count) tiles across $($pages.Count) pages."