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
        $text = [regex]::Replace(
            $text,
            '(?m)^(\s*)customEvents\b',
            { param($match) $match.Groups[1].Value + $appInsightsTable })

        if ($text -match '(?m)^\s*customEvents\b') {
            throw "Client query '$RelativePath' contains an unqualified customEvents reference."
        }
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
    @{ Page = "Overview"; Title = "Overview KPI definitions"; MarkdownText = "**KPI** means key performance indicator: each card is one headline value for the selected range. **Observed attempts** is the number of finished telemetry attempts received. **Estimated attempts/data** apply inverse `SamplingRate` weighting; when every event has `SamplingRate=1`, estimated equals observed and is not an independent measurement. **Completion rate** counts `Completed` and `CompletedWithSkipped` finished jobs. **Unmatched starts** have a start event but no finish event in the selected range."; Visual = "markdownCard"; X = 0; Y = 0; W = 22; H = 4 },
    @{ Page = "Overview"; Title = "Overview KPIs"; File = "queries/client/01_overview_cards.kql"; Visual = "multistat"; X = 0; Y = 4; W = 22; H = 6 },
    @{ Page = "Overview"; Title = "Volume by topology"; File = "queries/client/02_volume_topology_trend.kql"; Visual = "stackedcolumn"; X = 0; Y = 10; W = 12; H = 8 },
    @{ Page = "Overview"; Title = "Outcomes by command"; File = "queries/client/03_outcome_distribution.kql"; Visual = "bar"; X = 12; Y = 10; W = 10; H = 8 },
    @{ Page = "Overview"; Title = "Weekly command and version trend"; File = "queries/client/10_weekly_command_version_trend.kql"; Visual = "stackedcolumn"; X = 0; Y = 18; W = 22; H = 8 },

    @{ Page = "Performance"; Title = "Performance percentiles"; File = "queries/client/04_performance_percentiles.kql"; Visual = "table"; X = 0; Y = 0; W = 22; H = 8 },
    @{ Page = "Performance"; Title = "Platform and version mix"; File = "queries/client/07_platform_mix.kql"; Visual = "bar"; X = 0; Y = 8; W = 22; H = 9 },
    @{ Page = "Performance"; Title = "Source and destination platform mix"; File = "queries/client/18_endpoint_platform_mix.kql"; Visual = "bar"; X = 0; Y = 17; W = 22; H = 9 },

    @{ Page = "Reliability"; Title = "Reliability-rate definitions"; MarkdownText = "**Completion rate** is `Completed` plus `CompletedWithSkipped` jobs divided by finished attempts. **Partial success rate** is `CompletedWithErrors` jobs divided by finished attempts. Failure and cancellation rates also use finished attempts. Failed-object rate uses scheduled objects; server-busy and network-error rates use Storage HTTP attempts; resume-success rate uses resume attempts. Because denominators differ, these percentages do not all add to 100%."; Visual = "markdownCard"; X = 0; Y = 0; W = 22; H = 4 },
    @{ Page = "Reliability"; Title = "Reliability rates"; File = "queries/client/05_reliability_cards.kql"; Visual = "multistat"; X = 0; Y = 4; W = 22; H = 6 },
    @{ Page = "Reliability"; Title = "Error-panel scope"; MarkdownText = "**Terminal errors** is job-level: each unsuccessful job contributes one final/primary category and code. The values are emitted telemetry strings, not a fixed dashboard enum. **Failed transfer-item error codes** is item-level: it expands the bounded per-job error histogram, so one job can contribute multiple codes and counts across failed files/objects (and some folder-property transfers). Neither panel counts individual HTTP failures."; Visual = "markdownCard"; X = 0; Y = 10; W = 22; H = 4 },
    @{ Page = "Reliability"; Title = "Terminal errors (one reason per job)"; Description = "Job-level view. Each unsuccessful job contributes one terminal category/code representing its final or primary failure reason. For multiple failures within a job, use the failed transfer-item panel."; File = "queries/client/06_error_distribution.kql"; Visual = "bar"; X = 0; Y = 14; W = 11; H = 9 },
    @{ Page = "Reliability"; Title = "Failed transfer-item error codes (per item)"; Description = "Transfer-item view. Expands the bounded error histogram across failed items within each job. One job may contribute multiple error codes and counts; items are normally files/objects and can include failed folder-property transfers."; File = "queries/client/17_failed_object_error_codes.kql"; Visual = "bar"; X = 11; Y = 14; W = 11; H = 9 },
    @{ Page = "Reliability"; Title = "New-code and lifecycle definitions"; MarkdownText = "**Newly observed** means an error code's first event in the retained 730-day lookback occurred during the last 30 days; it does not prove the code never occurred elsewhere. **Starts eligible after 30 min** are old enough for the missing-finish check. An eligible start without a finish is an abandonment/telemetry proxy, not proof of abandonment. Cancellation values describe jobs explicitly reported as `Cancelled` and their completion percentage at cancellation."; Visual = "markdownCard"; X = 0; Y = 23; W = 22; H = 4 },
    @{ Page = "Reliability"; Title = "Newly observed error codes (30 days)"; File = "queries/client/19_new_error_codes_30d.kql"; Visual = "table"; X = 0; Y = 27; W = 11; H = 9 },
    @{ Page = "Reliability"; Title = "Job lifecycle gaps and cancellation progress"; Description = "Job-level lifecycle view. Eligible starts are at least 30 minutes old. A start without a finish after that grace period is a telemetry/abandonment proxy, not proof the job was abandoned. Cancellation rows show completion percentage for jobs explicitly reported as Cancelled."; File = "queries/client/16_abandonment_and_cancellation.kql"; Visual = "table"; X = 11; Y = 27; W = 11; H = 9 },

    @{ Page = "Adoption"; Title = "Weekly job frequency and change"; File = "queries/client/14_weekly_job_frequency.kql"; Visual = "timechart"; X = 0; Y = 0; W = 22; H = 8 },
    @{ Page = "Adoption"; Title = "Installation-behavior definition"; MarkdownText = "This panel groups finished jobs in the selected range by pseudonymous **InstallationID**. InstallationID contributes to this installation-behavior analysis, not to the other aggregate business metrics. The rates describe overlapping behaviors: first observed attempt succeeded, 2+ attempts observed, and any observed attempt succeeded. This is not a sequential acquisition funnel or first-ever/second-ever job analysis because jobs outside the selected range are invisible."; Visual = "markdownCard"; X = 0; Y = 8; W = 22; H = 4 },
    @{ Page = "Adoption"; Title = "Observed installation behavior (range proxy)"; Description = "Unique InstallationIDs with finished jobs in the selected range. Rates show whether the first observed attempt succeeded, whether 2+ attempts were observed, and whether any observed attempt succeeded. Time cards measure first observed attempt to first observed success. This is not a true acquisition funnel or first-ever/second-ever job analysis because jobs outside the selected range are invisible."; File = "queries/client/15_observed_installation_funnel.kql"; Visual = "multistat"; X = 0; Y = 12; W = 22; H = 7 },

    @{ Page = "Data Quality"; Title = "Acceptance definition"; MarkdownText = "**Accepted** is a client-telemetry integrity result only. It requires one supported schema (v2 or v3), exactly one start and one finish, required fields, a complete finish metric set, a finish after its start, and a reported duration within the allowed tolerance (the greater of 60 seconds or 20%). Storage request evidence is not considered."; Visual = "markdownCard"; X = 0; Y = 0; W = 22; H = 3 },
    @{ Page = "Data Quality"; Title = "Telemetry acceptance"; Description = "Client-telemetry integrity only; Storage request evidence is not considered. Accepted means the attempt uses one supported schema (v2 or v3), has exactly one start and one finish, includes required fields and a complete finish metric set, finishes after it starts, and reports a duration within the allowed tolerance (the greater of 60 seconds or 20%)."; File = "queries/client/08_telemetry_quality.kql"; Visual = "multistat"; X = 0; Y = 3; W = 22; H = 6 },
    @{ Page = "Data Quality"; Title = "Rejected and suspect definitions"; MarkdownText = "**Rejected (`InvalidSchema`)** means the attempt used an unsupported or mixed schema. **Suspect** means a supported-schema attempt has a missing or duplicate start/finish, missing required fields, an incomplete finish metric set, a finish before its start, or a duration mismatch. These are client-telemetry checks only; Storage request evidence is not considered. Valid attempts are omitted from the table."; Visual = "markdownCard"; X = 0; Y = 9; W = 22; H = 3 },
    @{ Page = "Data Quality"; Title = "Rejected and suspect attempts"; Description = "Client-telemetry integrity only; Storage request evidence is not considered. InvalidSchema is rejected because its schema is unsupported or mixed. Suspect means a supported-schema attempt has a missing or duplicate start/finish, missing required fields, an incomplete finish metric set, a finish before its start, or a duration mismatch. Valid attempts are omitted from this table."; File = "queries/client/12_telemetry_rejections.kql"; Visual = "table"; X = 0; Y = 12; W = 22; H = 10 },
    @{ Page = "Data Quality"; Title = "Storage request evidence definition"; MarkdownText = "This is an **independent corroboration check**; it does not change client acceptance or filter business metrics. Each structurally valid attempt produces one row per recognized Azure Storage endpoint. **SupportingEvidence** means at least one server-observed AzCopy request matched the same account within the expanded five-minute job window. **Suspect** means a recognized account had no matching request. **InsufficientEvidence** means no recognized Azure Storage account was available. Storage requests have no AzCopy JobID, so account/time matching is supporting evidence, not exact job attribution."; Visual = "markdownCard"; X = 0; Y = 22; W = 22; H = 4 },
    @{ Page = "Data Quality"; Title = "Per-attempt Storage request evidence"; Description = "Independent corroboration check; it does not change client acceptance or filter business metrics. Each structurally valid attempt produces one row per recognized Azure Storage endpoint. SupportingEvidence means at least one server-observed AzCopy request matched the same account within the expanded five-minute job window; Suspect means a recognized account had no matching request; InsufficientEvidence means no recognized Azure Storage account was available. Storage requests have no AzCopy JobID, so account/time matching is supporting evidence, not exact job attribution."; File = "queries/server/05_job_storage_evidence.kql"; Visual = "table"; X = 0; Y = 26; W = 22; H = 10 },

    @{ Page = "Customer Drilldown"; Title = "Account-proxy definition"; MarkdownText = "A Storage account is an endpoint/resource proxy, not a unique customer identity. The account filter matches either Azure Storage endpoint. Observed-attempt rows are received telemetry records; estimated values apply inverse `SamplingRate` weighting. Source and destination mover rows must remain separate because one service-to-service job can contribute to both. Account ownership is server-side Storage metadata, and its logical tenant is not the customer's Microsoft Entra tenant ID."; Visual = "markdownCard"; X = 0; Y = 0; W = 22; H = 4 },
    @{ Page = "Customer Drilldown"; Title = "Selected account business summary (proxy)"; File = "queries/client/13_account_business_summary.kql"; Visual = "multistat"; X = 0; Y = 4; W = 22; H = 6 },
    @{ Page = "Customer Drilldown"; Title = "Observed job attempts"; File = "queries/client/09_account_drilldown.kql"; Visual = "table"; X = 0; Y = 10; W = 22; H = 10 },
    @{ Page = "Customer Drilldown"; Title = "Top source and destination movers"; File = "queries/client/11_top_account_movers.kql"; Visual = "bar"; X = 0; Y = 20; W = 12; H = 9 },
    @{ Page = "Customer Drilldown"; Title = "Account ownership"; File = "queries/server/03_account_ownership.kql"; Visual = "table"; X = 12; Y = 20; W = 10; H = 9 },
    @{ Page = "Customer Drilldown"; Title = "Customer-tenant definition"; MarkdownText = "**Customer tenant** means the Microsoft Entra tenant that owns the ARM subscription owning a Storage account. It is an ownership grouping, not a canonical commercial or contractual customer name. Account ownership is selected as of each activity day from XStore; subscription-to-tenant mapping uses the latest non-deleted ARG 1P record in the required 60-hour window. **Subscription names** are ARG display labels for the owning subscriptions; they are not tenant or customer names and can change. **Mapped** rows have both joins. MissingAccountOwnership and MissingTenantInventory expose coverage gaps. Friendly tenant names are intentionally not inferred. Source and destination roles remain separate and must not be summed as global volume."; Visual = "markdownCard"; X = 0; Y = 29; W = 22; H = 4 },
    @{ Page = "Customer Drilldown"; Title = "Top customer tenants and subscriptions"; Description = "Ranks Microsoft Entra tenant IDs for resource-owning subscriptions and shows example ARG subscription display names. Account ownership is joined as of each activity day; subscription inventory is the latest non-deleted ARG 1P record in the required 60-hour window. Subscription names are mutable display labels, not tenant/customer names. MappingStatus exposes missing ownership or tenant inventory. Friendly tenant names require a separately approved enrichment source. Source and destination roles remain separate."; File = "queries/server/06_top_customer_tenants.kql"; Visual = "table"; X = 0; Y = 33; W = 22; H = 10 },

    @{ Page = "Server Correlation"; Title = "Server-data provenance"; MarkdownText = "**Server-observed AzCopy requests** comes from `XAggUserAgentTelemetryMetric()` in the XStore database and identifies AzCopy from Storage's observed User-Agent telemetry. **Storage API operation mix** comes from `XStoreAccountTransactionsHourly` in XDataAnalytics; it is account transaction telemetry and can include non-AzCopy callers. These are server-side request aggregates, not client job events."; Visual = "markdownCard"; X = 0; Y = 0; W = 22; H = 4 },
    @{ Page = "Server Correlation"; Title = "Server-observed AzCopy requests"; File = "queries/server/01_xagg_azcopy_requests.kql"; Visual = "timechart"; X = 0; Y = 4; W = 22; H = 8 },
    @{ Page = "Server Correlation"; Title = "Aggregate-correlation definition"; MarkdownText = "**Requests per estimated job** joins client finished jobs and server-observed AzCopy requests by destination account and hour. Storage requests do not contain AzCopy JobID, so this is aggregate supporting context, not exact job attribution. Estimated jobs apply inverse `SamplingRate` weighting; with `SamplingRate=1`, estimated jobs equal observed jobs. The full outer join intentionally shows hours present on only one side."; Visual = "markdownCard"; X = 0; Y = 12; W = 22; H = 4 },
    @{ Page = "Server Correlation"; Title = "Storage API operation mix"; File = "queries/server/02_storage_operation_mix.kql"; Visual = "bar"; X = 0; Y = 16; W = 10; H = 10 },
    @{ Page = "Server Correlation"; Title = "Requests per estimated job"; File = "queries/server/04_client_server_account_hour.kql"; Visual = "table"; X = 10; Y = 16; W = 12; H = 10 }
)

foreach ($item in $manifest) {
    $minimumWidth = if ($item.Visual -eq "markdownCard") { 2 } else { 3 }
    $minimumHeight = if ($item.Visual -eq "markdownCard") { 1 } else { 6 }
    if ($item.W -lt $minimumWidth -or $item.H -lt $minimumHeight) {
        throw "Tile '$($item.Title)' is $($item.W)x$($item.H); '$($item.Visual)' requires at least ${minimumWidth}x${minimumHeight}."
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
    if ($item.Visual -eq "markdownCard") {
        $tiles += [ordered]@{
            id = New-StableGuid "azcopy-business-metrics:tile:markdown:$($item.Title)"
            title = $item.Title
            description = ""
            visualType = $item.Visual
            pageId = $pageIds[$item.Page]
            layout = [ordered]@{ x = $item.X; y = $item.Y; width = $item.W; height = $item.H }
            markdownText = $item.MarkdownText
            visualOptions = [ordered]@{}
        }
        continue
    }

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