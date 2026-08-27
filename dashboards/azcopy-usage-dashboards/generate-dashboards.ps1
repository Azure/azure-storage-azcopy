[CmdletBinding()]
param(
    [string]$AppInsightsResourceId = "/subscriptions/31347be8-d066-464e-9866-7e58d85027b7/resourceGroups/sharankur_playground/providers/Microsoft.Insights/components/sharankur_insights1",
    [string]$AppInsightsDatabase = "sharankur_insights1",
    [string]$OutputSuffix = "",
    [switch]$EnableLiveArgEnrichment = $true,
    [switch]$EnableAipddEnrichment = $true
)

$ErrorActionPreference = "Stop"

if ($args.Count -gt 0) {
    throw "Unrecognized arguments: $($args -join ' ')"
}

$root = Split-Path -Parent $PSScriptRoot
$dataRoot = Join-Path $root "azcopy-data-metrics"
$outputRoot = Join-Path $PSScriptRoot "generated"
$grafanaDatasource = [ordered]@{
    type = "grafana-azure-data-explorer-datasource"
    uid = "azcopy-xstore"
}
$grafanaPluginVersion = "7.2.6"
$grafanaBaseUrl = "https://azcopy-telemetry-ankur-cbbcech2ecd9gad6.eus.grafana.azure.com"

New-Item -ItemType Directory -Force -Path $outputRoot | Out-Null

function New-StableGuid([string]$Value) {
    $bytes = [Text.Encoding]::UTF8.GetBytes($Value)
    $hash = [Security.Cryptography.SHA256]::HashData($bytes)
    $guidBytes = New-Object byte[] 16
    [Array]::Copy($hash, $guidBytes, 16)
    return ([Guid]::new($guidBytes)).ToString()
}

function Get-AppInsightsExpression {
    $resourceId = $AppInsightsResourceId.Trim("/").ToLowerInvariant()
    return "cluster('https://adx.monitor.azure.com/$resourceId').database('$AppInsightsDatabase').customEvents"
}

function Get-EnrichedJobsText([bool]$UseAipddEnrichment) {
    $text = Get-Content -Raw (Join-Path $dataRoot "queries\common\enriched_finished_jobs.kql")
    $aipddSource = if ($EnableAipddEnrichment -and $UseAipddEnrichment) {
        "cluster('https://aipddprod.kusto.windows.net').database('AIPDD_Usage').SubscriptionSnapshotV2"
    } else {
        "datatable(SubscriptionGuid:string, FriendlySubscriptionName:string, AI_OfferType:string, TPID:string, TPName:string, CurrentSubscriptionStatus:string, AI_SubscriptionBusinessStatus:string, AI_UpdatedAt:datetime)[]"
    }
    return $text.
        Replace("__APP_INSIGHTS_CUSTOM_EVENTS__", (Get-AppInsightsExpression)).
        Replace("__XSTORE_ACCOUNT_PROPERTIES__", "cluster('https://xdataanalytics.westcentralus.kusto.windows.net').database('XDataAnalytics').XStoreAccountPropertiesDaily").
        Replace("__AIPDD_SUBSCRIPTION_SNAPSHOT__", $aipddSource).
        Replace("__ARG_RESOURCES__", "cluster('https://argeusarm1pone.eastus.kusto.windows.net').database('AzureResourceGraph').Resources").
        Replace("__ARG_SUBSCRIPTIONS__", "cluster('https://argeusarm1pone.eastus.kusto.windows.net').database('AzureResourceGraph').InternalSubscriptionResources").
        Replace("__ENABLE_LIVE_ARG_ENRICHMENT__", $(if ($EnableLiveArgEnrichment) { "true" } else { "false" })).
        Replace("__ENABLE_AIPDD_ENRICHMENT__", $(if ($EnableAipddEnrichment -and $UseAipddEnrichment) { "true" } else { "false" }))
}

$adxAdaptationFilters = @'
let FilteredJobs = materialize(
    EnrichedFinishedJobs
    | where isempty(_installationID) or InstallationID =~ _installationID
    | where isempty(_account)
        or SourceAccount =~ _account
        or DestinationAccount =~ _account
    | where InvocationContext in (_invocationContext) or isempty(_invocationContext)
    | where SourceType in (_sourceType) or isempty(_sourceType)
    | where DestType in (_destType) or isempty(_destType)
    | where SubscriptionScope in (_subscriptionScope) or isempty(_subscriptionScope)
    {{CUSTOMER_FILTER}}
);
'@

$grafanaAdaptationFilters = @'
let FilteredJobs = materialize(
    EnrichedFinishedJobs
    | where isempty('${InstallationID}') or InstallationID =~ '${InstallationID}'
    | where isempty('${Account}')
        or SourceAccount =~ '${Account}'
        or DestinationAccount =~ '${Account}'
    | where $__contains(InvocationContext, ${InvocationContext})
    | where $__contains(SourceType, ${SourceType})
    | where $__contains(DestType, ${DestType})
    | where $__contains(SubscriptionScope, ${SubscriptionScope})
    {{CUSTOMER_FILTER}}
);
'@

function Expand-Query(
    [string]$Text,
    [ValidateSet("adx", "grafana")] [string]$Target,
    [bool]$UseAipddEnrichment = $false
) {
    $adaptationFilters = if ($Target -eq "adx") {
        $adxAdaptationFilters.Replace(
            "{{CUSTOMER_FILTER}}",
            $(if ($UseAipddEnrichment) { "| where CustomerKey in (_customer) or isempty(_customer)" } else { "" }))
    } else {
        $grafanaAdaptationFilters.Replace(
            "{{CUSTOMER_FILTER}}",
            $(if ($UseAipddEnrichment) { '| where $__contains(CustomerKey, ${Customer})' } else { "" }))
    }
    $query = $Text.
        Replace("__APP_INSIGHTS_CUSTOM_EVENTS__", (Get-AppInsightsExpression)).
        Replace("{{ENRICHED_FINISHED_JOBS}}", (Get-EnrichedJobsText $UseAipddEnrichment)).
        Replace("{{ADAPTATION_FILTERS}}", $adaptationFilters)

    if ($Target -eq "grafana") {
        $query = $query.
            Replace("let QueryStart = _startTime;", 'let QueryStart = datetime(${__from:date});').
            Replace("let QueryEnd = _endTime;", 'let QueryEnd = datetime(${__to:date});')
    }

    return $query.Trim()
}

function Expand-DataQuery([string]$Path, [ValidateSet("adx", "grafana")] [string]$Target) {
    $text = Get-Content -Raw (Join-Path $dataRoot $Path)
    $usesSharedProjection = $text.Contains("{{ENRICHED_FINISHED_JOBS}}")
    $text = $text.
        Replace("{{ENRICHED_FINISHED_JOBS}}", (Get-EnrichedJobsText $true)).
        Replace("{{ENRICHMENT_FILTERS}}", (Get-Content -Raw (Join-Path $dataRoot "queries\common\enrichment_filters.kql")))
    if (-not $usesSharedProjection) {
        $text = $text.Replace("customEvents", (Get-AppInsightsExpression))
    }

    if ($Target -eq "grafana") {
        $grafanaFilters = @'
let FilteredFinishedJobs = materialize(
    EnrichedFinishedJobs
    | where $__contains(SourceType, ${SourceType})
    | where $__contains(DestType, ${DestType})
    | where $__contains(FromTo, ${FromTo})
    | where $__contains(SourceMountType, ${SourceMountType})
    | where $__contains(DestEndpointKind, ${DestEndpointKind})
    | where $__contains(ClientRegion, ${ClientRegion})
    | where $__contains(DestinationSubscriptionKey, ${DestinationSubscription})
    | where $__contains(CustomerKey, ${Customer})
    | where $__contains(OfferType, ${OfferType})
    | where $__contains(SubscriptionScope, ${SubscriptionScope})
    | where isempty('${Account}')
        or SourceAccount =~ '${Account}'
        or DestinationAccount =~ '${Account}'
);
'@
        $adxFilters = Get-Content -Raw (Join-Path $dataRoot "queries\common\enrichment_filters.kql")
        $text = $text.
            Replace($adxFilters.Trim(), $grafanaFilters.Trim()).
            Replace("let QueryStart = _startTime;", 'let QueryStart = datetime(${__from:date});').
            Replace("let QueryEnd = _endTime;", 'let QueryEnd = datetime(${__to:date});').
            Replace("timestamp between (_startTime .. _endTime)", 'timestamp between (datetime(${__from:date}) .. datetime(${__to:date}))')
    }

    return $text.Trim()
}

function New-QueryVariable([string]$Name, [string]$Label, [string]$Query) {
    return [ordered]@{
        name = $Name
        label = $Label
        type = "query"
        datasource = $grafanaDatasource
        definition = $Query
        query = [ordered]@{
            queryType = "KQL"
            querySource = "raw"
            rawMode = $true
            database = "Xstore"
            query = $Query
            resultFormat = "table"
            pluginVersion = $grafanaPluginVersion
        }
        current = [ordered]@{ text = "All"; value = @('$__all') }
        includeAll = $true
        multi = $true
        refresh = 1
        sort = 1
        options = @()
        regex = ""
    }
}

function New-TextVariable([string]$Name, [string]$Label) {
    return [ordered]@{
        name = $Name
        label = $Label
        type = "textbox"
        current = [ordered]@{ text = ""; value = "" }
        options = @()
        query = ""
    }
}

function New-GrafanaTarget([string]$Query, [string]$Format = "table") {
    return [ordered]@{
        refId = "A"
        datasource = $grafanaDatasource
        database = "Xstore"
        queryType = "KQL"
        querySource = "raw"
        rawMode = $true
        query = $Query
        resultFormat = $Format
        pluginVersion = $grafanaPluginVersion
    }
}

function New-GrafanaPanel([hashtable]$Definition, [int]$Id, [bool]$UseAipddEnrichment = $false) {
    $type = switch ($Definition.Visual) {
        "multistat" { "stat" }
        "timechart" { "timeseries" }
        "bar" { "barchart" }
        "funnel" { "bargauge" }
        "markdownCard" { "text" }
        default { "table" }
    }
    $panel = [ordered]@{
        id = $Id
        title = $Definition.Title
        description = $Definition.Description
        type = $type
        pluginVersion = "12.4.6"
        gridPos = [ordered]@{
            x = [int]($Definition.X * 24 / 22)
            y = $Definition.Y
            w = [Math]::Max(1, [int][Math]::Round($Definition.W * 24 / 22))
            h = $Definition.H
        }
        fieldConfig = [ordered]@{
            defaults = [ordered]@{
                color = [ordered]@{ mode = "palette-classic" }
                custom = [ordered]@{}
                mappings = @()
                thresholds = [ordered]@{
                    mode = "absolute"
                    steps = @(
                        [ordered]@{ color = "green"; value = $null }
                    )
                }
            }
            overrides = @()
        }
        options = [ordered]@{}
    }

    if ($type -eq "text") {
        $panel.options = [ordered]@{ mode = "markdown"; content = $Definition.MarkdownText }
        return $panel
    }

    $format = if ($type -eq "timeseries") { "time_series" } else { "table" }
    $panel.targets = @(New-GrafanaTarget (Expand-Query $Definition.Query "grafana" $UseAipddEnrichment) $format)

    switch ($type) {
        "stat" {
            $panel.options = [ordered]@{
                reduceOptions = [ordered]@{ values = $true; calcs = @("lastNotNull"); fields = "" }
                orientation = "horizontal"
                textMode = "auto"
                colorMode = "value"
                graphMode = "none"
                justifyMode = "auto"
                wideLayout = $true
            }
        }
        "timeseries" {
            $panel.options = [ordered]@{
                legend = [ordered]@{ displayMode = "table"; placement = "bottom"; calcs = @() }
                tooltip = [ordered]@{ mode = "multi"; sort = "desc" }
            }
            $panel.fieldConfig.defaults.custom = [ordered]@{
                drawStyle = "line"
                lineInterpolation = "linear"
                lineWidth = 1
                fillOpacity = 12
                showPoints = "auto"
                spanNulls = $false
            }
        }
        "barchart" {
            $panel.options = [ordered]@{
                orientation = "horizontal"
                showValue = "always"
                stacking = "none"
                xTickLabelRotation = 0
                xTickLabelSpacing = 0
                legend = [ordered]@{ displayMode = "table"; placement = "bottom"; calcs = @() }
                tooltip = [ordered]@{ mode = "multi"; sort = "desc" }
            }
        }
        "bargauge" {
            $panel.options = [ordered]@{
                displayMode = "gradient"
                orientation = "horizontal"
                reduceOptions = [ordered]@{ values = $true; calcs = @("lastNotNull"); fields = "" }
                showUnfilled = $true
                valueMode = "color"
            }
        }
        "table" {
            $panel.options = [ordered]@{
                showHeader = $true
                cellHeight = "sm"
                sortBy = @()
                footer = [ordered]@{ show = $false; reducer = @("sum"); countRows = $false; fields = "" }
            }
        }
    }

    if ($Definition.Unit) {
        $panel.fieldConfig.defaults.unit = $Definition.Unit
    }
    if ($Definition.Links) {
        $overrides = @()
        foreach ($link in $Definition.Links) {
            $overrides += [ordered]@{
                matcher = [ordered]@{ id = "byName"; options = $link.Field }
                properties = @(
                    [ordered]@{
                        id = "links"
                        value = @(
                            [ordered]@{
                                title = $link.Title
                                url = $link.Url
                                targetBlank = $false
                            }
                        )
                    }
                )
            }
        }
        $panel.fieldConfig.overrides = $overrides
    }
    return $panel
}

function New-AdxVisualOptions([string]$Visual) {
    if ($Visual -eq "multistat") {
        return [ordered]@{
            multiStat__textSize = "large"
            multiStat__valueColumn = "Value"
            multiStat__labelColumn = "Metric"
            multiStat__displayOrientation = "horizontal"
            colorRulesDisabled = $true
            colorStyle = "light"
            colorRules = @()
        }
    }
    if ($Visual -eq "table" -or $Visual -eq "funnel") {
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
    return [ordered]@{
        hideLegend = $false
        legendLocation = "bottom"
        xColumnTitle = ""
        xColumn = $null
        yColumns = $null
        seriesColumns = $null
        xAxisScale = "linear"
        crossFilterDisabled = $false
        drillthroughDisabled = $false
        crossFilter = @()
        drillthrough = @()
    }
}

function Get-AdaptationParameterQueries(
    [ValidateSet("adx", "grafana")] [string]$Target,
    [bool]$UseAipddEnrichment
) {
    $queries = [ordered]@{}
    $definitions = @(
        @{ Name = "InvocationContext"; Column = "InvocationContext" },
        @{ Name = "SourceType"; Column = "SourceType" },
        @{ Name = "DestType"; Column = "DestType" },
        @{ Name = "SubscriptionScope"; Column = "SubscriptionScope" }
    )
    if ($UseAipddEnrichment) {
        $definitions += @{ Name = "Customer"; Column = "CustomerKey" }
    }
    foreach ($definition in $definitions) {
        $projection = if ($definition.Name -eq "Customer") {
            @"
| where CustomerInventoryStatus == 'Mapped'
| summarize Label = any(CustomerName) by Value = CustomerKey
"@
        } else {
            @"
| distinct Value = $($definition.Column)
| project Value, Label = Value
"@
        }
        $text = @"
let QueryStart = $(if ($Target -eq "adx") { "_startTime" } else { 'datetime(${__from:date})' });
let QueryEnd = $(if ($Target -eq "adx") { "_endTime" } else { 'datetime(${__to:date})' });
{{ENRICHED_FINISHED_JOBS}}
EnrichedFinishedJobs
$projection
| order by Label asc
"@
        $queries[$definition.Name] = Expand-Query $text $Target $UseAipddEnrichment
    }
    return $queries
}

function New-AdxDashboard([hashtable]$Definition) {
    $prefix = "azcopy-usage:$($Definition.Name)"
    $pageId = New-StableGuid "${prefix}:page"
    $dataSourceId = New-StableGuid "${prefix}:datasource"
    $queries = @()
    $tiles = @()

    $useAipddEnrichment = [bool]$Definition.EnableCustomerEnrichment
    $parameterQueries = Get-AdaptationParameterQueries "adx" $useAipddEnrichment
    $parameters = @(
        [ordered]@{
            kind = "duration"
            id = New-StableGuid "${prefix}:parameter:time"
            displayName = "Time range"
            beginVariableName = "_startTime"
            endVariableName = "_endTime"
            defaultValue = [ordered]@{ kind = "dynamic"; count = 90; unit = "days" }
            showOnPages = [ordered]@{ kind = "all" }
        }
    )
    $queryVariables = @(
        @{ Name = "InvocationContext"; Variable = "_invocationContext"; Label = "Invocation context" },
        @{ Name = "SourceType"; Variable = "_sourceType"; Label = "Source type" },
        @{ Name = "DestType"; Variable = "_destType"; Label = "Destination type" },
        @{ Name = "SubscriptionScope"; Variable = "_subscriptionScope"; Label = "Destination subscription scope" }
    )
    if ($useAipddEnrichment) {
        $queryVariables += @{ Name = "Customer"; Variable = "_customer"; Label = "Customer" }
    }
    foreach ($variable in $queryVariables) {
        $queryId = New-StableGuid "${prefix}:parameter-query:$($variable.Name)"
        $queries += [ordered]@{
            dataSource = [ordered]@{ kind = "inline"; dataSourceId = $dataSourceId }
            text = $parameterQueries[$variable.Name]
            id = $queryId
            usedVariables = @("_startTime", "_endTime")
        }
        $parameters += [ordered]@{
            kind = "string"
            selectionType = "array"
            id = New-StableGuid "${prefix}:parameter:$($variable.Variable)"
            displayName = $variable.Label
            variableName = $variable.Variable
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
    foreach ($textVariable in @(
        @{ Variable = "_installationID"; Label = "Installation ID" },
        @{ Variable = "_account"; Label = "Storage account" }
    )) {
        $parameters += [ordered]@{
            kind = "string"
            selectionType = "freetext"
            id = New-StableGuid "${prefix}:parameter:$($textVariable.Variable)"
            displayName = $textVariable.Label
            variableName = $textVariable.Variable
            defaultValue = [ordered]@{ kind = "value"; value = "" }
            showOnPages = [ordered]@{ kind = "all" }
        }
    }

    $index = 0
    foreach ($item in $Definition.Panels) {
        $tile = [ordered]@{
            id = New-StableGuid "${prefix}:tile:${index}:$($item.Title)"
            title = $item.Title
            description = $item.Description
            visualType = $(if ($item.Visual -eq "funnel") { "table" } else { $item.Visual })
            pageId = $pageId
            layout = [ordered]@{ x = $item.X; y = $item.Y; width = $item.W; height = $item.H }
        }
        if ($item.Visual -eq "markdownCard") {
            $tile.markdownText = $item.MarkdownText
            $tile.visualOptions = [ordered]@{}
        } else {
            $queryId = New-StableGuid "${prefix}:query:${index}:$($item.Title)"
            $queries += [ordered]@{
                dataSource = [ordered]@{ kind = "inline"; dataSourceId = $dataSourceId }
                text = Expand-Query $item.Query "adx" $useAipddEnrichment
                id = $queryId
                usedVariables = @(
                    "_startTime",
                    "_endTime",
                    "_installationID",
                    "_account",
                    "_invocationContext",
                    "_sourceType",
                    "_destType",
                    "_subscriptionScope"
                ) + $(if ($useAipddEnrichment) { @("_customer") } else { @() })
            }
            $tile.queryRef = [ordered]@{ kind = "query"; queryId = $queryId }
            $tile.visualOptions = New-AdxVisualOptions $item.Visual
        }
        $tiles += $tile
        $index++
    }

    $dashboard = [ordered]@{
        '$schema' = "https://dataexplorer.azure.com/static/d/schema/60/dashboard.json"
        id = New-StableGuid "${prefix}:dashboard"
        eTag = New-StableGuid "${prefix}:etag"
        title = $Definition.Title
        schema_version = "60"
        tiles = $tiles
        dataSources = @(
            [ordered]@{
                id = $dataSourceId
                name = "AzCopy analytics"
                scopeId = "https://azcore.centralus.kusto.windows.net/Xstore"
                type = "kusto"
                clusterUri = "https://azcore.centralus.kusto.windows.net"
                database = "Xstore"
            }
        )
        pages = @([ordered]@{ name = $Definition.Title; id = $pageId })
        parameters = $parameters
        queries = $queries
    }
    $path = Join-Path $outputRoot "$($Definition.Name)$OutputSuffix.dashboard.json"
    $dashboard | ConvertTo-Json -Depth 100 | Set-Content -Encoding utf8 $path
}

function New-GrafanaDashboard([hashtable]$Definition) {
    $useAipddEnrichment = [bool]$Definition.EnableCustomerEnrichment
    $parameterQueries = Get-AdaptationParameterQueries "grafana" $useAipddEnrichment
    $variables = @(
        (New-TextVariable "InstallationID" "Installation ID"),
        (New-TextVariable "Account" "Storage account"),
        (New-QueryVariable "InvocationContext" "Invocation context" $parameterQueries.InvocationContext),
        (New-QueryVariable "SourceType" "Source type" $parameterQueries.SourceType),
        (New-QueryVariable "DestType" "Destination type" $parameterQueries.DestType),
        (New-QueryVariable "SubscriptionScope" "Destination subscription scope" $parameterQueries.SubscriptionScope)
    )
    if ($useAipddEnrichment) {
        $variables += New-QueryVariable "Customer" "Customer" $parameterQueries.Customer
    }
    $panels = @()
    for ($i = 0; $i -lt $Definition.Panels.Count; $i++) {
        $panels += New-GrafanaPanel $Definition.Panels[$i] ($i + 1) $useAipddEnrichment
    }
    $dashboard = [ordered]@{
        uid = $Definition.Uid
        title = $Definition.Title
        description = $Definition.Description
        tags = @("AzCopy", "Telemetry", "StorageMover-adaptation")
        schemaVersion = 42
        version = 1
        editable = $true
        fiscalYearStartMonth = 0
        graphTooltip = 1
        timezone = "browser"
        time = [ordered]@{ from = "now-90d"; to = "now" }
        timepicker = [ordered]@{}
        refresh = ""
        links = @()
        annotations = [ordered]@{ list = @() }
        templating = [ordered]@{ list = $variables }
        panels = $panels
    }
    $wrapper = [ordered]@{
        dashboard = $dashboard
        folderUid = ""
        message = "Generated AzCopy adaptation of Storage Mover $($Definition.Title)"
        overwrite = $true
    }
    $path = Join-Path $outputRoot "$($Definition.Name)$OutputSuffix.grafana.json"
    $wrapper | ConvertTo-Json -Depth 100 | Set-Content -Encoding utf8 $path
}

$customerUrl = "$grafanaBaseUrl/d/azcopy-customer-drilldown/azcopy-customer-drilldown"
$installationLink = @{
    Field = "InstallationID"
    Title = "Open installation drilldown"
    Url = "$customerUrl`${__url_time_range}&var-InstallationID=`${__value.raw}"
}
$accountLink = @{
    Field = "DestinationAccount"
    Title = "Open account drilldown"
    Url = "$customerUrl`${__url_time_range}&var-Account=`${__value.raw}"
}

$overviewQuery = @'
let QueryStart = _startTime;
let QueryEnd = _endTime;
{{ENRICHED_FINISHED_JOBS}}
{{ADAPTATION_FILTERS}}
FilteredJobs
| summarize
    ObservedJobs = dcount(JobID),
    ObservedInstallations = dcount(InstallationID),
    SuccessfulJobs = countif(JobStatus =~ 'Completed' and TransfersFailed == 0),
    FailedJobs = countif(JobStatus !~ 'Completed' or TransfersFailed > 0),
    BytesTransferred = sum(BytesTransferred),
    ObjectsCompleted = sum(ObjectsCompleted)
| project Metrics = pack_array(
    pack('Metric', 'Observed jobs', 'Value', todouble(ObservedJobs)),
    pack('Metric', 'Observed installations', 'Value', todouble(ObservedInstallations)),
    pack('Metric', 'Successful terminal jobs', 'Value', todouble(SuccessfulJobs)),
    pack('Metric', 'Failed terminal jobs', 'Value', todouble(FailedJobs)),
    pack('Metric', 'Data (TB)', 'Value', round(BytesTransferred / 1e12, 3)),
    pack('Metric', 'Completed objects', 'Value', round(ObjectsCompleted, 0)))
| mv-expand Metrics
| project Metric = tostring(Metrics.Metric), Value = todouble(Metrics.Value)
'@

$definitions = @(
    @{
        Name = "azcopy-customer-drilldown"
        Uid = "azcopy-customer-drilldown"
        Title = "AzCopy Customer Drilldown"
        Description = "Selected-range drilldown by approved server-side customer enrichment, pseudonymous installation ID, or Storage account."
        EnableCustomerEnrichment = $true
        Panels = @(
            @{
                Title = "How to read this dashboard"; Visual = "markdownCard"; Description = ""
                X = 0; Y = 0; W = 22; H = 5
                MarkdownText = "Customer names come from the approved AIPDD SubscriptionSnapshotV2 top-parent mapping after AzCopy-observed destination accounts resolve to subscriptions. The TPID is used only as the server-side filter key and is not shown as a label. Missing customer mappings remain explicit. InstallationID is pseudonymous and results are limited to the selected time range."
            },
            @{
                Title = "Selected installation/account summary"; Visual = "multistat"; Description = "Observed entities and payload totals after all filters."
                X = 0; Y = 5; W = 22; H = 6; Query = $overviewQuery
            },
            @{
                Title = "Daily terminal job activity"; Visual = "timechart"; Description = "Terminal jobs by status."
                X = 0; Y = 11; W = 11; H = 8; Query = @'
let QueryStart = _startTime;
let QueryEnd = _endTime;
{{ENRICHED_FINISHED_JOBS}}
{{ADAPTATION_FILTERS}}
FilteredJobs
| summarize Jobs = count() by timestamp = bin(EndTime, 1d), JobStatus
| order by timestamp asc
'@
            },
            @{
                Title = "Terminal outcomes"; Visual = "bar"; Description = "Terminal jobs grouped by final status."
                X = 11; Y = 11; W = 11; H = 8; Query = @'
let QueryStart = _startTime;
let QueryEnd = _endTime;
{{ENRICHED_FINISHED_JOBS}}
{{ADAPTATION_FILTERS}}
FilteredJobs
| summarize Jobs = count() by JobStatus
| order by Jobs desc
'@
            },
            @{
                Title = "Data by source-target pair"; Visual = "bar"; Description = "Logical bytes grouped by AzCopy topology."
                X = 0; Y = 19; W = 11; H = 8; Unit = "decbits"; Query = @'
let QueryStart = _startTime;
let QueryEnd = _endTime;
{{ENRICHED_FINISHED_JOBS}}
{{ADAPTATION_FILTERS}}
FilteredJobs
| summarize BytesTransferred = sum(BytesTransferred) by FromTo
| project FromTo, DataGB = round(BytesTransferred / 1e9, 3)
| order by DataGB desc
'@
            },
            @{
                Title = "AzCopy version and operating system"; Visual = "table"; Description = "Observed software and platform mix."
                X = 11; Y = 19; W = 11; H = 8; Query = @'
let QueryStart = _startTime;
let QueryEnd = _endTime;
{{ENRICHED_FINISHED_JOBS}}
{{ADAPTATION_FILTERS}}
FilteredJobs
| summarize ObservedJobs = dcount(JobID), ObservedInstallations = dcount(InstallationID) by AzCopyVersion, OSType
| order by ObservedJobs desc
'@
            },
            @{
                Title = "Terminal error codes"; Visual = "bar"; Description = "Observed error codes from failed or partially failed terminal jobs."
                X = 0; Y = 27; W = 11; H = 8; Query = @'
let QueryStart = _startTime;
let QueryEnd = _endTime;
{{ENRICHED_FINISHED_JOBS}}
{{ADAPTATION_FILTERS}}
FilteredJobs
| where JobStatus !~ 'Completed' or TransfersFailed > 0
| summarize ObservedJobs = dcount(JobID) by JobErrorCode
| top 20 by ObservedJobs desc
'@
            },
            @{
                Title = "Destination account profile"; Visual = "table"; Description = "Destination-centric current and historical enrichment."
                X = 11; Y = 27; W = 11; H = 8; Links = @($accountLink); Query = @'
let QueryStart = _startTime;
let QueryEnd = _endTime;
{{ENRICHED_FINISHED_JOBS}}
{{ADAPTATION_FILTERS}}
FilteredJobs
| where isnotempty(DestinationAccount)
| summarize
    ObservedJobs = dcount(JobID),
    DataTB = round(sum(BytesTransferred) / 1e12, 3),
    LastObserved = max(EndTime)
  by DestinationAccount, CustomerName, CustomerInventoryStatus, DestinationSubscription, OfferType, SubscriptionScope, StorageRegion, StorageKind, StorageSku, StorageRedundancy, StorageNamespace
| top 100 by DataTB desc
'@
            },
            @{
                Title = "Recent terminal jobs"; Visual = "table"; Description = "Latest 200 deduplicated terminal jobs."
                X = 0; Y = 35; W = 22; H = 10; Links = @($installationLink, $accountLink); Query = @'
let QueryStart = _startTime;
let QueryEnd = _endTime;
{{ENRICHED_FINISHED_JOBS}}
{{ADAPTATION_FILTERS}}
FilteredJobs
| project EndTime, InstallationID, JobID, Command, FromTo, SourceAccount, DestinationAccount, CustomerName, CustomerInventoryStatus, DestinationSubscription, OfferType, JobStatus, JobErrorCategory, JobErrorCode, BytesTransferred, ObjectsCompleted, ObjectsFailed, JobDurationSeconds, JobThroughputMbps
| top 200 by EndTime desc
'@
            }
        )
    },
    @{
        Name = "azcopy-observed-funnel"
        Uid = "azcopy-observed-funnel"
        Title = "AzCopy Observed Funnel"
        Description = "Short selected-range behavioral funnel based only on AzCopy telemetry stages."
        Panels = @(
            @{
                Title = "How to read this dashboard"; Visual = "markdownCard"; Description = ""
                X = 0; Y = 0; W = 22; H = 4
                MarkdownText = "This is an observed selected-range behavior funnel, not an acquisition, installation, or lifetime customer funnel. Counts are distinct pseudonymous InstallationID values from received telemetry."
            },
            @{
                Title = "Observed installation funnel"; Visual = "funnel"; Description = "Six telemetry-backed stages; the layout is intentionally shorter than the Storage Mover funnel."
                X = 0; Y = 4; W = 11; H = 9; Query = @'
let QueryStart = _startTime;
let QueryEnd = _endTime;
let Events = materialize(
    __APP_INSIGHTS_CUSTOM_EVENTS__
    | where timestamp between (QueryStart .. QueryEnd)
    | where tostring(customDimensions.SchemaVersion) in ('2', '3')
    | extend InstallationID = tostring(customDimensions.InstallationID), InvocationContext = tostring(customDimensions.InvocationContext)
    | where isnotempty(InstallationID)
);
{{ENRICHED_FINISHED_JOBS}}
let Commands = Events | where name == 'azcopy.command.invoked' | where InvocationContext in (_invocationContext) or isempty(_invocationContext);
let Starts = Events | where name == 'azcopy.job.started' | where InvocationContext in (_invocationContext) or isempty(_invocationContext);
let Terminal = EnrichedFinishedJobs | where InvocationContext in (_invocationContext) or isempty(_invocationContext);
union
    (Commands | summarize Installations = dcount(InstallationID) | extend StageOrder = 1, Stage = '1. Any command'),
    (Starts | summarize Installations = dcount(InstallationID) | extend StageOrder = 2, Stage = '2. Transfer job started'),
    (Terminal | summarize Installations = dcount(InstallationID) | extend StageOrder = 3, Stage = '3. Terminal job observed'),
    (Terminal | where JobStatus =~ 'Completed' and TransfersFailed == 0 | summarize Installations = dcount(InstallationID) | extend StageOrder = 4, Stage = '4. Successful terminal job'),
    (Terminal | where JobStatus =~ 'Completed' and TransfersFailed == 0 and BytesTransferred > 0 | summarize Installations = dcount(InstallationID) | extend StageOrder = 5, Stage = '5. Nonzero data transferred'),
    (Terminal | where JobStatus =~ 'Completed' and TransfersFailed == 0 and BytesTransferred > 1073741824 | summarize Installations = dcount(InstallationID) | extend StageOrder = 6, Stage = '6. More than 1 GiB transferred')
| order by StageOrder asc
| project Stage, Installations
'@
            },
            @{
                Title = "Stage retention and drop-off"; Visual = "table"; Description = "Retention versus the prior stage and versus all observed command installations."
                X = 11; Y = 4; W = 11; H = 9; Query = @'
let QueryStart = _startTime;
let QueryEnd = _endTime;
let Events = materialize(
    __APP_INSIGHTS_CUSTOM_EVENTS__
    | where timestamp between (QueryStart .. QueryEnd)
    | where tostring(customDimensions.SchemaVersion) in ('2', '3')
    | extend InstallationID = tostring(customDimensions.InstallationID), InvocationContext = tostring(customDimensions.InvocationContext)
    | where isnotempty(InstallationID)
);
{{ENRICHED_FINISHED_JOBS}}
let Commands = Events | where name == 'azcopy.command.invoked' | where InvocationContext in (_invocationContext) or isempty(_invocationContext);
let Starts = Events | where name == 'azcopy.job.started' | where InvocationContext in (_invocationContext) or isempty(_invocationContext);
let Terminal = EnrichedFinishedJobs | where InvocationContext in (_invocationContext) or isempty(_invocationContext);
let Stages = union
    (Commands | summarize Installations = dcount(InstallationID) | extend StageOrder = 1, Stage = 'Any command'),
    (Starts | summarize Installations = dcount(InstallationID) | extend StageOrder = 2, Stage = 'Transfer job started'),
    (Terminal | summarize Installations = dcount(InstallationID) | extend StageOrder = 3, Stage = 'Terminal job observed'),
    (Terminal | where JobStatus =~ 'Completed' and TransfersFailed == 0 | summarize Installations = dcount(InstallationID) | extend StageOrder = 4, Stage = 'Successful terminal job'),
    (Terminal | where JobStatus =~ 'Completed' and TransfersFailed == 0 and BytesTransferred > 0 | summarize Installations = dcount(InstallationID) | extend StageOrder = 5, Stage = 'Nonzero data transferred'),
    (Terminal | where JobStatus =~ 'Completed' and TransfersFailed == 0 and BytesTransferred > 1073741824 | summarize Installations = dcount(InstallationID) | extend StageOrder = 6, Stage = 'More than 1 GiB transferred');
let Baseline = toscalar(Stages | where StageOrder == 1 | project Installations);
Stages
| order by StageOrder asc
| serialize
| extend PreviousInstallations = prev(Installations)
| project Stage,
    Installations,
    RetainedFromPriorPct = iff(StageOrder == 1 or PreviousInstallations == 0, 100.0, round(100.0 * Installations / PreviousInstallations, 1)),
    RetainedFromFirstPct = iff(Baseline == 0, 0.0, round(100.0 * Installations / Baseline, 1))
'@
            },
            @{
                Title = "Weekly funnel stages"; Visual = "timechart"; Description = "Distinct observed installations per week for the major executable stages."
                X = 0; Y = 13; W = 22; H = 8; Query = @'
let QueryStart = _startTime;
let QueryEnd = _endTime;
{{ENRICHED_FINISHED_JOBS}}
EnrichedFinishedJobs
| where InvocationContext in (_invocationContext) or isempty(_invocationContext)
| summarize
    Terminal = dcount(InstallationID),
    Successful = dcountif(InstallationID, JobStatus =~ 'Completed' and TransfersFailed == 0),
    NonzeroData = dcountif(InstallationID, JobStatus =~ 'Completed' and TransfersFailed == 0 and BytesTransferred > 0),
    Over1GiB = dcountif(InstallationID, JobStatus =~ 'Completed' and TransfersFailed == 0 and BytesTransferred > 1073741824)
  by timestamp = startofweek(EndTime)
| order by timestamp asc
'@
            }
        )
    },
    @{
        Name = "azcopy-no-observed-success"
        Uid = "azcopy-no-observed-success"
        Title = "AzCopy Installations and Accounts with No Observed Success"
        Description = "Selected-range terminal failures without a successful terminal job."
        EnableCustomerEnrichment = $true
        Panels = @(
            @{
                Title = "How to read this dashboard"; Visual = "markdownCard"; Description = ""
                X = 0; Y = 0; W = 22; H = 5
                MarkdownText = "No observed success means at least one terminal job and no successful terminal job in the selected range after filters. It does not prove lifetime failure. Customer names use approved server-side AIPDD top-parent mappings; missing mappings remain explicit. InstallationID is pseudonymous."
            },
            @{
                Title = "Selected-range no-success summary"; Visual = "multistat"; Description = "Observed installations, destination accounts, terminal jobs, and failures in the no-success populations."
                X = 0; Y = 5; W = 22; H = 6; Query = @'
let QueryStart = _startTime;
let QueryEnd = _endTime;
{{ENRICHED_FINISHED_JOBS}}
{{ADAPTATION_FILTERS}}
let InstallationSummary = FilteredJobs
| summarize TerminalJobs = dcount(JobID), SuccessJobs = dcountif(JobID, JobStatus =~ 'Completed' and TransfersFailed == 0), FailedJobs = dcountif(JobID, JobStatus !~ 'Completed' or TransfersFailed > 0) by InstallationID
| where SuccessJobs == 0 and TerminalJobs > 0;
let AccountSummary = FilteredJobs
| where isnotempty(DestinationAccount)
| summarize TerminalJobs = dcount(JobID), SuccessJobs = dcountif(JobID, JobStatus =~ 'Completed' and TransfersFailed == 0), FailedJobs = dcountif(JobID, JobStatus !~ 'Completed' or TransfersFailed > 0) by DestinationAccount
| where SuccessJobs == 0 and TerminalJobs > 0;
let InstallationCount = toscalar(InstallationSummary | count);
let AccountCount = toscalar(AccountSummary | count);
let FailedJobCount = toscalar(InstallationSummary | summarize sum(FailedJobs));
print Metrics = pack_array(
    pack('Metric', 'Installations with no observed success', 'Value', todouble(InstallationCount)),
    pack('Metric', 'Destination accounts with no observed success', 'Value', todouble(AccountCount)),
    pack('Metric', 'Observed failed jobs', 'Value', todouble(FailedJobCount)))
| mv-expand Metrics
| project Metric = tostring(Metrics.Metric), Value = todouble(Metrics.Value)
'@
            },
            @{
                Title = "Installations with no observed success"; Visual = "table"; Description = "Pseudonymous installations with terminal activity but no selected-range success."
                X = 0; Y = 11; W = 22; H = 9; Links = @($installationLink); Query = @'
let QueryStart = _startTime;
let QueryEnd = _endTime;
{{ENRICHED_FINISHED_JOBS}}
{{ADAPTATION_FILTERS}}
FilteredJobs
| summarize
    FirstObserved = min(EndTime),
    LastObserved = max(EndTime),
    TerminalJobs = dcount(JobID),
    SuccessJobs = dcountif(JobID, JobStatus =~ 'Completed' and TransfersFailed == 0),
    FailedJobs = dcountif(JobID, JobStatus !~ 'Completed' or TransfersFailed > 0),
    AttemptedDataGB = round(sum(BytesTransferred) / 1e9, 3),
    arg_max(EndTime, JobErrorCode)
  by InstallationID
| where SuccessJobs == 0 and TerminalJobs > 0
| project InstallationID, FirstObserved, LastObserved, TerminalJobs, FailedJobs, AttemptedDataGB, LatestErrorCode = JobErrorCode
| top 500 by FailedJobs desc
'@
            },
            @{
                Title = "Destination accounts with no observed success"; Visual = "table"; Description = "Destination account proxies with no selected-range success."
                X = 0; Y = 20; W = 22; H = 9; Links = @($accountLink); Query = @'
let QueryStart = _startTime;
let QueryEnd = _endTime;
{{ENRICHED_FINISHED_JOBS}}
{{ADAPTATION_FILTERS}}
FilteredJobs
| where isnotempty(DestinationAccount)
| summarize
    FirstObserved = min(EndTime),
    LastObserved = max(EndTime),
    TerminalJobs = dcount(JobID),
    SuccessJobs = dcountif(JobID, JobStatus =~ 'Completed' and TransfersFailed == 0),
    FailedJobs = dcountif(JobID, JobStatus !~ 'Completed' or TransfersFailed > 0),
    arg_max(EndTime, JobErrorCode)
  by DestinationAccount, CustomerName, CustomerInventoryStatus, DestinationSubscription, OfferType, SubscriptionScope, StorageKind, StorageSku, StorageRedundancy
| where SuccessJobs == 0 and TerminalJobs > 0
| project DestinationAccount, CustomerName, CustomerInventoryStatus, DestinationSubscription, OfferType, SubscriptionScope, StorageKind, StorageSku, StorageRedundancy, FirstObserved, LastObserved, TerminalJobs, FailedJobs, LatestErrorCode = JobErrorCode
| top 500 by FailedJobs desc
'@
            },
            @{
                Title = "No-success error distribution"; Visual = "bar"; Description = "Error codes for installations with no selected-range success."
                X = 0; Y = 29; W = 11; H = 8; Query = @'
let QueryStart = _startTime;
let QueryEnd = _endTime;
{{ENRICHED_FINISHED_JOBS}}
{{ADAPTATION_FILTERS}}
let NoSuccess = FilteredJobs
| summarize SuccessJobs = countif(JobStatus =~ 'Completed' and TransfersFailed == 0) by InstallationID
| where SuccessJobs == 0;
FilteredJobs
| join kind=inner NoSuccess on InstallationID
| where JobStatus !~ 'Completed' or TransfersFailed > 0
| summarize ObservedJobs = dcount(JobID), ObservedInstallations = dcount(InstallationID) by JobErrorCode
| top 20 by ObservedJobs desc
'@
            },
            @{
                Title = "Failed terminal jobs"; Visual = "table"; Description = "Latest failed or partially failed jobs in the no-success installation population."
                X = 11; Y = 29; W = 11; H = 8; Links = @($installationLink, $accountLink); Query = @'
let QueryStart = _startTime;
let QueryEnd = _endTime;
{{ENRICHED_FINISHED_JOBS}}
{{ADAPTATION_FILTERS}}
let NoSuccess = FilteredJobs
| summarize SuccessJobs = countif(JobStatus =~ 'Completed' and TransfersFailed == 0) by InstallationID
| where SuccessJobs == 0;
FilteredJobs
| join kind=inner NoSuccess on InstallationID
| where JobStatus !~ 'Completed' or TransfersFailed > 0
| project EndTime, InstallationID, JobID, FromTo, DestinationAccount, CustomerName, CustomerInventoryStatus, DestinationSubscription, JobStatus, JobErrorCategory, JobErrorCode, TransfersFailed, ObjectsFailed, BytesTransferred
| top 300 by EndTime desc
'@
            }
        )
    },
    @{
        Name = "azcopy-recurring-trends"
        Uid = "azcopy-recurring-trends"
        Title = "AzCopy Recurring Usage Trends"
        Description = "Selected-range recurring usage by pseudonymous installation and destination Storage account proxy."
        Panels = @(
            @{
                Title = "How to read this dashboard"; Visual = "markdownCard"; Description = ""
                X = 0; Y = 0; W = 22; H = 5
                MarkdownText = "Recurring means activity in at least two calendar months within the selected range. Counts are observed pseudonymous installations or destination account proxies, not canonical customers. Data totals use all received finished-job telemetry."
            },
            @{
                Title = "Recurring usage summary"; Visual = "multistat"; Description = "Recurring installations, recurring destination accounts, their data volume, and average active months."
                X = 0; Y = 5; W = 22; H = 6; Query = @'
let QueryStart = _startTime;
let QueryEnd = _endTime;
{{ENRICHED_FINISHED_JOBS}}
{{ADAPTATION_FILTERS}}
let Installations = FilteredJobs
| summarize ActiveMonths = dcount(startofmonth(EndTime)), BytesTransferred = sum(BytesTransferred) by InstallationID
| where ActiveMonths >= 2;
let Accounts = FilteredJobs
| where isnotempty(DestinationAccount)
| summarize ActiveMonths = dcount(startofmonth(EndTime)) by DestinationAccount
| where ActiveMonths >= 2;
let RecurringInstallations = toscalar(Installations | count);
let RecurringAccounts = toscalar(Accounts | count);
let RecurringBytes = toscalar(Installations | summarize sum(BytesTransferred));
let AverageMonths = toscalar(Installations | summarize avg(ActiveMonths));
print Metrics = pack_array(
    pack('Metric', 'Recurring installations', 'Value', todouble(RecurringInstallations)),
    pack('Metric', 'Recurring destination accounts', 'Value', todouble(RecurringAccounts)),
    pack('Metric', 'Recurring data (TB)', 'Value', round(RecurringBytes / 1e12, 3)),
    pack('Metric', 'Average active months', 'Value', round(AverageMonths, 2)))
| mv-expand Metrics
| project Metric = tostring(Metrics.Metric), Value = todouble(Metrics.Value)
'@
            },
            @{
                Title = "Monthly recurring installations"; Visual = "timechart"; Description = "Installations active in a month after their first observed month in the selected range."
                X = 0; Y = 11; W = 11; H = 8; Query = @'
let QueryStart = _startTime;
let QueryEnd = _endTime;
{{ENRICHED_FINISHED_JOBS}}
{{ADAPTATION_FILTERS}}
let Monthly = FilteredJobs | summarize by InstallationID, Month = startofmonth(EndTime);
let Firsts = Monthly | summarize FirstMonth = min(Month) by InstallationID;
Monthly
| join kind=inner Firsts on InstallationID
| where Month > FirstMonth
| summarize RecurringInstallations = dcount(InstallationID) by timestamp = Month
| order by timestamp asc
'@
            },
            @{
                Title = "Monthly recurring destination accounts"; Visual = "timechart"; Description = "Destination accounts active after their first observed month in the selected range."
                X = 11; Y = 11; W = 11; H = 8; Query = @'
let QueryStart = _startTime;
let QueryEnd = _endTime;
{{ENRICHED_FINISHED_JOBS}}
{{ADAPTATION_FILTERS}}
let Monthly = FilteredJobs | where isnotempty(DestinationAccount) | summarize by DestinationAccount, Month = startofmonth(EndTime);
let Firsts = Monthly | summarize FirstMonth = min(Month) by DestinationAccount;
Monthly
| join kind=inner Firsts on DestinationAccount
| where Month > FirstMonth
| summarize RecurringAccounts = dcount(DestinationAccount) by timestamp = Month
| order by timestamp asc
'@
            },
            @{
                Title = "Monthly recurring data"; Visual = "timechart"; Description = "Bytes from installations active after their first observed month."
                X = 0; Y = 19; W = 22; H = 8; Unit = "decbytes"; Query = @'
let QueryStart = _startTime;
let QueryEnd = _endTime;
{{ENRICHED_FINISHED_JOBS}}
{{ADAPTATION_FILTERS}}
let Firsts = FilteredJobs | summarize FirstMonth = min(startofmonth(EndTime)) by InstallationID;
FilteredJobs
| extend Month = startofmonth(EndTime)
| join kind=inner Firsts on InstallationID
| where Month > FirstMonth
| summarize BytesTransferred = sum(BytesTransferred) by timestamp = Month
| order by timestamp asc
'@
            },
            @{
                Title = "Recurring installation details"; Visual = "table"; Description = "Installations active in two or more selected-range calendar months."
                X = 0; Y = 27; W = 22; H = 10; Links = @($installationLink); Query = @'
let QueryStart = _startTime;
let QueryEnd = _endTime;
{{ENRICHED_FINISHED_JOBS}}
{{ADAPTATION_FILTERS}}
FilteredJobs
| summarize
    FirstObserved = min(EndTime),
    LastObserved = max(EndTime),
    ActiveMonths = dcount(startofmonth(EndTime)),
    ObservedJobs = dcount(JobID),
    SuccessfulJobs = dcountif(JobID, JobStatus =~ 'Completed' and TransfersFailed == 0),
    DataTB = round(sum(BytesTransferred) / 1e12, 3),
    DestinationAccounts = dcountif(DestinationAccount, isnotempty(DestinationAccount))
  by InstallationID
| where ActiveMonths >= 2
| order by ActiveMonths desc, DataTB desc
| take 500
'@
            }
        )
    }
)

foreach ($definition in $definitions) {
    New-AdxDashboard $definition
    New-GrafanaDashboard $definition
}

$dataManifest = @(
    @{ Title = "How to read this dashboard"; Visual = "markdownCard"; Description = ""; X = 0; Y = 0; W = 22; H = 5; MarkdownText = "AzCopy logical payload and object metrics use all received finished-job telemetry. Destination accounts are enriched server-side with historical XStore properties, current Azure Resource Graph metadata, and approved AIPDD subscription/customer dimensions. Unmapped resources and customers remain explicit." },
    @{ Title = "Selected-range data totals"; Path = "queries\tiles\01_selected_range_totals.kql"; Visual = "multistat"; Description = "Observed jobs and payload totals."; X = 0; Y = 5; W = 22; H = 6 },
    @{ Title = "Current versus previous month"; Path = "queries\tiles\02_current_previous_month.kql"; Visual = "multistat"; Description = "Calendar-month comparison."; X = 0; Y = 11; W = 22; H = 6 },
    @{ Title = "Monthly data transferred"; Path = "queries\tiles\03_monthly_data_trend.kql"; Visual = "timechart"; Description = "Logical payload by month."; X = 0; Y = 17; W = 11; H = 8; Unit = "decbytes" },
    @{ Title = "Monthly completed objects"; Path = "queries\tiles\04_monthly_objects_trend.kql"; Visual = "timechart"; Description = "Completed payload objects by month."; X = 11; Y = 17; W = 11; H = 8 },
    @{ Title = "Data by source-target pair"; Path = "queries\tiles\05_data_by_source_target.kql"; Visual = "bar"; Description = "Data grouped by FromTo."; X = 0; Y = 25; W = 11; H = 8 },
    @{ Title = "Completed objects by source-target pair"; Path = "queries\tiles\06_objects_by_source_target.kql"; Visual = "bar"; Description = "Completed objects grouped by FromTo."; X = 11; Y = 25; W = 11; H = 8 },
    @{ Title = "Data by destination service type"; Path = "queries\tiles\07_data_by_destination_type.kql"; Visual = "bar"; Description = "Data grouped by AzCopy destination service type."; X = 0; Y = 33; W = 11; H = 8 },
    @{ Title = "Data by source mount type"; Path = "queries\tiles\08_data_by_source_mount.kql"; Visual = "bar"; Description = "Data grouped by source mount classification."; X = 11; Y = 33; W = 11; H = 8 },
    @{ Title = "Data by destination endpoint kind"; Path = "queries\tiles\09_data_by_destination_endpoint.kql"; Visual = "bar"; Description = "Destination hostname classification."; X = 0; Y = 41; W = 11; H = 8 },
    @{ Title = "Data by telemetry-sender country/region"; Path = "queries\tiles\10_data_by_client_region.kql"; Visual = "bar"; Description = "Application Insights sender-IP geography."; X = 11; Y = 41; W = 11; H = 8 },
    @{ Title = "Recent observed finished jobs"; Path = "queries\tiles\11_recent_finished_jobs.kql"; Visual = "table"; Description = "Latest 200 unique terminal jobs."; X = 0; Y = 49; W = 22; H = 10; Links = @($installationLink, $accountLink) },
    @{ Title = "Data by destination Storage account kind"; Path = "queries\tiles\12_data_by_storage_kind.kql"; Visual = "bar"; Description = "Current ARM account kind."; X = 0; Y = 59; W = 11; H = 8 },
    @{ Title = "Data by destination Storage SKU"; Path = "queries\tiles\13_data_by_storage_sku.kql"; Visual = "bar"; Description = "Current ARM SKU."; X = 11; Y = 59; W = 11; H = 8 },
    @{ Title = "Data by destination redundancy"; Path = "queries\tiles\14_data_by_storage_redundancy.kql"; Visual = "bar"; Description = "Historical XStore redundancy."; X = 0; Y = 67; W = 11; H = 8 },
    @{ Title = "Data by destination namespace"; Path = "queries\tiles\15_data_by_storage_namespace.kql"; Visual = "bar"; Description = "Historical HNS/FNS state."; X = 11; Y = 67; W = 11; H = 8 },
    @{ Title = "Data by destination access tier"; Path = "queries\tiles\16_data_by_storage_access_tier.kql"; Visual = "bar"; Description = "Historical XStore access tier."; X = 0; Y = 75; W = 11; H = 8 },
    @{ Title = "Data by destination account class"; Path = "queries\tiles\17_data_by_storage_account_class.kql"; Visual = "bar"; Description = "Historical billing/account class."; X = 11; Y = 75; W = 11; H = 8 },
    @{ Title = "Destination enrichment mapping coverage"; Path = "queries\tiles\18_enrichment_mapping_coverage.kql"; Visual = "table"; Description = "Mapped, missing, and not-applicable coverage."; X = 0; Y = 83; W = 22; H = 8 }
)

function New-DataGrafanaDashboard {
    $dataVariables = @()
    $parameterMap = @(
        @{ Name = "SourceType"; Label = "Source type"; Path = "queries\parameters\01_source_type.kql" },
        @{ Name = "DestType"; Label = "Destination type"; Path = "queries\parameters\02_destination_type.kql" },
        @{ Name = "FromTo"; Label = "From-to"; Path = "queries\parameters\03_from_to.kql" },
        @{ Name = "SourceMountType"; Label = "Source mount type"; Path = "queries\parameters\04_source_mount_type.kql" },
        @{ Name = "DestEndpointKind"; Label = "Destination endpoint kind"; Path = "queries\parameters\05_destination_endpoint_kind.kql" },
        @{ Name = "ClientRegion"; Label = "Client country/region"; Path = "queries\parameters\06_client_region.kql" },
        @{ Name = "DestinationSubscription"; Label = "Destination owning subscription"; Path = "queries\parameters\destination_subscription.kql" },
        @{ Name = "Customer"; Label = "Customer"; Path = "queries\parameters\customer.kql" },
        @{ Name = "OfferType"; Label = "Destination subscription offer type"; Path = "queries\parameters\offer_type.kql" },
        @{ Name = "SubscriptionScope"; Label = "Destination subscription scope"; Path = "queries\parameters\subscription_scope.kql" }
    )
    foreach ($variable in $parameterMap) {
        $dataVariables += New-QueryVariable $variable.Name $variable.Label (Expand-DataQuery $variable.Path "grafana")
    }
    $dataVariables += New-TextVariable "Account" "Storage account"

    $panels = @()
    for ($i = 0; $i -lt $dataManifest.Count; $i++) {
        $definition = $dataManifest[$i].Clone()
        if ($definition.Path) {
            $definition.Query = Expand-DataQuery $definition.Path "grafana"
        }
        $panels += New-GrafanaPanel $definition ($i + 1)
    }
    $dashboard = [ordered]@{
        uid = "azcopy-data-metrics"
        title = "AzCopy Data Metrics"
        description = "AzCopy adaptation of the Storage Mover Data dashboard."
        tags = @("AzCopy", "Telemetry", "Data")
        schemaVersion = 42
        version = 1
        editable = $true
        timezone = "browser"
        graphTooltip = 1
        time = [ordered]@{ from = "now-1y"; to = "now" }
        timepicker = [ordered]@{}
        refresh = ""
        links = @()
        annotations = [ordered]@{ list = @() }
        templating = [ordered]@{ list = $dataVariables }
        panels = $panels
    }
    $wrapper = [ordered]@{
        dashboard = $dashboard
        folderUid = ""
        message = "Generated AzCopy Data Metrics dashboard"
        overwrite = $true
    }
    $path = Join-Path $outputRoot "azcopy-data-metrics$OutputSuffix.grafana.json"
    $wrapper | ConvertTo-Json -Depth 100 | Set-Content -Encoding utf8 $path
}

New-DataGrafanaDashboard

$jsonFiles = Get-ChildItem $outputRoot -Filter "*$OutputSuffix*.json"
foreach ($file in $jsonFiles) {
    $null = Get-Content -Raw $file.FullName | ConvertFrom-Json
}

Write-Host "Generated $($jsonFiles.Count) dashboard artifacts in $outputRoot."
