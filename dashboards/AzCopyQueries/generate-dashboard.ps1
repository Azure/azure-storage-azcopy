[CmdletBinding()]
param(
    [string]$OutputPath = (Join-Path $PSScriptRoot 'azcopy-top-movers.dashboard.json'),
    [string]$DatasourceUid = 'azcopy-usage-analytics',
    [string]$Database = 'usage-analytics'
)

$ErrorActionPreference = 'Stop'
. (Join-Path $PSScriptRoot '../metric-descriptions.ps1')
if ($args.Count -gt 0) { throw "Unrecognized arguments: $($args -join ' ')" }

function New-Target([string]$Query, [string]$RefId) {
    return [ordered]@{
        datasource = [ordered]@{ type = 'grafana-azure-data-explorer-datasource'; uid = $DatasourceUid }
        queryType = 'KQL'
        refId = $RefId
        resultFormat = 'table'
        database = $Database
        query = $Query.Trim()
    }
}

function New-RowPanel([int]$Id, [string]$Title, [int]$Y) {
    return [ordered]@{
        id = $Id
        title = $Title
        type = 'row'
        collapsed = $false
        gridPos = [ordered]@{ h = 1; w = 24; x = 0; y = $Y }
        panels = @()
    }
}

function New-BarGaugePanel([int]$Id, [string]$Title, [int]$X, [int]$Y, [int]$Height, [string]$Query, [string]$Description) {
    return [ordered]@{
        id = $Id
        title = $Title
        description = $Description
        type = 'bargauge'
        datasource = [ordered]@{ type = 'grafana-azure-data-explorer-datasource'; uid = $DatasourceUid }
        gridPos = [ordered]@{ h = $Height; w = 8; x = $X; y = $Y }
        fieldConfig = [ordered]@{
            defaults = [ordered]@{
                color = [ordered]@{ fixedColor = 'green'; mode = 'fixed' }
                mappings = @()
                min = 0
                thresholds = [ordered]@{
                    mode = 'absolute'
                    steps = @([ordered]@{ color = 'green'; value = 0 })
                }
                unit = 'decbytes'
                displayName = '${__field.name}'
            }
            overrides = @()
        }
        options = [ordered]@{
            displayMode = 'gradient'
            legend = [ordered]@{ calcs = @(); displayMode = 'list'; placement = 'bottom'; showLegend = $false }
            maxVizHeight = 300
            minVizHeight = 16
            minVizWidth = 8
            namePlacement = 'auto'
            orientation = 'horizontal'
            reduceOptions = [ordered]@{ calcs = @(); fields = ''; values = $true }
            showUnfilled = $true
            sizing = 'auto'
            text = [ordered]@{ titleSize = 17 }
            valueMode = 'text'
        }
        pluginVersion = '12.4.6'
        targets = @(New-Target $Query "A$Id")
        transformations = @([ordered]@{
            id = 'rowsToFields'
            options = [ordered]@{
                mappings = @(
                    [ordered]@{ fieldName = 'Customer'; handlerKey = 'field.name' },
                    [ordered]@{ fieldName = 'DataTransferred'; handlerKey = 'field.value' }
                )
            }
        })
    }
}

function New-DetailsPanel([string]$Query) {
    return [ordered]@{
        id = 1
        title = 'Top 100 AzCopy movers (selected range) (Filters: All)'
        description = 'Detailed equivalent of the Storage Mover Top 100 panel for the selected datetime range.' + "`n`n" + (Get-AzCopyFieldDescription @('SourceType', 'TargetType'))
        type = 'table'
        datasource = [ordered]@{ type = 'grafana-azure-data-explorer-datasource'; uid = $DatasourceUid }
        gridPos = [ordered]@{ h = 29; w = 24; x = 0; y = 33 }
        fieldConfig = [ordered]@{
            defaults = [ordered]@{
                custom = [ordered]@{ align = 'auto'; cellOptions = [ordered]@{ type = 'auto' }; inspect = $false }
                mappings = @()
                thresholds = [ordered]@{ mode = 'absolute'; steps = @([ordered]@{ color = 'green'; value = $null }) }
            }
            overrides = @(
                [ordered]@{ matcher = [ordered]@{ id = 'byName'; options = 'Data Transferred' }; properties = @([ordered]@{ id = 'unit'; value = 'decbytes' }, [ordered]@{ id = 'decimals'; value = 1 }) },
                [ordered]@{ matcher = [ordered]@{ id = 'byName'; options = 'First Seen' }; properties = @([ordered]@{ id = 'unit'; value = 'dateTimeAsIso' }) },
                [ordered]@{ matcher = [ordered]@{ id = 'byName'; options = 'Last Seen' }; properties = @([ordered]@{ id = 'unit'; value = 'dateTimeAsIso' }) }
            )
        }
        options = [ordered]@{
            cellHeight = 'sm'
            footer = [ordered]@{ countRows = $false; enablePagination = $true; fields = ''; reducer = @('sum'); show = $false }
            showHeader = $true
            sortBy = @([ordered]@{ desc = $true; displayName = 'Data Transferred' })
        }
        pluginVersion = '12.4.6'
        targets = @(New-Target $Query 'A1')
        transformations = @([ordered]@{
            id = 'organize'
            options = [ordered]@{
                excludeByName = [ordered]@{ Rank = $true; CustomerKey = $true }
                indexByName = [ordered]@{ Customer = 0; DataTransferred = 1; Movers = 2; Subscriptions = 3; Scenarios = 4; Commands = 5; Sources = 6; Targets = 7; SubscriptionIds = 8; FirstSeen = 9; LastSeen = 10; CustomerKey = 11; Rank = 12 }
                renameByName = [ordered]@{ Customer = 'Customer Name'; DataTransferred = 'Data Transferred'; Movers = 'AzCopy Installations'; Subscriptions = 'Subscriptions'; Scenarios = 'Scenarios'; Commands = 'Commands'; Sources = 'Sources'; Targets = 'Targets'; SubscriptionIds = 'Subscription IDs'; FirstSeen = 'First Seen'; LastSeen = 'Last Seen' }
            }
        })
    }
}

function New-QueryVariable([string]$Name, [string]$Label, [string]$FilterName, [int]$Sort) {
    $query = "AzCopy_FilterValues() | where FilterName == '$FilterName' | project text=FilterLabel, value=FilterValue | order by text asc"
    return [ordered]@{
        name = $Name
        label = $Label
        description = if ($Name -in @('SourceType', 'TargetType')) { Get-AzCopyFieldDescription @($Name) } else { '' }
        type = 'query'
        hide = 0
        multi = $true
        includeAll = $true
        skipUrlSync = $false
        refresh = 1
        sort = $Sort
        datasource = [ordered]@{ type = 'grafana-azure-data-explorer-datasource'; uid = $DatasourceUid }
        definition = $query
        query = [ordered]@{ query = $query; refId = "Variable-$Name" }
        current = [ordered]@{ selected = $true; text = @('All'); value = @('$__all') }
        options = @()
    }
}

$all = "dynamic(['`$__all'])"
$customer = 'dynamic(${Customer:json})'
$command = 'dynamic(${Command:json})'
$source = 'dynamic(${SourceType:json})'
$target = 'dynamic(${TargetType:json})'
$topN = 'tolong(${TopN:json})'

function Get-TopQuery([string]$Start, [string]$End, [string]$CustomerFilter, [string]$CommandFilter, [string]$SourceFilter, [string]$TargetFilter, [string]$Limit, [switch]$Bar) {
    $query = "AzCopy_TopMovers($Start, $End, $Limit, $CustomerFilter, $CommandFilter, $SourceFilter, $TargetFilter)"
    if ($Bar) { $query += ' | project Customer, DataTransferred' }
    return $query
}

$panels = @(
    (New-RowPanel 7 'README' 0),
    (New-RowPanel 16 'Metrics' 1),
    (New-BarGaugePanel 17 'Top data movers (All time) (Filters: None)' 0 2 16 (Get-TopQuery 'datetime(1970-01-01)' 'now() + 1m' $all $all $all $all $topN -Bar) 'Top identified AzCopy customers by data transferred, matching the Storage Mover panel.'),
    (New-BarGaugePanel 10 'Top AzCopy movers (Current month) (Filters: Source, Target)' 8 2 16 (Get-TopQuery 'startofmonth(now())' 'now() + 1m' $all $all $source $target $topN -Bar) 'Current month with source and target filters.'),
    (New-BarGaugePanel 13 'Top AzCopy movers (Previous month) (Filters: Source, Target)' 16 2 16 (Get-TopQuery "startofmonth(datetime_add('month', -1, now()))" 'startofmonth(now())' $all $all $source $target $topN -Bar) 'Previous calendar month with source and target filters.'),
    (New-BarGaugePanel 21 'Top AzCopy movers (Previous semester) (Filters: None)' 0 18 15 (Get-TopQuery 'AzCopy_PreviousSemesterStart()' 'AzCopy_CurrentSemesterStart()' $all $all $all $all $topN -Bar) 'Previous April-September or October-March semester.'),
    (New-BarGaugePanel 26 'Top AzCopy movers (Current semester) (Filters: Source, Target)' 8 18 15 (Get-TopQuery 'AzCopy_CurrentSemesterStart()' 'now() + 1m' $all $all $source $target $topN -Bar) 'Current April-September or October-March semester with source and target filters.'),
    (New-BarGaugePanel 11 'Top AzCopy movers (selected range) (Filters: All)' 16 18 15 (Get-TopQuery '$__timeFrom' '$__timeTo' $customer $command $source $target $topN -Bar) 'Selected Grafana range with customer, command, source, and target filters.'),
    (New-DetailsPanel (Get-TopQuery '$__timeFrom' '$__timeTo' $customer $command $source $target '100'))
)

$dashboard = [ordered]@{
    id = $null
    uid = 'azcopy-top-movers'
    title = 'AzCopy Top Movers'
    description = 'Storage Mover-style Top Movers dashboard backed by persisted daily AzCopy aggregates.'
    tags = @('azcopy', 'telemetry', 'top-movers')
    timezone = 'browser'
    editable = $true
    graphTooltip = 0
    fiscalYearStartMonth = 3
    liveNow = $false
    links = @()
    panels = $panels
    refresh = ''
    schemaVersion = 42
    time = [ordered]@{ from = 'now-90d'; to = 'now' }
    timepicker = [ordered]@{}
    templating = [ordered]@{
        list = @(
            (New-QueryVariable 'Customer' 'Customer' 'Customer' 1),
            [ordered]@{ name = 'TopN'; label = 'Top N'; type = 'custom'; query = '10,25,50,100'; current = [ordered]@{ selected = $true; text = '10'; value = '10' }; options = @([ordered]@{ selected = $true; text = '10'; value = '10' }, [ordered]@{ selected = $false; text = '25'; value = '25' }, [ordered]@{ selected = $false; text = '50'; value = '50' }, [ordered]@{ selected = $false; text = '100'; value = '100' }); multi = $false; includeAll = $false; hide = 0; skipUrlSync = $false },
            (New-QueryVariable 'Command' 'Command' 'Command' 2),
            (New-QueryVariable 'SourceType' 'Source' 'SourceType' 3),
            (New-QueryVariable 'TargetType' 'Target' 'TargetType' 4)
        )
    }
    annotations = [ordered]@{ list = @() }
    version = 1
    weekStart = ''
}

Add-AzCopyVisibleFieldGuides $dashboard
$json = $dashboard | ConvertTo-Json -Depth 100
[System.IO.File]::WriteAllText($OutputPath, $json + [Environment]::NewLine, [System.Text.UTF8Encoding]::new($false))
$parsed = Get-Content -Raw $OutputPath | ConvertFrom-Json
if ($parsed.uid -ne 'azcopy-top-movers' -or $parsed.panels.Count -ne 10 -or $parsed.templating.list.Count -ne 5) { throw 'Generated dashboard failed structural validation.' }
if (@($parsed.panels | Where-Object type -eq 'bargauge').Count -ne 6 -or @($parsed.panels | Where-Object type -eq 'table').Count -ne 1) { throw 'Generated dashboard does not match the active Storage Mover panel types.' }
$generatedDatabases = @(@($parsed.panels.targets.database) | Where-Object { $_ } | Sort-Object -Unique)
if ($generatedDatabases.Count -ne 1 -or $generatedDatabases[0] -ne $Database) { throw 'Generated dashboard database references do not match the requested database.' }
$generatedContent = Get-Content -Raw $OutputPath
if ($generatedContent -match 'customEvents|AzCopy_KO_UsageAggregates') { throw 'Grafana must query persisted aggregate helpers, not raw telemetry or aggregate producers.' }
Write-Host "Generated $OutputPath"
