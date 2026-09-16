[CmdletBinding()]
param([string]$BaselineRef)

$ErrorActionPreference = 'Stop'
. (Join-Path $PSScriptRoot 'metric-descriptions.ps1')
$repo = Split-Path $PSScriptRoot -Parent
$files = @(& git -C $repo ls-files -- 'dashboards/*.json' 'dashboards/**/*.json')
if ($LASTEXITCODE -ne 0) { throw 'Cannot list dashboard artifacts' }
$defaultTelemetry = 'dashboards/azcopy-telemetry-metrics/azcopy-telemetry-metrics.dashboard.json'
if (Test-Path (Join-Path $repo $defaultTelemetry)) { $files += $defaultTelemetry }
$files = @($files | Sort-Object -Unique)
$checks = 0
$dashboards = 0
$guideCount = 0

function Assert-VisibleGuide($Dashboard, $Surface, [string[]]$Fields, [string]$Context) {
    if ($Dashboard.tiles) {
        $pages = if ($Surface.pageId) { @($Surface.pageId) } else { @($Dashboard.pages.id) }
        foreach ($page in $pages) {
            $guide = @($Dashboard.tiles | Where-Object { $_.title -eq 'Field definitions' -and $_.pageId -eq $page })
            if ($guide.Count -ne 1 -or $guide[0].visualType -ne 'markdownCard' -or $guide[0].queryRef) {
                throw "Missing static visible ADX guide: $Context page $page"
            }
            foreach ($field in $Fields) {
                if (-not $guide[0].markdownText.Contains((Get-AzCopyFieldDescription @($field)))) { throw "Missing visible ${field}: $Context" }
            }
        }
    } else {
        $guide = @($Dashboard.panels | Where-Object title -eq 'Field definitions')
        if ($guide.Count -ne 1 -or $guide[0].type -ne 'text' -or $guide[0].options.mode -ne 'markdown' -or $guide[0].targets) {
            throw "Missing static visible Grafana guide: $Context"
        }
        foreach ($field in $Fields) {
            if (-not $guide[0].options.content.Contains((Get-AzCopyFieldDescription @($field)))) { throw "Missing visible ${field}: $Context" }
        }
    }
}

function Assert-Description($Surface, [string[]]$Fields, [string]$Context) {
    foreach ($field in $Fields) {
        $expected = Get-AzCopyFieldDescription @($field)
        if (-not ([string]$Surface.description).Contains($expected)) {
            throw "Missing $field description: $Context"
        }
        $script:checks++
    }
}

function Assert-DescriptionOnly($Before, $After, [string]$Path) {
    if ($Before -is [System.Collections.IDictionary] -and $After -is [System.Collections.IDictionary]) {
        $keys = @(@($Before.Keys) + @($After.Keys) | Sort-Object -Unique)
        foreach ($key in $keys) {
            if ($key -eq 'description') { continue }
            if ($key -eq 'markdownText' -and $Before.title -eq 'Byte metric definitions') { continue }
            if (-not $Before.Contains($key) -or -not $After.Contains($key)) { throw "Non-description key change: $Path/$key" }
            Assert-DescriptionOnly $Before[$key] $After[$key] "$Path/$key"
        }
    } elseif ($Before -is [array] -and $After -is [array]) {
        if ($Path -match '/(tiles|panels)$') { $After = @($After | Where-Object title -ne 'Field definitions') }
        if ($Before.Count -ne $After.Count) { throw "Array length changed: $Path" }
        for ($index = 0; $index -lt $Before.Count; $index++) {
            Assert-DescriptionOnly $Before[$index] $After[$index] "$Path/$index"
        }
    } elseif (($null -eq $Before) -ne ($null -eq $After) -or $Before -cne $After) {
        throw "Non-description value changed: $Path"
    }
}

foreach ($file in $files) {
    $document = Get-Content (Join-Path $repo $file) -Raw | ConvertFrom-Json -AsHashtable
    if ($BaselineRef -and $file -ne $defaultTelemetry) {
        $original = & git -C $repo show "${BaselineRef}:$file"
        if ($LASTEXITCODE -ne 0) { throw "Cannot read baseline artifact: $file" }
        $baseline = $original -join "`n" | ConvertFrom-Json -AsHashtable
        if ($file -eq 'dashboards/AzCopyQueries/generated/suite/manifest.json') {
            foreach ($entry in $baseline) {
                $entry.grafanaPanels++
                $entry.adxTiles++
            }
        }
        Assert-DescriptionOnly $baseline $document $file
    }
    $dashboard = if ($document.dashboard) { $document.dashboard } else { $document }
    if (-not $dashboard.tiles -and -not $dashboard.panels) { continue }
    $dashboards++
    $aggregate = $file -like 'dashboards/AzCopyQueries/*'
    $parameters = if ($dashboard.tiles) { $dashboard.parameters } else { $dashboard.templating.list }
    foreach ($parameter in $parameters) {
        $field = switch ($parameter.variableName) {
            '_sourceType' { 'SourceType' }
            '_source' { 'SourceType' }
            '_sourceMountType' { 'SourceMountType' }
            '_sourceEndpointKind' { 'SourceEndpointKind' }
            '_destType' { 'DestType' }
            '_destEndpointKind' { 'DestEndpointKind' }
            '_target' { 'TargetType' }
            '_invocationContext' { 'InvocationContext' }
        }
        if ($parameter.name -in @('SourceType', 'SourceMountType', 'SourceEndpointKind', 'DestType', 'DestEndpointKind', 'TargetType', 'InvocationContext')) { $field = $parameter.name }
        if ($field) {
            Assert-Description $parameter @($field) "$file parameter $($parameter.displayName)$($parameter.name)"
            Assert-VisibleGuide $dashboard $parameter @($field) "$file parameter $field"
        }
    }
    $surfaces = if ($dashboard.tiles) { $dashboard.tiles } else { $dashboard.panels }
    $guides = @($surfaces | Where-Object title -eq 'Field definitions')
    if (-not $guides.Count) { throw "No visible field definitions in $file" }
    $guideCount += $guides.Count
    if (@($surfaces.id | Sort-Object -Unique).Count -ne $surfaces.Count) { throw "Duplicate panel IDs in $file" }
    foreach ($guide in $guides) {
        $layout = if ($dashboard.tiles) { $guide.layout } else { @{ x = $guide.gridPos.x; y = $guide.gridPos.y; width = $guide.gridPos.w; height = $guide.gridPos.h } }
        if ($layout.width -lt 3 -or $layout.height -lt 6) { throw "Undersized guide in $file" }
        foreach ($other in ($surfaces | Where-Object { $_.id -ne $guide.id -and $_.pageId -eq $guide.pageId })) {
            $otherLayout = if ($dashboard.tiles) { $other.layout } else { @{ x = $other.gridPos.x; y = $other.gridPos.y; width = $other.gridPos.w; height = $other.gridPos.h } }
            if ($layout.x -lt $otherLayout.x + $otherLayout.width -and $layout.x + $layout.width -gt $otherLayout.x -and
                $layout.y -lt $otherLayout.y + $otherLayout.height -and $layout.y + $layout.height -gt $otherLayout.y) { throw "Overlapping guide in $file" }
        }
    }
    foreach ($surface in $surfaces) {
        $fields = @()
        if ($aggregate) {
            $query = if ($surface.queryRef) {
                ($dashboard.queries | Where-Object id -eq $surface.queryRef.queryId).text
            } else { $surface.targets.query -join "`n" }
            if ($surface.title -like 'Top 100 AzCopy movers*' -or $query -match "AzCopy_(UsageDetails|NoSuccessDetails)\(|\bTargets\b|'(target|topology)'\)") {
                $fields = @('TargetType')
            }
            if ($surface.title -like 'Top 100 AzCopy movers*' -or $query -match "AzCopy_(UsageDetails|NoSuccessDetails)\(|\bSources\b|'(source|topology)'\)") {
                $fields += 'SourceType'
            }
        } else {
            $fields = switch -Regex ($surface.title) {
                '^(Transfer dimension mix|Platform and version mix|Data by source-target pair|Completed objects by source-target pair)$' { @('SourceType', 'DestType') }
                '^Data by destination service type$' { @('DestType') }
                '^(Recent observed finished jobs|Recent finished jobs)$' { @('SourceType', 'SourceEndpointKind', 'DestType', 'DestEndpointKind') }
                '^Source and destination platform mix$' { @('SourceType', 'SourceProtocol', 'DestType', 'DestProtocol') }
                '^Recent finished attempts and dimensions$' { @('SourceProtocol', 'SourceMountType', 'SourceScope', 'SourceEndpointKind', 'SourceCloudType', 'SourceAuthMechanism', 'DestProtocol', 'DestScope', 'DestEndpointKind') }
                '^Data by destination endpoint kind$' { @('DestEndpointKind') }
                '^Data by source endpoint kind$' { @('SourceEndpointKind') }
                '^Data by source mount type$' { @('SourceMountType') }
                '^Bytes$' { @('azcopy.bytes_enumerated', 'azcopy.source_bytes_scanned') }
                '^Complete metric catalog$' { @('azcopy.bytes_enumerated', 'azcopy.source_bytes_scanned', 'azcopy.source_max_directory_depth', 'azcopy.hardlinks_converted_scheduled', 'azcopy.storage_http_attempt_count', 'azcopy.avg_iops') }
                '^Scanned and touched inventory$' { @('azcopy.source_bytes_scanned') }
                '^Scheduled object composition$' { @('azcopy.hardlinks_converted_scheduled') }
                '^Scanned source small-object share and directory depth$' { @('azcopy.source_max_directory_depth') }
                '^(Storage latency and IOPS|HTTP, network, and overflow error counts)$' { @('azcopy.storage_http_attempt_count', 'azcopy.avg_iops') }
                '^(Invocation context|Combined host and runtime details)$' { @('InvocationContext') }
                '^(Reliability rates|Per-attempt Storage request evidence)$' { @('azcopy.storage_http_attempt_count') }
            }
            if ($surface.title -eq 'Data by source-target pair' -and $file -like '*azcopy-customer-drilldown*') { $fields = @() }
        }
        if ($fields) {
            Assert-Description $surface $fields "$file panel $($surface.title)"
            Assert-VisibleGuide $dashboard $surface $fields "$file panel $($surface.title)"
        }
        if ($surface.title -eq 'Byte metric definitions') {
            foreach ($phrase in @('after traversal filters', 'not payload bytes read', '80 GiB source bytes scanned', 'job-cumulative')) {
                if (-not $surface.markdownText.Contains($phrase)) { throw "Missing byte guide explanation: $phrase" }
            }
            $checks++
        }
    }
}
if ($dashboards -lt 33 -or $checks -lt 200) { throw "Insufficient description coverage: $dashboards dashboards, $checks checks" }
Write-Host "PASS: $checks field-description checks and $guideCount visible non-overlapping guides across $dashboards dashboard artifacts."
if ($BaselineRef) { Write-Host "PASS: original queries, IDs, layouts, and data sources match $BaselineRef; only descriptions, guide panels, and guide counts changed." }