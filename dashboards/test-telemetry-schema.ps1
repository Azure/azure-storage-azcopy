$ErrorActionPreference = 'Stop'
$root = Split-Path $PSScriptRoot -Parent
$checkedGates = 0

function Test-SchemaGates([string]$Text, [string]$Context) {
    foreach ($line in ($Text -split "`n")) {
        if ($line -notmatch "SchemaVersion\)?\s+!?in\s+\(([^)]+)\)") { continue }
        $versions = @([regex]::Matches($Matches[1], "'([0-9]+)'") | ForEach-Object { $_.Groups[1].Value })
        if ('1' -notin $versions -or '3' -notin $versions -or @($versions | Where-Object { $_ -notin @('1', '2', '3') }).Count) {
            throw "Incorrect schema compatibility gate: $Context"
        }
        $script:checkedGates++
    }
    if ($Text -match "SchemaVersion\)\s*==\s*'3'|SchemaVersion\s+!?in\s+\('2', '3'\)") {
        throw "Schema 1 would be excluded: $Context"
    }
}

foreach ($file in (Get-ChildItem $PSScriptRoot -Recurse -Filter '*.kql')) {
    Test-SchemaGates (Get-Content $file.FullName -Raw) $file.FullName
}
$artifacts = @(& git -C $root ls-files -- 'dashboards/*.json' 'dashboards/**/*.json')
if ($LASTEXITCODE) { throw 'Cannot list generated artifacts' }
foreach ($path in $artifacts) {
    $text = Get-Content (Join-Path $root $path) -Raw
    $document = $text | ConvertFrom-Json -AsHashtable
    $dashboard = if ($document.dashboard) { $document.dashboard } else { $document }
    if (-not $dashboard.tiles -and -not $dashboard.panels) { continue }
    $queries = if ($dashboard.tiles) { @($dashboard.queries.text) } else { @($dashboard.panels.targets.query) + @($dashboard.templating.list.query.query) }
    foreach ($query in $queries) { Test-SchemaGates $query $path }
}

$dataArtifacts = @(
    'azcopy-data-metrics/azcopy-data-metrics.dashboard.json',
    'azcopy-data-metrics/azcopy-data-metrics.telemetry-test.dashboard.json',
    'azcopy-usage-dashboards/generated/azcopy-data-metrics.grafana.json',
    'azcopy-usage-dashboards/generated/azcopy-data-metrics.telemetry-test.grafana.json'
)
foreach ($path in $dataArtifacts) {
    $document = Get-Content (Join-Path $PSScriptRoot $path) -Raw | ConvertFrom-Json -AsHashtable
    $dashboard = if ($document.dashboard) { $document.dashboard } else { $document }
    $isAdx = [bool]$dashboard.tiles
    $parameters = if ($isAdx) { $dashboard.parameters } else { $dashboard.templating.list }
    $source = @($parameters | Where-Object { $_.variableName -eq '_sourceEndpointKind' -or $_.name -eq 'SourceEndpointKind' })
    $dest = @($parameters | Where-Object { $_.variableName -eq '_destEndpointKind' -or $_.name -eq 'DestEndpointKind' })
    if ($source.Count -ne 1 -or $dest.Count -ne 1) { throw "Independent endpoint filters missing: $path" }
    $panels = if ($isAdx) { $dashboard.tiles } else { $dashboard.panels }
    $panel = @($panels | Where-Object title -eq 'Data by source endpoint kind')
    if ($panel.Count -ne 1) { throw "Source endpoint chart missing: $path" }
    $query = if ($isAdx) { ($dashboard.queries | Where-Object id -eq $panel[0].queryRef.queryId).text } else { $panel[0].targets[0].query }
    if ($query -notmatch "SourceEndpointKind = iif\(isempty\(SourceEndpointKindValue\), 'unknown'" -or $query -notmatch 'by SourceEndpointKind') {
        throw "Historical missing endpoint classification is not preserved: $path"
    }
    if ($isAdx) {
        foreach ($item in ($dashboard.queries | Where-Object { $_.text.Contains('_sourceEndpointKind') })) {
            if ('_sourceEndpointKind' -notin $item.usedVariables) { throw "Source parameter not declared: $path" }
        }
    } elseif ($query -match '_sourceEndpointKind' -or -not $query.Contains('${SourceEndpointKind}')) {
        throw "Grafana source filter substitution failed: $path"
    }
}
$replay = & (Join-Path $PSScriptRoot 'AzCopyQueries/test-aggregate-replay.ps1') -QueryOnly
if ($replay -notmatch "SchemaVersion\) in \('1', '3'\)" -or $replay -notmatch 'Schema 1 account names enrich customer' -or
    [regex]::Matches($replay, 'print Test=').Count -ne 19) { throw 'Aggregate schema-1 replay coverage missing' }
if ($checkedGates -lt 100) { throw "Unexpectedly few schema checks: $checkedGates" }
Write-Host "PASS: $checkedGates schema gates; four ADX/Grafana data variants have independent endpoint filters and source charts; 19-case replay generated offline."