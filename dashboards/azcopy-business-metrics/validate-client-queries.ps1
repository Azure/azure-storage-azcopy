param(
    [string]$App = "sharankur_insights1",
    [string]$ResourceGroup = "sharankur_playground",
    [string]$Offset = "2d"
)

$ErrorActionPreference = "Stop"
$queryRoot = Join-Path $PSScriptRoot "queries/client"
$failed = @()

foreach ($file in Get-ChildItem -Path $queryRoot -Filter *.kql | Sort-Object Name) {
    $query = (Get-Content $file.FullName | Where-Object { $_ -notmatch '^\s*//' }) -join ' '
    $query = $query.Replace('_startTime', "ago($Offset)")
    $query = $query.Replace('_endTime', 'now()')
    $query = $query.Replace('_account', "''")

    & az monitor app-insights query `
        --app $App `
        --resource-group $ResourceGroup `
        --offset $Offset `
        --analytics-query $query `
        --output none 2>$null

    if ($LASTEXITCODE -eq 0) {
        Write-Host "PASS $($file.Name)"
    } else {
        Write-Host "FAIL $($file.Name)" -ForegroundColor Red
        $failed += $file.Name
    }
}

if ($failed.Count -gt 0) {
    throw "Client query validation failed: $($failed -join ', ')"
}

Write-Host "All client dashboard queries passed."