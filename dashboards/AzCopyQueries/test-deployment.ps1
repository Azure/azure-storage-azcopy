[CmdletBinding()]
param()

$ErrorActionPreference = 'Stop'
$deploymentPath = Join-Path $PSScriptRoot 'deploy-functions.ps1'
$parseErrors = $null
$null = [System.Management.Automation.Language.Parser]::ParseFile($deploymentPath, [ref]$null, [ref]$parseErrors)
if ($parseErrors.Count -gt 0) { throw ($parseErrors | Out-String) }

function az { 'mock-token' }

function Invoke-RestMethod {
    param($Method, $Uri, $Headers, $ContentType, $Body)

    $command = ($Body | ConvertFrom-Json).csl
    $commands.Add($command)
    $phase = if ($command -match '^\.set AzCopyUsageAggregatesStaging_') { 'stage' }
        elseif ($command -match '^set queryconsistency') { 'validate' }
        elseif ($command -match '^\.set-or-replace AzCopyUsageAggregates1') { 'publish' }
        else { 'other' }
    if ($testCase.ThrowAt -eq $phase) { throw "Injected $phase failure" }
    if ($testCase.PartialAt -eq $phase) { return @{ Exceptions = @('Injected partial query failure') } }
    if ($command -match '^set queryconsistency') {
        if ($command -notmatch 'NewCount < PreviousCount' -or $command -notmatch 'join kind=leftouter') { throw 'Historical reductions are not compared.' }
        return @{ Tables = @(@{ Rows = @(,@($testCase.Rows, $testCase.InvalidRows, $testCase.MissingSlices, $testCase.HistoricalReductions)) }) }
    }
    if ($phase -eq 'publish') { $state.LiveData = 'new aggregate rows' }
    return @{ Tables = @(@{ Rows = @() }) }
}

$cases = @(
    @{ Name = 'Empty backfill'; Rows = 0; InvalidRows = 0; MissingSlices = 0; Publishes = $false },
    @{ Name = 'Invalid counts'; Rows = 10; InvalidRows = 1; MissingSlices = 0; Publishes = $false },
    @{ Name = 'Missing historical days'; Rows = 10; InvalidRows = 0; MissingSlices = 1; Publishes = $false },
    @{ Name = 'Valid backfill'; Rows = 10; InvalidRows = 0; MissingSlices = 0; Publishes = $true },
    @{ Name = 'Missing validation count'; Rows = 10; InvalidRows = $null; MissingSlices = 0; Publishes = $false },
    @{ Name = 'Staging request failure'; ThrowAt = 'stage'; Publishes = $false },
    @{ Name = 'Staging partial failure'; PartialAt = 'stage'; Publishes = $false },
    @{ Name = 'Validation request failure'; ThrowAt = 'validate'; Publishes = $false },
    @{ Name = 'Validation partial failure'; PartialAt = 'validate'; Publishes = $false },
    @{ Name = 'Publication request failure'; Rows = 10; InvalidRows = 0; MissingSlices = 0; ThrowAt = 'publish'; Publishes = $false },
    @{ Name = 'Partial same-day loss'; Rows = 10; InvalidRows = 0; MissingSlices = 0; HistoricalReductions = 1; Publishes = $false },
    @{ Name = 'Unapproved resume date migration'; Rows = 10; InvalidRows = 0; MissingSlices = 1; HistoricalReductions = 4; Publishes = $false },
    @{ Name = 'Approved historical reconciliation'; Rows = 10; InvalidRows = 0; MissingSlices = 1; HistoricalReductions = 4; AllowHistoricalReductions = $true; Publishes = $true },
    @{ Name = 'Approval cannot bypass invalid data'; Rows = 10; InvalidRows = 1; MissingSlices = 0; HistoricalReductions = 1; AllowHistoricalReductions = $true; Publishes = $false }
)
foreach ($testCase in $cases) {
    if (-not $testCase.ContainsKey('HistoricalReductions')) { $testCase.HistoricalReductions = 0 }
    $commands = [System.Collections.Generic.List[string]]::new()
    $state = @{ LiveData = 'original aggregate rows' }
    $LASTEXITCODE = 0
    $failed = $false
    $failure = ''
    try { & $deploymentPath -AllowHistoricalReductions:([bool]$testCase.AllowHistoricalReductions) *> $null }
    catch {
        $failed = $true
        $failure = $_.Exception.Message
    }
    $publishAttempts = @($commands | Where-Object { $_ -match '^\.set-or-replace AzCopyUsageAggregates1' }).Count
    $published = $state.LiveData -ne 'original aggregate rows'
    if ($published -ne $testCase.Publishes -or $failed -eq $testCase.Publishes) {
        throw "$($testCase.Name): published=$published, failed=$failed, error=$failure"
    }
    if (-not $testCase.Publishes -and $testCase.ThrowAt -ne 'publish' -and $publishAttempts -ne 0) {
        throw "$($testCase.Name): replacement was attempted without validated staging data."
    }
    if (@($commands | Where-Object { $_ -match '^\.drop table AzCopyUsageAggregatesStaging_' }).Count -ne 1) {
        throw "$($testCase.Name): staging cleanup was not attempted."
    }
    Write-Host "PASS: $($testCase.Name)"
}