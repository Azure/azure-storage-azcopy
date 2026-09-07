[CmdletBinding()]
param(
    [ValidateRange(3, 31)][int]$Samples = 9,
    [ValidateRange(0.1, 60)][double]$Seconds = 5,
    [string]$BaselineRef = 'telemetry/00-main-base',
    [string]$OutputDirectory = 'telemetry-performance-results',
    [switch]$ReportOnly
)

$ErrorActionPreference = 'Stop'
$repo = Split-Path $PSScriptRoot -Parent
Push-Location $repo
$baselinePath = Join-Path ([IO.Path]::GetTempPath()) ('azcopy-perf-baseline-' + [guid]::NewGuid().ToString('N'))
$worktreeCreated = $false
$savedEnvironment = @{}
$settings = @{
    AZCOPY_RUN_TELEMETRY_PERF = '1'
    AZCOPY_TELEMETRY_PERF_SAMPLES = [string]$Samples
    AZCOPY_TELEMETRY_PERF_SECONDS = $Seconds.ToString([cultureinfo]::InvariantCulture)
    AZCOPY_TELEMETRY_PERF_ENFORCE = ([string](-not $ReportOnly)).ToLowerInvariant()
    GOMAXPROCS = '8'
}
try {
    $OutputDirectory = [IO.Path]::GetFullPath($OutputDirectory)
    $null = New-Item -ItemType Directory -Force -Path $OutputDirectory
    $settings.AZCOPY_TELEMETRY_PERF_OUTPUT = $OutputDirectory
    foreach ($entry in $settings.GetEnumerator()) {
        $savedEnvironment[$entry.Key] = [Environment]::GetEnvironmentVariable($entry.Key, 'Process')
        [Environment]::SetEnvironmentVariable($entry.Key, $entry.Value, 'Process')
    }
    $baselineCommit = $null
    foreach ($candidate in @($BaselineRef, "origin/$BaselineRef", "fork/$BaselineRef")) {
        $resolved = git rev-parse --verify --quiet --end-of-options "${candidate}^{commit}"
        if ($LASTEXITCODE -eq 0) { $baselineCommit = $resolved; break }
    }
    if ([string]::IsNullOrWhiteSpace($baselineCommit)) { throw "Baseline '$BaselineRef' is unavailable. Fetch the baseline or provide an explicit commit." }
    $candidateCommit = git rev-parse HEAD
    $suffix = if ($IsWindows) { '.exe' } else { '' }
    $candidateBinary = Join-Path $OutputDirectory "azcopy-candidate$suffix"
    $baselineBinary = Join-Path $OutputDirectory "azcopy-baseline$suffix"
    go build -trimpath -buildvcs=false '-ldflags=-s -w' -o $candidateBinary .
    if ($LASTEXITCODE -ne 0) { throw 'Candidate build failed.' }
    git worktree add --detach $baselinePath $baselineCommit
    if ($LASTEXITCODE -ne 0) { throw 'Could not create isolated baseline worktree.' }
    $worktreeCreated = $true
    Push-Location $baselinePath
    try {
        go build -trimpath -buildvcs=false '-ldflags=-s -w' -o $baselineBinary .
        if ($LASTEXITCODE -ne 0) { throw 'Baseline build failed.' }
    } finally { Pop-Location }
    $growth = (Get-Item $candidateBinary).Length - (Get-Item $baselineBinary).Length
    $binaryReport = [ordered]@{
        BaselineRef = $BaselineRef
        BaselineCommit = $baselineCommit
        CandidateCommit = $candidateCommit
        BaselineBytes = (Get-Item $baselineBinary).Length
        CandidateBytes = (Get-Item $candidateBinary).Length
        GrowthBytes = $growth
        LimitBytes = 5 * 1024 * 1024
        Pass = $growth -le 5 * 1024 * 1024
        ReportOnly = [bool]$ReportOnly
    }
    $binaryReport | ConvertTo-Json | Set-Content -Encoding utf8 (Join-Path $OutputDirectory 'binary-size.json')
    go test -tags telemetryperf ./azcopy -run '^TestTelemetryPerformance(Analysis|PipelineIsManual|Gate)$' -count=1 -timeout=45m -v 2>&1 |
        Tee-Object -FilePath (Join-Path $OutputDirectory 'performance-tests.log')
    $testExitCode = $LASTEXITCODE
    if ($testExitCode -ne 0) { throw "Performance harness failed (exit $testExitCode). See the published measurements." }
    if (-not $ReportOnly -and -not $binaryReport.Pass) { throw "Binary growth $growth exceeds the 5 MiB gate." }
    Write-Host "Binary growth: $growth bytes. Measurements: $OutputDirectory"
    if ($ReportOnly) { Write-Warning 'Report-only run: threshold failures were not enforced.' }
} finally {
    if ($worktreeCreated) {
        git worktree remove --force $baselinePath
        if ($LASTEXITCODE -ne 0) { Write-Warning "Could not remove generated baseline worktree: $baselinePath" }
    }
    foreach ($entry in $savedEnvironment.GetEnumerator()) {
        [Environment]::SetEnvironmentVariable($entry.Key, $entry.Value, 'Process')
    }
    Pop-Location
}