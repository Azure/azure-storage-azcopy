[CmdletBinding()]
param(
    [Parameter(Mandatory)]
    [string] $AzCopyExecutablePath,

    [Parameter(Mandatory)]
    [string] $ConnectionString,

    [Parameter(Mandatory)]
    [string] $WorkspaceId,

    [Parameter(Mandatory)]
    [string] $RunId,

    [ValidateRange(1, 3600)]
    [int] $TimeoutSeconds = 600,

    [ValidateRange(1, 300)]
    [int] $PollIntervalSeconds = 15
)

Set-StrictMode -Version Latest
$ErrorActionPreference = 'Stop'

if (-not (Test-Path -LiteralPath $AzCopyExecutablePath -PathType Leaf)) {
    throw "Telemetry canary configuration is invalid: AzCopy executable '$AzCopyExecutablePath' does not exist."
}
if ($ConnectionString -notmatch '(?i)(^|;)InstrumentationKey=[^;]+') {
    throw 'Telemetry canary configuration is invalid: the Application Insights connection string has no InstrumentationKey.'
}
if ($WorkspaceId -notmatch '^[0-9a-fA-F-]{36}$') {
    throw "Telemetry canary configuration is invalid: workspace ID '$WorkspaceId' is not a GUID."
}
if ($RunId -notmatch '^[A-Za-z0-9._:-]+$') {
    throw "Telemetry canary configuration is invalid: run ID '$RunId' contains unsupported characters."
}

$canaryRunId = "$RunId-canary"
$startedAt = [DateTime]::UtcNow.AddMinutes(-1)
$deadline = [DateTime]::UtcNow.AddSeconds($TimeoutSeconds)
$canaryRoot = Join-Path ([IO.Path]::GetTempPath()) "azcopy-telemetry-canary-$canaryRunId"
$canaryLogPath = Join-Path $canaryRoot 'logs'
$canaryPlanPath = Join-Path $canaryRoot 'plans'

$telemetryEnvironment = @(
    'AZCOPY_TELEMETRY_CONNECTION_STRING',
    'AZCOPY_E2E_TELEMETRY_RUN_ID',
    'AZCOPY_LOG_LOCATION',
    'AZCOPY_JOB_PLAN_LOCATION'
)
$previousEnvironment = @{}
foreach ($name in $telemetryEnvironment) {
    $previousEnvironment[$name] = [Environment]::GetEnvironmentVariable($name)
}

try {
    $null = New-Item -Path $canaryLogPath -ItemType Directory -Force
    $null = New-Item -Path $canaryPlanPath -ItemType Directory -Force
    $env:AZCOPY_TELEMETRY_CONNECTION_STRING = $ConnectionString
    $env:AZCOPY_E2E_TELEMETRY_RUN_ID = $canaryRunId
    $env:AZCOPY_LOG_LOCATION = $canaryLogPath
    $env:AZCOPY_JOB_PLAN_LOCATION = $canaryPlanPath

    & $AzCopyExecutablePath jobs list --output-type=json --telemetry-sampling-rate=1 --check-version=false | Out-Null
    if ($LASTEXITCODE -ne 0) {
        throw "Telemetry canary emission failed: 'azcopy jobs list' exited with code $LASTEXITCODE."
    }

    Write-Output "Telemetry canary command completed for run '$canaryRunId'; waiting for ingestion."

    $queryStartedAt = $startedAt.ToString('yyyy-MM-ddTHH:mm:ss.fffffffZ')
    $query = "AppEvents | where TimeGenerated >= datetime($queryStartedAt) | where Name == 'azcopy.command.invoked' | extend EventProperties = todynamic(Properties) | where tostring(EventProperties.E2ETestRunID) == '$canaryRunId' | where tostring(EventProperties.Command) == 'jobs.list' | summarize CanaryCount = count()"

    $lastQueryError = $null
    $querySucceeded = $false
    do {
        $queryOutput = (& az monitor log-analytics query `
            --workspace $WorkspaceId `
            --analytics-query $query `
            --only-show-errors `
            --query '[0].CanaryCount' `
            --output tsv 2>&1) | Out-String
        $queryExitCode = $LASTEXITCODE

        if ($queryExitCode -eq 0) {
            $canaryCount = 0
            if ([int]::TryParse($queryOutput.Trim(), [ref] $canaryCount)) {
                $querySucceeded = $true
            } else {
                $lastQueryError = "The query returned an unexpected count: '$($queryOutput.Trim())'."
            }
            if ($canaryCount -gt 0) {
                Write-Output "Telemetry ingestion canary succeeded for run '$canaryRunId'."
                return
            }
        } else {
            $lastQueryError = $queryOutput.Trim()
            if ($lastQueryError -match '(?i)\b(401|403|AuthorizationFailed|Forbidden|insufficient privileges)\b') {
                throw "Telemetry canary query authorization failed for workspace '$WorkspaceId': $lastQueryError"
            }
        }

        $remainingSeconds = ($deadline - [DateTime]::UtcNow).TotalSeconds
        if ($remainingSeconds -gt 0) {
            Start-Sleep -Seconds ([Math]::Min($PollIntervalSeconds, [Math]::Ceiling($remainingSeconds)))
        }
    } while ([DateTime]::UtcNow -lt $deadline)

    if (-not $querySucceeded) {
        throw "Telemetry canary query failed for the entire ${TimeoutSeconds}-second polling window. Last query error: $lastQueryError"
    }

    throw "Telemetry canary timed out after $TimeoutSeconds seconds. The AzCopy command succeeded, but its event was not observed in Application Insights; telemetry emission or ingestion failed."
}
finally {
    foreach ($name in $telemetryEnvironment) {
        [Environment]::SetEnvironmentVariable($name, $previousEnvironment[$name])
    }
}
