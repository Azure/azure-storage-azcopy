[CmdletBinding()]
param()

$ErrorActionPreference = 'Stop'
if ($args.Count -gt 0) { throw "Unrecognized arguments: $($args -join ' ')" }
$previousGate = $env:AZCOPY_RUN_TELEMETRY_CLI_INTEGRATION
$env:AZCOPY_RUN_TELEMETRY_CLI_INTEGRATION = '1'
Push-Location (Split-Path $PSScriptRoot -Parent)
try {
    & go test '-tags=telemetrylive' ./azcopy -run '^TestTelemetryCLIIntegration$' -count=1 -v -timeout=5m
    if ($LASTEXITCODE -ne 0) { throw 'Loopback CLI integration tests failed.' }
} finally {
    Pop-Location
    $env:AZCOPY_RUN_TELEMETRY_CLI_INTEGRATION = $previousGate
}
