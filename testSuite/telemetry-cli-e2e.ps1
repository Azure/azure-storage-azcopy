[CmdletBinding()]
param()

$ErrorActionPreference = 'Stop'
if ($args.Count -gt 0) { throw "Unrecognized arguments: $($args -join ' ')" }
foreach ($name in @('NEW_E2E_AZCOPY_PATH', 'AZCOPY_TELEMETRY_CONNECTION_STRING', 'NEW_E2E_APP_INSIGHTS_WORKSPACE_ID', 'AZCOPY_E2E_TELEMETRY_RUN_ID')) {
    if ([string]::IsNullOrWhiteSpace([Environment]::GetEnvironmentVariable($name, 'Process'))) {
        throw "Configure $name through the existing New_E2E setup before running functional telemetry E2E. No separate test account or role grant is used."
    }
}
if (-not $env:NEW_E2E_ENVIRONMENT -and -not $env:NEW_E2E_STANDARD_ACCOUNT_NAME) {
    throw 'Load the existing New_E2E dynamic or static account/identity configuration first.'
}
Get-Command go -ErrorAction Stop | Out-Null
$previousDisable = $env:AZCOPY_DISABLE_TELEMETRY
$env:AZCOPY_DISABLE_TELEMETRY = 'true'
Push-Location (Split-Path $PSScriptRoot -Parent)
try {
    $suites = '^(BasicFunctionalitySuite|SyncTestSuite|JobsListSuite|DryrunSuite|FileOAuthTestSuite|BlobFSTestSuite|S2STestSuite|FilesNFSTestSuite|TelemetryFunctionalSuite)$'
    $scenarios = '^Scenario_(SingleFile|MultiFileUploadDownload|EntireDirectory_S2SContainer|JobResume|JobsList.*|TestSyncRemoveDestination|TestSyncDeleteDestinationIfNecessary|UploadSync_Encoded|DownloadSync_Encoded|ExtraProps|FileBlobOAuthNoError|CopyFileBlobOAuth|SyncBlobOAuth|UploadFile|UploadFileMultiflushOAuth|BlobBlobOAuth|NonOverwriteSingleFile|Hardlink(CopyCancel_.*|SyncCancel_.*|Sync_IdempotentResync|Sync_NestedDirectories)|CommandExclusions|TransferOptOut|Benchmark|ConcurrentIdentity|UnreachableIngestion)$'
    $filter = '^TestNewE2E$/' + $suites + '/' + $scenarios
    & go test ./e2etest -run $filter -count=1 -v -timeout=3h
    if ($LASTEXITCODE -ne 0) { throw 'Real Azure telemetry E2E failed.' }
} finally {
    Pop-Location
    $env:AZCOPY_DISABLE_TELEMETRY = $previousDisable
}