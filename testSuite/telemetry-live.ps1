[CmdletBinding()]
param(
    [Parameter(Mandatory)][guid]$SubscriptionId,
    [Parameter(Mandatory)][ValidatePattern('^[a-zA-Z0-9_.()-]{1,90}$')][string]$ResourceGroup,
    [ValidatePattern('^[a-z0-9-]{1,12}$')][string]$Suffix = 'manual',
    [string]$Location = 'eastus',
    [switch]$Provision,
    [switch]$Run,
    [ValidateSet('all', 'shutdown', 'cli-shutdown', 'workspace', 'application')][string]$Scenario = 'all',
    [string]$StorageAccountName,
    [switch]$GrantStoragePermission,
    [string]$OutputDirectory = (Join-Path ([IO.Path]::GetTempPath()) "azcopy-telemetry-live-$([guid]::NewGuid().ToString('N'))")
)

$ErrorActionPreference = 'Stop'
Set-StrictMode -Version Latest
if (-not $Provision -and -not $Run) {
    throw 'Specify -Provision to create isolated resources and/or -Run to send real, billable telemetry.'
}
Get-Command az -ErrorAction Stop | Out-Null
$root = Split-Path $PSScriptRoot -Parent
$prefix = "azcopy-telfault-$Suffix"
$OutputDirectory = [IO.Path]::GetFullPath($OutputDirectory)
New-Item -ItemType Directory -Path $OutputDirectory -Force | Out-Null

function Invoke-AzureJson {
    param([string[]]$Arguments)
    $json = & az @Arguments --only-show-errors --output json
    if ($LASTEXITCODE -ne 0) { throw "Azure command failed: $($Arguments[0])" }
    if ($json) { return ($json | ConvertFrom-Json) }
}

Push-Location $root
try {
    if ($Provision) {
        $existing = @(Invoke-AzureJson @('resource', 'list', '--subscription', "$SubscriptionId", '--resource-group', $ResourceGroup,
            '--query', "[?starts_with(name, '$prefix-')].{name:name,tags:tags}"))
        foreach ($resource in $existing) {
            if ($resource.tags.purpose -ne 'manual-telemetry-faults' -or $resource.tags.environment -ne 'test') {
                throw "Refusing to overwrite resource without fault-test tags: $($resource.name)"
            }
        }
        $preview = Invoke-AzureJson @('deployment', 'group', 'what-if', '--subscription', "$SubscriptionId", '--resource-group', $ResourceGroup,
            '--name', "$prefix-infra", '--template-file', 'telemetry/infra/manual-faults.bicep',
            '--parameters', "suffix=$Suffix", "location=$Location", '--no-pretty-print')
        if ($preview.status -ne 'Succeeded') { throw 'Infrastructure preview did not succeed.' }
        foreach ($change in $preview.changes) {
            if ($change.changeType -in @('Ignore', 'NoChange')) { continue }
            $resourceName = ($change.resourceId -split '/')[-1]
            if (-not $resourceName.StartsWith("$prefix-") -or $change.changeType -eq 'Delete') {
                throw "Refusing unexpected infrastructure change: $($change.changeType) $($change.resourceId)"
            }
        }
        $targets = @(Invoke-AzureJson @('deployment', 'group', 'create', '--subscription', "$SubscriptionId", '--resource-group', $ResourceGroup,
            '--name', "$prefix-infra", '--template-file', 'telemetry/infra/manual-faults.bicep',
            '--parameters', "suffix=$Suffix", "location=$Location", '--query', 'properties.outputs.targets.value'))
        if ($targets.Count -ne 3) { throw 'Deployment did not return the three isolated telemetry targets.' }
        foreach ($target in $targets) {
            $billingUrl = "https://management.azure.com$($target.componentId)/currentbillingfeatures?api-version=2015-05-01"
            $billing = Invoke-AzureJson @('rest', '--method', 'get', '--url', $billingUrl)
            $cap = [double]::Parse($target.applicationCapGb, [cultureinfo]::InvariantCulture)
            $body = @{
                CurrentBillingFeatures = @($billing.CurrentBillingFeatures)
                DataVolumeCap = @{ Cap = $cap; StopSendNotificationWhenHitCap = $true }
            }
            $bodyFile = Join-Path $OutputDirectory "$($target.scenario)-billing.json"
            $body | ConvertTo-Json -Depth 5 | Set-Content -LiteralPath $bodyFile -Encoding utf8NoBOM
            Invoke-AzureJson @('rest', '--method', 'put', '--url', $billingUrl, '--body', "@$bodyFile") | Out-Null
            $verified = Invoke-AzureJson @('rest', '--method', 'get', '--url', $billingUrl)
            if ([math]::Abs($verified.DataVolumeCap.Cap - $cap) -gt 0.000001) {
                throw "Application Insights cap readback mismatch for $($target.scenario)"
            }
            $workspace = Invoke-AzureJson @('resource', 'show', '--ids', $target.workspaceId, '--api-version', '2023-09-01')
            $expectedWorkspaceCap = if ($target.scenario -eq 'application') { 1.0 } else { 0.024 }
            if ([math]::Abs($workspace.properties.workspaceCapping.dailyQuotaGb - $expectedWorkspaceCap) -gt 0.000001) {
                throw "Workspace cap readback mismatch for $($target.scenario)"
            }
            Write-Output "$($target.scenario): Application Insights cap=$cap GB/day; workspace cap=$expectedWorkspaceCap GB/day"
        }
        $targets | ConvertTo-Json -Depth 5 | Set-Content -LiteralPath (Join-Path $OutputDirectory 'targets.json') -Encoding utf8NoBOM
    }
    if ($Run) {
        Get-Command go -ErrorAction Stop | Out-Null
        $cliExecutable = ''
        $storagePrincipal = ''
        $storageResourceId = ''
        if ($GrantStoragePermission -and $Scenario -ne 'cli-shutdown') {
            throw '-GrantStoragePermission is only supported with -Scenario cli-shutdown.'
        }
        if ($Scenario -eq 'cli-shutdown') {
            if ($StorageAccountName -notmatch '^[a-z0-9]{3,24}$') {
                throw '-Scenario cli-shutdown requires -StorageAccountName for an Azure Blob test account accessible through your Azure CLI login.'
            }
            if ($GrantStoragePermission) {
                $storagePrincipal = Invoke-AzureJson @('ad', 'signed-in-user', 'show', '--query', 'id')
                $storageResourceId = Invoke-AzureJson @('resource', 'list', '--subscription', "$SubscriptionId", '--name', $StorageAccountName,
                    '--resource-type', 'Microsoft.Storage/storageAccounts', '--query', '[0].id')
                if (-not $storagePrincipal -or -not $storageResourceId) {
                    throw 'Could not resolve the signed-in user and storage account for the temporary container-scoped grant.'
                }
            }
            $extension = if ($IsWindows) { '.exe' } else { '' }
            $cliExecutable = Join-Path $OutputDirectory "azcopy-live$extension"
            & go build -o $cliExecutable .
            if ($LASTEXITCODE -ne 0) { throw 'Failed to build the current AzCopy CLI for the live E2E test.' }
        }
        $settings = @{
            AZCOPY_RUN_LIVE_TELEMETRY = '1'
            AZCOPY_LIVE_TELEMETRY_SUBSCRIPTION = "$SubscriptionId"
            AZCOPY_LIVE_TELEMETRY_RESOURCE_GROUP = $ResourceGroup
            AZCOPY_LIVE_TELEMETRY_SUFFIX = $Suffix
            AZCOPY_DISABLE_TELEMETRY = 'true'
            AZCOPY_LIVE_TELEMETRY_EXECUTABLE = $cliExecutable
            AZCOPY_LIVE_TELEMETRY_STORAGE_ACCOUNT = $StorageAccountName
            AZCOPY_LIVE_TELEMETRY_STORAGE_PRINCIPAL = $storagePrincipal
            AZCOPY_LIVE_TELEMETRY_STORAGE_RESOURCE_ID = $storageResourceId
            AZCOPY_LIVE_TELEMETRY_OUTPUT = $OutputDirectory
        }
        $previous = @{}
        foreach ($name in $settings.Keys) {
            $previous[$name] = [Environment]::GetEnvironmentVariable($name, 'Process')
            [Environment]::SetEnvironmentVariable($name, $settings[$name], 'Process')
        }
        $failed = @()
        try {
            $scenarios = if ($Scenario -eq 'all') { @('shutdown', 'workspace', 'application') } else { @($Scenario) }
            foreach ($selected in $scenarios) {
                $filter = switch ($selected) {
                    'shutdown' { '^TestLiveTelemetryEmergencyShutdown$' }
                    'cli-shutdown' { '^TestLiveTelemetryCLIEmergencyShutdown$' }
                    default { "^TestLiveTelemetryQuota$/^$selected`$" }
                }
                if ($selected -eq 'cli-shutdown') {
                    Write-Output 'Running full CLI shutdown E2E: real Blob downloads and Application Insights; no mock server or quota filling.'
                } else {
                    Write-Output "Running real-endpoint scenario: $selected (maximum 64 MiB request bodies; no mock server)."
                }
                & go test -tags telemetrylive ./azcopy -run $filter -count=1 -timeout=25m -json |
                    Tee-Object -FilePath (Join-Path $OutputDirectory "$selected.jsonl") |
                    ForEach-Object {
                        $entry = $_ | ConvertFrom-Json
                        if ($entry.Action -eq 'output' -and $entry.Output -notmatch 'telemetry: sent packed') {
                            Write-Output $entry.Output.TrimEnd()
                        }
                    }
                if ($LASTEXITCODE -ne 0) { $failed += $selected }
            }
        } finally {
            foreach ($name in $settings.Keys) {
                [Environment]::SetEnvironmentVariable($name, $previous[$name], 'Process')
            }
        }
        if ($failed.Count -gt 0) { throw "Live telemetry scenarios failed: $($failed -join ', '). Evidence: $OutputDirectory" }
    }
} finally {
    Pop-Location
    Write-Output "Manual telemetry evidence: $OutputDirectory"
}