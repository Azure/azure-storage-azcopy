[CmdletBinding()]
param(
    [string]$GrafanaUrl = 'https://azcopy-telemetry-ankur-cbbcech2ecd9gad6.eus.grafana.azure.com',
    [string]$ArtifactDirectory = (Join-Path $PSScriptRoot 'generated\suite'),
    [string]$Database = 'usage-analytics'
)

$ErrorActionPreference = 'Stop'
$allowedUids = @('azcopy-data-metrics', 'azcopy-customer-drilldown', 'azcopy-recurring-trends', 'azcopy-no-observed-success')
$manifest = @(Get-Content -Raw (Join-Path $ArtifactDirectory 'manifest.json') | ConvertFrom-Json)
if ((($manifest.uid | Sort-Object) -join ',') -ne (($allowedUids | Sort-Object) -join ',')) { throw 'Unexpected suite dashboard identities.' }
$token = az account get-access-token --resource 'ce34e7e5-485f-4d76-964f-b3d2b16d1e4f' --query accessToken -o tsv
if ($LASTEXITCODE -ne 0 -or [string]::IsNullOrWhiteSpace($token)) { throw 'Could not authenticate to Grafana.' }
$headers = @{ Authorization = "Bearer $token" }
$backupDirectory = Join-Path $ArtifactDirectory ('backups\' + [datetime]::UtcNow.ToString('yyyyMMddTHHmmssZ'))
$null = [System.IO.Directory]::CreateDirectory($backupDirectory)
foreach ($item in $manifest) {
    try {
        $existing = Invoke-RestMethod -Uri "$GrafanaUrl/api/dashboards/uid/$($item.uid)" -Headers $headers
        [System.IO.File]::WriteAllText((Join-Path $backupDirectory "$($item.uid).json"), ($existing | ConvertTo-Json -Depth 100), [System.Text.UTF8Encoding]::new($false))
    } catch {
        if ($_.Exception.Response.StatusCode.value__ -ne 404) { throw }
    }
    & (Join-Path $PSScriptRoot 'publish-dashboard.ps1') -GrafanaUrl $GrafanaUrl -Database $Database -DashboardPath (Join-Path $ArtifactDirectory "$($item.uid).grafana.json")
}
Write-Host "Previous dashboards backed up to $backupDirectory"