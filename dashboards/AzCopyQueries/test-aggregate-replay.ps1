[CmdletBinding()]
param(
    [string]$ClusterUri = 'https://sm-prd-westeurope.westeurope.kusto.windows.net',
    [string]$Database = 'usage-analytics',
    [switch]$QueryOnly
)

$ErrorActionPreference = 'Stop'
$producer = Get-Content -Raw (Join-Path $PSScriptRoot 'AggregateQueries/01_AzCopy_KO_UsageAggregates.kql')
$bodyStart = $producer.IndexOf('{')
$bodyEnd = $producer.LastIndexOf('}')
if ($bodyStart -lt 0 -or $bodyEnd -le $bodyStart) { throw 'Producer function body was not found.' }
$body = $producer.Substring($bodyStart + 1, $bodyEnd - $bodyStart - 1).
    Replace('__APP_INSIGHTS_CUSTOM_EVENTS__', 'InputEvents').
    Replace('__XSTORE_ACCOUNT_PROPERTIES__', 'Ownership').
    Replace('__AIPDD_SUBSCRIPTION_SNAPSHOT__', 'Inventory')
$query = (Get-Content -Raw (Join-Path $PSScriptRoot 'test-aggregate-replay.kql')).Replace('__AGGREGATE_BODY__', $body)
if ($query -match '__[A-Z_]+__|\bcluster\s*\(|\bdatabase\s*\(|(?m)^\s*\.') {
    throw 'Replay query contains unresolved sources, remote dependencies, or management commands.'
}
if ($QueryOnly) { return $query }

$token = az account get-access-token --resource $ClusterUri --query accessToken -o tsv
if ($LASTEXITCODE -ne 0 -or [string]::IsNullOrWhiteSpace($token)) { throw 'Could not acquire a Kusto token.' }
try {
    $result = Invoke-RestMethod -Method Post -Uri "$ClusterUri/v1/rest/query" -Headers @{ Authorization = "Bearer $token" } `
        -ContentType 'application/json' -Body (@{ db = $Database; csl = $query } | ConvertTo-Json -Compress) -TimeoutSec 120
} finally { $token = $null }
if ($result.Exceptions.Count -gt 0 -or $null -ne $result.error) { throw 'Kusto returned an incomplete replay query result.' }
$table = @($result.Tables | Where-Object { $_.Columns[0].ColumnName -eq 'Test' -and $_.Columns[1].ColumnName -eq 'Pass' })
if ($table.Count -ne 1 -or $table[0].Rows.Count -ne 19) { throw 'Expected exactly 19 replay assertions.' }
$failed = @($table[0].Rows | Where-Object { $_[1] -ne $true })
foreach ($row in $table[0].Rows) { Write-Host "$(if ($row[1]) { 'PASS' } else { 'FAIL' }): $($row[0])" }
if ($failed.Count -gt 0) { throw "$($failed.Count) aggregate replay assertions failed." }