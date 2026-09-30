# Stages the Ev2 service group root for the PROD telemetry rollout: compiles the Bicep
# into the ARM template/parameters the ServiceModel references and stamps the version file.
param(
    [Parameter(Mandatory)][string]$OutputDirectory,
    [Parameter(Mandatory)][string]$Version
)

$ErrorActionPreference = 'Stop'

$repoRoot = Resolve-Path (Join-Path $PSScriptRoot '../..')
$serviceGroupRoot = Join-Path $OutputDirectory 'ServiceGroupRoot'

if (Test-Path $serviceGroupRoot) {
    Remove-Item -Recurse -Force $serviceGroupRoot
}
New-Item -ItemType Directory -Force $OutputDirectory | Out-Null
Copy-Item -Recurse (Join-Path $PSScriptRoot 'ServiceGroupRoot') $serviceGroupRoot
New-Item -ItemType Directory -Force (Join-Path $serviceGroupRoot 'Templates') | Out-Null

az bicep build `
    --file (Join-Path $repoRoot 'infra/telemetry/main.bicep') `
    --outfile (Join-Path $serviceGroupRoot 'Templates/AzCopyTelemetry.Template.json')
if ($LASTEXITCODE -ne 0) { throw 'Bicep template compilation failed.' }

az bicep build-params `
    --file (Join-Path $repoRoot 'infra/telemetry/prod.bicepparam') `
    --outfile (Join-Path $serviceGroupRoot 'Parameters/AzCopyTelemetry.Parameters.json')
if ($LASTEXITCODE -ne 0) { throw 'Bicep parameter compilation failed.' }

[IO.File]::WriteAllText((Join-Path $serviceGroupRoot 'version.txt'), $Version, [Text.Encoding]::ASCII)

Write-Output "Staged Ev2 service group root at $serviceGroupRoot (version $Version)"
