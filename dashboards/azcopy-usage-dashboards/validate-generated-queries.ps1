param(
    [string]$ArtifactPattern = "*.telemetry-test.dashboard.json",
    [string]$CentralClusterUri = "https://azcore.centralus.kusto.windows.net",
    [string]$CentralDatabase = "Xstore",
    [string]$Offset = "90d",
    [ValidateRange(1, 10)] [int]$MaxAttempts = 3,
    [ValidateRange(0, [int]::MaxValue)] [int]$SkipUniqueQueries = 0
)

$ErrorActionPreference = "Stop"
$artifactRoot = Join-Path $PSScriptRoot "generated"

$token = (& az account get-access-token --resource https://api.kusto.windows.net --query accessToken --output tsv).Trim()
if ($LASTEXITCODE -ne 0 -or [string]::IsNullOrWhiteSpace($token)) {
    throw "Could not acquire an Azure Data Explorer token."
}

$parameterPrefix = @"
let _startTime = ago($Offset);
let _endTime = now();
let _installationID = '';
let _account = '';
let _invocationContext = '';
let _sourceType = '';
let _destType = '';
let _subscriptionScope = '';
let _customer = '';
"@

function Get-KustoQueryStatusFailures([object]$Response) {
    $failures = [Collections.Generic.List[string]]::new()
    foreach ($table in @($Response.Tables)) {
        $columnNames = @($table.Columns | ForEach-Object { $_.ColumnName })
        $statusCodeIndex = [array]::IndexOf($columnNames, "StatusCode")
        if ($statusCodeIndex -lt 0) {
            continue
        }

        $severityIndex = [array]::IndexOf($columnNames, "SeverityName")
        $descriptionIndex = [array]::IndexOf($columnNames, "StatusDescription")
        foreach ($row in @($table.Rows)) {
            if ([long]$row[$statusCodeIndex] -eq 0) {
                continue
            }

            $severity = if ($severityIndex -ge 0) { [string]$row[$severityIndex] } else { "Unknown" }
            $description = if ($descriptionIndex -ge 0) { [string]$row[$descriptionIndex] } else { "No description" }
            $failures.Add("$severity`: $description")
        }
    }
    return @($failures)
}

function Test-IsExpectedEnrichmentStatusFailure([string]$Failure) {
    return $Failure -match '(?i)^Warning: Cross-cluster query failure .*cluster\(''https://(argeusarm1pone\.eastus|aipddprod|xdataanalytics\.westcentralus)\.kusto\.windows\.net/'
}

function Test-IsTransientKustoFailure([System.Management.Automation.ErrorRecord]$ErrorRecord) {
    $exception = $ErrorRecord.Exception
    $errorText = "$($ErrorRecord.ErrorDetails.Message)`n$($exception.Message)"
    $failureCodeMatch = [regex]::Match($errorText, '"@failureCode"\s*:\s*"?(\d{3})"?')
    if ($failureCodeMatch.Success) {
        $failureCode = [int]$failureCodeMatch.Groups[1].Value
        return $failureCode -eq 408 -or $failureCode -eq 429 -or $failureCode -ge 500
    }

    for ($current = $exception; $null -ne $current; $current = $current.InnerException) {
        $statusCodeProperty = $current.PSObject.Properties["StatusCode"]
        if ($null -ne $statusCodeProperty -and $null -ne $statusCodeProperty.Value) {
            $statusCode = [int]$statusCodeProperty.Value
            return $statusCode -eq 408 -or $statusCode -eq 429 -or $statusCode -ge 500
        }

        $responseProperty = $current.PSObject.Properties["Response"]
        if ($null -ne $responseProperty -and
            $null -ne $responseProperty.Value -and
            $null -ne $responseProperty.Value.StatusCode) {
            $statusCode = [int]$responseProperty.Value.StatusCode
            return $statusCode -eq 408 -or $statusCode -eq 429 -or $statusCode -ge 500
        }

        if ($current -is [System.TimeoutException] -or
            $current -is [System.Threading.Tasks.TaskCanceledException]) {
            return $true
        }

        if ($current -is [System.Net.Http.HttpRequestException]) {
            return $true
        }
    }

    return $errorText -match '(?i)Unable to read data from the transport connection|connection was forcibly closed by the remote host|E_QUERY_CANCELLED|Request aborted due to an internal service error'
}

function Invoke-KustoQuery([string]$Name, [string]$Query) {
    $body = @{
        db = $CentralDatabase
        csl = "$parameterPrefix`n$Query"
        properties = @{
            Options = @{
                queryconsistency = "strongconsistency"
            }
        }
    } | ConvertTo-Json -Depth 6
    $headers = @{
        Authorization = "Bearer $token"
        Accept = "application/json"
        "Content-Type" = "application/json; charset=utf-8"
        "x-ms-client-request-id" = "AzCopyUsageDashboardValidation;$([Guid]::NewGuid())"
    }

    $stopwatch = [Diagnostics.Stopwatch]::StartNew()
    for ($attempt = 1; $attempt -le $MaxAttempts; $attempt++) {
        try {
            $response = Invoke-RestMethod `
                -Method Post `
                -Uri "$($CentralClusterUri.TrimEnd('/'))/v1/rest/query" `
                -Headers $headers `
                -Body $body `
                -TimeoutSec 600
            $queryStatusFailures = @(Get-KustoQueryStatusFailures $response)
            if ($queryStatusFailures.Count -gt 0) {
                $hasPrimaryResult = @(
                    $response.Tables | Where-Object {
                        $_.TableKind -eq 'PrimaryResult' -or
                        (@($_.Columns | ForEach-Object { $_.ColumnName }) -notcontains 'StatusCode')
                    }
                ).Count -gt 0
                $unexpectedStatusFailures = @(
                    $queryStatusFailures | Where-Object {
                        -not (Test-IsExpectedEnrichmentStatusFailure $_)
                    }
                )
                if (-not $hasPrimaryResult -or $unexpectedStatusFailures.Count -gt 0) {
                    throw "Kusto returned query status failures: $($queryStatusFailures -join ' | ')"
                }
            }
            $stopwatch.Stop()
            $statusSuffix = if ($queryStatusFailures.Count -gt 0) {
                " with $($queryStatusFailures.Count) expected enrichment warning(s)"
            } else {
                ""
            }
            Write-Host "PASS $Name$statusSuffix ($([Math]::Round($stopwatch.Elapsed.TotalSeconds, 1))s)"
            return
        }
        catch {
            $isTransient = Test-IsTransientKustoFailure $_
            if (-not $isTransient -or $attempt -eq $MaxAttempts) {
                $stopwatch.Stop()
                Write-Host "FAIL $Name ($([Math]::Round($stopwatch.Elapsed.TotalSeconds, 1))s)" -ForegroundColor Red
                throw
            }

            $delaySeconds = [Math]::Pow(2, $attempt)
            Write-Warning "Transient query failure for '$Name'; retrying in $delaySeconds seconds (attempt $($attempt + 1) of $MaxAttempts)."
            Start-Sleep -Seconds $delaySeconds
        }
    }
}

$artifacts = @(Get-ChildItem $artifactRoot -Filter $ArtifactPattern | Sort-Object Name)
if ($artifacts.Count -eq 0) {
    throw "No generated ADX artifacts matched '$ArtifactPattern'."
}

$validatedHashes = [Collections.Generic.HashSet[string]]::new()
$validationCount = 0
foreach ($artifact in $artifacts) {
    $dashboard = Get-Content -Raw $artifact.FullName | ConvertFrom-Json
    for ($index = 0; $index -lt $dashboard.queries.Count; $index++) {
        $query = [string]$dashboard.queries[$index].text
        $hashBytes = [Security.Cryptography.SHA256]::HashData([Text.Encoding]::UTF8.GetBytes($query))
        $hash = [Convert]::ToHexString($hashBytes)
        if (-not $validatedHashes.Add($hash)) {
            continue
        }
        if ($validatedHashes.Count -le $SkipUniqueQueries) {
            continue
        }

        Invoke-KustoQuery "$($artifact.BaseName) query $index" $query
        $validationCount++
    }
}

Write-Host "Validated $validationCount unique generated ADX queries from $($artifacts.Count) artifacts after skipping $SkipUniqueQueries."
