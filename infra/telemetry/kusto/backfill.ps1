# Copies AzCopy telemetry from the Log Analytics workspace into the Kusto export database: the history from
# before the export existed, or a gap an export outage left. For each time slice, Kusto reads the workspace
# with a cross-service query (as the caller), expands the rows with ExpandAppEventsFrom, the function the live
# path uses, and appends to AppEvents only the events AppEventsDedup lacks. Overlapping or repeated runs add
# nothing, so a failed run can simply be rerun.
#
# The caller needs Log Analytics Reader on the workspace, and Admin (or both Viewer and Ingestor) on the database:
# Ingestor for the appends, Viewer for the counts and the AppEventsDedup lookups. Only data in the
# workspace's interactive retention is readable; older data must be restored or searched into a table first.
# A slice of 20,000 events takes about 10 seconds. For a long range, run several instances on separate date ranges
# at once; Log Analytics allows 5 concurrent queries per user.
#
# Example:
#   ./backfill.ps1 -ClusterUri https://sm-ppe-westeurope.westeurope.kusto.windows.net -Database azcopy-telemetry `
#       -WorkspaceResourceId /subscriptions/<id>/resourceGroups/<rg>/providers/Microsoft.OperationalInsights/workspaces/<name> `
#       -Start 2026-10-20T00:00:00Z -End 2026-11-18T18:00:00Z
param(
    [Parameter(Mandatory)][string]$ClusterUri,
    [Parameter(Mandatory)][string]$Database,
    [Parameter(Mandatory)][string]$WorkspaceResourceId,
    [Parameter(Mandatory)][datetime]$Start,
    [Parameter(Mandatory)][datetime]$End,
    # Kusto's cross-service read of each slice fails above 64 MB (LimitsExceeded / E_QUERY_RESULT_SET_TOO_LARGE),
    # and neither notruncation nor truncationmaxsize lifts that inside a command. AzCopy events are about 2 KB there,
    # so 20,000 events is about 40 MB; a slice that still fails is split in half and retried.
    [int]$MaxEventsPerSlice = 20000,
    # Lists the slices without copying anything.
    [switch]$PlanOnly
)

$ErrorActionPreference = 'Stop'

$ClusterUri = $ClusterUri.TrimEnd('/')
$Start = $Start.ToUniversalTime()
$End = $End.ToUniversalTime()
if ($End -le $Start) { throw 'End must be after Start.' }
if ($WorkspaceResourceId -notmatch '^/subscriptions/[^/]+/resourceGroups/[^/]+/providers/Microsoft\.OperationalInsights/workspaces/([^/]+)$') {
    throw 'WorkspaceResourceId must be a Log Analytics workspace resource ID.'
}
$workspace = "cluster('https://ade.loganalytics.io$($WorkspaceResourceId.ToLowerInvariant())').database('$($Matches[1])')"

$script:token = $null
$script:tokenExpires = [datetime]::MinValue

function Get-KustoToken {
    if ((Get-Date) -ge $script:tokenExpires) {
        $script:token = az account get-access-token --resource $ClusterUri --query accessToken --output tsv
        if ($LASTEXITCODE -ne 0 -or -not $script:token) { throw 'Failed to get a Kusto token; run az login.' }
        $script:tokenExpires = (Get-Date).AddMinutes(30)
    }
    $script:token
}

function Invoke-Kusto([string]$Csl, [switch]$Command) {
    $endpoint = if ($Command) { "$ClusterUri/v1/rest/mgmt" } else { "$ClusterUri/v2/rest/query" }
    $body = @{ db = $Database; csl = $Csl; properties = @{ Options = @{ servertimeout = '00:30:00' } } } | ConvertTo-Json -Depth 5 -Compress
    try {
        $response = Invoke-RestMethod -Method Post -Uri $endpoint -Body $body -ContentType 'application/json; charset=utf-8' `
            -Headers @{ Authorization = "Bearer $(Get-KustoToken)" } -TimeoutSec 2400
    } catch {
        $detail = if ($_.ErrorDetails.Message) { $_.ErrorDetails.Message } else { $_.Exception.Message }
        throw "Kusto request failed: $detail"
    }
    $table = if ($Command) { $response.Tables[0] } else {
        # Query failures such as truncation arrive in a 200 response, flagged in the completion frame.
        $completion = $response | Where-Object FrameType -eq 'DataSetCompletion'
        if ($completion.HasErrors) { throw "Kusto query failed: $($completion.OneApiErrors | ConvertTo-Json -Depth 10 -Compress)" }
        $response | Where-Object TableKind -eq 'PrimaryResult' | Select-Object -First 1
    }
    foreach ($row in $table.Rows) {
        $record = @{}
        for ($i = 0; $i -lt $table.Columns.Count; $i++) { $record[$table.Columns[$i].ColumnName] = $row[$i] }
        New-Object -TypeName psobject -Property $record
    }
}

function Format-KustoTime([datetime]$Time) { "datetime($($Time.ToUniversalTime().ToString('o')))" }

function Copy-Slice([datetime]$SliceStart, [datetime]$SliceEnd) {
    $from = Format-KustoTime $SliceStart
    $to = Format-KustoTime $SliceEnd
    # The record shape matches the diagnostic-setting stream, so ExpandAppEventsFrom treats both paths alike.
    # EventHubOffset 'backfill' marks the rows; the workspace's receive time stands in for the enqueue time.
    $csl = @"
.set-or-append AppEvents <|
let SliceStart = $from;
let SliceEnd = $to;
let Messages = $workspace.AppEvents
    | where TimeGenerated >= SliceStart and TimeGenerated < SliceEnd
    | project Message = bag_pack('records', pack_array(bag_pack('time', TimeGenerated, 'Name', Name,
            'Properties', Properties, 'Measurements', Measurements, 'ClientCountryOrRegion', ClientCountryOrRegion,
            'ClientStateOrProvince', ClientStateOrProvince, 'ClientCity', ClientCity, 'ItemCount', ItemCount,
            '_BilledSize', _BilledSize))),
        EventHubEnqueuedTime = _TimeReceived, EventHubSequenceNumber = long(null), EventHubOffset = 'backfill';
ExpandAppEventsFrom(Messages)
| join kind=leftanti (AppEventsDedup | where TimeGenerated >= SliceStart and TimeGenerated < SliceEnd | project Name, InvocationID) on Name, InvocationID
"@
    try {
        $extents = @(Invoke-Kusto $csl -Command)
    } catch {
        # An oversized slice fails rather than returning partial rows; halve it until it fits.
        if ($_.Exception.Message -notmatch 'LimitsExceeded|E_QUERY_RESULT_SET_TOO_LARGE' -or ($SliceEnd - $SliceStart).TotalSeconds -le 60) { throw }
        $middle = $SliceStart.AddTicks(($SliceEnd - $SliceStart).Ticks / 2)
        Write-Host "  slice too large; splitting at $($middle.ToString('o'))"
        return (Copy-Slice $SliceStart $middle) + (Copy-Slice $middle $SliceEnd)
    }
    [long](($extents | Measure-Object -Property RowCount -Sum).Sum)
}

Write-Host "Counting workspace events from $($Start.ToString('o')) to $($End.ToString('o'))"
$hours = @(Invoke-Kusto @"
$workspace.AppEvents
| where TimeGenerated >= $(Format-KustoTime $Start) and TimeGenerated < $(Format-KustoTime $End)
| summarize Events = count() by Hour = bin(TimeGenerated, 1h)
| order by Hour asc
"@)

# Slices tile the whole range, so events that reach the workspace late still fall in one. Consecutive hours share a
# slice while it stays under the limit; a busier hour is split evenly.
# The script avoids .NET types beyond core ones so it also runs in ConstrainedLanguage mode (SAW, Windows PowerShell 5.1).
function New-Boundary([datetime]$BoundaryEnd, [long]$Events) { New-Object -TypeName psobject -Property @{ End = $BoundaryEnd; Events = $Events } }
function Get-CeilingDivision([long]$Dividend, [long]$Divisor) { [long](($Dividend - $Dividend % $Divisor) / $Divisor) + [long]($Dividend % $Divisor -ne 0) }
$pending = 0L
$ends = @(
    foreach ($hour in $hours) {
        $hourStart = ([datetime]$hour.Hour).ToUniversalTime()
        $hourEnd = $hourStart.AddHours(1)
        if ($hourStart -lt $Start) { $hourStart = $Start }
        if ($hourEnd -gt $End) { $hourEnd = $End }
        $events = [long]$hour.Events
        if ($events -gt $MaxEventsPerSlice) {
            if ($pending -gt 0) { New-Boundary $hourStart $pending; $pending = 0 }
            $parts = Get-CeilingDivision $events $MaxEventsPerSlice
            $step = [long](($hourEnd - $hourStart).Ticks / $parts)
            for ($i = 1; $i -le $parts; $i++) {
                $partEnd = if ($i -eq $parts) { $hourEnd } else { $hourStart.AddTicks($step * $i) }
                New-Boundary $partEnd (Get-CeilingDivision $events $parts)
            }
        } elseif ($pending + $events -gt $MaxEventsPerSlice) {
            New-Boundary $hourStart $pending
            $pending = $events
        } else {
            $pending += $events
        }
    }
    New-Boundary $End $pending
)
$sliceStart = $Start
$slices = @(
    foreach ($boundary in $ends) {
        if ($boundary.End -gt $sliceStart) {
            New-Object -TypeName psobject -Property @{ Start = $sliceStart; End = $boundary.End; Events = $boundary.Events }
            $sliceStart = $boundary.End
        }
    }
)

$total = ($hours | Measure-Object -Property Events -Sum).Sum
Write-Host "$total workspace events in $($hours.Count) hours -> $($slices.Count) slices"
if ($PlanOnly) {
    $slices | Format-Table @{ n = 'Start'; e = { $_.Start.ToString('u') } }, @{ n = 'End'; e = { $_.End.ToString('u') } }, Events | Out-String | Write-Host
    return
}

function Format-Duration([timespan]$Span) { '{0}:{1:mm\:ss}' -f ($Span.Days * 24 + $Span.Hours), $Span }

$appended = 0L
$started = Get-Date
for ($i = 0; $i -lt $slices.Count; $i++) {
    $slice = $slices[$i]
    $rows = Copy-Slice $slice.Start $slice.End
    $appended += $rows
    $elapsed = (Get-Date) - $started
    $remaining = New-TimeSpan -Seconds ([int]($elapsed.TotalSeconds / ($i + 1) * ($slices.Count - $i - 1)))
    Write-Host ("[{0}/{1}] {2:u} .. {3:u}: ~{4} workspace events, {5} appended ({6} elapsed, ~{7} left)" -f ($i + 1), $slices.Count, $slice.Start, $slice.End, $slice.Events, $rows, (Format-Duration $elapsed), (Format-Duration $remaining))
}
Write-Host "Appended $appended rows to AppEvents."

# Each day: distinct (Name, InvocationID) in the workspace, the key AppEventsDedup keeps one row per, against the view.
# A key seen on two days (possible only for events without an InvocationID) counts on both here but once in the view.
Write-Host 'Comparing the workspace with AppEventsDedup by day (UTC):'
$comparison = for ($day = $Start.Date; $day -lt $End; $day = $day.AddDays(1)) {
    $from = Format-KustoTime $(if ($day -lt $Start) { $Start } else { $day })
    $to = Format-KustoTime $(if ($day.AddDays(1) -gt $End) { $End } else { $day.AddDays(1) })
    $counts = Invoke-Kusto @"
let WorkspaceEvents = toscalar($workspace.AppEvents
    | where TimeGenerated >= $from and TimeGenerated < $to
    | summarize by Name, InvocationID = tostring(Properties.InvocationID)
    | count);
let KustoEvents = toscalar(AppEventsDedup | where TimeGenerated >= $from and TimeGenerated < $to | count);
print Workspace = WorkspaceEvents, Kusto = KustoEvents
"@
    Write-Host ("  {0:yyyy-MM-dd}: workspace {1}, Kusto {2}" -f $day, $counts.Workspace, $counts.Kusto)
    if ([long]$counts.Workspace -gt 0 -or [long]$counts.Kusto -gt 0) {
        New-Object -TypeName psobject -Property @{ Day = $day.ToString('yyyy-MM-dd'); Workspace = [long]$counts.Workspace; Kusto = [long]$counts.Kusto; Missing = [long]$counts.Workspace - [long]$counts.Kusto }
    }
}
$comparison | Format-Table Day, Workspace, Kusto, Missing | Out-String | Write-Host
$short = @($comparison | Where-Object Missing -gt 0)
if ($short) {
    Write-Warning "$($short.Count) day(s) have events missing from Kusto; rerun for those days."
    exit 1
}
Write-Host 'Kusto has every workspace event in the range. (Kusto may have more on days the workspace cap dropped events.)'
