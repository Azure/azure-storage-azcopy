[CmdletBinding()]
param(
    [string]$OutputPath = (Join-Path $PSScriptRoot 'generated/azcopy-engineering-no-success.grafana.json'),
    [string]$DatasourceUid = 'azcopy-usage-analytics',
    [string]$Database = 'usage-analytics'
)

$ErrorActionPreference = 'Stop'
if ($args.Count -gt 0) { throw "Unrecognized arguments: $($args -join ' ')" }
$datasource = [ordered]@{ type = 'grafana-azure-data-explorer-datasource'; uid = $DatasourceUid }
$usage = @'
let Usage = materialize(
    AzCopy_Usage($__timeFrom, $__timeTo, dynamic([]), dynamic([]), dynamic(${SourceType:json}), dynamic(${TargetType:json}))
    | where Jobs > 0
    | extend Mapped = IsMapped and isnotempty(CustomerKey)
);
'@
$customers = @'
let Customers = Usage
    | where Mapped
    | summarize Jobs=sum(Jobs), FailedJobs=sumif(Jobs, IsFailure), SuccessfulJobs=sumif(Jobs, IsSuccess),
        CancelledJobs=sumif(Jobs, JobStatus == 'Cancelled'), LastErrorDay=maxif(SliceDate, IsFailure),
        Customer=take_any(CustomerName), Subscriptions=make_set(SubscriptionId, 100), Scenarios=make_set(FromTo, 100)
        by CustomerKey
    | extend FailureRate=100.0 * FailedJobs / Jobs;
'@
$customerSummary = @'
Customers
| summarize ActiveCustomers=count(), AffectedCustomers=countif(FailedJobs > 0), Jobs=sum(Jobs), FailedJobs=sum(FailedJobs)
| extend AffectedPercent=iff(ActiveCustomers > 0, 100.0 * AffectedCustomers / ActiveCustomers, real(null)),
    FailureRate=iff(Jobs > 0, 100.0 * FailedJobs / Jobs, real(null))
'@
$raw = @'
let Events = materialize(
    cluster('https://adx.monitor.azure.com/subscriptions/31347be8-d066-464e-9866-7e58d85027b7/resourcegroups/azcopy-telemetry-test-rg/providers/microsoft.insights/components/azcopy-telemetry-test-ai').database('azcopy-telemetry-test-ai').customEvents
    | where timestamp between ($__timeFrom .. $__timeTo)
    | where name == 'azcopy.job.finished' and tostring(customDimensions.SchemaVersion) in ('1', '3')
    | project timestamp, InvocationID=tostring(customDimensions.InvocationID), RunID=tostring(customDimensions.E2ETestRunID),
        SourceType=tostring(customDimensions.SourceType), TargetType=tostring(customDimensions.DestType),
        JobStatus=tostring(customDimensions.JobStatus), Category=tostring(customDimensions.JobErrorCategory),
        Code=tostring(customDimensions.JobErrorCode), Stage=tostring(customDimensions.TerminalStage),
        Command=tostring(customDimensions.Command), FromTo=tostring(customDimensions.FromTo),
        OS=tostring(customDimensions.OSType), Version=tostring(customDimensions.AzCopyVersion),
        Measurements=customMeasurements, ErrorHistogram=tostring(customDimensions.FailureErrorCodes)
);
let IsAll = (values:dynamic) { array_length(values) == 0 or '$__all' in (values) or '__all' in (values) };
let Raw = materialize(
    union (Events | where isnotempty(InvocationID) | summarize arg_max(timestamp, *) by InvocationID),
          (Events | where isempty(InvocationID))
    | where IsAll(dynamic(${SourceType:json})) or SourceType in (dynamic(${SourceType:json}))
    | where IsAll(dynamic(${TargetType:json})) or TargetType in (dynamic(${TargetType:json}))
    | where toint(dynamic(${RawRunScope:json})) == 0 or (toint(dynamic(${RawRunScope:json})) == 1 and isnotempty(RunID))
        or (toint(dynamic(${RawRunScope:json})) == 2 and isempty(RunID))
    | extend IsFailure=JobStatus in ('Failed', 'CompletedWithErrors', 'CompletedWithErrorsAndSkipped'),
        Category=iff(isempty(Category), 'Not reported', Category), Code=iff(isempty(Code), 'Not reported', Code),
        Stage=iff(isempty(Stage), 'Not reported', Stage)
);
'@

$reliability = @'
let Reliability = Raw
    | extend HTTPAttempts=todouble(Measurements['azcopy.storage_http_attempt_count']),
        NetworkErrors=todouble(Measurements['azcopy.network_error_attempt_count']),
        Busy503=todouble(Measurements['azcopy.server_busy_503_count']),
        BusyThroughput=todouble(Measurements['azcopy.server_busy_throughput_count']),
        BusyIOPS=todouble(Measurements['azcopy.server_busy_iops_count']),
        BusyOther=todouble(Measurements['azcopy.server_busy_other_count']),
        OmittedErrors=todouble(Measurements['azcopy.failure_error_other_count']),
        Progress=todouble(Measurements['azcopy.percent_complete']);
let Requests = Reliability
    | where HTTPAttempts >= 0 and NetworkErrors >= 0 and Busy503 >= 0
        and NetworkErrors <= HTTPAttempts and Busy503 <= HTTPAttempts;
'@

function New-QueryPanel([int]$Id, [string]$Title, [string]$Type, [int]$X, [int]$Y, [int]$Width, [int]$Height, [string]$Query, [string]$Description, [string]$Unit = 'none') {
    $defaults = @{ unit = $Unit; decimals = $(if ($Unit -eq 'percent') { 1 } else { 0 }); noValue = 'No data'; color = @{ mode = 'palette-classic' }; mappings = @() }
    $options = @{ showHeader = $true; cellHeight = 'sm'; footer = @{ show = $false }; sortBy = @() }
    $format = 'table'
    if ($Type -eq 'stat') {
        $defaults.color = @{ mode = 'fixed'; fixedColor = $(if ($Unit -eq 'dateTimeAsIso') { 'text' } else { 'orange' }) }
        $options = @{ colorMode = 'value'; graphMode = 'none'; textMode = 'value'; reduceOptions = @{ calcs = @('lastNotNull'); fields = ''; values = $false } }
    } elseif ($Type -eq 'timeseries') {
        $format = 'time_series'
        $defaults.custom = @{ drawStyle = 'line'; lineInterpolation = 'stepAfter'; lineWidth = 2; fillOpacity = 12; showPoints = 'always'; pointSize = 5; spanNulls = $false }
        $options = @{ legend = @{ displayMode = 'list'; placement = 'bottom' }; tooltip = @{ mode = 'single' } }
    } elseif ($Type -eq 'barchart') {
        $defaults.min = 0
        $defaults.custom = @{ axisWidth = 210 }
        $options = @{ orientation = 'horizontal'; xField = 'ErrorCode'; showValue = 'always'; groupWidth = 0.7; barWidth = 0.9; barRadius = 0
            xTickLabelMaxLength = 64; legend = @{ showLegend = $false }; tooltip = @{ mode = 'single' } }
    }
    return [ordered]@{
        id = $Id; title = $Title; type = $Type; description = $Description
        gridPos = [ordered]@{ h = $Height; w = $Width; x = $X; y = $Y }
        datasource = $datasource
        targets = @([ordered]@{ datasource = $datasource; database = $Database; query = $Query.Trim(); queryType = 'KQL'; rawMode = $true; refId = 'A'; resultFormat = $format })
        fieldConfig = [ordered]@{
            defaults = $defaults
            overrides = @(
                @{ matcher = @{ id = 'byRegexp'; options = '.*(Rate|Percent)$' }; properties = @(@{ id = 'unit'; value = 'percent' }, @{ id = 'decimals'; value = 1 }) },
                @{ matcher = @{ id = 'byRegexp'; options = '^(NetworkErrorPercent|ServerBusyPercent)$' }; properties = @(@{ id = 'decimals'; value = 4 }) },
                @{ matcher = @{ id = 'byRegexp'; options = '.*(Day|Seen|Refresh)$' }; properties = @(@{ id = 'unit'; value = 'dateTimeAsIso' }) },
                @{ matcher = @{ id = 'byName'; options = 'Code' }; properties = @(@{ id = 'custom.width'; value = 260 }) },
                @{ matcher = @{ id = 'byName'; options = 'FailedAttempts' }; properties = @(@{ id = 'custom.width'; value = 120 }) },
                @{ matcher = @{ id = 'byName'; options = 'Category' }; properties = @(@{ id = 'displayName'; value = 'Error category' }) },
                @{ matcher = @{ id = 'byName'; options = 'Stage' }; properties = @(@{ id = 'displayName'; value = 'Stage at exit' }) },
                @{ matcher = @{ id = 'byName'; options = 'CustomerKey' }; properties = @(@{ id = 'custom.hidden'; value = $true }) },
                @{ matcher = @{ id = 'byName'; options = 'Customer' }; properties = @(@{ id = 'links'; value = @(@{ title = 'Customer drilldown'; targetBlank = $false; url = '/d/azcopy-customer-drilldown?${__url_time_range}&var-Customer=${__data.fields.CustomerKey:percentencode}&var-SubscriptionId=$__all&var-SourceType=$__all&var-TargetType=$__all' }) }) }
            )
        }
        options = $options
    }
}

function New-Variable([string]$Name, [string]$Label, [string]$Query, [int]$Refresh) {
    return [ordered]@{
        name = $Name; label = $Label; type = 'query'; datasource = $datasource
        multi = $true; includeAll = $true; allValue = '[]'; refresh = $Refresh; sort = 1
        query = @{ database = $Database; query = $Query; queryType = 'KQL'; refId = "Variable-$Name" }
        definition = $Query; options = @(); current = @{ selected = $true; text = @('All'); value = @('$__all') }
    }
}

$description = 'Test telemetry only. Customer impact uses daily job summaries; error diagnostics use raw attempts and are not linked to customer identities.'
$panels = @(
    @{ id = 10; title = 'Customer impact | persisted TEST aggregates'; type = 'row'; collapsed = $false; panels = @(); gridPos = @{ h = 1; w = 24; x = 0; y = 0 } },
    (New-QueryPanel 1 'Customers with errors' 'stat' 0 1 6 5 "$usage`n$customers`n$customerSummary`n| project AffectedCustomers" 'Customers with at least one failed job, including those with successful jobs. Only customers we can identify are counted.'),
    (New-QueryPanel 2 'Customers affected (%)' 'stat' 6 1 6 5 "$usage`n$customers`n$customerSummary`n| project AffectedPercent" 'Share of active, identified customers with at least one failed job in this view.' 'percent'),
    (New-QueryPanel 3 'Failed jobs (mapped)' 'stat' 12 1 6 5 "$usage`n$customers`n$customerSummary`n| project FailedJobs" 'Jobs whose latest reported outcome was failed or completed with errors. Cancelled jobs are excluded.'),
    (New-QueryPanel 4 'Job failure rate (%)' 'stat' 18 1 6 5 "$usage`n$customers`n$customerSummary`n| project FailureRate" 'Failed jobs divided by all jobs for identified customers. Cancelled jobs are included in the total, but not counted as failures.' 'percent'),
    (New-QueryPanel 5 'Latest aggregate activity day' 'stat' 0 6 12 4 "$usage`nUsage | summarize LatestDay=max(SliceDate) | project LatestDay=(LatestDay-datetime(1970-01-01))/1ms" 'Latest day with recorded job activity. An old date means the data may be stale.' 'dateTimeAsIso'),
    (New-QueryPanel 6 'Aggregate refreshed at' 'stat' 12 6 12 4 "$usage`nUsage | summarize LastRefresh=max(LastRefresh) | project LastRefresh=(LastRefresh-datetime(1970-01-01))/1ms" 'When the summary data was last refreshed.' 'dateTimeAsIso'),
    (New-QueryPanel 7 'Customers encountering errors' 'table' 0 10 24 11 @"
$usage
$customers
Customers | where FailedJobs > 0
| project Customer, CustomerKey, FailedJobs, Jobs, FailureRate, SuccessfulJobs, CancelledJobs, LastErrorDay, Subscriptions, Scenarios
| order by FailedJobs desc, FailureRate desc, Customer asc
| take 100
"@ 'Top 100 identified customers by failed jobs, with successes and failure rates shown alongside.'),
    (New-QueryPanel 8 'Affected customers by day' 'timeseries' 0 21 12 9 @"
$usage
Usage | where Mapped
| summarize FailedJobs=sumif(Jobs, IsFailure) by SliceDate, CustomerKey
| summarize AffectedCustomers=countif(FailedJobs > 0) by Time=SliceDate
| order by Time asc
"@ 'Customers with failed jobs each day (UTC). Gaps mean no data, not zero errors.'),
    (New-QueryPanel 9 'Errors by transfer scenario | mapped' 'table' 12 21 12 9 @"
$usage
Usage | where Mapped
| summarize Jobs=sum(Jobs), FailedJobs=sumif(Jobs, IsFailure) by Command, FromTo, CustomerKey
| summarize Jobs=sum(Jobs), FailedJobs=sum(FailedJobs), AffectedCustomers=countif(FailedJobs > 0) by Command, FromTo
| where FailedJobs > 0
| extend FailureRate=100.0 * FailedJobs / Jobs
| project Command, FromTo, AffectedCustomers, FailedJobs, Jobs, FailureRate
| order by FailedJobs desc
"@ 'Failures and rates by command and transfer direction. A customer may appear in more than one row.'),
    (New-QueryPanel 11 'Terminal outcomes | mapped' 'table' 0 30 12 8 @"
$usage
Usage | where Mapped
| summarize Jobs=sum(Jobs) by JobStatus, CustomerKey
| summarize Customers=count(), Jobs=sum(Jobs) by JobStatus
| order by Jobs desc
"@ 'Reported job outcomes, including successes and cancellations. These are statuses, not error causes.'),
    (New-QueryPanel 12 'Customer mapping coverage' 'table' 12 30 12 8 @"
$usage
Usage | summarize Jobs=sum(Jobs), FailedJobs=sumif(Jobs, IsFailure) by Attribution=iff(Mapped, 'Mapped', 'Unmapped')
| extend FailureRate=iff(Jobs > 0, 100.0 * FailedJobs / Jobs, real(null))
| order by Attribution asc
"@ 'Jobs split by whether we can identify the customer. Unmapped jobs are excluded from customer counts.'),
    @{
        id = 30; title = 'Jobs, attempts and customer counts'; type = 'text'
        gridPos = @{ h = 14; w = 24; x = 0; y = 38 }
        options = @{
            mode = 'markdown'
            content = @'
A **job** is one logical AzCopy operation, identified by `JobID`. An **attempt** is one execution of that job, identified by `InvocationID`.

**Example:** `azcopy copy` fails, then `azcopy jobs resume` completes the same job. That is **1 job, 2 attempts and 1 failed attempt**. The job panels count the latest reported job outcome, so this example becomes a successful job. The raw panels still count its earlier failed attempt.

Running `azcopy copy` again from scratch generally creates a new job. Internal HTTP retries, chunks and individual files are **not** additional attempts here. Duplicate finish events are counted once when an invocation ID is available.

### Comparing the counts

Customer panels use persisted daily job summaries for identified customers. Raw panels count execution attempts, are not linked to customer identities, and can contain newer data. **Their totals are not directly comparable:** check the freshness cards and selected filters. Both sections show test telemetry, not production usage.

Failed outcomes include `Failed`, `CompletedWithErrors` and `CompletedWithErrorsAndSkipped`. Cancellations are not counted as failures. Only delivered finish telemetry is represented; missing events or telemetry opt-out can leave gaps.
'@
        }
    },
    @{ id = 20; title = 'Error diagnostics | raw TEST attempts, not customer-attributed'; type = 'row'; collapsed = $false; panels = @(); gridPos = @{ h = 1; w = 24; x = 0; y = 52 } },
    (New-QueryPanel 21 'Failed attempts | raw' 'stat' 0 53 8 5 "$raw`nRaw | summarize FailedAttempts=countif(IsFailure)" 'Failed attempts, deduplicated where an invocation ID is available. Includes failures followed by a successful retry.'),
    (New-QueryPanel 22 'E2E-tagged failed attempts | raw' 'stat' 8 53 8 5 "$raw`nRaw | summarize E2EFailedAttempts=countif(IsFailure and isnotempty(RunID))" 'Failed attempts tagged with an E2E test run ID. Untagged events can still be test traffic.'),
    (New-QueryPanel 23 'Latest finish event | raw' 'stat' 16 53 8 5 "$raw`nRaw | summarize LastSeen=max(timestamp) | project LastSeen=(LastSeen-datetime(1970-01-01))/1ms" 'Time of the latest recorded finish event, including successful runs. This is event time, not ingestion time.' 'dateTimeAsIso'),
    (New-QueryPanel 24 'Reported error categories | raw' 'table' 0 58 12 10 @"
$raw
Raw | where IsFailure
| summarize FailedAttempts=count() by Category, Stage
| order by FailedAttempts desc, Category asc
"@ 'Failed attempts grouped by reported error category and execution stage. Categories may be broad, not root causes.'),
    (New-QueryPanel 25 'Reported terminal error codes | raw' 'table' 12 58 12 10 @"
$raw
Raw | where IsFailure
| summarize FailedAttempts=count() by Code
| order by FailedAttempts desc, Code asc
"@ 'Error codes reported by AzCopy and the number of failed attempts. Some codes are generic, not Storage service codes.'),
    (New-QueryPanel 26 'Error patterns by command, OS and version | raw' 'table' 0 68 24 11 @"
$raw
Raw | where IsFailure
| summarize FailedAttempts=count(), E2EAttempts=countif(isnotempty(RunID)), LastSeen=max(timestamp)
    by Command, FromTo, OS, Version, Category, Code
| order by FailedAttempts desc
| take 100
"@ 'Top 100 error patterns by command, transfer direction, OS and version. Counts are attempts, not customers.'),
    @{
        id = 31; title = 'Error categories, stages and codes'; type = 'text'
        gridPos = @{ h = 21; w = 24; x = 0; y = 79 }
        options = @{
            mode = 'markdown'
            content = @'
- **Category:** the reported kind of error, such as `authentication`, `authorization`, `not-found`, `timeout`, `network` or `local-io`.
- **Stage:** the execution phase recorded when the attempt ended. It is not the phase of every individual file failure.
- **Code:** a more specific reported identifier, such as `NoAuthenticationInformation` or `ResourceNotFound`. Generic codes such as `completion-error` do not establish a root cause.

| Stage | Meaning |
| --- | --- |
| `initialization` | Setting up the operation. |
| `enumeration` | Discovering objects and deciding what to transfer. |
| `transfer` | Executing or waiting for transfers. |
| `completion` | Finalizing and checking the result. |
| `completed` | Reached a completed outcome, possibly with transfer errors. |

**Why can an error have stage `completed`?** A job can finish processing with `CompletedWithErrors`. AzCopy then reports category `transfer`, stage `completed` and code `transfer-failures`. **Completed does not mean every transfer succeeded.**

For example, category `authentication` with stage `enumeration` means an authentication error ended the attempt while discovering objects. Categories named `initialization`, `enumeration`, `transfer` or `completion` may be broad fallback classifications when a more specific cause is unavailable.

**Reading category / stage pairs:** `completion / completion` means a generic error while finalizing the job; `transfer / transfer` means a generic error while transfers were executing. Repeated words are two fields, not two failures. `transfer / completed` means processing finished with transfer errors. `completed` and `completion` are different values; read the **Error category** and **Stage at exit** columns in that order. `completion / transfer` is not the normal fallback pairing and should not be interpreted as completed-with-errors without inspecting its event.
'@
        }
    },
    @{ id = 40; title = 'Request reliability | raw TEST telemetry'; type = 'row'; collapsed = $false; panels = @(); gridPos = @{ h = 1; w = 24; x = 0; y = 100 } },
    (New-QueryPanel 41 'Storage request reliability' 'table' 0 101 24 8 @"
$raw
$reliability
Requests
| summarize ReportingExecutions=count(), HTTPAttempts=sum(HTTPAttempts), NetworkErrors=sum(NetworkErrors), Busy503=sum(Busy503) by JobStatus
| extend NetworkErrorPercent=iff(HTTPAttempts > 0, 100.0 * NetworkErrors / HTTPAttempts, real(null)),
    ServerBusyPercent=iff(HTTPAttempts > 0, 100.0 * Busy503 / HTTPAttempts, real(null))
| project JobStatus, ReportingExecutions, HTTPAttempts, NetworkErrors, NetworkErrorPercent, Busy503, ServerBusyPercent
| order by HTTPAttempts desc
"@ 'Storage HTTP attempts, including retries, grouped by final outcome. Rates use matching request counters, including from successful executions.'),
    (New-QueryPanel 42 'Request error rates by finish hour' 'timeseries' 0 109 24 9 @"
$raw
$reliability
Requests
| summarize HTTPAttempts=sum(HTTPAttempts), NetworkErrors=sum(NetworkErrors), Busy503=sum(Busy503) by Time=bin(timestamp, 1h)
| project Time, NetworkErrorPercent=iff(HTTPAttempts > 0, 100.0 * NetworkErrors / HTTPAttempts, real(null)),
    ServerBusyPercent=iff(HTTPAttempts > 0, 100.0 * Busy503 / HTTPAttempts, real(null))
| order by Time asc
"@ 'Errors divided by total HTTP attempts, grouped by execution finish hour. Not an average of execution percentages or a real-time request timeline.' 'percent'),
    (New-QueryPanel 43 'Server busy (503) reasons' 'table' 0 118 24 6 @"
$raw
$reliability
Reliability
| where Busy503 >= 0 and BusyThroughput >= 0 and BusyIOPS >= 0 and BusyOther >= 0
| summarize ReportingExecutions=count(), Total503=sum(Busy503), Throughput=sum(BusyThroughput), IOPS=sum(BusyIOPS), Other=sum(BusyOther),
    MismatchedExecutions=countif(Busy503 != BusyThroughput + BusyIOPS + BusyOther)
| where ReportingExecutions > 0
"@ '503 responses split by reported reason. The total includes all three reasons; mismatches flag inconsistent counters.'),
    (New-QueryPanel 44 'Progress at failure or cancellation' 'table' 0 124 24 8 @"
$raw
$reliability
Reliability | where IsFailure or JobStatus == 'Cancelled'
| extend ValidProgress=iff(Progress >= 0 and Progress <= 100, Progress, real(null))
| summarize Executions=count(), WithProgress=countif(Progress >= 0 and Progress <= 100),
    AveragePercent=avg(ValidProgress), P50Percent=percentile(ValidProgress, 50),
    P95Percent=percentile(ValidProgress, 95) by JobStatus
| extend AveragePercent=iff(WithProgress > 0, AveragePercent, real(null))
| order by Executions desc
"@ 'Reported transfer progress when an execution ended. Includes cancellations; progress is not a success rate and can include earlier resumed work.'),
    (New-QueryPanel 45 'Reported errors' 'barchart' 0 132 24 9 @"
$raw
Raw | where IsFailure
| extend ReportedCode=trim(@'\s+', Code)
| extend ErrorCode=case(
    not(ReportedCode matches regex @'^(?:[A-Za-z][A-Za-z0-9_.-]{0,63}|[45][0-9]{2})$'), 'Unknown / other',
    ReportedCode in~ ('unknown', 'other', 'none', 'null', 'not-reported', 'initialization', 'enumeration', 'transfer', 'completion', 'completed',
        'initialization-error', 'enumeration-error', 'transfer-error', 'completion-error', 'transfer-failures', 'job-failed',
        'storage-service-error', 'context-deadline-exceeded', 'local-path-error', 'network-timeout', 'network-error'), 'Unknown / other',
    ReportedCode startswith 'http-' and not(tolower(ReportedCode) matches regex @'^http-[45][0-9]{2}$'), 'Unknown / other',
    ReportedCode)
| summarize FailedAttempts=count() by ErrorCode
| order by FailedAttempts desc, ErrorCode asc
"@ 'Failed executions by reported error code. Missing codes and generic error labels are grouped as Unknown / other.'),
    @{
        id = 32; title = 'Request reliability and progress'; type = 'text'
        gridPos = @{ h = 16; w = 24; x = 0; y = 141 }
        options = @{
            mode = 'markdown'
            content = @'
An **HTTP attempt** is one instrumented Storage request attempt, including retries. A single AzCopy execution can make many HTTP attempts. Network error counts exclude cancellations; telemetry sends and local disk operations are not Storage HTTP attempts. The request panels include successful, failed and cancelled executions because a recovered request error can occur in any of them.

**Request error rates** are total network errors or 503 responses divided by total HTTP attempts from the same executions, not averages of per-execution percentages. Missing or inconsistent counters are excluded; zero HTTP attempts gives no rate. Hourly points assign each execution's counters to its finish hour, not the hour when every request occurred.

**Server busy (503)** has throughput, IOPS and other reason buckets. Add the buckets to get the total; do not add the total again. Mismatched executions flag counters that do not reconcile. No reported 503s does not rule out other errors or other forms of throttling.

**Progress** is the reported transfer percentage at exit, not an error or success rate. The table separates terminal outcomes and shows coverage plus an unweighted average, median (P50) and 95th percentile (P95). Progress can include work from an earlier attempt of a resumed job.

**Reported errors** counts failed executions by their reported terminal error code, not individual file failures or HTTP requests. Specific codes such as `NoAuthenticationInformation`, `ResourceNotFound`, HTTP error statuses and AzCopy numeric codes keep their own bars. Missing or malformed codes and generic labels such as `completion-error`, `transfer-failures` and `network-error` are grouped as **Unknown / other**. Successes and cancellations are excluded; a failed attempt followed by a successful resume still counts once.
'@
        }
    }
)
$variables = @(
    (New-Variable 'SourceType' 'Source type' "AzCopy_DashboardFilters() | where FilterName == 'SourceType' | project text=FilterLabel, value=FilterValue" 1),
    (New-Variable 'TargetType' 'Target type' "AzCopy_DashboardFilters() | where FilterName == 'TargetType' | project text=FilterLabel, value=FilterValue" 1),
    @{ name = 'RawRunScope'; label = 'Raw run scope'; type = 'custom'; multi = $false; includeAll = $false
        query = 'All : 0,E2E tagged : 1,Untagged : 2'; current = @{ text = 'All'; value = '0' }
        options = @(@{ text = 'All'; value = '0'; selected = $true }, @{ text = 'E2E tagged'; value = '1'; selected = $false }, @{ text = 'Untagged'; value = '2'; selected = $false }) }
)
$dashboard = [ordered]@{
    id = $null; uid = 'azcopy-engineering-no-success'; title = 'AzCopy Customer Errors - Engineering (Test)'
    description = $description; version = 1; schemaVersion = 42; timezone = 'utc'
    editable = $true; refresh = ''; tags = @('azcopy', 'engineering', 'customer-errors', 'test-telemetry')
    time = @{ from = 'now-90d'; to = 'now' }; timepicker = @{}
    templating = @{ list = $variables }; annotations = @{ list = @() }; panels = $panels
    links = @(@{ title = 'Existing AzCopy no-observed-success dashboard'; url = '/d/azcopy-no-observed-success'; keepTime = $true; includeVars = $false; targetBlank = $false })
}
$parent = Split-Path ([IO.Path]::GetFullPath($OutputPath)) -Parent
$null = New-Item -ItemType Directory -Path $parent -Force
$dashboard | ConvertTo-Json -Depth 100 | Set-Content -LiteralPath $OutputPath -Encoding utf8NoBOM
Write-Output "Generated $OutputPath"