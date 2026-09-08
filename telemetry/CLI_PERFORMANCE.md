# Actual CLI Telemetry Performance Comparison

This manual Windows test runs the same built AzCopy executable against real Azure
Blob Storage with telemetry configured on and off. Enabled telemetry uses the real
isolated Application Insights endpoint. No mock HTTP service or synthetic local
file-copy loop substitutes for the CLI transfer engine.

## Run

```powershell
./testSuite/telemetry-live.ps1 -Run -Scenario cli-performance -PerformancePairs 8 `
  -SubscriptionId '31347be8-d066-464e-9866-7e58d85027b7' `
  -ResourceGroup 'azcopy-telemetry-test-rg' -Suffix 'sep08' `
  -StorageAccountName 'ankursstorage' -GrantStoragePermission
```

Prerequisites and isolated telemetry provisioning are described in
[LIVE_TESTING.md](LIVE_TESTING.md). This scenario requires Windows, PowerShell 7,
Go, and an Azure CLI login. Omit `-GrantStoragePermission` when your identity already
has Blob upload/read and container create/delete permissions. When explicitly set,
that switch grants the signed-in user Blob Data Contributor on only the unique
test container and removes the grant during cleanup. No account keys or account-wide
roles are used. Server sampling, quotas, and authentication settings are not changed.

The scenario is excluded from `all`, the normal E2E pipeline, and the existing
offline performance pipeline. It requires both `telemetrylive` and `telemetryperf`
build tags plus explicit environment opt-ins, set by the runner. The runner builds
one binary with `-trimpath -buildvcs=false -ldflags='-s -w'`, and every condition uses
that same binary. Performance pairs can range from 3 to 15; eight is the default.

## Method

- Workloads: one 64 MiB blob; 128 blobs of 16 KiB each (2 MiB total). The test seeds
  a unique temporary container outside the timed measurements.
- Eight enabled/disabled pairs per workload, alternating disabled-first and
  enabled-first. One warm-up of each mode per workload is recorded but excluded
  from the comparison. The default run launches 36 CLI processes and downloads
  about 1.16 GiB in total. Each process has a three-minute bound; the experiment
  has a 35-minute context and the runner a 40-minute test timeout.
- Fresh output, logs, plans, and user directories per process. The same valid
  installation-ID fixture is precreated for both modes to represent existing
  installations. First-ever identity creation is not measured. OS/storage caches
  are not cleared; the alternating order and warm-ups reduce, but cannot eliminate,
  cache and remote-service variability.
- The command is an actual recursive `azcopy copy --from-to=BlobLocal`, with
  `GOMAXPROCS=8`, `AZCOPY_CONCURRENCY_VALUE=16`, and a 4 MiB block size. No bandwidth
  cap is used. Logging and authentication configuration are equal in both modes.
- Elapsed time runs from process launch through exit, including CLI startup, Azure
  CLI authentication, enumeration, transfers, output, and telemetry flush. Fixture
  setup, post-exit hashing, and cleanup are outside this measurement.
- CPU time is the AzCopy process's OS-reported user plus kernel time, not CPU
  utilization or wall time. CPU and memory exclude its Azure CLI authentication
  subprocesses; elapsed time includes waiting for them.
- Memory is Windows process peak working set and peak commit from
  `K32GetProcessMemoryInfo`. The observer samples high-water counters every 10 ms
  with the same observer in both conditions. Exit-time reads can fail, so an unseen
  final interval remains possible. Working set includes resident shared/image pages;
  peak commit is a different measure. Neither is retained Go heap or total allocated
  bytes. Do not apply the separate 100 KiB heap gate directly to these counters.
- Every sample must exit 0, report `Completed`, zero failed/skipped transfers,
  expected bytes/file count, and pass SHA-256 validation for every downloaded file.
- Healthy enabled samples must log successful started and finished telemetry.
  Configured-enabled samples that latch off on a delivery failure are retained and
  explicitly labeled `enabled-stopped`, never silently retried or reported as
  healthy. Disabled samples must have no successful telemetry send or stop warning.
- Primary results include all configured-on/off pairs. A separate healthy-only
  subset is reported when at least three healthy pairs exist. That subset is
  conditional on delivery success and can be selection-biased; it is not a replacement
  for the full comparison. The raw trials include warm-ups and failure reasons.
- Summary uses per-mode medians and the median of paired enabled-minus-disabled
  differences, plus paired percentage changes. A deterministic 2,000-resample
  bootstrap provides descriptive 95% intervals. The paired delta need not equal
  the difference between per-mode medians. This report-only test does not enforce
  numerical budgets or establish a universal performance guarantee.

The runner retains the binary, per-process stdout/stderr/job logs, metadata, raw
trials, and comparisons in its printed evidence directory. Downloaded files are
deleted after hash checks. The container and optional temporary role are deleted
on normal completion and assertion failures. Forced process termination can prevent
cleanup; the log records the IDs needed to remove those test resources.

## Results: 2026-09-08

These recorded measurements used the one-second telemetry send deadline and
two-second process-exit flush limit. The current code subsequently increased them
to five seconds and four seconds, respectively. These results have not been rerun with the
new deadline; do not treat them as measurements of that change.

Environment: Windows amd64, 32 logical CPUs, Go 1.26.5, East US Blob Storage and
Application Insights. The production checkout was `d0f4b02c`; this change only adds
test/runner code. One stripped binary was used throughout, with SHA-256
`ed557e20d057cab2cbbd4bb24b1f5f968d34dc9ac3a8ab2fa7b2a112530bca00`.

All 36 transfers passed exit/status/count/byte/hash validation. The run took about
552 seconds including fixture creation and temporary RBAC propagation. Six of eight
measured enabled large-blob runs sent both lifecycle events; two stopped on the
existing one-second job-start telemetry deadline. All eight measured enabled
small-blob runs and both enabled warm-ups were healthy.

### All Configured-On/Off Pairs

Values below are medians; differences are paired medians. Positive means higher
with telemetry configured on.

| Workload | Measure | Disabled | Enabled | Paired difference | Paired change |
| --- | --- | ---: | ---: | ---: | ---: |
| 1 x 64 MiB | Elapsed seconds | 9.334 | 10.992 | +1.587 s | +16.78% |
| 1 x 64 MiB | CPU seconds | 9.578 | 11.211 | +1.594 s | +16.64% |
| 1 x 64 MiB | Peak working set | 142.777 MiB | 197.498 MiB | +54.791 MiB | +38.38% |
| 1 x 64 MiB | Peak commit | 140.822 MiB | 141.127 MiB | +0.336 MiB | +0.24% |
| 128 x 16 KiB | Elapsed seconds | 9.346 | 10.954 | +1.575 s | +16.80% |
| 128 x 16 KiB | CPU seconds | 9.914 | 11.484 | +1.594 s | +16.10% |
| 128 x 16 KiB | Peak working set | 141.742 MiB | 140.385 MiB | -1.385 MiB | -0.98% |
| 128 x 16 KiB | Peak commit | 86.127 MiB | 83.912 MiB | -2.244 MiB | -2.62% |

Elapsed paired-change 95% intervals were +9.07% to +18.10% for the large blob and
+16.51% to +18.11% for small blobs. CPU intervals were +8.58% to +19.70% and +12.42%
to +16.75%, respectively.

### Healthy Large-Blob Subset

Restricting to the six pairs whose enabled run sent both events: elapsed medians
were 9.337 s disabled and 10.999 s enabled, with a paired increase of 1.635 s / 17.39%
(95% +10.43% to +18.27%). CPU paired increase was 1.688 s / 17.63%. Peak working-set
paired increase was 55.195 MiB and peak commit 0.334 MiB. Small-blob results are
unchanged because every enabled sample was healthy.

### Interpretation

This host/run shows a material end-to-end time and CPU increase with telemetry,
roughly 1.6 seconds per short CLI invocation. It does not support a claim of less
than 1% overhead for these workloads. This is not a measurement of steady-state
large-job throughput: startup/authentication/flush occupy a substantial part of
these short runs, and only Blob-to-local downloads were tested.

The large working-set increase is not equivalent to 55 MiB of new telemetry heap:
peak commit changed by only about 0.34 MiB. The different memory trends in the two
workloads require profiling before attributing the result to particular allocations,
resident image pages, GC behavior, or the transfer engine. The negative small-file
memory deltas are observations, not evidence that telemetry saves memory in general.

These are eight pairs on one development host with real network services, not an
exclusive performance machine or a cross-platform acceptance result. The confidence
intervals describe these samples and do not account for every environmental bias.
The performance harness passed its correctness assertions, not a numeric performance
budget. The initial strict attempt aborted during an enabled warm-up after a telemetry
timeout; it is not included in this completed experiment. No retries were used to
replace failing telemetry samples in the reported experiment.

Independent cleanup checks confirmed the test container was absent and its temporary
container-scoped role list was empty. Server authentication was never disabled by
this performance experiment.

Machine-readable evidence is retained under [results/cli-2026-09-08](results/cli-2026-09-08):
[metadata](results/cli-2026-09-08/metadata.json),
[raw trials](results/cli-2026-09-08/trials.jsonl), and
[comparisons](results/cli-2026-09-08/comparison.json).

## Local Validation

```powershell
go test -tags 'telemetrylive telemetryperf' ./azcopy -run '^Test(TelemetryCLIProcessMeasurement|TelemetryCLIPerformanceAnalysis|LiveTelemetryCLIContract)$' -count=1
```

The process counter check launches a real allocation/CPU subprocess without Azure
traffic. The other checks validate analysis, environment isolation, and CLI parsing.

## Memory Meaning And Collection Costs

Windows working set counts pageable process-address-space pages currently resident
in physical RAM, including private pages and shared executable/DLL or mapped-file
pages. Private commit is private memory for which Windows has committed backing;
it can be resident, paged out, or not yet touched. It is not simply pagefile space
in use. The harness records the separate lifetime peaks of these values, not a
simultaneous private-versus-shared memory breakdown.

The large-blob results show approximately +55 MiB working set but only +0.34 MiB
private commit. That does not establish a 55 MiB telemetry heap allocation. More
resident image/mapped pages, a different fraction of existing private transfer
buffers being touched or trimmed, and changes to allocation/GC/concurrency timing
are possible explanations. These are hypotheses, not findings from a memory map.
Subtracting the two independent peaks does not identify the shared-page contribution.
VMMap or working-set/page-fault tracing plus Go heap/CPU profiling would be needed
to attribute the difference.

The current implementation adds synchronous per-process host/OS/NIC/identity
collection and a best-effort IMDS request with its own one-second timeout. Windows
hardware probes use registry/Win32 calls, not a spawned PowerShell/WMI process.
Job dimensions and source-shape aggregation add local CPU and allocations. Start
and finish send two packed custom events through the direct Application Insights
backend: property/measurement maps, JSON serialization, and HTTP/TLS processing.
It does not instantiate the alternative OTel metrics backend for CLI telemetry.
Requests are asynchronous, but startup probes are synchronous and final flush can
hold process exit for up to four seconds. A network wait can add elapsed time, not
1.6 seconds of CPU by itself. The measured CPU increase has not been attributed
to individual functions.

Transferred/enumerated/expected bytes, wire bytes, completed/failed/skipped counts,
average storage HTTP attempt latency, attempt/error/503 counts, IOPS, and durations
are included in the finished event. Byte counters and the storage request statistics
already run for core progress and concurrency tuning even when telemetry is disabled.
The new event reads their aggregates; it does not retain per-request latency samples
or rescan/copy file content to calculate transferred bytes. The on/off benchmark
therefore does not measure the absolute cost of those always-running engine counters.

The telemetry-only shape tracker uses the already enumerated object metadata:
a mutex, scalar counters, ten size buckets, path-depth calculation, and two capped
scope maps. Each map holds at most 128 names of at most 256 bytes (up to 64 KiB of
name bytes combined, plus map overhead). Normal single-container jobs do not fill
that bound. Temporary path-splitting allocations and lock contention can scale
with object count. Final failure-code aggregation also depends on the failure list;
the successful-summary benchmark below does not measure that case.

Collection-only microbenchmarks ran on the same Windows Xeon Platinum 8370C host,
three one-second repetitions per case, using `BenchmarkTelemetryCollection`:

| Operation | Median time | Allocated bytes/op | Allocations/op |
| --- | ---: | ---: | ---: |
| Disabled scan + schedule calls, flat path | 18.84 ns | 0 | 0 |
| Enabled scan + schedule, flat path | 122.0 ns | 16 | 1 |
| Disabled scan + schedule calls, four-component path | 18.82 ns | 0 | 0 |
| Enabled scan + schedule, four-component path | 202.7 ns | 64 | 1 |
| Shape snapshot + successful finished-event construction | 2.379 us | 480 | 20 |

These are single-threaded, uncontended synthetic metadata operations, not network
or whole-transfer measurements. They exclude startup, summary retrieval from the
engine, JSON serialization, HTTP/TLS, and final flush. The 16/64-byte allocation
is temporary path-component storage, not retained bytes per file. At one million
objects the measured incremental scan/schedule work extrapolates to roughly
0.10-0.18 seconds and 16-64 MB of cumulative allocations; contention, different
paths, and GC can change this substantially. For 128 objects, this local work is
only tens of microseconds and cannot explain the observed 1.6-second CLI difference.

```powershell
go test -tags telemetryperf ./azcopy -run '^$' -bench '^BenchmarkTelemetryCollection$' -benchmem -benchtime=1s -count=3
```

References: [Windows working sets](https://learn.microsoft.com/en-us/windows/win32/memory/working-set),
[Windows process memory counters](https://learn.microsoft.com/en-us/windows/win32/api/psapi/ns-psapi-process_memory_counters_ex),
[source-shape collection](../azcopy/telemetryShape.go),
[storage request statistics](../ste/xferStatsPolicy.go),
[event construction](../azcopy/telemetry.go), and
[packed-event serialization](../telemetry/send_events.go).