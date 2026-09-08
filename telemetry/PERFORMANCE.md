# Manual Telemetry Performance Gate

For a comparison using the **actual AzCopy executable and real Blob transfers**,
see [CLI_PERFORMANCE.md](CLI_PERFORMANCE.md). It reports elapsed time, process peak
memory, CPU time, and observed telemetry failures from paired on/off runs. The
offline instrumentation benchmark below remains separate and unchanged.

Create an Azure Pipelines definition pointing to [telemetry-performance.yml](../telemetry-performance.yml), then queue it manually. The default is the existing `AzCopyPerfTestUbuntu` pool and `azcopyPerfTestUbuntu22.04` image. The agent needs PowerShell 7, Git, sufficient local scratch space, and exclusive performance-test use. GoTool installs Go 1.25.11. No Azure login, storage credentials, E2E scenarios, or cloud ingestion are used. No definition is provisioned automatically by this change.

## Run Locally

From the repository root, on a quiet Linux performance host:

```powershell
./testSuite/telemetry-performance.ps1 -Samples 9 -Seconds 5 -BaselineRef telemetry/00-main-base -OutputDirectory /tmp/telemetry-performance
```

The baseline must resolve locally or under `origin` or `fork`. The pipeline defaults to the pinned pre-telemetry commit `35dc27add1e640506b1221172f6c0f3d317dff65`, available in the full checkout history. Both baseline and candidate are built with the same installed Go compiler, `-trimpath`, `-buildvcs=false`, and `-ldflags='-s -w'`. The script creates and removes only its own detached baseline worktree. Binary growth includes all changes since the selected baseline, not just telemetry.

For workflow smoke checks only:

```powershell
./testSuite/telemetry-performance.ps1 -Samples 3 -Seconds 0.2 -ReportOnly -OutputDirectory /tmp/telemetry-performance-smoke
```

Report-only mode still validates subprocesses and delivery but does not enforce numeric thresholds. It must not be reported as a performance-budget pass. Normal builds/tests do not include the tagged harness; even with the tag, measurements skip unless `AZCOPY_RUN_TELEMETRY_PERF=1` is set.

## Measurements

- Each sample runs in a fresh subprocess. Disabled and enabled runs alternate AB/BA order, with one discarded warm-up of each per workload. Defaults are nine paired samples of five seconds, with `GOMAXPROCS=8`.
- Workloads repeatedly copy an 8 KiB or 32 MiB source file to a local destination using a reusable buffer. Cached filesystem I/O is intentional: it reduces remote-service variance and exposes collector overhead. The production shape collector, lifecycle finalizer, serializer, dispatcher, and HTTP reporter run alongside that I/O. This is an instrumentation benchmark, not the full AzCopy transfer engine or a cloud throughput SLA test.
- The same prepared source, buffer, object metadata, and scope-name inputs are used in each condition. Scope names exercise overflow and worst-case retained-name sizes; no actual resource identifiers are used.
- Enabled runs call the production `getTelemetryAgent` initialization path, including host hardware, IMDS, and persisted identity probes. Each workload has fresh-identity and existing-identity variants in isolated temporary application directories. Initialization time has its own gate; it is not hidden in the steady-state throughput denominator. Both lifecycle events use a loopback receiver, with two requests required when enabled and zero when disabled. Ingestion never targets a live Application Insights resource. Azure ingestion TLS and the full transfer engine are still outside this local instrumentation workload.
- Transfer throughput is bytes divided by the workload interval, excluding final flush. CPU is the OS-reported child process user plus system CPU time per byte, including process setup, telemetry sends, flush, and GC. It is intentionally a broader measure than collector CPU alone. It is not wall time mislabeled as CPU.
- Retained heap is measured after GC, keeping the tracker/agent alive, relative to a pre-initialization GC snapshot. A symmetric 1 ms sampler plus explicit lifecycle-boundary samples measures peak live heap before GC; total allocated bytes are also retained in each trial. Sampled peak and retained enabled-minus-disabled deltas have separate gates. Sampling is not a proof of an instantaneous allocation ceiling: sub-millisecond spikes can be missed, and sampler cost affects both conditions. These measurements are not RSS.
- Separate unreachable and stalled local sinks exercise shutdown. A delivery failure latches telemetry off, suppresses subsequent events including the finish, and stops source-shape collection; the benchmark tracker uses the same active-state check as copy/sync. Flush is measured independently so network delay cannot be hidden inside a transfer throughput ratio. Historical stalled-sink timings below predate this stop-on-first-delivery-failure policy.

## Gates

| Measurement | Default limit |
| --- | --- |
| Paired transfer-throughput loss | 1% |
| Paired CPU time per byte increase | 1% |
| Paired retained heap increase | 100 KiB |
| Paired sampled peak live-heap increase | 100 KiB |
| Paired production initialization increase | 2.25 seconds |
| Failed-sink flush duration | 4 seconds + 250 ms scheduler allowance |
| Stripped binary growth against baseline | 5 MiB |

For each paired metric, the gate uses the median and a deterministic 2,000-resample bootstrap 95% interval. An upper bound at/below the limit passes; a lower bound above it fails; an interval crossing the limit is inconclusive and blocks an enforced run. Do not loosen limits to turn noise green. Repeat with longer samples on an idle host, inspect raw trials, and use more pairs when needed. The bootstrap is a practical noise check, not proof of a universal overhead guarantee.

The pipeline always publishes raw per-trial JSON, gate results, Go/OS/architecture/CPU-count metadata, the test log, baseline/candidate commit IDs, binary sizes, and both binaries. Results remain available when a threshold fails.

## Initial Validation

A three-pair, 0.2-second Windows report-only smoke run validated process isolation, healthy/disabled delivery checks, failed sinks, baseline builds, and artifact generation. Binary growth was 238,592 bytes against the pre-telemetry baseline. Small-file throughput and retained heap exceeded the proposed limits, and several other intervals were inconclusive. That run is not a dedicated-host performance result or a gate pass. In particular, the observed retained heap included roughly 185 KiB additional memory in the small-file workload; investigate that result before claiming the 100 KiB gate is met. A full enforced Linux run remains required.

After replacing the synthetic initialization fixture, a three-pair, 0.1-second Windows report-only run covered both identity variants. Initialization deltas were about 24-30 ms, stalled flush about 1.9 seconds, and binary growth 246,272 bytes. Sampled peak heap deltas were roughly 190-275 KiB and exceeded the 100 KiB gate; CPU/throughput intervals included failures and inconclusive results. These are diagnostic smoke measurements, not an acceptance pass.