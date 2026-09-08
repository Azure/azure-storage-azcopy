# CLI CPU And Memory Investigation: 2026-09-08

## Conclusions

- The earlier approximately 1.6-second CPU increase was strongly confounded by a
  preexisting CLI input loop. `cmd.(*lifecycleMgr).watchInputs` immediately retries
  every stdin read error, including EOF. `exec.Cmd` with nil stdin supplies the null
  device. The loop consumes CPU while the process waits for unrelated work.
- Holding stdin open reduced process CPU from approximately 10-12 seconds to
  0.4-1.2 seconds. Neither corrected three-pair experiment resolves a stable telemetry
  CPU increment: descriptive paired intervals span zero in both workloads.
- The large-transfer working-set gap is principally resident pages of the existing
  AzCopy executable, almost entirely shareable, not an additional 55 MiB Go heap.
  The owner and allocation class are established; the page-in/prefetch trigger is not.
- Approximately 1.6-1.7 seconds of extra elapsed time remains on these short real
  transfers. It has not been attributed quantitatively to synchronous startup probes,
  network/service variability, or final telemetry flush. This is not a 1% budget pass.

No production input-loop or collector behavior was changed in this investigation.
The harness now keeps an idle stdin pipe open in both modes. Production deadlines
in all three profiling experiments were five seconds per send and four seconds
for process-exit flush. The old eight-pair experiment used one and two seconds.

## Experiments And Evidence

All runs used real Blob Storage and the separate shutdown-test Application Insights
component, with no changes to server authentication, sampling, or quotas. Each run
performed three AB/BA pairs per workload plus four warm-ups: 16 CLI processes, with
exit/status/count/bytes/SHA-256 checks. All 48 transfers passed; every enabled process
in these profiling runs delivered both lifecycle events. Temporary containers and
container-scoped Blob Data Contributor grants were removed by test cleanup.

The Windows amd64 host had 32 logical CPUs and Go 1.26.5. Each comparison used the
same stripped binary, GOMAXPROCS 8, transfer concurrency 16, 4 MiB blocks, no bandwidth
cap, and existing installation-ID fixtures. CPU/heap profiling was on in both modes.
The binary SHA-256 was
`2a695b1561cbb0cf3957748c9adcde5e668a3bc4013e6bea62ee5673cc4fd2a1`.

| Evidence prefix | Stdin | Additional observation | Runtime including setup |
| --- | --- | --- | ---: |
| `null-stdin` | Null device | CPU/heap profiles | 295.77 s |
| `open-stdin` | Open idle pipe | Resident region classes | 399.29 s |
| `image-detail` | Open idle pipe | Image owners, shareability, resident timeline | 278.69 s |

Each prefix has byte-for-byte copied `.metadata.json`, `.trials.jsonl`, and
`.comparison.json` artifacts here. Warm-ups remain in the raw trials but are excluded
from comparisons. Raw profiles, binaries, and logs remain in the local temporary
evidence directories, not Git:

- `azcopy-telemetry-profile-sep08/comparison-d85aff30-2fb2-4a7a-835e-a568b9222fb0`
- `azcopy-telemetry-profile-openstdin-sep08/comparison-e6ebb216-57f4-41df-8bc9-c084bd5526f1`
- `azcopy-telemetry-profile-images-sep08/comparison-3fea14a8-37bc-4654-b7ba-2768528385d1`

## Corrected CPU And Elapsed Results

Final `image-detail` run, medians and paired enabled-minus-disabled differences:

| Workload | Measure | Disabled | Enabled | Paired difference | Descriptive 95% interval |
| --- | --- | ---: | ---: | ---: | ---: |
| 1 x 64 MiB | CPU seconds | 0.594 | 0.688 | +0.094 | -0.109 to +0.297 |
| 128 x 16 KiB | CPU seconds | 0.938 | 0.938 | +0.016 | -0.219 to +0.047 |
| 1 x 64 MiB | Elapsed seconds | 9.429 | 10.992 | +1.562 | +1.396 to +1.811 |
| 128 x 16 KiB | Elapsed seconds | 9.370 | 11.191 | +1.702 | +1.626 to +1.921 |
| 1 x 64 MiB | Peak working set, MiB | 142.949 | 199.840 | +57.254 | +56.730 to +65.477 |
| 1 x 64 MiB | Peak commit, MiB | 143.484 | 143.516 | +0.031 | -0.047 to +0.332 |
| 128 x 16 KiB | Peak working set, MiB | 144.465 | 142.422 | -2.168 | -3.043 to -0.570 |
| 128 x 16 KiB | Peak commit, MiB | 90.113 | 87.105 | -3.023 | -3.117 to -1.410 |

The first open-stdin run independently showed CPU paired differences of -0.125 s
(large) and -0.062 s (small), also with intervals spanning zero. Its elapsed paired
differences remained +1.624 s and +1.656 s. These short diagnostic runs do not prove
zero CPU cost, a CPU saving, or a stable percentage overhead. CPU excludes Azure CLI
authentication subprocesses; wall time includes them. These are sequential cloud
experiments, not a randomized stdin-only causal estimate or an exclusive-host gate.

### CPU Profile Limitation On This Host

With null stdin, a representative small-file enabled profile had 9.73 seconds under
`watchInputs`/`ReadString`, mostly `syscall.ReadFile`, supporting the inspected EOF
loop. However, even with an open pipe, a profile lasting 10.33 seconds reported
20.98 seconds of samples and 9.90 seconds under the now-blocked input reader, while
Windows reported only 1.109 seconds of process CPU. This Go/Windows profile counts
blocked syscall time and must not be interpreted as CPU attribution. The OS
user-plus-kernel counters, combined with the controlled stdin change and source
inspection, are the CPU evidence. A representative TLS-focused profile was only
about 80 ms cumulative, but its syscall accounting also prevents a precise CPU claim.

## Memory Attribution

The observer samples `K32GetProcessMemoryInfo` high-water counters every 10 ms.
In profile mode, it additionally calls `K32QueryWorkingSet` about every 250 ms,
classifies address regions with `VirtualQueryEx`, and resolves image names with
`K32GetMappedFileNameW`. Filenames are stored without directory paths. The diagnostic
buffer holds at most 256K entries (about 1 GiB of 4 KiB resident pages); an unavailable
snapshot is skipped and a run with no valid snapshot fails. Observer allocations
are in the parent, not the measured child, but can perturb scheduling/cache behavior.

The largest classified snapshot is not necessarily the exact OS working-set peak.
`MEM_IMAGE` is an allocation class, not proof of sharing: copy-on-write image pages
can be private. `ShareableImageBytes` separately counts the working-set Shared bit,
which means shareable, not necessarily currently shared with another process.

Representative final-run pair 1:

| Resident measure, MiB | Large disabled | Large enabled | Small disabled | Small enabled |
| --- | ---: | ---: | ---: | ---: |
| AzCopy executable image | 18.68 | 76.24 | 76.24 | 76.24 |
| All image regions | 28.85 | 86.41 | 86.41 | 86.41 |
| Shareable image pages | 27.43 | 84.99 | 84.99 | 85.00 |
| Private regions | 107.14 | 108.55 | 50.85 | 51.45 |

The largest DLL contributors were unchanged: ntdll about 1.93 MiB, KernelBase
0.99 MiB, crypt32 0.81 MiB, rpcrt4 0.62 MiB. This is not a new 55 MiB telemetry DLL.
Enabled image residency rose from about 26 MiB at 4.6 seconds to 86.4 MiB by 5.8
seconds. Small-file disabled runs also reached 86.4 MiB later in their lifetime.
The raw timeline distinguishes residency from the separate private transfer-buffer
growth; it does not identify which thread or OS component brought image pages in.

Matched post-GC heap profiles for final-run large pair 1 showed 82.95 MiB enabled
versus 85.95 MiB disabled. The common slice pool retained about 60.02 versus 64.02
MiB, chunk-status logging 7.63 MiB in both, and `ste.NewJobMgr` 4.19 MiB direct in
both. Heap profiles are sampled and post-GC, not peak allocation accounting; they
cannot exclude small transient telemetry allocations. They do not show the large
retained telemetry heap suggested by interpreting working set alone.

## Reproduction

```powershell
./testSuite/telemetry-live.ps1 -Run -Scenario cli-performance -PerformancePairs 3 -Profile `
  -SubscriptionId 31347be8-d066-464e-9866-7e58d85027b7 `
  -ResourceGroup azcopy-telemetry-test-rg -Suffix sep08 `
  -StorageAccountName ankursstorage -GrantStoragePermission
```

Use `go tool pprof -top -sample_index=inuse_space <exe> <trial>/heap.pprof`
for retained heap and `go tool pprof -top -cum <exe> <trial>/cpu.pprof` for sampled
stacks, subject to the Windows syscall limitation above. Remove `-Profile` for
unprofiled on/off measurements. The corrected eight-pair unprofiled run and detailed
off-CPU/page-fault tracing remain unperformed; no universal performance claim is made.

Sources: [input loop](../../../cmd/uihooks.go),
[host/IMDS probe](../../../azcopy/hostinfo.go),
[process observer](../../../azcopy/telemetry_cli_perf_windows_test.go),
[transfer harness](../../../azcopy/telemetry_cli_perf_transfer_windows_test.go), and
[historical performance report](../../CLI_PERFORMANCE.md).