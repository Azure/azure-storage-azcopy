# HTTP/2 Upload Retry Investigation Handoff

Last updated: 2026-10-05.

## Status

A deterministic local HTTP/2 test reproduced an upload retry sending an incomplete block. A short-term guard fixes that reproduction. **The customer's exact failure sequence is not yet conclusively attributed to this bug.**

## Branch and Environment

- Branch: `users/shnayak/http2-e2e-tests`.
- Fix commit: `6e95a564` - read/seek synchronization and regression tests.
- Earlier HTTP/2 tests: `4008efdf`.
- Before adding this document, the worktree was clean and local tracking showed one commit ahead of origin. Ensure the receiving engineer gets `6e95a564`; this is a local tracking snapshot, not a fresh remote verification.
- Customer: AzCopy **10.32.6**, Go **1.25.11**.
- Local testing: Windows, Go **1.25.11**.
- Tested dependencies: `azcore v1.22.0`, `azblob v1.8.0`, `azdatalake v1.6.0`, `azfile v1.7.0`, and `golang.org/x/net v0.57.0`. See [go.mod](go.mod). No dependency upgrade was made.

## Customer Symptom and Evidence

An 8-MiB block upload through a proxy/Azure Front Door endpoint failed with HTTP 400 and an HTML `ClientDisconnected` response.

| Event | UTC / Bytes |
| --- | --- |
| SDK Try 1 reports `context deadline exceeded` | 23:45:46, after 15 minutes |
| First AFD request starts | 23:45:47.928128 |
| Second AFD request starts, separate TCP connection/POP | 23:45:48.081447 |
| First request ends/cancels | 23:45:55.002203; received **524,288 bytes** |
| Second request ends | 23:46:03.685469; received **7,864,320 of 8,388,608 bytes** |
| SDK Try 2 reports terminal failure | HTTP 400 |

These byte counts represent data **received downstream**, not forwarded upstream. Both AFD requests start after the logged Try 1 timeout, so **do not label them SDK Try 1 and Try 2 solely from the matching byte counts**. AFD internal 408s reportedly recovered; the terminal failure being investigated is 400.

Evidence location on the investigation machine:

- Log directory: `C:\work\logs\afd-proxy`.
- Log ID: `fe18e213-0738-694e-7157-6ddefbc96cb4` (the corresponding `.log` file).
- Target client request ID: `bf2b7c74-1932-4958-71b3-b01312b1a99a`.
- Target URL-encoded block ID: `MDAwMDAT4hj%2BOAdOaXFXbd77yWy0MDAwMDAwMDAwNDAwNDI2`.

AFD does not log `X-Ms-Client-Request-Id`, so further correlation requires other evidence. AzCopy's displayed Try duration is affected by `WroteRequest`; do not interpret it as the full request lifetime. Early-close diagnostics identify a transfer, not necessarily the exact reader or block. Keep customer URLs and SAS credentials redacted.

## Reproduced Mechanism

1. AzCopy's paced request body uses the **transfer context**; the SDK creates a separate **attempt timeout context**.
2. An upload `Read` blocks waiting for pacing allocation.
3. The HTTP attempt times out, but the paced read remains active. The SDK's transport-body `Close` is a no-op.
4. The SDK rewinds the shared reader for retry before the old pending read finishes.
5. The old read resumes and consumes **512 KiB from the rewound cursor**, which the cancelled stream does not deliver.
6. The retry sends only the remaining **7.5 MiB**, despite declaring an 8-MiB content length.

```text
Old HTTP attempt              Shared body                  SDK retry
----------------              -----------                  ---------
Read waits in pacer
Attempt times out
                                                           Seek(0)
                              Cursor reset to zero
Pending Read resumes
                              Consumes first 512 KiB
Cancelled stream drops data
                                                           Reads remaining 7.5 MiB
                                                           Server returns HTTP 400
```

SDK retries and Go's transparent HTTP/2 retries are separate layers. An intermediary may also independently replay a request. SDK `GetBody` rewinds and returns the same body, rather than an independent cursor.

### Relevant Implementation

- [ste/sender-blockBlobFromLocal.go](ste/sender-blockBlobFromLocal.go): wraps a chunk reader with pacing and calls the SDK upload operation.
- [ste/pacedReadSeeker.go](ste/pacedReadSeeker.go): pacing and the short-term guard; used for both uploads and downloads.
- [common/singleChunkReader.go](common/singleChunkReader.go): one mutable cursor, source rereads, and pooled-buffer cleanup. Its existing locks do not cover time spent in the outer pacer.
- [ste/mgr-JobPartMgr.go](ste/mgr-JobPartMgr.go): `NewClientOptions` and SDK policy registration.
- [ste/pacer-tokenBucketPacer.go](ste/pacer-tokenBucketPacer.go): context-aware pacing waits and allocation refunds.
- [azcore retry policy](https://github.com/Azure/azure-sdk-for-go/blob/sdk/azcore/v1.22.0/sdk/azcore/runtime/policy_retry.go): shared body wrapper, no-op transport close, rewind before creating the next attempt context and invoking per-retry policies.
- [azcore request implementation](https://github.com/Azure/azure-sdk-for-go/blob/sdk/azcore/v1.22.0/sdk/azcore/internal/exported/request.go): `SetBody`, `GetBody`, and `RewindBody`.
- [Go HTTP request contract](https://pkg.go.dev/net/http#Request): concurrent `Read`/`Close` and unblocking requirements. Go's HTTP/2 timeout path can return after body close without waiting for the pending body read to finish when close does not unblock it.

## Short-Term Fix

Commit `6e95a564` changes production behavior in [ste/pacedReadSeeker.go](ste/pacedReadSeeker.go#L59):

- Capture a seek-generation number before waiting for pacing.
- After allocation, acquire the reader mutex and compare generations before reading the source.
- A successful `Seek` increments the generation under the same mutex. A failed seek does not increment it.
- If the generation changed, refund the entire allocation and return `0, io.ErrClosedPipe`, without consuming source bytes.
- Pacing waits occur **outside** the lock. The generation check and underlying read are atomic relative to seek.
- `Close` remains unlocked so it can unblock a pending underlying read.

This is distinct from the earlier stale `singleChunkReader.isClosed` fix in commit `35dc27ad` (PR #3506). That fix clears stale close state before a retry disk read while still detecting a concurrent close during the read. Preserve it and its [regression tests](common/zt_singleChunkReader_test.go#L53), including the cache-budget leak check.

## Reproduction and Verification

### Timeout Regression

[TestHTTP2TryTimeoutReplaysFullBlockWithPendingRead](ste/zt_http2_request_body_replay_test.go#L303) uses:

- Real SDK `StageBlock`, production paced/chunk readers, and a local TLS HTTP/2 server.
- An 8-MiB deterministic body, a controlled pending read, a 1-second attempt timeout, and one SDK retry.
- A test transport that deliberately forces separate TCP connections. This does not prove production must select separate connections.
- A HEAD warm-up on each connection so the HTTP/2 512-KiB frame setting is processed before upload reads. Without warm-up, the initial reproduction was only 16 KiB short.
- Assertions for first-attempt deadline expiry, identical request ID and URI, declared content length, exact retry bytes, zero stale-read consumption, and successful `StageBlock`.

| Result | Before guard | After guard |
| --- | --- | --- |
| Repeated runs | 5/5 failures | 5/5 passes |
| First request received | 524,288 bytes | 524,288 bytes |
| Stale pending read consumed after rewind | 524,288 bytes | 0 bytes |
| Retry received | 7,864,320 bytes | 8,388,608 bytes, exact full body |
| Upload result | HTTP 400 | Success |

### Other Focused Coverage

- [TestHTTP2ConnectionResetReplaysFullBlock](ste/zt_http2_request_body_replay_test.go#L154): the server sends `RST_STREAM(REFUSED_STREAM)` after a prefix. Go transparently replays on the same TCP connection within one SDK transport call. The test includes the production paced wrapper. Despite its name, this is not a TCP RST test.
- [TestPacedReadSeekerPendingReadAcrossSeek](ste/zt_pacedReadSeeker_test.go#L37): rewind, forward seek, failed seek, unchanged output on rejected reads, and pacing refunds.
- [TestPacedReadSeekerCloseUnblocksResponseRead](ste/zt_pacedReadSeeker_test.go#L115): close unblocks an underlying response read without waiting for the new mutex.

Run from the repository root; these focused tests do not need cloud credentials:

```powershell
go test ./ste -run '^(TestPacedReadSeeker.*|TestHTTP2ConnectionResetReplaysFullBlock|TestHTTP2TryTimeoutReplaysFullBlockWithPendingRead)$' -count=5 -timeout=60s -v
```

All focused tests passed five consecutive times after the fix. The latest focused VS Code run also passed, reporting seven passed tests/subtests. Formatting and editor diagnostics were clean at implementation time.

Race testing was attempted but blocked by disabled CGO and no C compiler on PATH. Broader suites and cloud tests have not been run for this change.

## Known Limitations

- **No newly introduced correctness defect was identified in the reviewed slice**, but the guard is not complete attempt isolation.
- An old attempt whose `Read` enters after rewind can capture the new generation. This is a source-level coverage gap, not another reproduced failure. The guard does not establish general protection against late attempt reads or closes.
- Stale pacing waits are rejected when they resume, not proactively cancelled.
- Locks are per reader, not global, so unrelated chunks remain parallel. A seek waits for an active underlying read to finish.
- Downloads also use this wrapper and incur the new locking despite not requiring upload-rewind protection. Performance impact has not been benchmarked.
- Passing the local reproduction does not establish which component generated both customer AFD requests or their exact mapping to SDK attempts.

## Customer Validation Recommendation

A **private, controlled customer validation build** is reasonable; do not describe it as production-qualified or a complete fix for all retry races.

1. Ensure the build contains `6e95a564` and record the exact build revision.
2. Initially use a test destination with representative files and the same proxy, concurrency, and bandwidth settings.
3. Verify content independently, preferably by downloading and comparing hashes against the source.
4. Collect retry logs, failures, throughput, and CPU usage for comparison with the original build.
5. Correlate client/proxy/AFD events without exposing URLs or SAS credentials.

## Continuing the Investigation

1. Confirm customer request provenance and timing. Do not infer one AFD request per SDK attempt from the byte split alone.
2. Run the focused tests with `-race` in a CGO-capable environment, followed by applicable broader upload/download regression tests.
3. Benchmark throughput and CPU, including download overhead. An upload-only guard is the smallest refinement, but does not eliminate the ownership limitation.
4. Investigate upload-only, attempt-scoped body handles with private cancellation and closed state. Cancel pacing and finish outstanding source access before allowing rewind; do not mutate a shared context between attempts.
5. Cover both SDK retries and `GetBody` replay. SDK rewind occurs before the next per-retry policy, so a policy-based design must retire the old handle before returning control to the SDK. Cancellation alone does not prove an active read has finished.
6. Add coverage for late reads, late closes, cancellation/refunds, and pooled-buffer lifetime. Independent cursors are another option but require careful source/cache ownership. No replacement design has been implemented or validated.

Reviewed newer azcore code retained the shared-body lifecycle. The azblob **1.8.1 download RetryReader fix is separate**: it broadens retry handling for errors such as HTTP/2 stream errors, but does not resolve this upload-body replay bug. The separate [download HTTP/2 regression](ste/zt_http2_response_body_retry_test.go#L157) is not part of the passing upload-focused command above.